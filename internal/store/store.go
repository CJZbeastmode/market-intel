package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/raft"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrTimeout   = errors.New("submit timed out")
)

const (
	maxRuns        = 1000
	maxRunsPerJob  = maxRuns
	submitTimeout  = 5 * time.Second
	snapThreshold  = 1
	defaultUserKey = "default"
)

// Store is the replicated state machine on top of Raft.
// Reads are served locally. All writes go through Raft.
type Store struct {
	mu        sync.RWMutex
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	// jobs is the latest agreed job state.
	jobs map[string]Job
	// runs is a bounded in-memory execution history slice.
	runs []JobRun

	// waiters let Submit block until a specific Raft index is applied locally.
	waiters map[int]chan struct{}
	// applyCount is a simple trigger for snapshot compaction.
	// Sprint 1 snapshots every mutation so restarted nodes can restore store state
	// even though this Raft implementation does not persist commitIndex.
	applyCount int
}

// NewStore creates a Store and starts its apply loop.
func NewStore(rf *raft.Raft, persister *raft.Persister, applyCh chan raft.ApplyMsg) *Store {
	s := &Store{
		rf:        rf,
		persister: persister,
		applyCh:   applyCh,
		jobs:      make(map[string]Job),
		runs:      make([]JobRun, 0),
		waiters:   make(map[int]chan struct{}),
	}
	if persister != nil {
		// If we already have a snapshot, load it before new log entries arrive.
		s.applySnapshot(persister.ReadSnapshot())
	}
	// One loop owns all committed state changes.
	go s.applyLoop()
	return s
}

// Make is kept as a small compatibility wrapper for existing callers.
func Make(rf *raft.Raft, persister *raft.Persister, applyCh chan raft.ApplyMsg) *Store {
	return NewStore(rf, persister, applyCh)
}

// Submit sends a mutation through Raft and waits for commit.
func (s *Store) Submit(op string, data any) error {
	// We always serialize into a stable command shape before handing work to Raft.
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	cmdBytes, err := json.Marshal(Command{Op: op, Data: payload})
	if err != nil {
		return err
	}
	if s.rf == nil {
		// Tests sometimes construct stores without a real Raft node.
		return ErrNotLeader
	}

	// Start only appends on the leader. Followers reject immediately.
	index, _, isLeader := s.rf.Start(cmdBytes)
	if !isLeader {
		return ErrNotLeader
	}
	// Wait for this exact log index to be applied on this node.
	ch := s.registerWaiter(index)
	select {
	case <-ch:
		return nil
	case <-time.After(submitTimeout):
		// If we time out, clean up the waiter so it does not leak.
		s.removeWaiter(index)
		return ErrTimeout
	}
}

func (s *Store) CreateJob(job Job) error {
	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	// Fill default fields in one place so API and tests stay consistent.
	normalizeJob(&job)
	return s.Submit(OpCreateJob, job)
}

func (s *Store) UpdateJob(job Job) error {
	job.UpdatedAt = time.Now()
	normalizeJob(&job)
	return s.Submit(OpUpdateJob, job)
}

func (s *Store) DeleteJob(id string) error {
	return s.Submit(OpDeleteJob, Job{ID: id})
}

func (s *Store) RecordRun(run JobRun) error {
	return s.Submit(OpRecordRun, run)
}

func (s *Store) GetJob(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

func (s *Store) ListJobs() []Job {
	return s.GetJobs()
}

func (s *Store) GetJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

func (s *Store) GetRuns(jobID string) []JobRun {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]JobRun, 0)
	for _, run := range s.runs {
		if run.JobID == jobID {
			out = append(out, run)
		}
	}
	return out
}

func (s *Store) AlreadyFired(idempotencyKey string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// This is the scheduler's first duplicate check before it runs external work.
	for _, run := range s.runs {
		if run.IdempotencyKey == idempotencyKey {
			return true
		}
	}
	return false
}

func (s *Store) LastRunTime(jobID string) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest time.Time
	for _, run := range s.runs {
		if run.JobID == jobID && run.StartedAt.After(latest) {
			latest = run.StartedAt
		}
	}
	return latest
}

func (s *Store) RaftState() (int, bool) {
	if s.rf == nil {
		return 0, false
	}
	return s.rf.GetState()
}

func RunIdempotencyKey(jobID string, scheduledTime time.Time) string {
	// We truncate to the minute because cron scheduling is minute-based in Sprint 1.
	minute := scheduledTime.UTC().Format("2006-01-02T15:04")
	sum := sha256.Sum256([]byte(jobID + ":" + minute))
	return hex.EncodeToString(sum[:])[:16]
}

func (s *Store) registerWaiter(index int) chan struct{} {
	ch := make(chan struct{})
	s.mu.Lock()
	// One waiter per Raft index is enough for this simple synchronous submit model.
	s.waiters[index] = ch
	s.mu.Unlock()
	return ch
}

func (s *Store) removeWaiter(index int) {
	s.mu.Lock()
	delete(s.waiters, index)
	s.mu.Unlock()
}

func (s *Store) notifyWaiter(index int) {
	s.mu.Lock()
	ch, ok := s.waiters[index]
	if ok {
		delete(s.waiters, index)
		close(ch)
	}
	s.mu.Unlock()
}

func (s *Store) applyLoop() {
	for msg := range s.applyCh {
		if msg.SnapshotValid {
			// Snapshot replace is another valid state transition path.
			s.applySnapshot(msg.Snapshot)
			continue
		}
		if !msg.CommandValid {
			continue
		}
		cmd, ok := decodeCommand(msg.Command)
		if !ok {
			// Ignore entries we cannot decode into store commands.
			continue
		}

		s.mu.Lock()
		// This is the only place committed commands mutate real store state.
		s.applyCommand(cmd)
		s.applyCount++
		shouldSnapshot := s.rf != nil && s.applyCount >= snapThreshold
		var snap []byte
		if shouldSnapshot {
			snap = s.encodeSnapshotLocked()
			s.applyCount = 0
		}
		s.mu.Unlock()

		if shouldSnapshot {
			// Snapshot before notifying Submit so successful writes are durable on disk.
			s.rf.Snapshot(msg.CommandIndex, snap)
		}
		// Wake up the Submit caller only after the command was applied and snapshotted.
		s.notifyWaiter(msg.CommandIndex)
	}
}

func decodeCommand(raw any) (Command, bool) {
	switch v := raw.(type) {
	case []byte:
		// Real Raft entries arrive as serialized bytes.
		var cmd Command
		if err := json.Unmarshal(v, &cmd); err != nil {
			return Command{}, false
		}
		return cmd, true
	case Command:
		return v, true
	case Cmd:
		return v.command(), true
	default:
		return Command{}, false
	}
}

func (s *Store) applyCommand(cmd Command) {
	switch cmd.Op {
	case OpCreateJob, OpUpdateJob:
		var job Job
		if json.Unmarshal(cmd.Data, &job) != nil {
			return
		}
		// Normalize again on apply so replayed state also gets safe defaults.
		normalizeJob(&job)
		if existing, ok := s.jobs[job.ID]; ok && job.CreatedAt.IsZero() {
			// Preserve original create time if an update did not send it back.
			job.CreatedAt = existing.CreatedAt
		}
		if job.UpdatedAt.IsZero() {
			job.UpdatedAt = time.Now()
		}
		s.jobs[job.ID] = job
	case OpDeleteJob:
		var job Job
		if json.Unmarshal(cmd.Data, &job) != nil {
			return
		}
		delete(s.jobs, job.ID)
		// Drop run history for deleted jobs so reads stay clean.
		dst := s.runs[:0]
		for _, run := range s.runs {
			if run.JobID != job.ID {
				dst = append(dst, run)
			}
		}
		s.runs = dst
	case OpRecordRun:
		var run JobRun
		if json.Unmarshal(cmd.Data, &run) != nil {
			return
		}
		s.upsertRun(run)
	}
}

func (s *Store) upsertRun(run JobRun) {
	for i, existing := range s.runs {
		if existing.IdempotencyKey != "" && existing.IdempotencyKey == run.IdempotencyKey {
			// Same logical firing: update the existing record instead of appending another.
			s.runs[i] = mergeRun(existing, run)
			s.updateJobRunFields(s.runs[i])
			return
		}
	}
	s.runs = append(s.runs, run)
	if len(s.runs) > maxRuns {
		// Keep history bounded so the replicated in-memory state does not grow forever.
		s.runs = s.runs[len(s.runs)-maxRuns:]
	}
	s.updateJobRunFields(run)
}

func mergeRun(existing, next JobRun) JobRun {
	// The final run update may only fill in fields that were unknown at "running" time.
	if next.ID == "" {
		next.ID = existing.ID
	}
	if next.JobName == "" {
		next.JobName = existing.JobName
	}
	if next.StartedAt.IsZero() {
		next.StartedAt = existing.StartedAt
	}
	if next.FinishedAt == nil {
		next.FinishedAt = existing.FinishedAt
	}
	if next.Status == "" {
		next.Status = existing.Status
	}
	if next.IdempotencyKey == "" {
		next.IdempotencyKey = existing.IdempotencyKey
	}
	return next
}

func (s *Store) updateJobRunFields(run JobRun) {
	job, ok := s.jobs[run.JobID]
	if !ok {
		return
	}
	// Job stores a quick summary. Detailed history stays in runs.
	job.LastRun = run.StartedAt
	job.LastStatus = run.Status
	job.UpdatedAt = time.Now()
	s.jobs[run.JobID] = job
}

func normalizeJob(job *Job) {
	if !job.Enabled && job.CreatedAt.IsZero() {
		// Older tests often omit Enabled. Treat that as "on" for compatibility.
		job.Enabled = true
	}
	if job.PartitionKey == "" {
		// Single-user default for Sprint 1.
		job.PartitionKey = defaultUserKey
	}
	if job.CatchupPolicy == "" {
		job.CatchupPolicy = "skip"
	}
	if job.Metadata == nil {
		// Avoid nil map checks in callers.
		job.Metadata = map[string]string{}
	}
}
