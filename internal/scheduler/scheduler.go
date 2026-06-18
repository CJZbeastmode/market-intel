package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
)

type leaderChecker interface {
	GetState() (int, bool)
}

type dispatcher interface {
	Dispatch(job store.Job) error
}

type Scheduler struct {
	lc   leaderChecker
	st   *store.Store
	exec dispatcher

	// submit is injectable so tests can bypass a real Raft node.
	submit func(op string, data any) error
	done   chan struct{}
}

func Make(lc leaderChecker, st *store.Store, exec dispatcher) *Scheduler {
	sc := &Scheduler{
		lc:   lc,
		st:   st,
		exec: exec,
		done: make(chan struct{}),
	}
	sc.submit = st.Submit
	// Start the periodic leader-only scheduling loop immediately.
	go sc.tickLoop()
	return sc
}

func (sc *Scheduler) Kill() {
	close(sc.done)
}

func (sc *Scheduler) tickLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sc.done:
			return
		case now := <-ticker.C:
			_, isLeader := sc.lc.GetState()
			if !isLeader {
				// Followers stay warm but never fire jobs.
				continue
			}
			sc.tick(now)
		}
	}
}

func (sc *Scheduler) tick(now time.Time) {
	for _, j := range sc.st.ListJobs() {
		if !j.Enabled || j.NextRun.IsZero() || j.NextRun.After(now) {
			continue
		}
		// Capture the scheduled time now. It becomes the dedupe key for this firing.
		scheduled := j.NextRun
		go sc.fireJob(j, scheduled, now)
	}
}

func (sc *Scheduler) fireJob(j store.Job, scheduledTime time.Time, now time.Time) {
	key := store.RunIdempotencyKey(j.ID, scheduledTime)
	if sc.st.AlreadyFired(key) {
		// Another leader or retry path already handled this logical run.
		return
	}

	next, ok := nextRun(j.CronExpr, now)
	if !ok {
		return
	}
	// Move the schedule forward before external work starts.
	// This keeps cluster state ahead of executor side effects.
	j.NextRun = next
	j.UpdatedAt = time.Now()
	if err := sc.submit(store.OpUpdateJob, j); err != nil {
		// If we lost leadership here, another leader will take over.
		return
	}

	run := store.JobRun{
		ID:             newID(),
		JobID:          j.ID,
		JobName:        j.Name,
		StartedAt:      time.Now(),
		Status:         "running",
		IdempotencyKey: key,
	}
	if err := sc.submit(store.OpRecordRun, run); err != nil {
		// No replicated run record means we should not continue with side effects.
		return
	}

	err := sc.exec.Dispatch(j)
	finished := time.Now()
	run.FinishedAt = &finished
	run.DurationMs = finished.Sub(run.StartedAt).Milliseconds()
	if err != nil {
		run.Status = "failed"
		run.ErrorMessage = err.Error()
	} else {
		run.Status = "success"
	}
	// Best effort finalization. The same idempotency key turns this into an update, not a new run.
	_ = sc.submit(store.OpRecordRun, run)
}

func (sc *Scheduler) FireNow(jobID string) error {
	j, ok := sc.st.GetJob(jobID)
	if !ok {
		return fmt.Errorf("job %q not found", jobID)
	}
	go sc.fireJob(j, time.Now(), time.Now())
	return nil
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Random ids are fine here because this is only the record id, not the dedupe key.
	return hex.EncodeToString(b)
}
