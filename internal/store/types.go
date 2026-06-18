package store

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job is the unit of work stored in the Raft-backed job store.
// PartitionKey is always set now and ignored until sharding is enabled.
type Job struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	CronExpr      string `json:"cron_expr"`
	Executor      string `json:"executor"`
	Payload       string `json:"payload"`
	Enabled       bool   `json:"enabled"`
	CatchupPolicy string `json:"catchup_policy"`
	// PartitionKey is not used for routing yet.
	// We still store it now so future sharding does not need a schema change.
	PartitionKey string            `json:"partition_key"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	Metadata     map[string]string `json:"metadata"`

	// These fields are runtime scheduler state, not just static job config.
	NextRun    time.Time `json:"next_run"`
	LastRun    time.Time `json:"last_run,omitempty"`
	LastStatus string    `json:"last_status,omitempty"`
}

// JobRun is a single execution record.
// IdempotencyKey prevents duplicate execution records after leader failover.
type JobRun struct {
	ID         string     `json:"id"`
	JobID      string     `json:"job_id"`
	JobName    string     `json:"job_name"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Status moves from "running" to a final state after the executor returns.
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
	DurationMs   int64  `json:"duration_ms,omitempty"`
	// This key is the main duplicate guard for leader failover and retries.
	IdempotencyKey string `json:"idempotency_key"`

	// These fields help debugging local executors and tests.
	ExitCode int    `json:"exit_code,omitempty"`
	Output   string `json:"output,omitempty"`
}

// StoreState is the full state serialized into Raft snapshots.
type StoreState struct {
	Jobs map[string]Job `json:"jobs"`
	Runs []JobRun       `json:"runs"`
}

// Command is a Raft log entry. Every mutation goes through Raft.
type Command struct {
	Op   string `json:"op"`
	Data []byte `json:"data"`
}

const (
	OpCreateJob = "create_job"
	OpUpdateJob = "update_job"
	OpDeleteJob = "delete_job"
	OpRecordRun = "record_run"
)

type CmdType uint8

const (
	CmdUpsert CmdType = iota
	CmdDelete
	CmdRecord
)

// Cmd and RunRecord keep older tests able to feed synthetic apply messages.
// New production code should use Command and JobRun.
type Cmd struct {
	Type  CmdType
	ReqID string
	Job   Job
	ID    string
	Run   RunRecord
}

type RunRecord struct {
	JobID      string
	StartedAt  time.Time
	FinishedAt time.Time
	ExitCode   int
	Output     string
	Status     string
}

func (c Cmd) command() Command {
	switch c.Type {
	case CmdUpsert:
		// Older tests use "upsert". The new store path treats that as an update/create write.
		data, _ := jsonMarshal(c.Job)
		return Command{Op: OpUpdateJob, Data: data}
	case CmdDelete:
		data, _ := jsonMarshal(Job{ID: c.ID})
		return Command{Op: OpDeleteJob, Data: data}
	case CmdRecord:
		// Legacy tests use a simpler run type. Convert it into the new JobRun shape here.
		run := JobRun{
			JobID:          c.Run.JobID,
			StartedAt:      c.Run.StartedAt,
			Status:         c.Run.Status,
			ExitCode:       c.Run.ExitCode,
			Output:         c.Run.Output,
			IdempotencyKey: legacyRunKey(c),
		}
		if !c.Run.FinishedAt.IsZero() {
			finished := c.Run.FinishedAt
			run.FinishedAt = &finished
			run.DurationMs = finished.Sub(c.Run.StartedAt).Milliseconds()
		}
		data, _ := jsonMarshal(run)
		return Command{Op: OpRecordRun, Data: data}
	default:
		return Command{}
	}
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func legacyRunKey(c Cmd) string {
	if c.ReqID != "" {
		// In tests the request id is the easiest stable uniqueness source.
		return c.ReqID
	}
	// Fall back to a synthetic unique key so old tests do not collapse into one run.
	return fmt.Sprintf("%s:%d:%s", c.Run.JobID, c.Run.StartedAt.UnixNano(), c.Run.Status)
}
