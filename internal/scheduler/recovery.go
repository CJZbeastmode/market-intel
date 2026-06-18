package scheduler

import (
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
	"github.com/CJZbeastmode/market-intel/pkg/cron"
)

// ReconcileMissedJobs: for new nodes joining in cluster
//
// It should be called once after startup, after the store
// has been restored from snapshot/log replay and a leader has emerged.
// It inspects every job whose NextRun is in the past and acts on CatchupPolicy:
//
//   - "skip"     → advance NextRun to the next future occurrence, do not fire
//   - "run_once" → fire the job exactly once now, then advance NextRun
//
// Only the leader's calls to submit will succeed; followers silently do nothing.
func (sc *Scheduler) ReconcileMissedJobs() {
	now := time.Now()
	for _, j := range sc.st.ListJobs() {
		if j.NextRun.IsZero() || !j.NextRun.Before(now) {
			continue // not missed
		}
		switch j.CatchupPolicy {
		case "skip":
			// Move the cursor forward and do not replay missed work.
			sc.advanceNextRun(j, now)
		case "run_once":
			// Fire exactly one catch-up run using the missed scheduled time.
			go sc.fireJob(j, j.NextRun, now)
		}
	}
}

// advanceNextRun computes the next future occurrence and submits an update
// without firing the executor.
func (sc *Scheduler) advanceNextRun(j store.Job, now time.Time) {
	next, ok := nextRun(j.CronExpr, now)
	if !ok {
		return
	}
	j.NextRun = next
	j.UpdatedAt = time.Now()
	// Best effort is fine here. Reconcile is cleanup work after startup.
	_ = sc.submit(store.OpUpdateJob, j)
}

// nextRun parses expr and returns the next occurrence after now.
// Returns false if the expression is invalid.
func nextRun(expr string, now time.Time) (time.Time, bool) {
	e, err := cron.Parse(expr)
	if err != nil {
		return time.Time{}, false
	}
	t := e.Next(now)
	if t.IsZero() {
		return time.Time{}, false
	}
	return t, true
}
