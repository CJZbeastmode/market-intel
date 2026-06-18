package api

import (
	"net/http"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
)

// jobStore is the store.Store subset the API needs.
// *store.Store satisfies this interface.
type jobStore interface {
	CreateJob(store.Job) error
	DeleteJob(string) error
	GetJob(string) (store.Job, bool)
	ListJobs() []store.Job
	GetRuns(string) []store.JobRun
}

// jobScheduler lets the API trigger immediate job runs.
// *scheduler.Scheduler satisfies this interface.
type jobScheduler interface {
	FireNow(jobID string) error
}

// leaderChecker exposes Raft state for the /cluster endpoint.
// *raft.Raft satisfies this interface.
type leaderChecker interface {
	GetState() (int, bool)
}

// Server holds the shared dependencies for all HTTP handlers.
type Server struct {
	st jobStore
	sc jobScheduler
	lc leaderChecker
}

// New wires up the REST API and returns a ready-to-run *http.Server.
// Call ListenAndServe on the result to start accepting requests.
func New(st jobStore, sc jobScheduler, lc leaderChecker, addr string) *http.Server {
	s := &Server{st: st, sc: sc, lc: lc}
	mux := http.NewServeMux()

	// Sprint 1 keeps the API surface intentionally small: job CRUD, manual trigger, cluster status.
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("DELETE /jobs/{id}", s.deleteJob)
	mux.HandleFunc("POST /jobs/{id}/trigger", s.triggerJob)
	mux.HandleFunc("GET /cluster", s.clusterStatus)

	return &http.Server{
		Addr:         addr,
		Handler:      logging(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}
