package store

import (
	"encoding/json"
	"log"
)

func (s *Store) encodeSnapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.encodeSnapshotLocked()
}

// encodeSnapshotLocked serializes current state. Caller must hold s.mu.
func (s *Store) encodeSnapshotLocked() []byte {
	// Snapshot only stores durable state machine data, not transient waiters.
	data, err := json.Marshal(StoreState{Jobs: s.jobs, Runs: s.runs})
	if err != nil {
		log.Panicf("store: snapshot encode: %v", err)
	}
	return data
}

func (s *Store) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	// Snapshot install replaces the current in-memory view with the saved one.
	var state StoreState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("store: snapshot decode: %v", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.Jobs == nil {
		state.Jobs = make(map[string]Job)
	}
	if state.Runs == nil {
		state.Runs = make([]JobRun, 0)
	}
	// Replace both collections together so store state stays internally consistent.
	s.jobs = state.Jobs
	s.runs = state.Runs
}
