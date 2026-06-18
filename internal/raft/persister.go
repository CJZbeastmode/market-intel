package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
	path      string
}

func MakePersister() *Persister {
	return &Persister{}
}

func MakeFilePersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ps := &Persister{path: filepath.Join(dir, "raft-state.json")}
	if err := ps.load(); err != nil {
		return nil, err
	}
	return ps, nil
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = clone(ps.raftstate)
	np.snapshot = clone(ps.snapshot)
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
	if ps.path != "" {
		if err := ps.saveLocked(); err != nil {
			panic(err)
		}
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

type persistedState struct {
	RaftState []byte `json:"raft_state"`
	Snapshot  []byte `json:"snapshot"`
}

func (ps *Persister) load() error {
	data, err := os.ReadFile(ps.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	ps.raftstate = clone(state.RaftState)
	ps.snapshot = clone(state.Snapshot)
	return nil
}

func (ps *Persister) saveLocked() error {
	data, err := json.Marshal(persistedState{
		RaftState: ps.raftstate,
		Snapshot:  ps.snapshot,
	})
	if err != nil {
		return err
	}
	tmp := ps.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, ps.path)
}
