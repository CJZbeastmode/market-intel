package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/api"
	"github.com/CJZbeastmode/market-intel/internal/executor"
	"github.com/CJZbeastmode/market-intel/internal/raft"
	"github.com/CJZbeastmode/market-intel/internal/scheduler"
	"github.com/CJZbeastmode/market-intel/internal/store"
)

// env vars:
//
//	BIND_ADDR  — TCP address to listen on for Raft RPC, e.g. ":8001"
//	PEERS      — comma-separated list of ALL peer addresses in order, e.g. "host1:8001,host2:8002,host3:8003"
//	ME         — 0-indexed position of this node in PEERS, e.g. "0"
//	API_ADDR   — optional HTTP address for the REST API, e.g. ":8080" (omit to disable)
//	RAFT_DATA_DIR — optional disk path for Raft state and snapshots, e.g. "/data"
func main() {
	bindAddr := mustEnv("BIND_ADDR")
	peersEnv := mustEnv("PEERS")
	meStr := mustEnv("ME")
	apiAddr := os.Getenv("API_ADDR") // optional
	dataDir := os.Getenv("RAFT_DATA_DIR")

	me, err := strconv.Atoi(meStr)
	if err != nil {
		log.Fatalf("ME must be an integer, got %q", meStr)
	}

	// build peer list — Raft needs all peers (including self) in the same order on every node
	addrs := strings.Split(peersEnv, ",")
	peers := make([]*raft.Peer, len(addrs))
	for i, addr := range addrs {
		peers[i] = raft.NewPeer(strings.TrimSpace(addr))
	}
	if me < 0 || me >= len(peers) {
		log.Fatalf("ME=%d out of range for %d peers", me, len(peers))
	}

	// start the TCP listener before creating Raft so peers can dial us immediately
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", bindAddr, err)
	}
	log.Printf("node %d listening on %s", me, bindAddr)

	// Raft and store share this channel
	applyCh := make(chan raft.ApplyMsg)

	var persister *raft.Persister
	if dataDir != "" {
		var err error
		// File-backed persistence lets a node recover Raft term, vote, log, and snapshot after restart.
		persister, err = raft.MakeFilePersister(dataDir)
		if err != nil {
			log.Fatalf("open raft data dir %s: %v", dataDir, err)
		}
	} else {
		// Tests and ad-hoc local runs can still use memory-only persistence.
		persister = raft.MakePersister()
	}

	// Raft owns consensus. The store and scheduler sit on top of it.
	rf := raft.Make(peers, me, persister, applyCh)

	// register Raft with the RPC server using the service name "Raft" so that
	// Peer.Call("Raft.RequestVote", ...) routes correctly
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &raft.RPCAdapter{Rf: rf}); err != nil {
		log.Fatalf("rpc register: %v", err)
	}
	go srv.Accept(ln)

	// Store is the replicated state machine.
	st := store.Make(rf, persister, applyCh)
	// Scheduler only decides when to run. Executors decide how to run.
	sc := scheduler.Make(rf, st, executor.NewDispatcher(os.Getenv("KAFKA_BROKERS")))

	// wait for the cluster to elect a leader, then catch up any missed jobs
	go func() {
		for {
			_, isLeader := rf.GetState()
			if isLeader {
				// Catch-up only matters once a leader exists.
				log.Printf("node %d is leader — running missed job reconciliation", me)
				sc.ReconcileMissedJobs()
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// start the REST API if API_ADDR is set
	if apiAddr != "" {
		apiServer := api.New(st, sc, rf, apiAddr)
		go func() {
			log.Printf("API server on %s", apiAddr)
			if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("API server error: %v", err)
			}
		}()
	}

	// block until SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("node %d shutting down (%v)", me, sig)

	sc.Kill()
	rf.Kill()
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env var %s is required", key)
	}
	return v
}
