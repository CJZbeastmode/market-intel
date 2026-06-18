package store

import "hash/fnv"

// ClusterRouter maps a partition key to a Raft cluster index.
// Sharding is disabled for Sprint 1, so the default route is always 0.
type ClusterRouter struct {
	numClusters int
	enabled     bool
}

func NewClusterRouter(numClusters int, enabled bool) *ClusterRouter {
	return &ClusterRouter{numClusters: numClusters, enabled: enabled}
}

func (r *ClusterRouter) Route(partitionKey string) int {
	if !r.enabled || r.numClusters <= 1 {
		// Sprint 1 is single-shard. Every job lands on cluster 0.
		return 0
	}
	// Later this gives stable routing for the same key across restarts.
	h := fnv.New32a()
	_, _ = h.Write([]byte(partitionKey))
	return int(h.Sum32()) % r.numClusters
}
