package executor

import (
	"fmt"
	"log"
	"strings"

	"github.com/CJZbeastmode/market-intel/internal/store"
)

type KafkaExecutor struct {
	Brokers []string
}

func (e *KafkaExecutor) Execute(job store.Job) error {
	// Sprint 1 uses a simple "topic:message" payload contract.
	parts := strings.SplitN(job.Payload, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf(`kafka executor payload must be "topic:message"`)
	}
	topic, message := parts[0], parts[1]
	if len(e.Brokers) == 0 {
		// No brokers is treated as a no-op for early local development.
		log.Printf("[kafka] no brokers configured; skipping publish topic=%s msg=%s", topic, message)
		return nil
	}
	// Full broker publishing is a later sprint task.
	return fmt.Errorf("kafka executor not yet implemented")
}
