package executor

import (
	"fmt"
	"strings"
)

import "github.com/CJZbeastmode/market-intel/internal/store"

// Executor is implemented by every concrete job executor.
type Executor interface {
	Execute(job store.Job) error
}

// Dispatcher keeps the scheduler simple.
// The scheduler asks for "run this job"; the dispatcher decides how.
type Dispatcher struct {
	executors map[string]Executor
}

func NewDispatcher(kafkaBrokers string) *Dispatcher {
	// Register executors in one place so later job types are easy to add.
	return &Dispatcher{executors: map[string]Executor{
		"shell": &ShellExecutor{},
		"http":  &HTTPExecutor{},
		"kafka": &KafkaExecutor{Brokers: splitCSV(kafkaBrokers)},
	}}
}

func (d *Dispatcher) Dispatch(job store.Job) error {
	exec, ok := d.executors[job.Executor]
	if !ok {
		return fmt.Errorf("unknown executor type: %s", job.Executor)
	}
	// After routing, execution logic is fully owned by the selected executor.
	return exec.Execute(job)
}

func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
