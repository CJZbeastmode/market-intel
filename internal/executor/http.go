package executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
)

type HTTPExecutor struct {
	Client *http.Client
}

type httpEnvelope struct {
	JobID       string    `json:"job_id"`
	JobName     string    `json:"job_name"`
	TriggeredAt time.Time `json:"triggered_at"`
}

func (e *HTTPExecutor) Execute(job store.Job) error {
	client := e.Client
	if client == nil {
		// Keep a hard timeout so an upstream service cannot block the scheduler forever.
		client = &http.Client{Timeout: 30 * time.Second}
	}
	// Send a small envelope instead of the raw payload alone so downstream systems
	// can understand why the webhook was fired.
	body, err := json.Marshal(httpEnvelope{
		JobID:       job.ID,
		JobName:     job.Name,
		TriggeredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	resp, err := client.Post(job.Payload, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Non-2xx means the job fired but the remote endpoint rejected it.
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
