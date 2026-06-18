package executor

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
)

const defaultShellTimeout = 10 * time.Minute

type ShellExecutor struct {
	Timeout time.Duration
}

func (e *ShellExecutor) Execute(job store.Job) error {
	timeout := e.Timeout
	if timeout == 0 {
		timeout = defaultShellTimeout
	}
	// Shell jobs are dangerous if they hang, so always put them behind a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", job.Payload)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	if ctx.Err() == context.DeadlineExceeded {
		// Include command output because that is often the only clue during debugging.
		return fmt.Errorf("shell timeout after %s: %s", timeout, string(out))
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return fmt.Errorf("shell exit %d: %s", exitErr.ExitCode(), string(out))
	}
	return fmt.Errorf("shell: %w: %s", err, string(out))
}
