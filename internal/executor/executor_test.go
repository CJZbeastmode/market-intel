package executor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
)

func TestShellExecutorSuccess(t *testing.T) {
	e := ShellExecutor{}
	if err := e.Execute(store.Job{ID: "j1", Payload: "echo hello"}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestShellExecutorFailure(t *testing.T) {
	e := ShellExecutor{}
	err := e.Execute(store.Job{ID: "j2", Payload: "exit 42"})
	if err == nil || !strings.Contains(err.Error(), "42") {
		t.Fatalf("expected exit 42 error, got %v", err)
	}
}

func TestShellExecutorTimeout(t *testing.T) {
	e := ShellExecutor{Timeout: 100 * time.Millisecond}
	err := e.Execute(store.Job{ID: "j3", Payload: "sleep 10"})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestHTTPExecutorSuccess(t *testing.T) {
	var gotMethod, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	e := HTTPExecutor{}
	if err := e.Execute(store.Job{ID: "h1", Name: "test-job", Payload: srv.URL}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("expected POST, got %q", gotMethod)
	}
	if !strings.Contains(gotContentType, "application/json") {
		t.Errorf("expected application/json content-type, got %q", gotContentType)
	}
}

func TestHTTPExecutorNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	e := HTTPExecutor{}
	err := e.Execute(store.Job{ID: "h2", Payload: srv.URL})
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("expected HTTP 500 error, got %v", err)
	}
}

func TestHTTPExecutorBadURL(t *testing.T) {
	e := HTTPExecutor{}
	if err := e.Execute(store.Job{ID: "h3", Payload: "http://127.0.0.1:1"}); err == nil {
		t.Fatal("expected error for unreachable URL")
	}
}

func TestKafkaExecutorNoBrokers(t *testing.T) {
	e := KafkaExecutor{}
	if err := e.Execute(store.Job{ID: "k1", Payload: "market.quotes:hello"}); err != nil {
		t.Fatalf("expected no-op success, got %v", err)
	}
}

func TestKafkaExecutorBadPayload(t *testing.T) {
	e := KafkaExecutor{}
	if err := e.Execute(store.Job{ID: "k2", Payload: "no-colon-here"}); err == nil {
		t.Fatal("expected error for bad payload")
	}
}

func TestDispatcherShell(t *testing.T) {
	d := NewDispatcher("")
	if err := d.Dispatch(store.Job{ID: "d1", Executor: "shell", Payload: "echo dispatch"}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestDispatcherUnknown(t *testing.T) {
	d := NewDispatcher("")
	err := d.Dispatch(store.Job{ID: "d2", Executor: "grpc"})
	if err == nil || !strings.Contains(err.Error(), "unknown executor") {
		t.Fatalf("expected unknown executor error, got %v", err)
	}
}
