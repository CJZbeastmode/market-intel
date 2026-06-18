package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestKafkaExecutorPublishes(t *testing.T) {
	producer := &fakeKafkaProducer{}
	e := KafkaExecutor{
		Brokers:  []string{"localhost:9092"},
		Producer: producer,
	}

	if err := e.Execute(store.Job{ID: "k3", Payload: `jobs.ml:{"job":"fetch_quotes"}`}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if producer.topic != "jobs.ml" {
		t.Fatalf("topic: got %q, want %q", producer.topic, "jobs.ml")
	}
	if string(producer.key) != "k3" {
		t.Fatalf("key: got %q, want %q", string(producer.key), "k3")
	}
	if string(producer.value) != `{"job":"fetch_quotes"}` {
		t.Fatalf("value: got %q", string(producer.value))
	}
	if len(producer.brokers) != 1 || producer.brokers[0] != "localhost:9092" {
		t.Fatalf("brokers: got %v", producer.brokers)
	}
}

func TestKafkaExecutorPublishError(t *testing.T) {
	e := KafkaExecutor{
		Brokers:  []string{"localhost:9092"},
		Producer: &fakeKafkaProducer{err: errors.New("broker down")},
	}

	err := e.Execute(store.Job{ID: "k4", Payload: `jobs.ml:{"job":"fetch_quotes"}`})
	if err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("expected broker error, got %v", err)
	}
}

func TestKafkaExecutorLivePublish(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION_TEST") != "1" {
		t.Skip("set KAFKA_INTEGRATION_TEST=1 to publish to a real Kafka broker")
	}
	brokers := splitCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	e := KafkaExecutor{
		Brokers: brokers,
		Timeout: 5 * time.Second,
	}
	if err := e.Execute(store.Job{ID: "k-live", Payload: `market.events:{"source":"go-test","event":"kafka-executor"}`}); err != nil {
		t.Fatalf("expected live publish success, got %v", err)
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

type fakeKafkaProducer struct {
	brokers []string
	topic   string
	key     []byte
	value   []byte
	err     error
}

func (f *fakeKafkaProducer) Publish(ctx context.Context, brokers []string, topic string, key []byte, value []byte) error {
	f.brokers = append([]string(nil), brokers...)
	f.topic = topic
	f.key = append([]byte(nil), key...)
	f.value = append([]byte(nil), value...)
	return f.err
}
