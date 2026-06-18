package executor

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/CJZbeastmode/market-intel/internal/store"
	"github.com/segmentio/kafka-go"
)

// KafkaExecutor is the bridge from the Go scheduler into the async ML side.
// The scheduler fires a cron job here, and this executor turns it into a Kafka message.
type KafkaExecutor struct {
	Brokers  []string
	Timeout  time.Duration
	Producer kafkaProducer
}

// Small interface so tests can fake Kafka without a real broker.
type kafkaProducer interface {
	Publish(ctx context.Context, brokers []string, topic string, key []byte, value []byte) error
}

type kafkaGoProducer struct{}

func (kafkaGoProducer) Publish(ctx context.Context, brokers []string, topic string, key []byte, value []byte) error {
	// We build a short-lived writer per publish.
	// This is simple and good enough for the current load.
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	return writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now().UTC(),
	})
}

func (e *KafkaExecutor) Execute(job store.Job) error {
	// We keep the payload contract simple:
	// "topic:message"
	// Example:
	// jobs.ml:{"job":"fetch_quotes"}
	parts := strings.SplitN(job.Payload, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf(`kafka executor payload must be "topic:message"`)
	}
	topic, message := strings.TrimSpace(parts[0]), parts[1]
	if topic == "" {
		return fmt.Errorf("kafka executor topic cannot be empty")
	}
	if message == "" {
		return fmt.Errorf("kafka executor message cannot be empty")
	}

	if len(e.Brokers) == 0 {
		// In very early local setup, empty brokers means "do nothing".
		// We log it so the user can see why nothing reached Kafka.
		log.Printf("[kafka] no brokers configured; skipping publish topic=%s msg=%s", topic, message)
		return nil
	}

	timeout := e.Timeout
	if timeout == 0 {
		// Keep executor calls bounded so one bad broker does not hang the scheduler.
		timeout = 10 * time.Second
	}
	producer := e.Producer
	if producer == nil {
		// Real runtime uses kafka-go. Tests can inject a fake producer.
		producer = kafkaGoProducer{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// We use job ID as the Kafka key.
	// That keeps messages from the same scheduled job grouped in a predictable way.
	if err := producer.Publish(ctx, e.Brokers, topic, []byte(job.ID), []byte(message)); err != nil {
		return fmt.Errorf("publish kafka message topic=%s: %w", topic, err)
	}
	return nil
}
