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

type KafkaExecutor struct {
	Brokers  []string
	Timeout  time.Duration
	Producer kafkaProducer
}

type kafkaProducer interface {
	Publish(ctx context.Context, brokers []string, topic string, key []byte, value []byte) error
}

type kafkaGoProducer struct{}

func (kafkaGoProducer) Publish(ctx context.Context, brokers []string, topic string, key []byte, value []byte) error {
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
	// Sprint 1 uses a simple "topic:message" payload contract.
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
		// No brokers is treated as a no-op for early local development.
		log.Printf("[kafka] no brokers configured; skipping publish topic=%s msg=%s", topic, message)
		return nil
	}

	timeout := e.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	producer := e.Producer
	if producer == nil {
		producer = kafkaGoProducer{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Use job ID as the key so repeated executions of one scheduled job map predictably.
	if err := producer.Publish(ctx, e.Brokers, topic, []byte(job.ID), []byte(message)); err != nil {
		return fmt.Errorf("publish kafka message topic=%s: %w", topic, err)
	}
	return nil
}
