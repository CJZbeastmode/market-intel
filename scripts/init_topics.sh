#!/usr/bin/env sh
set -eu

# Creates the Kafka/Redpanda topics used by the scheduler and ML pipeline.
# The script is safe to rerun: existing topics are detected and skipped.

REPLICAS="${KAFKA_REPLICAS:-1}"

if command -v rpk >/dev/null 2>&1; then
    RPK_MODE="local"
    DEFAULT_BROKER="localhost:9092"
else
    RPK_MODE="docker"
    DEFAULT_BROKER="redpanda:9092"
fi

BROKER="${KAFKA_BROKERS:-$DEFAULT_BROKER}"

run_rpk() {
    if [ "$RPK_MODE" = "local" ]; then
        rpk "$@"
    else
        docker compose exec -T redpanda rpk "$@"
    fi
}

topic_exists() {
    run_rpk topic describe "$1" -X "brokers=$BROKER" >/dev/null 2>&1
}

create_topic() {
    topic="$1"
    partitions="$2"

    if topic_exists "$topic"; then
        echo "topic exists: $topic"
        return
    fi

    echo "creating topic: $topic partitions=$partitions replicas=$REPLICAS"
    run_rpk topic create "$topic" \
        --partitions "$partitions" \
        --replicas "$REPLICAS" \
        -X "brokers=$BROKER"
}

echo "using broker: $BROKER"

# ML work queue. More partitions let multiple ML workers consume jobs in parallel.
create_topic "jobs.ml" 6

# Market data/event topics. These can scale independently from the job queue.
create_topic "market.quotes" 3
create_topic "market.news" 3
create_topic "market.earnings" 3
create_topic "market.events" 3
create_topic "market.indicators" 3
create_topic "market.predictions" 3

echo "topics ready"
