#!/usr/bin/env sh
set -eu

# Creates the Kafka/Redpanda topics used by the scheduler and ML pipeline.
# The script is safe to rerun: existing topics are detected and skipped.

REPLICAS="${KAFKA_REPLICAS:-1}"

if command -v rpk >/dev/null 2>&1; then
    # Use host rpk if it exists.
    RPK_MODE="local"
    DEFAULT_BROKER="localhost:9092"
else
    # Otherwise use the Redpanda container's rpk.
    RPK_MODE="docker"
    DEFAULT_BROKER="redpanda:9092"
fi

BROKER="${KAFKA_BROKERS:-$DEFAULT_BROKER}"

run_rpk() {
    # One helper so the rest of the script does not care where rpk comes from.
    if [ "$RPK_MODE" = "local" ]; then
        rpk "$@"
    else
        docker compose exec -T redpanda rpk "$@"
    fi
}

topic_exists() {
    # "describe" is enough to tell us whether the topic is already there.
    run_rpk topic describe "$1" -X "brokers=$BROKER" >/dev/null 2>&1
}

create_topic() {
    topic="$1"
    partitions="$2"

    if topic_exists "$topic"; then
        # Safe rerun behavior.
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

# ML work queue.
# We give this more partitions because many ML workers may share it later.
create_topic "jobs.ml" 6

# Market data topics.
# These are separate from jobs.ml so job scheduling and data fan-out stay decoupled.
create_topic "market.quotes" 3
create_topic "market.news" 3
create_topic "market.earnings" 3
create_topic "market.events" 3
create_topic "market.indicators" 3
create_topic "market.predictions" 3

echo "topics ready"
