#!/usr/bin/env sh
set -eu

# Seeds the default scheduler jobs through the REST API.
# Safe to rerun: this script deletes the fixed job ID before recreating it.

# We probe all three API ports because only one node is leader at a time.
API_URLS="${API_URLS:-http://127.0.0.1:8080 http://127.0.0.1:8081 http://127.0.0.1:8082}"
LEADER_WAIT_SECONDS="${LEADER_WAIT_SECONDS:-30}"
JOB_ID="${JOB_ID:-fetch_live_quotes}"
JOB_NAME="${JOB_NAME:-fetch_live_quotes}"
JOB_CRON="${JOB_CRON:-*/1 * * * 1-5}"
# Payload must stay JSON-escaped because it is itself a JSON string inside the API request.
JOB_PAYLOAD_JSON='jobs.ml:{\"job\":\"fetch_quotes\"}'

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT

find_leader_api() {
    # Wait a little for elections because the cluster may still be starting.
    deadline=$(( $(date +%s) + LEADER_WAIT_SECONDS ))
    while [ "$(date +%s)" -le "$deadline" ]; do
        for api in $API_URLS; do
            # /cluster tells us which API node is currently leader.
            curl -sS --max-time 2 "$api/cluster" > "$tmp_file" 2>/dev/null || true
            if grep -q '"is_leader":true' "$tmp_file"; then
                printf '%s\n' "$api"
                return 0
            fi
        done
        sleep 1
    done
    return 1
}

request() {
    # Small helper for POST/GET-like calls where non-2xx should fail the script.
    method="$1"
    url="$2"
    body="${3:-}"

    if [ -n "$body" ]; then
        curl -fsS --max-time 10 \
            -X "$method" \
            -H "Content-Type: application/json" \
            -d "$body" \
            "$url" > "$tmp_file"
    else
        curl -fsS --max-time 10 \
            -X "$method" \
            "$url" > "$tmp_file"
    fi
}

delete_job() {
    # Delete uses a softer curl mode because "job not found" is still okay for reruns.
    curl -sS --max-time 10 \
        -X DELETE \
        "$api/jobs/$JOB_ID" > "$tmp_file"
}

if [ -n "${API_URL:-}" ]; then
    api="$API_URL"
else
    if ! api="$(find_leader_api)"; then
        echo "could not find a leader API in: $API_URLS" >&2
        exit 1
    fi
fi

echo "using API: $api"

# First remove any older copy of the job so reruns stay idempotent.
if ! delete_job; then
    echo "failed to delete existing job" >&2
    cat "$tmp_file" >&2
    exit 1
fi
if grep -q "job not found" "$tmp_file"; then
    echo "job not present: $JOB_ID"
elif [ ! -s "$tmp_file" ]; then
    echo "deleted existing job: $JOB_ID"
else
    echo "failed to delete existing job" >&2
    cat "$tmp_file" >&2
    exit 1
fi

# This is the real scheduler job we install into the cluster.
# It means:
# run every minute on weekdays
# use the Kafka executor
# publish {"job":"fetch_quotes"} to jobs.ml
job_json="$(printf '{"id":"%s","name":"%s","cron_expr":"%s","executor":"kafka","payload":"%s","catchup_policy":"skip","partition_key":"default","enabled":true}' \
    "$JOB_ID" \
    "$JOB_NAME" \
    "$JOB_CRON" \
    "$JOB_PAYLOAD_JSON")"

if ! request POST "$api/jobs" "$job_json"; then
    echo "failed to create job" >&2
    cat "$tmp_file" >&2
    exit 1
fi

echo "created job: $JOB_NAME"
cat "$tmp_file"
