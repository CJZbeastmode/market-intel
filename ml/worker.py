import importlib
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ml.worker")


@dataclass(frozen=True)
class JobEnvelope:
    # "job" tells us which Python module to load.
    job: str
    # "payload" is the job-specific input.
    payload: dict[str, Any]
    # We keep the idempotency key so downstream jobs can log or reuse it.
    idempotency_key: str


class Worker:
    def __init__(self) -> None:
        # These env vars let the same worker run locally or in Docker.
        self.topic = os.getenv("ML_JOBS_TOPIC", "jobs.ml")
        self.group_id = os.getenv("ML_WORKER_GROUP", "market-intel-ml-worker")
        self.brokers = split_csv(os.getenv("KAFKA_BROKERS", "redpanda:9092"))
        self.running = True

    def run(self) -> None:
        # We disable auto-commit on purpose.
        # A message is only marked done after the job finishes without error.
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=decode_value,
            key_deserializer=decode_key,
            consumer_timeout_ms=1000,
        )
        logger.info("listening topic=%s brokers=%s group=%s", self.topic, self.brokers, self.group_id)

        try:
            while self.running:
                for msg in consumer:
                    if not self.running:
                        break
                    try:
                        # Turn the raw Kafka message into one consistent internal shape.
                        envelope = parse_envelope(msg.value, msg.key)
                        # Load and run the matching Python job module.
                        dispatch(envelope)
                        # Only commit after success.
                        consumer.commit()
                    except Exception:
                        logger.exception("job failed topic=%s partition=%s offset=%s", msg.topic, msg.partition, msg.offset)
                        # Leave the offset uncommitted so the job can be retried.
                        break
        finally:
            consumer.close()
            logger.info("worker stopped")

    def stop(self, *_: Any) -> None:
        # Signal handlers call this so the worker can leave the loop cleanly.
        self.running = False


def split_csv(value: str) -> list[str]:
    # "a,b,c" -> ["a", "b", "c"]
    return [part.strip() for part in value.split(",") if part.strip()]


def decode_key(raw: bytes | None) -> str:
    # Kafka keys are optional.
    if raw is None:
        return ""
    return raw.decode("utf-8")


def decode_value(raw: bytes) -> dict[str, Any]:
    # All job messages are JSON.
    return json.loads(raw.decode("utf-8"))


def parse_envelope(value: dict[str, Any], key: str) -> JobEnvelope:
    # We accept both "job" and "job_name" so producers can be a little flexible.
    job_name = value.get("job") or value.get("job_name")
    if not job_name:
        raise ValueError(f"missing job field in message: {value}")

    payload = value.get("payload", {})
    if isinstance(payload, str):
        try:
            # If payload itself is JSON text, unpack it into a dict.
            payload = json.loads(payload)
        except json.JSONDecodeError:
            # If not, keep it as raw text so the job can still inspect it.
            payload = {"raw": payload}
    if not isinstance(payload, dict):
        # Keep one stable shape for downstream code.
        payload = {"value": payload}

    # Reuse the most useful id source we can find.
    idempotency_key = value.get("idempotency_key") or key or value.get("job_id") or ""
    return JobEnvelope(job=str(job_name), payload=payload, idempotency_key=str(idempotency_key))


def dispatch(envelope: JobEnvelope) -> None:
    # "fetch_quotes" becomes "ml.jobs.fetch_quotes".
    module_name = f"ml.jobs.{envelope.job}"
    logger.info("dispatching job=%s idempotency_key=%s", envelope.job, envelope.idempotency_key)

    module = importlib.import_module(module_name)
    if hasattr(module, "run"):
        # Small jobs can expose a top-level run() function.
        module.run(envelope.payload, idempotency_key=envelope.idempotency_key)
        return

    job_class = getattr(module, "Job", None)
    if job_class is None:
        raise AttributeError(f"{module_name} must expose run() or Job")
    # Or a job can expose a class with a run() method.
    job_class().run(envelope.payload, idempotency_key=envelope.idempotency_key)


def main() -> int:
    # Keep signal handling in one place so Docker stop works properly.
    worker = Worker()
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    try:
        worker.run()
    except KafkaError:
        logger.exception("kafka error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
