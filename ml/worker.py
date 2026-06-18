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
    job: str
    payload: dict[str, Any]
    idempotency_key: str


class Worker:
    def __init__(self) -> None:
        self.topic = os.getenv("ML_JOBS_TOPIC", "jobs.ml")
        self.group_id = os.getenv("ML_WORKER_GROUP", "market-intel-ml-worker")
        self.brokers = split_csv(os.getenv("KAFKA_BROKERS", "redpanda:9092"))
        self.running = True

    def run(self) -> None:
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
                        envelope = parse_envelope(msg.value, msg.key)
                        dispatch(envelope)
                        consumer.commit()
                    except Exception:
                        logger.exception("job failed topic=%s partition=%s offset=%s", msg.topic, msg.partition, msg.offset)
                        # Leave the offset uncommitted so the job can be retried.
                        break
        finally:
            consumer.close()
            logger.info("worker stopped")

    def stop(self, *_: Any) -> None:
        self.running = False


def split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def decode_key(raw: bytes | None) -> str:
    if raw is None:
        return ""
    return raw.decode("utf-8")


def decode_value(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


def parse_envelope(value: dict[str, Any], key: str) -> JobEnvelope:
    job_name = value.get("job") or value.get("job_name")
    if not job_name:
        raise ValueError(f"missing job field in message: {value}")

    payload = value.get("payload", {})
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {"raw": payload}
    if not isinstance(payload, dict):
        payload = {"value": payload}

    idempotency_key = value.get("idempotency_key") or key or value.get("job_id") or ""
    return JobEnvelope(job=str(job_name), payload=payload, idempotency_key=str(idempotency_key))


def dispatch(envelope: JobEnvelope) -> None:
    module_name = f"ml.jobs.{envelope.job}"
    logger.info("dispatching job=%s idempotency_key=%s", envelope.job, envelope.idempotency_key)

    module = importlib.import_module(module_name)
    if hasattr(module, "run"):
        module.run(envelope.payload, idempotency_key=envelope.idempotency_key)
        return

    job_class = getattr(module, "Job", None)
    if job_class is None:
        raise AttributeError(f"{module_name} must expose run() or Job")
    job_class().run(envelope.payload, idempotency_key=envelope.idempotency_key)


def main() -> int:
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

