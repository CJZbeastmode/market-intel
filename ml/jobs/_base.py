import logging
import os
import time
from typing import Any

from ml.db.redis_client import RedisClient
from ml.db.timescale import TimescaleClient


class BaseJob:
    # Concrete jobs override this so logs show the real job name.
    job_name = "base"

    def __init__(
        self,
        user_id: str | None = None,
        db: TimescaleClient | None = None,
        redis: RedisClient | None = None,
    ) -> None:
        # Every job carries the user id because DB rows and Redis keys are per user.
        self.user_id = user_id or os.getenv("USER_ID", "default")
        self.logger = logging.getLogger(f"ml.jobs.{self.job_name}")

        # Jobs get ready-to-use DB and Redis helpers.
        # That keeps the concrete job code focused on its own logic.
        self.db = db or TimescaleClient(user_id=self.user_id)
        self.redis = redis or RedisClient(user_id=self.user_id)

    def run(self, payload: dict[str, Any] | None = None, idempotency_key: str = "") -> Any:
        payload = payload or {}
        start = time.monotonic()
        self.logger.info("starting job user_id=%s idempotency_key=%s", self.user_id, idempotency_key)

        try:
            result = self.execute(payload, idempotency_key=idempotency_key)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            self.logger.info("finished job elapsed_ms=%s", elapsed_ms)
            return result
        except Exception:
            self.logger.exception("job failed")
            raise
        finally:
            # Close the DB connection after each job run.
            # Redis uses its own pooling internally.
            self.db.close()

    def execute(self, payload: dict[str, Any], idempotency_key: str = "") -> Any:
        # Concrete jobs implement this.
        raise NotImplementedError
