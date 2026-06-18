import json
import os
from datetime import datetime
from typing import Any

import redis


class RedisClient:
    def __init__(self, user_id: str | None = None, url: str | None = None) -> None:
        self.user_id = user_id or os.getenv("USER_ID", "default")
        self.url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.client = redis.Redis.from_url(self.url, decode_responses=True)

    def quote_key(self, ticker: str) -> str:
        return f"quote:{self.user_id}:{ticker.upper()}"

    def quote_ticks_key(self, ticker: str) -> str:
        return f"quote_ticks:{self.user_id}:{ticker.upper()}"

    def quote_channel(self, ticker: str) -> str:
        return f"quotes:{self.user_id}:{ticker.upper()}"

    def set_quote(self, ticker: str, quote: dict[str, Any], ttl_seconds: int = 300) -> None:
        payload = normalize_payload(quote)
        self.client.set(self.quote_key(ticker), json.dumps(payload), ex=ttl_seconds)

    def get_quote(self, ticker: str) -> dict[str, Any] | None:
        raw = self.client.get(self.quote_key(ticker))
        if raw is None:
            return None
        return json.loads(raw)

    def push_quote_tick(self, ticker: str, quote: dict[str, Any], max_ticks: int = 100) -> None:
        key = self.quote_ticks_key(ticker)
        payload = json.dumps(normalize_payload(quote))
        pipe = self.client.pipeline()
        pipe.lpush(key, payload)
        pipe.ltrim(key, 0, max_ticks - 1)
        pipe.execute()

    def publish_quote(self, ticker: str, quote: dict[str, Any]) -> int:
        return int(self.client.publish(self.quote_channel(ticker), json.dumps(normalize_payload(quote))))

    def cache_and_publish_quote(self, ticker: str, quote: dict[str, Any]) -> None:
        self.set_quote(ticker, quote)
        self.push_quote_tick(ticker, quote)
        self.publish_quote(ticker, quote)

    def ping(self) -> bool:
        return bool(self.client.ping())


def normalize_payload(value: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, datetime):
            out[key] = item.isoformat()
        else:
            out[key] = item
    return out
