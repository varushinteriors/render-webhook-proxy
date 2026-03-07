from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
SESSION_TTL = 86400  # 24h
WEBHOOK_TTL = 300    # 5 minutes
SUMMARY_BATCH_SIZE = 5

redis_client: redis.Redis | None = None
if REDIS_URL:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _client() -> redis.Redis:
    if not redis_client:
        raise RuntimeError("REDIS_URL is not configured")
    return redis_client


def get_session(phone: str) -> Dict[str, Any]:
    data = _client().hgetall(f"chat:{phone}")
    if not data:
        return {
            "recent_history": "[]",
            "overflow_history": "[]",
            "summary": "",
            "phase": "discovery",
        }
    data.setdefault("recent_history", "[]")
    data.setdefault("overflow_history", "[]")
    data.setdefault("summary", "")
    return data


def save_session(phone: str, session: Dict[str, Any]) -> None:
    key = f"chat:{phone}"
    _client().hset(key, mapping=session)
    _client().expire(key, SESSION_TTL)


def append_history(
    phone: str,
    role: str,
    message: str,
    limit: int = 5,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    session = get_session(phone)
    history = json.loads(session.get("recent_history", "[]"))
    overflow = json.loads(session.get("overflow_history", "[]"))

    entry = {"role": role, "text": message}
    history.append(entry)
    if len(history) > limit:
        overflow.extend(history[:-limit])
        history = history[-limit:]

    summary_batch: List[Dict[str, str]] = []
    if len(overflow) >= SUMMARY_BATCH_SIZE:
        summary_batch = overflow.copy()
        overflow = []

    session["recent_history"] = json.dumps(history)
    session["overflow_history"] = json.dumps(overflow)
    session["last_activity"] = datetime.utcnow().isoformat()
    save_session(phone, session)
    return history, summary_batch


def append_summary(phone: str, summary_text: str) -> None:
    summary_text = summary_text.strip()
    if not summary_text:
        return
    session = get_session(phone)
    existing = session.get("summary", "").strip()
    combined = f"{existing}\n{summary_text}".strip() if existing else summary_text
    session["summary"] = combined
    save_session(phone, session)


def mark_webhook(message_id: str) -> bool:
    key = f"webhook:{message_id}"
    success = _client().setnx(key, "1")
    if success:
        _client().expire(key, WEBHOOK_TTL)
    return bool(success)
