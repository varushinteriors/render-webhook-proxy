from __future__ import annotations

import os
from typing import List, Sequence

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")

_pg_pool: ConnectionPool | None = None
if DATABASE_URL:
    _pg_pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, kwargs={"autocommit": True})


def _pool() -> ConnectionPool:
    if not _pg_pool:
        raise RuntimeError("DATABASE_URL is not configured")
    return _pg_pool


def upsert_lead(
    *,
    source: str,
    phone: str,
    name: str | None = None,
    location: str | None = None,
    project_type: str | None = None,
    area: str | None = None,
    budget: str | None = None,
    timeline: str | None = None,
    finish: str | None = None,
) -> None:
    query = """
        INSERT INTO leads (source, phone, name, location, project_type, area, budget, timeline, finish)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (phone) DO UPDATE SET
            source = EXCLUDED.source,
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            project_type = EXCLUDED.project_type,
            area = EXCLUDED.area,
            budget = EXCLUDED.budget,
            timeline = EXCLUDED.timeline,
            finish = EXCLUDED.finish;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (source, phone, name, location, project_type, area, budget, timeline, finish),
            )


def log_conversation(phone: str, role: str, message: str) -> None:
    query = """
        INSERT INTO conversations (phone, role, message)
        VALUES (%s, %s, %s);
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone, role, message))


def log_booking(phone: str, meeting_time: str, meeting_link: str, status: str) -> None:
    query = """
        INSERT INTO bookings (phone, meeting_time, meeting_link, status)
        VALUES (%s, %s, %s, %s);
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone, meeting_time, meeting_link, status))


def store_embedding(phone: str, message: str, embedding: Sequence[float]) -> None:
    query = """
        INSERT INTO conversation_embeddings (phone, message, embedding)
        VALUES (%s, %s, %s);
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone, message, list(embedding)))


def similar_messages(query_embedding: Sequence[float], limit: int = 3) -> List[str]:
    query = """
        SELECT message
        FROM conversation_embeddings
        ORDER BY embedding <-> %s
        LIMIT %s;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (list(query_embedding), limit))
            rows = cur.fetchall()
    return [row["message"] for row in rows]
