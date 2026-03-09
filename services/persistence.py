from __future__ import annotations

import os
from datetime import datetime, timezone
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


def upsert_lead(phone: str, name: str | None = None, location: str | None = None) -> int:
    if not phone:
        raise ValueError("phone is required")
    fallback_name = name or f"WhatsApp Lead {phone[-4:]}" if phone else "WhatsApp Lead"
    query = """
        INSERT INTO leads (name, phone, location)
        VALUES (%s, %s, %s)
        ON CONFLICT (phone) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, leads.name),
            location = COALESCE(EXCLUDED.location, leads.location),
            updated_at = NOW()
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (fallback_name, phone, location))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to upsert lead")
            return int(row[0])


def ensure_conversation(lead_id: int) -> int:
    select_query = "SELECT id FROM conversations WHERE lead_id = %s LIMIT 1"
    insert_query = """
        INSERT INTO conversations (lead_id, unread_count, is_live)
        VALUES (%s, 0, TRUE)
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(select_query, (lead_id,))
            row = cur.fetchone()
            if row:
                return int(row[0])
            cur.execute(insert_query, (lead_id,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to create conversation row")
            return int(row[0])


def insert_message(
    conversation_id: int,
    sender: str,
    content: str,
    *,
    message_type: str = "text",
    sent_at: datetime | None = None,
) -> int:
    if not sent_at:
        sent_at = datetime.now(timezone.utc)
    query = """
        INSERT INTO messages (conversation_id, sender, content, message_type, sent_at)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (conversation_id, sender, content, message_type, sent_at))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert message")
            return int(row[0])


def update_conversation(conversation_id: int, last_message: str, sent_at: datetime, sender: str) -> None:
    role = (sender or "").lower()
    query = """
        UPDATE conversations
        SET last_message = %s,
            last_message_time = %s,
            unread_count = CASE %s
                WHEN 'client' THEN unread_count + 1
                WHEN 'designer' THEN 0
                ELSE unread_count
            END,
            updated_at = NOW()
        WHERE id = %s;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (last_message, sent_at, role, conversation_id))


def store_embedding(phone: str, message: str, embedding: Sequence[float]) -> None:
    query = """
        INSERT INTO conversation_embeddings (phone, message, embedding)
        VALUES (%s, %s, %s);
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone, message, list(embedding)))


def similar_messages(query_embedding: Sequence[float], limit: int = 5) -> List[str]:
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


def log_booking(phone: str, meeting_time: str, meeting_link: str, status: str) -> None:
    query = """
        INSERT INTO bookings (phone, meeting_time, meeting_link, status)
        VALUES (%s, %s, %s, %s);
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone, meeting_time, meeting_link, status))


def health_check() -> bool:
    try:
        with _pool().connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except Exception:
        return False
