from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List, Sequence

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")

# Phase 0 (migration scaffolding): optional raw WhatsApp event storage.
# Enabled by default when DATABASE_URL is present; can be disabled for emergency rollback.
WA_EVENT_STORE_ENABLED = os.getenv("WA_EVENT_STORE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}

_pg_pool: ConnectionPool | None = None
if DATABASE_URL:
    _pg_pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, kwargs={"autocommit": True})


def _pool() -> ConnectionPool:
    if not _pg_pool:
        raise RuntimeError("DATABASE_URL is not configured")
    return _pg_pool


def upsert_lead(phone: str, name: str | None = None, location: str | None = None, email: str | None = None) -> int:
    if not phone:
        raise ValueError("phone is required")
    fallback_name = name or f"WhatsApp Lead {phone[-4:]}" if phone else "WhatsApp Lead"
    query = """
        INSERT INTO leads (name, phone, location, email)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (phone) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, leads.name),
            location = COALESCE(EXCLUDED.location, leads.location),
            email = COALESCE(EXCLUDED.email, leads.email),
            updated_at = NOW()
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (fallback_name, phone, location, email))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to upsert lead")
            return int(row[0])


def update_lead_fields(phone: str, fields: dict) -> None:
    """Update leads table for a phone with provided fields (already validated)."""
    if not phone or not fields:
        return

    allowed = {
        "name",
        "email",
        "location",
        "intent",
        "priority",
        "budget_min_lakhs",
        "budget_max_lakhs",
        "notes",
        "pipeline_stage",
        "status",
    }
    items = [(k, v) for k, v in fields.items() if k in allowed]
    if not items:
        return

    set_parts = []
    params = []
    for k, v in items:
        set_parts.append(f"{k} = %s")
        params.append(v)
    set_parts.append("updated_at = NOW()")

    query = f"UPDATE leads SET {', '.join(set_parts)} WHERE phone = %s;"
    params.append(phone)

    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)


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
    # pgvector expects a VECTOR on the RHS of the <-> operator.
    # Psycopg will otherwise pass a double precision array, which breaks the operator.
    vec_literal = "[" + ",".join(f"{float(x):.8f}" for x in query_embedding) + "]"
    query = """
        SELECT message
        FROM conversation_embeddings
        ORDER BY embedding <-> (%s)::vector
        LIMIT %s;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (vec_literal, limit))
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


def ensure_whatsapp_event_tables() -> None:
    """Create Phase-0 tables if missing (backward compatible).

    This does NOT modify existing tables; it only adds new ones.
    """
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return

    ddl = """
    CREATE TABLE IF NOT EXISTS wa_raw_events (
        id BIGSERIAL PRIMARY KEY,
        received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source TEXT NOT NULL DEFAULT 'meta',
        object_type TEXT NULL,
        provider_event_id TEXT NULL,
        wa_phone_id TEXT NULL,
        wa_from TEXT NULL,
        wa_message_id TEXT NULL,
        dedupe_key TEXT NOT NULL,
        headers_json JSONB NULL,
        payload_json JSONB NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS wa_raw_events_dedupe_key_uidx
        ON wa_raw_events(dedupe_key);

    CREATE INDEX IF NOT EXISTS wa_raw_events_received_at_idx
        ON wa_raw_events(received_at DESC);

    CREATE INDEX IF NOT EXISTS wa_raw_events_wa_from_idx
        ON wa_raw_events(wa_from);

    CREATE INDEX IF NOT EXISTS wa_raw_events_message_id_idx
        ON wa_raw_events(wa_message_id);
    """

    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def get_wa_raw_event_id_by_dedupe_key(dedupe_key: str) -> int | None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return None
    if not dedupe_key:
        return None
    query = "SELECT id FROM wa_raw_events WHERE dedupe_key = %s LIMIT 1;"
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (dedupe_key,))
            row = cur.fetchone()
            return int(row[0]) if row else None


def insert_wa_raw_event(
    *,
    dedupe_key: str,
    payload: dict,
    headers: dict | None = None,
    object_type: str | None = None,
    provider_event_id: str | None = None,
    wa_phone_id: str | None = None,
    wa_from: str | None = None,
    wa_message_id: str | None = None,
    source: str = "meta",
) -> int | None:
    """Insert raw event if not already present.

    Returns row id if inserted, else None when deduped or disabled.
    """
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return None
    if not dedupe_key:
        raise ValueError("dedupe_key is required")

    query = """
        INSERT INTO wa_raw_events (
            source, object_type, provider_event_id, wa_phone_id, wa_from, wa_message_id,
            dedupe_key, headers_json, payload_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (dedupe_key) DO NOTHING
        RETURNING id;
    """

    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    source,
                    object_type,
                    provider_event_id,
                    wa_phone_id,
                    wa_from,
                    wa_message_id,
                    dedupe_key,
                    Jsonb(headers) if headers is not None else None,
                    Jsonb(payload),
                ),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None


def ensure_whatsapp_agent_tables() -> None:
    """Create Phase-1 (shadow orchestrator) + Phase-A tables if missing (backward compatible)."""
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return

    ddl = """
    CREATE TABLE IF NOT EXISTS wa_agent_events (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw_event_id BIGINT NULL REFERENCES wa_raw_events(id) ON DELETE SET NULL,
        agent_name TEXT NOT NULL,
        model TEXT NULL,
        latency_ms INTEGER NULL,
        input_summary TEXT NULL,
        output_json JSONB NOT NULL,
        usage_json JSONB NULL,
        error_text TEXT NULL
    );

    CREATE INDEX IF NOT EXISTS wa_agent_events_created_at_idx
        ON wa_agent_events(created_at DESC);

    CREATE INDEX IF NOT EXISTS wa_agent_events_raw_event_id_idx
        ON wa_agent_events(raw_event_id);

    CREATE TABLE IF NOT EXISTS wa_orchestrator_offsets (
        name TEXT PRIMARY KEY,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_raw_event_id BIGINT NOT NULL DEFAULT 0
    );

    INSERT INTO wa_orchestrator_offsets(name, last_raw_event_id)
    VALUES ('shadow', 0)
    ON CONFLICT (name) DO NOTHING;

    -- Phase A: conversation memory state (non-LLM) + idempotency guard.
    CREATE TABLE IF NOT EXISTS wa_conversation_state (
        lead_phone TEXT PRIMARY KEY,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        phase TEXT NULL,
        last_intent TEXT NULL,
        known_fields JSONB NOT NULL DEFAULT '{}'::jsonb,
        summary TEXT NOT NULL DEFAULT ''
    );

    CREATE INDEX IF NOT EXISTS wa_conversation_state_updated_at_idx
        ON wa_conversation_state(updated_at DESC);

    CREATE TABLE IF NOT EXISTS wa_processed_events (
        raw_event_id BIGINT PRIMARY KEY,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Step 1: minimal KB facts for retrieval grounding.
    CREATE TABLE IF NOT EXISTS wa_kb_facts (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'seed',
        source_id TEXT NOT NULL DEFAULT 'seed:v1',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS wa_kb_facts_updated_at_idx
        ON wa_kb_facts(updated_at DESC);

    -- Step 2: multi-project support per lead_phone.
    CREATE TABLE IF NOT EXISTS wa_projects (
        id BIGSERIAL PRIMARY KEY,
        lead_phone TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status TEXT NOT NULL DEFAULT 'active',
        summary TEXT NOT NULL DEFAULT '',
        fields_json JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE INDEX IF NOT EXISTS wa_projects_phone_created_idx
        ON wa_projects(lead_phone, created_at DESC);

    -- Step 3b: admin notification dedupe
    CREATE TABLE IF NOT EXISTS wa_admin_notifications (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        event_type TEXT NOT NULL,
        dedupe_key TEXT NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS wa_admin_notifications_uidx
        ON wa_admin_notifications(event_type, dedupe_key);

    -- Step 2+: admin console session state (WhatsApp interactive admin flows)
    CREATE TABLE IF NOT EXISTS wa_admin_sessions (
        admin_phone TEXT PRIMARY KEY,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        state_json JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE INDEX IF NOT EXISTS wa_admin_sessions_updated_at_idx
        ON wa_admin_sessions(updated_at DESC);

    -- Step 2+: canonical appointments ledger (auditable)
    CREATE TABLE IF NOT EXISTS wa_appointments (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        client_phone TEXT NULL,
        client_name TEXT NULL,
        google_event_id TEXT NULL,
        zoom_meeting_id TEXT NULL,
        zoom_join_url TEXT NULL,
        start_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL DEFAULT 'booked',
        booked_by_admin_phone TEXT NULL,
        designer_name TEXT NULL,
        designer_phone TEXT NULL,
        notes_json JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE UNIQUE INDEX IF NOT EXISTS wa_appointments_google_event_id_uidx
        ON wa_appointments(google_event_id);

    CREATE INDEX IF NOT EXISTS wa_appointments_start_at_idx
        ON wa_appointments(start_at);

    CREATE INDEX IF NOT EXISTS wa_appointments_client_phone_idx
        ON wa_appointments(client_phone);

    -- Step 6: media assets tracking
    CREATE TABLE IF NOT EXISTS wa_media_assets (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        lead_phone TEXT NOT NULL,
        raw_event_id BIGINT NULL REFERENCES wa_raw_events(id) ON DELETE SET NULL,
        wa_message_id TEXT NULL,
        media_type TEXT NOT NULL,
        r2_bucket TEXT NULL,
        r2_key TEXT NULL,
        public_url TEXT NULL,
        mime_type TEXT NULL,
        filename TEXT NULL,
        caption TEXT NULL
    );

    CREATE INDEX IF NOT EXISTS wa_media_assets_phone_created_idx
        ON wa_media_assets(lead_phone, created_at DESC);

    -- Step 1 (admin stats): track leadgen submissions
    CREATE TABLE IF NOT EXISTS wa_leadgen_events (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        lead_phone TEXT NULL,
        payload_json JSONB NOT NULL
    );

    CREATE INDEX IF NOT EXISTS wa_leadgen_events_created_at_idx
        ON wa_leadgen_events(created_at DESC);
    """

    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def get_orchestrator_offset(name: str = "shadow") -> int:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return 0
    query = "SELECT last_raw_event_id FROM wa_orchestrator_offsets WHERE name = %s"
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (name,))
            row = cur.fetchone()
            return int(row[0]) if row else 0


def set_orchestrator_offset(last_raw_event_id: int, name: str = "shadow") -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return
    query = """
        INSERT INTO wa_orchestrator_offsets(name, last_raw_event_id)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE
        SET last_raw_event_id = EXCLUDED.last_raw_event_id,
            updated_at = NOW();
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (name, int(last_raw_event_id)))


def insert_agent_event(
    *,
    agent_name: str,
    output: dict,
    raw_event_id: int | None = None,
    model: str | None = None,
    latency_ms: int | None = None,
    usage: dict | None = None,
    input_summary: str | None = None,
    error_text: str | None = None,
) -> int:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        raise RuntimeError("DATABASE_URL/WA_EVENT_STORE_ENABLED not configured")
    query = """
        INSERT INTO wa_agent_events (
            raw_event_id, agent_name, model, latency_ms, input_summary,
            output_json, usage_json, error_text
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    raw_event_id,
                    agent_name,
                    model,
                    latency_ms,
                    input_summary,
                    Jsonb(output),
                    Jsonb(usage) if usage is not None else None,
                    error_text,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert agent event")
            return int(row[0])


def was_event_processed(raw_event_id: int) -> bool:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return False
    query = "SELECT 1 FROM wa_processed_events WHERE raw_event_id = %s;"
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(raw_event_id),))
            return cur.fetchone() is not None


def mark_event_processed(raw_event_id: int) -> bool:
    """Idempotency guard: mark event as processed.

    Returns True if newly marked, False if it was already present.
    """
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return True
    query = """
        INSERT INTO wa_processed_events(raw_event_id)
        VALUES (%s)
        ON CONFLICT (raw_event_id) DO NOTHING
        RETURNING raw_event_id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(raw_event_id),))
            row = cur.fetchone()
            return row is not None


def get_conversation_state(lead_phone: str) -> dict:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return {}
    query = """
        SELECT lead_phone, phase, last_intent, known_fields, summary
        FROM wa_conversation_state
        WHERE lead_phone = %s
        LIMIT 1;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (lead_phone,))
            row = cur.fetchone()
            return dict(row) if row else {}


def upsert_conversation_state(
    *,
    lead_phone: str,
    phase: str | None = None,
    last_intent: str | None = None,
    known_fields_patch: dict | None = None,
    summary: str | None = None,
) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return

    # Merge known_fields in SQL.
    query = """
        INSERT INTO wa_conversation_state (lead_phone, phase, last_intent, known_fields, summary)
        VALUES (%s, %s, %s, COALESCE(%s,'{}'::jsonb), COALESCE(%s,''))
        ON CONFLICT (lead_phone) DO UPDATE
        SET updated_at = NOW(),
            phase = COALESCE(EXCLUDED.phase, wa_conversation_state.phase),
            last_intent = COALESCE(EXCLUDED.last_intent, wa_conversation_state.last_intent),
            known_fields = wa_conversation_state.known_fields || COALESCE(EXCLUDED.known_fields,'{}'::jsonb),
            summary = CASE
                WHEN EXCLUDED.summary IS NULL THEN wa_conversation_state.summary
                WHEN EXCLUDED.summary = '' THEN wa_conversation_state.summary
                WHEN wa_conversation_state.summary = '' THEN EXCLUDED.summary
                ELSE wa_conversation_state.summary
            END;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lead_phone, phase, last_intent, Jsonb(known_fields_patch or {}), summary))


def create_project(lead_phone: str, *, summary: str = "", fields: dict | None = None, status: str = "active") -> int:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        raise RuntimeError("DB not configured")
    query = """
        INSERT INTO wa_projects(lead_phone, status, summary, fields_json)
        VALUES (%s,%s,%s,%s)
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lead_phone, status, summary or "", Jsonb(fields or {})))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to create project")
            return int(row[0])


def admin_notify_should_send(event_type: str, dedupe_key: str) -> bool:
    """Returns True if this notification hasn't been sent before (deduped in DB)."""
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return True
    if not event_type or not dedupe_key:
        return True
    query = """
        INSERT INTO wa_admin_notifications(event_type, dedupe_key)
        VALUES (%s,%s)
        ON CONFLICT (event_type, dedupe_key) DO NOTHING
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (event_type, dedupe_key))
            return cur.fetchone() is not None


def list_projects(lead_phone: str, limit: int = 10) -> list[dict]:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return []
    query = """
        SELECT id, created_at, status, summary, fields_json
        FROM wa_projects
        WHERE lead_phone = %s
        ORDER BY created_at DESC
        LIMIT %s;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (lead_phone, int(limit)))
            rows = cur.fetchall() or []
    out = []
    for r in rows:
        d = dict(r)
        if hasattr(d.get("created_at"), "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        out.append(d)
    return out


def insert_leadgen_event(*, lead_phone: str | None, payload: dict) -> int:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        raise RuntimeError("DB not configured")
    query = "INSERT INTO wa_leadgen_events(lead_phone, payload_json) VALUES (%s,%s) RETURNING id;"
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lead_phone, Jsonb(payload)))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert leadgen event")
            return int(row[0])


def insert_media_asset(
    *,
    lead_phone: str,
    media_type: str,
    raw_event_id: int | None = None,
    wa_message_id: str | None = None,
    r2_bucket: str | None = None,
    r2_key: str | None = None,
    public_url: str | None = None,
    mime_type: str | None = None,
    filename: str | None = None,
    caption: str | None = None,
) -> int:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        raise RuntimeError("DB not configured")
    query = """
        INSERT INTO wa_media_assets(
            lead_phone, raw_event_id, wa_message_id, media_type,
            r2_bucket, r2_key, public_url, mime_type, filename, caption
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    lead_phone,
                    raw_event_id,
                    wa_message_id,
                    media_type,
                    r2_bucket,
                    r2_key,
                    public_url,
                    mime_type,
                    filename,
                    caption,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert media asset")
            return int(row[0])


def fetch_recent_messages_by_phone(lead_phone: str, limit: int = 3) -> list[dict]:
    """Best-effort: pull last messages from CRM tables using lead phone."""
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return []
    query = """
        SELECT m.sender, m.content, m.sent_at
        FROM leads l
        JOIN conversations c ON c.lead_id = l.id
        JOIN messages m ON m.conversation_id = c.id
        WHERE l.phone = %s
        ORDER BY m.sent_at DESC
        LIMIT %s;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (lead_phone, int(limit)))
            rows = cur.fetchall() or []
    # return chronological order
    return list(reversed([{"sender": r["sender"], "content": r["content"], "sent_at": r["sent_at"].isoformat() if r.get("sent_at") else None} for r in rows]))


def fetch_lead_snapshot(lead_phone: str) -> dict:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return {}
    query = "SELECT id, name, phone, email, location, intent, budget_min_lakhs, budget_max_lakhs, pipeline_stage, status, created_at, updated_at FROM leads WHERE phone = %s LIMIT 1;"
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (lead_phone,))
            row = cur.fetchone()
            if not row:
                return {}
            snap = dict(row)
            for k in ("created_at", "updated_at"):
                v = snap.get(k)
                if hasattr(v, "isoformat"):
                    snap[k] = v.isoformat()
            return snap


def upsert_kb_fact(key: str, value: str, *, source: str = "seed", source_id: str = "seed:v1") -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return
    if not key or not value:
        return
    query = """
        INSERT INTO wa_kb_facts(key, value, source, source_id)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            source = EXCLUDED.source,
            source_id = EXCLUDED.source_id,
            updated_at = NOW();
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (key, value, source, source_id))


def get_kb_fact(key: str) -> dict | None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return None
    query = "SELECT key, value, source, source_id FROM wa_kb_facts WHERE key=%s LIMIT 1;"
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (key,))
            row = cur.fetchone()
            return dict(row) if row else None


def seed_kb_defaults() -> None:
    """Seed minimal KB facts (safe to call repeatedly)."""
    # Canonical defaults (keep stable; can be overridden later).
    default_portfolio = (
        os.getenv("PORTFOLIO_LINK")
        or os.getenv("DRIVE_PORTFOLIO_LINK")
        or "https://pub-a083917787a641dcb9ac60c1f3efe283.r2.dev/varush_portfolio/Varush_Portfolio.pdf"
    )
    meeting_link = os.getenv("MEETING_LINK") or "https://meet.varushinteriors.com/intro"
    services = (
        "Interior design & execution for homes (2/3/4 BHK), modular kitchen, wardrobes, living/bedroom design, "
        "false ceiling & lighting, turnkey renovation."
    )
    upsert_kb_fact("portfolio_link", default_portfolio, source="seed", source_id="seed:portfolio:v1")
    upsert_kb_fact("meeting_link", meeting_link, source="seed", source_id="seed:meeting:v1")
    upsert_kb_fact("services_overview", services, source="seed", source_id="seed:services:v1")


def health_check() -> bool:
    try:
        with _pool().connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except Exception:
        return False


# ------------------------------
# Admin console helpers
# ------------------------------

def get_admin_session(admin_phone: str) -> dict:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and admin_phone):
        return {}
    query = "SELECT state_json FROM wa_admin_sessions WHERE admin_phone = %s LIMIT 1;"
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (admin_phone,))
            row = cur.fetchone()
            if not row:
                return {}
            return dict(row.get("state_json") or {})


def upsert_admin_session(admin_phone: str, state: dict) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and admin_phone):
        return
    query = """
        INSERT INTO wa_admin_sessions(admin_phone, state_json)
        VALUES (%s,%s)
        ON CONFLICT (admin_phone) DO UPDATE
        SET updated_at = NOW(),
            state_json = COALESCE(EXCLUDED.state_json, '{}'::jsonb);
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (admin_phone, Jsonb(state or {})))


def clear_admin_session(admin_phone: str) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and admin_phone):
        return
    query = "DELETE FROM wa_admin_sessions WHERE admin_phone = %s;"
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (admin_phone,))


# ------------------------------
# Appointment ledger helpers
# ------------------------------

def upsert_appointment_from_calendar(
    *,
    google_event_id: str,
    start_at: datetime,
    status: str = "booked",
    client_phone: str | None = None,
    client_name: str | None = None,
    designer_name: str | None = None,
    designer_phone: str | None = None,
    notes: dict | None = None,
) -> int:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        raise RuntimeError("DB not configured")
    if not google_event_id:
        raise ValueError("google_event_id required")
    query = """
        INSERT INTO wa_appointments(
            google_event_id, start_at, status,
            client_phone, client_name,
            designer_name, designer_phone,
            notes_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (google_event_id) DO UPDATE
        SET updated_at = NOW(),
            start_at = EXCLUDED.start_at,
            status = EXCLUDED.status,
            client_phone = COALESCE(EXCLUDED.client_phone, wa_appointments.client_phone),
            client_name = COALESCE(EXCLUDED.client_name, wa_appointments.client_name),
            designer_name = COALESCE(EXCLUDED.designer_name, wa_appointments.designer_name),
            designer_phone = COALESCE(EXCLUDED.designer_phone, wa_appointments.designer_phone),
            notes_json = wa_appointments.notes_json || COALESCE(EXCLUDED.notes_json, '{}'::jsonb)
        RETURNING id;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    google_event_id,
                    start_at,
                    status,
                    client_phone,
                    client_name,
                    designer_name,
                    designer_phone,
                    Jsonb(notes or {}),
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to upsert appointment")
            return int(row[0])


def list_appointments_between(*, start_at: datetime, end_at: datetime) -> list[dict]:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return []
    query = """
        SELECT id, google_event_id, start_at, status, client_phone, client_name, designer_name, designer_phone, notes_json
        FROM wa_appointments
        WHERE start_at >= %s AND start_at < %s
        ORDER BY start_at ASC;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (start_at, end_at))
            rows = cur.fetchall() or []
    out = []
    for r in rows:
        d = dict(r)
        if hasattr(d.get("start_at"), "isoformat"):
            d["start_at"] = d["start_at"].isoformat()
        out.append(d)
    return out


def list_next_appointments(*, start_at: datetime, limit: int = 7) -> list[dict]:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED):
        return []
    query = """
        SELECT id, google_event_id, start_at, status, client_phone, client_name, designer_name, designer_phone, notes_json
        FROM wa_appointments
        WHERE start_at >= %s
          AND status IN ('booked','rescheduled')
        ORDER BY start_at ASC
        LIMIT %s;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (start_at, int(limit)))
            rows = cur.fetchall() or []
    out = []
    for r in rows:
        d = dict(r)
        if hasattr(d.get("start_at"), "isoformat"):
            d["start_at"] = d["start_at"].isoformat()
        out.append(d)
    return out


def list_appointments_for_ist_date(*, day_ist: str) -> list[dict]:
    """day_ist: YYYY-MM-DD in Asia/Kolkata."""
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and day_ist):
        return []
    query = """
        SELECT id, google_event_id, start_at, status, client_phone, client_name, designer_name, designer_phone, notes_json
        FROM wa_appointments
        WHERE (start_at AT TIME ZONE 'Asia/Kolkata')::date = %s::date
          AND status IN ('booked','rescheduled')
        ORDER BY start_at ASC;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (day_ist,))
            rows = cur.fetchall() or []
    out = []
    for r in rows:
        d = dict(r)
        if hasattr(d.get("start_at"), "isoformat"):
            d["start_at"] = d["start_at"].isoformat()
        out.append(d)
    return out


def list_upcoming_appointments_by_client_phone(*, client_phone: str, limit: int = 5) -> list[dict]:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and client_phone):
        return []
    query = """
        SELECT id, google_event_id, start_at, status, client_phone, client_name, designer_name, designer_phone, notes_json
        FROM wa_appointments
        WHERE client_phone = %s
          AND start_at >= NOW()
          AND status IN ('booked','rescheduled')
        ORDER BY start_at ASC
        LIMIT %s;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (client_phone, int(limit)))
            rows = cur.fetchall() or []
    out = []
    for r in rows:
        d = dict(r)
        if hasattr(d.get("start_at"), "isoformat"):
            d["start_at"] = d["start_at"].isoformat()
        out.append(d)
    return out


def update_appointment_designer(*, appointment_id: int, designer_name: str | None, designer_phone: str | None) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and appointment_id):
        return
    query = """
        UPDATE wa_appointments
        SET updated_at = NOW(),
            designer_name = %s,
            designer_phone = %s
        WHERE id = %s;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (designer_name, designer_phone, int(appointment_id)))


def update_appointment_status(*, appointment_id: int, status: str) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and appointment_id and status):
        return
    query = """
        UPDATE wa_appointments
        SET updated_at = NOW(),
            status = %s
        WHERE id = %s;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (status, int(appointment_id)))


def update_appointment_start(*, appointment_id: int, start_at: datetime, status: str | None = None) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and appointment_id and start_at):
        return
    query = """
        UPDATE wa_appointments
        SET updated_at = NOW(),
            start_at = %s,
            status = COALESCE(%s, status)
        WHERE id = %s;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (start_at, status, int(appointment_id)))


def get_appointment_by_id(*, appointment_id: int) -> dict | None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and appointment_id):
        return None
    query = """
        SELECT id, google_event_id, start_at, status, client_phone, client_name, designer_name, designer_phone, notes_json
        FROM wa_appointments
        WHERE id = %s
        LIMIT 1;
    """
    with _pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (int(appointment_id),))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            if hasattr(d.get("start_at"), "isoformat"):
                d["start_at"] = d["start_at"].isoformat()
            return d


def mark_appointment_missing_in_calendar(*, google_event_id: str) -> None:
    if not (DATABASE_URL and WA_EVENT_STORE_ENABLED and google_event_id):
        return
    query = """
        UPDATE wa_appointments
        SET updated_at = NOW(),
            status = CASE WHEN status IN ('cancelled') THEN status ELSE 'needs_reconcile' END,
            notes_json = wa_appointments.notes_json || %s
        WHERE google_event_id = %s;
    """
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (Jsonb({"reconcile": {"missing_in_calendar": True, "ts": datetime.now(timezone.utc).isoformat()}}), google_event_id))
