#!/usr/bin/env python3
"""Lead reminders runner.

Reads webhook JSON state and sends WhatsApp template reminders at +6h and +24h
if the lead has not replied.

Designed to be run periodically (e.g., via OpenClaw cron every 5 minutes).
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

STATE_PATH = Path(os.getenv("LEAD_STATE_PATH", "logs/conversations.json"))
LANG_CODE = os.getenv("LEAD_TEMPLATE_LANG", "en_US")
PHONE_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID") or os.getenv("WHATSAPP_PHONE_ID")
ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
GRAPH_API_BASE = os.getenv("GRAPH_API_BASE", "https://graph.facebook.com/v20.0")

TEMPLATE_REMINDER_6H = os.getenv("LEAD_TEMPLATE_REMINDER_6H", "first_reminder_lead_message")
TEMPLATE_REMINDER_24H = os.getenv("LEAD_TEMPLATE_REMINDER_24H", "second_reminder_lead_message")

SIX_HOURS = 6 * 3600
TWENTY_FOUR_HOURS = 24 * 3600


def _now_ts() -> float:
    return time.time()


def _parse_iso_to_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _best_name(convo: Dict[str, Any]) -> str:
    canonical = convo.get("lead_canonical") or {}
    name = (
        convo.get("contact_name")
        or canonical.get("full_name")
        or canonical.get("client_name")
        or canonical.get("name")
    )
    name = (name or "").strip()
    return name or "there"


def _http_post_json(url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), body
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def send_template(to_phone: str, template_name: str, client_name: str) -> bool:
    if not (PHONE_ID and ACCESS_TOKEN):
        print("[REMINDERS][ERROR] Missing WHATSAPP_PHONE_NUMBER_ID/WHATSAPP_PHONE_ID or WHATSAPP_ACCESS_TOKEN")
        return False

    url = f"{GRAPH_API_BASE}/{PHONE_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": LANG_CODE},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": client_name}],
                }
            ],
        },
    }
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    code, body = _http_post_json(url, payload, headers)
    ok = 200 <= code < 300
    if ok:
        print(f"[REMINDERS] Template sent: {template_name} -> {to_phone} ({client_name})")
    else:
        print(f"[REMINDERS][ERROR] Template failed: {template_name} -> {to_phone} code={code} body={body}")
    return ok


def main() -> int:
    if not STATE_PATH.exists():
        print(f"[REMINDERS] No state file at {STATE_PATH} (nothing to do)")
        return 0

    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8") or "{}")
    except Exception as exc:  # noqa: BLE001
        print(f"[REMINDERS][ERROR] Failed to read state: {exc}")
        return 2

    now = _now_ts()
    updated = False
    scanned = 0
    sent = 0

    for phone, convo in list(state.items()):
        if not isinstance(convo, dict):
            continue
        scanned += 1

        lead_ts = _parse_iso_to_ts(convo.get("lead_context_received_at"))
        if not lead_ts:
            continue

        last_client_ts = convo.get("last_client_ts")
        if isinstance(last_client_ts, (int, float)) and last_client_ts > lead_ts:
            continue

        name = _best_name(convo)
        r6_sent = _parse_iso_to_ts(convo.get("reminder_6h_sent_at"))
        r24_sent = _parse_iso_to_ts(convo.get("reminder_24h_sent_at"))

        if not r6_sent and (now - lead_ts) >= SIX_HOURS:
            if send_template(phone, TEMPLATE_REMINDER_6H, name):
                convo["reminder_6h_sent_at"] = datetime.now(timezone.utc).isoformat()
                updated = True
                sent += 1

        if not r24_sent and (now - lead_ts) >= TWENTY_FOUR_HOURS:
            if send_template(phone, TEMPLATE_REMINDER_24H, name):
                convo["reminder_24h_sent_at"] = datetime.now(timezone.utc).isoformat()
                updated = True
                sent += 1

        state[phone] = convo

    if updated:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        tmp.replace(STATE_PATH)

    print(f"[REMINDERS] scanned={scanned} sent={sent} updated={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
