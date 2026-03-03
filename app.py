import json
import os
from collections import deque
from pathlib import Path
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, Request, Response, HTTPException, Query, Header
from pydantic import BaseModel

app = FastAPI()

VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "")
FORWARD_URL = os.getenv("FORWARD_URL", "https://varush-webhook.onrender.com")
LOG_PATH = Path(os.getenv("LOG_PATH", "logs/webhook-events.log"))
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN", "")


@app.get("/webhook")
async def verify(
    mode: str | None = Query(default=None, alias="hub.mode"),
    hub_challenge: str | None = Query(default=None, alias="hub.challenge"),
    hub_verify_token: str | None = Query(default=None, alias="hub.verify_token"),
    plain_mode: str | None = None,
    plain_challenge: str | None = None,
    plain_verify_token: str | None = None,
):
    mode = mode or plain_mode
    hub_challenge = hub_challenge or plain_challenge
    hub_verify_token = hub_verify_token or plain_verify_token
    if mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return Response(content=hub_challenge or "", media_type="text/plain")
    raise HTTPException(status_code=403, detail="Invalid verify token")


@app.post("/webhook")
async def handle_webhook(request: Request):
    payload = await request.json()
    _append_log(payload)
    await _forward(payload)
    return {"status": "ok"}


class SendMessageRequest(BaseModel):
    to: str
    message: str
    preview_url: bool = False


@app.get("/events/latest")
async def latest_events(
    limit: int = Query(default=20, ge=1, le=200),
    token: str | None = Query(default=None),
    header_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    _require_admin_token(token or header_token)
    events = _read_latest_events(limit)
    return {"count": len(events), "events": events}


@app.post("/admin/send-message")
async def admin_send_message(
    body: SendMessageRequest,
    token: str | None = Query(default=None),
    header_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    _require_admin_token(token or header_token)
    result = await _send_whatsapp_text(body.to, body.message, body.preview_url)
    return result


def _append_log(payload: Dict[str, Any]) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def _read_latest_events(limit: int) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    if not LOG_PATH.exists():
        return events
    lines = deque(maxlen=limit)
    with LOG_PATH.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                lines.append(line)
    for line in lines:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def _require_admin_token(provided: str | None) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="Admin token not configured")
    if not provided:
        raise HTTPException(status_code=401, detail="Missing admin token")
    if provided != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token")


async def _forward(payload: Dict[str, Any]) -> None:
    if not FORWARD_URL:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.post(FORWARD_URL, json=payload)
            resp.raise_for_status()
        except httpx.HTTPError:
            pass


async def _send_whatsapp_text(to: str, message: str, preview_url: bool) -> Dict[str, Any]:
    if not WHATSAPP_PHONE_ID or not WHATSAPP_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="WhatsApp credentials not configured")
    url = f"https://graph.facebook.com/v20.0/{WHATSAPP_PHONE_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": preview_url, "body": message},
    }
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=payload, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()
