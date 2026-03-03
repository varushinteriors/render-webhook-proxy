import json
import os
from pathlib import Path
from typing import Any, Dict

import httpx
from fastapi import FastAPI, Request, Response, HTTPException, Query

app = FastAPI()

VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "")
FORWARD_URL = os.getenv("FORWARD_URL", "https://varush-webhook.onrender.com")
LOG_PATH = Path(os.getenv("LOG_PATH", "logs/webhook-events.log"))
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


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


def _append_log(payload: Dict[str, Any]) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


async def _forward(payload: Dict[str, Any]) -> None:
    if not FORWARD_URL:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.post(FORWARD_URL, json=payload)
            resp.raise_for_status()
        except httpx.HTTPError:
            pass
