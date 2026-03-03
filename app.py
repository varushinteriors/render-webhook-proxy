import json
import os
from collections import deque
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request, Response, HTTPException, Query, Header
from pydantic import BaseModel

app = FastAPI()

VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "")
FORWARD_URL = os.getenv("FORWARD_URL", "https://varush-webhook.onrender.com")
LOG_PATH = Path(os.getenv("LOG_PATH", "logs/webhook-events.log"))
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_LOG_PATH = Path(os.getenv("LEAD_LOG_PATH", "logs/leadgen-events.log"))
LEAD_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_DETAILS_PATH = Path(os.getenv("LEAD_DETAILS_PATH", "logs/leadgen-details.log"))
LEAD_DETAILS_PATH.parent.mkdir(parents=True, exist_ok=True)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN", "")
LEAD_ACCESS_TOKEN = os.getenv("LEAD_ACCESS_TOKEN", "") or WHATSAPP_ACCESS_TOKEN
STATE_PATH = Path(os.getenv("STATE_PATH", "logs/conversations.json"))
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
MEETING_LINK = os.getenv("MEETING_LINK", "https://meet.varushinteriors.com/intro")
IST = ZoneInfo("Asia/Kolkata")

QUESTION_FLOW = [
    "name",
    "service_type",
    "location",
    "project_type",
    "area",
    "timeline",
    "finish",
    "budget",
    "assets",
    "portfolio",
]

QUESTION_PROMPTS = {
    "name": "Hi there! 👋 I’m Kavya from Varush Architect & Interiors. May I have your name?",
    "service_type": "Thanks {name}! What type of service are you looking for—interior design, architectural services, turnkey, or something else?",
    "location": "Got it. Where is the project located? (Delhi, Gurugram, Faridabad, Noida, or another city—please mention.)",
    "project_type": "What type of project is it (e.g., 2/3/4 BHK flat, villa, farmhouse, independent house, office space, etc.)?",
    "area": "Approximately how many square feet is the space?",
    "timeline": "When are you planning to start? (Immediately, within 3 months, within 6 months?)",
    "finish": "What finish level would you like—budget-friendly, premium, or luxury?",
    "budget": "What budget bracket should we plan for? (<10 lacs, 10–20 lacs, 20–30 lacs, >30 lacs, or flexible as per design.)",
    "assets": "Do you have any layouts or site photos you can share here?",
    "portfolio": "Would you like me to send over our latest work portfolio?",
}


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
    await _auto_reply(payload)
    return {"status": "ok"}


@app.get("/leadgen")
async def leadgen_verify(
    mode: str | None = Query(default=None, alias="hub.mode"),
    hub_challenge: str | None = Query(default=None, alias="hub.challenge"),
    hub_verify_token: str | None = Query(default=None, alias="hub.verify_token"),
    plain_mode: str | None = None,
    plain_challenge: str | None = None,
    plain_verify_token: str | None = None,
):
    # Reuse the same verify token logic
    return await verify(mode, hub_challenge, hub_verify_token, plain_mode, plain_challenge, plain_verify_token)


@app.post("/leadgen")
async def handle_leadgen(request: Request):
    payload = await request.json()
    _append_lead_log(payload)
    await _process_leadgen_payload(payload)
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


async def _auto_reply(payload: Dict[str, Any]) -> None:
    entries = payload.get("entry", [])
    for entry in entries:
        for change in entry.get("changes", []):
            value = change.get("value", {})
            messages = value.get("messages")
            if not messages:
                continue
            metadata = value.get("metadata", {})
            business_phone_id = metadata.get("phone_number_id")
            contacts = value.get("contacts", [])
            for message in messages:
                # Skip non-text or messages originating from our own number
                if message.get("type") != "text":
                    continue
                wa_id = message.get("from")
                if not wa_id or wa_id == business_phone_id:
                    continue
                text_body = message.get("text", {}).get("body", "").strip()
                contact_name = _match_contact_name(contacts, wa_id)
                await _handle_conversation_turn(wa_id, contact_name, text_body)


def _match_contact_name(contacts: List[Dict[str, Any]], wa_id: str) -> str | None:
    for contact in contacts:
        if contact.get("wa_id") == wa_id:
            profile = contact.get("profile", {})
            return profile.get("name")
    return None


async def _handle_conversation_turn(wa_id: str, contact_name: str | None, incoming_text: str) -> None:
    state = _load_state()
    convo = state.get(wa_id, {
        "answers": {},
        "current_index": 0,
        "awaiting_field": None,
        "completed": False,
    })

    # Record the contact name if we have it and haven't saved one yet
    if contact_name and "contact_name" not in convo:
        convo["contact_name"] = contact_name

    # Save answer for the question we were waiting on
    awaiting_field = convo.get("awaiting_field")
    if awaiting_field:
        convo.setdefault("answers", {})[awaiting_field] = incoming_text
        if awaiting_field == "name" and not convo.get("contact_name"):
            convo["contact_name"] = incoming_text
        convo["awaiting_field"] = None

    if convo.get("completed"):
        # Send a polite acknowledgement but avoid restarting the flow
        ack = _build_followup_ack(convo)
        await _send_whatsapp_text(wa_id, ack, preview_url=False)
        state[wa_id] = convo
        _save_state(state)
        return

    if convo.get("current_index", 0) >= len(QUESTION_FLOW):
        await _send_meeting_prompt(wa_id, convo)
        convo["completed"] = True
        state[wa_id] = convo
        _save_state(state)
        return

    # Ask the next question
    field = QUESTION_FLOW[convo.get("current_index", 0)]
    prompt = _build_question_prompt(field, convo)
    convo["awaiting_field"] = field
    convo["current_index"] = convo.get("current_index", 0) + 1
    await _send_whatsapp_text(wa_id, prompt, preview_url=False)
    state[wa_id] = convo
    _save_state(state)


def _build_question_prompt(field: str, convo: Dict[str, Any]) -> str:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    template = QUESTION_PROMPTS.get(field, "Could you share more details?")
    return template.format(name=name)


def _build_followup_ack(convo: Dict[str, Any]) -> str:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    return (
        f"Thanks for the update, {name}. I’ve logged your details and will share them with our lead designer. "
        "If you’d like to tweak anything or book another slot, just let me know."
    )


async def _send_meeting_prompt(wa_id: str, convo: Dict[str, Any]) -> None:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    slots = _generate_meeting_slots()
    slot_lines = [f"{i+1}. {slot}" for i, slot in enumerate(slots)]
    slot_text = "\n".join(slot_lines)
    message = (
        f"Thanks, {name}! I have all the essentials. Let’s schedule a complimentary 10-min session with our designer.\n"
        f"Here are the next available slots:\n{slot_text}\n\n"
        f"Reply with the slot number that works best, and I’ll confirm it plus share the meeting link: {MEETING_LINK}"
    )
    await _send_whatsapp_text(wa_id, message, preview_url=True)


def _generate_meeting_slots() -> List[str]:
    now = datetime.now(IST)
    slot_hours = [time(11, 0), time(15, 0), time(19, 0)]
    slots: List[str] = []
    day_offset = 0
    while len(slots) < 3 and day_offset < 5:
        day = (now + timedelta(days=day_offset)).date()
        for slot_time in slot_hours:
            slot_dt = datetime.combine(day, slot_time, tzinfo=IST)
            if slot_dt <= now:
                continue
            slots.append(slot_dt.strftime("%a, %d %b · %I:%M %p IST"))
            if len(slots) >= 3:
                break
        day_offset += 1
    if not slots:
        slots.append("Please suggest a time that suits you.")
    return slots


def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        with STATE_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    tmp_path = STATE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh)
    tmp_path.replace(STATE_PATH)


def _append_log(payload: Dict[str, Any]) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def _append_lead_log(payload: Dict[str, Any]) -> None:
    with LEAD_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def _append_lead_details(data: Dict[str, Any]) -> None:
    with LEAD_DETAILS_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(data) + "\n")


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


async def _process_leadgen_payload(payload: Dict[str, Any]) -> None:
    entries = payload.get("entry", [])
    for entry in entries:
        for change in entry.get("changes", []):
            value = change.get("value", {})
            lead_id = value.get("leadgen_id")
            if not lead_id:
                continue
            details = await _fetch_lead_details(lead_id)
            if details:
                _append_lead_details(details)


async def _fetch_lead_details(lead_id: str) -> Dict[str, Any] | None:
    token = LEAD_ACCESS_TOKEN
    if not token:
        # Nothing to do if we can’t fetch the lead details
        return None
    url = f"https://graph.facebook.com/v20.0/{lead_id}"
    params = {
        "access_token": token,
        "fields": "created_time,ad_id,ad_name,form_id,field_data,platform,leadgen_id,page_id"
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
    if resp.status_code >= 400:
        # Log the failure for visibility
        _append_lead_details({"leadgen_id": lead_id, "error": resp.text})
        return None
    return resp.json()


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
