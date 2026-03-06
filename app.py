import asyncio
import json
import mimetypes
import os
from collections import deque
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import httpx
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, Request, Response, HTTPException, Query, Header
from pydantic import BaseModel

from agents.lead_scoring import LeadInput, LeadScoringAgent
from services.conversation_agent import ConversationAgent, ConversationAgentResult

app = FastAPI()


@app.on_event("startup")
async def startup_event() -> None:
    asyncio.create_task(_inactivity_watcher())
    asyncio.create_task(_meeting_reminder_watcher())
    print("VARUSH WEBHOOK SERVER STARTED")
    print("Lead webhook ready")
    print("WhatsApp webhook ready")

VERIFY_TOKEN_VALUES = {
    token.strip()
    for token in os.getenv("META_VERIFY_TOKEN", "").split(",")
    if token.strip()
}
PAGE_VERIFY_TOKEN = os.getenv("PAGE_VERIFY_TOKEN", "").strip()
if PAGE_VERIFY_TOKEN:
    VERIFY_TOKEN_VALUES.add(PAGE_VERIFY_TOKEN)
FORWARD_URL = os.getenv("FORWARD_URL", "https://varush-webhook.onrender.com")
LOG_PATH = Path(os.getenv("LOG_PATH", "logs/webhook-events.log"))
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_LOG_PATH = Path(os.getenv("LEAD_LOG_PATH", "logs/leadgen-events.log"))
LEAD_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_DETAILS_PATH = Path(os.getenv("LEAD_DETAILS_PATH", "logs/leadgen-details.log"))
LEAD_DETAILS_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_INDEX_PATH = Path(os.getenv("LEAD_INDEX_PATH", "logs/lead-index.json"))
LEAD_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_SCORE_PATH = Path(os.getenv("LEAD_SCORE_PATH", "logs/lead-scores.json"))
LEAD_SCORE_PATH.parent.mkdir(parents=True, exist_ok=True)
MEETINGS_PATH = Path(os.getenv("MEETINGS_PATH", "logs/meetings.json"))
MEETINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
LEAD_ENGAGEMENT_PATH = Path(os.getenv("LEAD_ENGAGEMENT_PATH", "logs/lead-engagement.json"))
LEAD_ENGAGEMENT_PATH.parent.mkdir(parents=True, exist_ok=True)
MEDIA_ARCHIVE_PATH = Path(os.getenv("MEDIA_ARCHIVE_PATH", "logs/media"))
MEDIA_ARCHIVE_PATH.mkdir(parents=True, exist_ok=True)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN", "")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
LEAD_ACCESS_TOKEN = PAGE_ACCESS_TOKEN
if not LEAD_ACCESS_TOKEN:
    print("ERROR: PAGE_ACCESS_TOKEN missing – leadgen fetch will fail")
ADMIN_ALERT_NUMBERS = [n.strip() for n in os.getenv("ADMIN_ALERT_NUMBERS", "").split(",") if n.strip()]
PORTFOLIO_LINK = os.getenv(
    "DRIVE_PORTFOLIO_LINK",
    "https://drive.google.com/drive/folders/1WBJf_7zCLb5XxpbryxCSoUiKc13DzerC",
)
GRAPH_API_BASE = "https://graph.facebook.com/v20.0"
STATE_PATH = Path(os.getenv("STATE_PATH", "logs/conversations.json"))
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
MEETING_LINK = os.getenv("MEETING_LINK", "https://meet.varushinteriors.com/intro")
IST = ZoneInfo("Asia/Kolkata")

scoring_agent = LeadScoringAgent()
conversation_agent = ConversationAgent()
credentials_info = None
credentials_json = os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON")
if credentials_json:
    try:
        credentials_info = json.loads(credentials_json)
    except json.JSONDecodeError:
        credentials_info = None

R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_PREFIX = os.getenv("R2_PREFIX", "clients").strip("/")
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").strip() or None

STATE_R2_KEY = os.getenv("STATE_R2_KEY") or (f"{R2_PREFIX}/state/conversations.json" if R2_PREFIX else "state/conversations.json")
LEAD_INDEX_R2_KEY = os.getenv("LEAD_INDEX_R2_KEY") or (f"{R2_PREFIX}/state/lead-index.json" if R2_PREFIX else "state/lead-index.json")
LEAD_SCORE_R2_KEY = os.getenv("LEAD_SCORE_R2_KEY") or (f"{R2_PREFIX}/state/lead-scores.json" if R2_PREFIX else "state/lead-scores.json")

r2_client = None
if all([R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET, R2_ENDPOINT]):
    r2_client = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )
else:
    print("R2 client not fully configured; media will only be archived locally.")

def _r2_ready() -> bool:
    return r2_client is not None

def _r2_prefix_for_key(key: str) -> str:
    return f"{R2_PREFIX}/{key}"

def _r2_prefix_for_wa(wa_id: str | None) -> str:
    key = _phone_key_from_wa(wa_id or "") or (wa_id or "unknown")
    return _r2_prefix_for_key(key)

def _r2_public_url(object_key: str) -> str | None:
    if not R2_PUBLIC_BASE_URL:
        return None
    base = R2_PUBLIC_BASE_URL.rstrip("/")
    return f"{base}/{object_key}"

def _upload_to_r2(wa_id: str, filename: str, mime_type: str, data: bytes) -> Dict[str, str] | None:
    if not r2_client:
        return None
    object_key = f"{_r2_prefix_for_wa(wa_id)}/{filename}"
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET,
            Key=object_key,
            Body=data,
            ContentType=mime_type,
        )
        public_url = _r2_public_url(object_key)
        record = {"bucket": R2_BUCKET, "key": object_key}
        if public_url:
            record["url"] = public_url
        print(f"R2 UPLOAD SUCCESS: {record}")
        return record
    except (BotoCoreError, ClientError) as exc:
        print(f"R2 UPLOAD ERROR: {exc}")
        return None

def _r2_download_to_path(key: str | None, path: Path) -> bool:
    if not (r2_client and key):
        return False
    tmp_path = path.with_suffix('.download')
    try:
        r2_client.download_file(R2_BUCKET, key, str(tmp_path))
        tmp_path.replace(path)
        return True
    except ClientError as exc:
        error_code = exc.response.get('Error', {}).get('Code') if hasattr(exc, 'response') else None
        if error_code not in {'NoSuchKey', '404'}:
            print(f"R2 DOWNLOAD ERROR ({key}): {exc}")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except (BotoCoreError, OSError) as exc:
        print(f"R2 DOWNLOAD ERROR ({key}): {exc}")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False


def _r2_upload_path(path: Path, key: str | None) -> None:
    if not (r2_client and key):
        return
    if not path.exists():
        return
    try:
        r2_client.upload_file(str(path), R2_BUCKET, key, ExtraArgs={"ContentType": "application/json"})
    except (BotoCoreError, ClientError) as exc:
        print(f"R2 STATE UPLOAD ERROR ({key}): {exc}")


QUESTION_FLOW = [
    "name",
    "service_type",
    "location",
    "project_type",
    "area",
    "budget",
    "timeline",
    "finish",
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
    "portfolio": f"Would you like to see our latest work portfolio? Here’s a quick look: {PORTFOLIO_LINK}",
}

CHATTER_RESPONSES = {
    "hi",
    "hello",
    "hey",
    "hii",
    "hiii",
    "hlo",
    "hola",
    "ok",
    "okay",
    "k",
    "kk",
    "h",
    "hmm",
    "hmmm",
    "yo",
    "sup",
}

OBJECTION_PHRASES = [
    "why so many",
    "are you mad",
    "you are just a bot",
    "you are just a",
    "without listening",
    "without application",
    "what's the point",
    "whats the point",
    "pls understand",
    "please understand",
    "i don't want",
    "i dont want",
    "where were we",
    "you keep on",
    "stop asking",
    "not interested",
    "i am asking you",
    "are you not",
    "intelligent enough",
]

AREA_UNKNOWN_PHRASES = [
    "don't know",
    "dont know",
    "not sure",
    "no idea",
    "tbd",
    "na",
]

SERVICE_TYPE_KEYWORDS = [
    "interior",
    "architect",
    "turnkey",
    "design",
    "designer",
    "renovation",
    "execution",
    "furniture",
]

FINISH_KEYWORDS = [
    "budget",
    "premium",
    "luxury",
    "mid",
    "classic",
    "bespoke",
    "custom",
]

TIMELINE_KEYWORDS = [
    "immed",
    "month",
    "week",
    "day",
    "soon",
    "later",
    "within",
    "after",
    "asap",
    "next",
    "now",
    "quarter",
]

BUDGET_FLEXIBLE_RESPONSES = {
    "flexible",
    "no idea",
    "not sure",
    "tbd",
    "depends",
    "depend",
    "open",
    "as per design",
}

YES_NO_RESPONSES = {
    "yes",
    "y",
    "yeah",
    "yup",
    "sure",
    "of course",
    "no",
    "n",
    "nah",
    "nope",
    "not yet",
}

INTENTS_THAT_SKIP_FORCED_PROMPT = {"smalltalk", "objection", "confusion"}
GENTLE_RECOVERY_FIELDS = ["location", "project_type", "area"]


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def _looks_like_objection(value: str) -> bool:
    text = value.lower()
    if text in CHATTER_RESPONSES:
        return True
    if any(phrase in text for phrase in OBJECTION_PHRASES):
        return True
    if text.count("?") >= 2:
        return True
    return False


def _is_valid_field_value(field: str, value: str | None) -> bool:
    text = _normalize_text(value)
    if not text:
        return False
    lower = text.lower()
    if lower in CHATTER_RESPONSES:
        return False
    if _looks_like_objection(lower):
        return False

    if field == "area":
        if any(keyword in lower for keyword in AREA_UNKNOWN_PHRASES):
            return True
        return any(ch.isdigit() for ch in lower)
    if field == "timeline":
        return any(keyword in lower for keyword in TIMELINE_KEYWORDS)
    if field == "finish":
        return any(keyword in lower for keyword in FINISH_KEYWORDS)
    if field == "budget":
        if any(keyword in lower for keyword in BUDGET_FLEXIBLE_RESPONSES):
            return True
        return any(ch.isdigit() for ch in lower)
    if field == "service_type":
        return any(keyword in lower for keyword in SERVICE_TYPE_KEYWORDS)
    if field in {"assets", "portfolio"}:
        if lower in YES_NO_RESPONSES:
            return True
        return not _looks_like_objection(lower)
    if field == "project_type":
        if any(keyword in lower for keyword in ["bhk", "villa", "office", "farm", "floor", "flat", "house", "bungalow"]):
            return True
        return len(lower) >= 3 and not _looks_like_objection(lower)
    if field == "location":
        return len(lower) >= 3
    return len(lower) >= 2


def _sanitize_answers(convo: Dict[str, Any]) -> None:
    answers = convo.get("answers") or {}
    to_remove = [field for field, value in answers.items() if not _is_valid_field_value(field, value)]
    for field in to_remove:
        answers.pop(field, None)


PHASE_DISCOVERY = "discovery"
PHASE_QUALIFICATION = "qualification"
PHASE_VALUE_BUILD = "value_build"
PHASE_BOOKING = "booking"
PHASE_POST_BOOKING = "post_booking"

MAX_HISTORY_LENGTH = 40
ESCALATION_INTENTS = {"pricing_query"}
ESCALATION_THRESHOLD = 2
CONFIDENCE_MIN_SCORE = 0.6
RECAP_COOLDOWN_SECONDS = 3600

INACTIVITY_INITIAL_DELAY = 180  # seconds (3 minutes)
INACTIVITY_INTERVAL = 60  # seconds between nudges
INACTIVITY_MESSAGES = [
    "Just checking in 😊 If you have a minute now, I can jot down the next detail and keep things moving for your space.",
    "I don’t want you to lose the momentum—you’ll be amazed how quickly we can map ideas once we have these basics. Shall we pick up where we left off?",
    "Still here whenever you need me! Even one quick line helps us tailor the perfect plan for your home.",
]
INACTIVITY_SOFT_CLOSE = (
    "All good—I’ll pause for now. Drop me a message whenever you’re free and we’ll resume right away. Your dream space isn’t going anywhere. ✨"
)

MEETING_REMINDER_WINDOWS = [
    (7200, "two_hours"),
    (3600, "one_hour"),
    (600, "ten_minutes"),
]
MEETING_REMINDER_MESSAGES = {
    "two_hours": "Hi {name}! We’re just 2 hours away from reimagining your space together. Get ready for a design huddle that’ll spark new ideas and show how intricate (and fun) the process can be.",
    "one_hour": "One hour to go! This session is where we unpack the latest finishes, trend insights, and the smart moves that set premium homes apart. Expect your mindset to shift in the best way.",
    "ten_minutes": "Final countdown—10 minutes! Keep your excitement up because we’re about to dive into the details that turn good spaces into unforgettable ones. Join via {link} and let’s create something special.",
}

CANONICAL_LEAD_FIELDS = [
    "full_name",
    "phone",
    "email",
    "service_type",
    "project_location",
    "project_type",
    "area_sqft",
    "timeline",
    "finish_level",
    "budget_bracket",
    "other_notes",
]

LEAD_FIELD_MAP = {
    "full_name": "full_name",
    "full name": "full_name",
    "email": "email",
    "phone": "phone",
    "what_is_your_property_type?": "project_type",
    "what is your property type?": "project_type",
    "what_is_your_budget_for_interior_project?": "budget_bracket",
    "what is your budget for interior project?": "budget_bracket",
    "how_soon_are_you_planning_to_get_started?": "timeline",
    "how soon are you planning to get started?": "timeline",
    "where_is_your_property_located?": "project_location",
    "where is your property located?": "project_location",
}

CANONICAL_TO_STATE_FIELD = {
    "full_name": "name",
    "service_type": "service_type",
    "project_location": "location",
    "project_type": "project_type",
    "area_sqft": "area",
    "timeline": "timeline",
    "finish_level": "finish",
    "budget_bracket": "budget",
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
    if mode == "subscribe" and _is_valid_verify_token(hub_verify_token):
        return Response(content=hub_challenge or "", media_type="text/plain")
    raise HTTPException(status_code=403, detail="Invalid verify token")


@app.post("/webhook")
async def handle_webhook(request: Request):
    payload = await request.json()
    if payload.get("object") == "page":
        await _handle_leadgen_payload(payload)
        return {"status": "ok"}
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
    await _handle_leadgen_payload(payload)
    return {"status": "ok"}


class SendMessageRequest(BaseModel):
    to: str
    message: str
    preview_url: bool = False


class ScheduleMeetingRequest(BaseModel):
    wa_id: str
    scheduled_at: str  # ISO 8601 string
    note: str | None = None


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


@app.post("/admin/schedule-meeting")
async def admin_schedule_meeting(
    body: ScheduleMeetingRequest,
    token: str | None = Query(default=None),
    header_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    _require_admin_token(token or header_token)
    meeting = _register_meeting(body)
    return meeting

@app.get("/status")
async def service_status():
    return {
        "agent_ready": conversation_agent.is_ready,
        "openai_key_loaded": bool(os.getenv("OPENAI_API_KEY")),
        "intent_router": sorted(INTENT_ROUTER.keys()),
    }


async def _auto_reply(payload: Dict[str, Any]) -> None:
    entries = payload.get("entry", [])
    for entry in entries:
        for change in entry.get("changes", []):
            if change.get("field") != "messages":
                continue
            value = change.get("value", {})
            messages = value.get("messages")
            if not messages:
                continue
            metadata = value.get("metadata", {})
            business_phone_id = metadata.get("phone_number_id")
            contacts = value.get("contacts", [])
            for message in messages:
                wa_id = message.get("from")
                if not wa_id or wa_id == business_phone_id:
                    continue
                msg_type = message.get("type")
                if msg_type == "text":
                    text_body = message.get("text", {}).get("body", "").strip()
                    contact_name = _match_contact_name(contacts, wa_id)
                    await _handle_conversation_turn(wa_id, contact_name, text_body)
                else:
                    await _handle_media_message(wa_id, msg_type, message, contacts)


def _match_contact_name(contacts: List[Dict[str, Any]], wa_id: str) -> str | None:
    for contact in contacts:
        if contact.get("wa_id") == wa_id:
            profile = contact.get("profile", {})
            return profile.get("name")
    return None


async def _handle_media_message(
    wa_id: str,
    msg_type: str,
    message: Dict[str, Any],
    contacts: List[Dict[str, Any]],
) -> None:
    media_info = message.get(msg_type, {}) or {}
    media_id = media_info.get("id")
    print(f"MEDIA TYPE: {msg_type}")
    print(f"MEDIA ID: {media_id}")
    if not media_id:
        return
    download = await _download_whatsapp_media(media_id, media_info)
    print(f"DOWNLOAD RESULT: {bool(download)}")
    print(f"R2 READY: {_r2_ready()}")
    if not download:
        return
    data, mime_type, filename = download
    archive_path = _archive_media_locally(wa_id, filename, data)
    storage_record = _upload_to_r2(wa_id, filename, mime_type, data)
    print(f"R2 UPLOAD RESULT: {storage_record}")

    if storage_record:
        ack = (
            "Saved your file to your secure Varush project vault. "
            "Feel free to keep sharing anything else that helps us design."
        )
    elif archive_path:
        ack = (
            "Got it and stored it safely on our end. I’ll sync it to your vault as soon as connectivity clears."
        )
    else:
        ack = (
            "Received your file—thank you! I’ll log it and keep things moving while our sync completes."
        )
    await _send_whatsapp_text(wa_id, ack, preview_url=False)


async def _handle_conversation_turn(wa_id: str, contact_name: str | None, incoming_text: str) -> None:
    state = _load_state()
    convo = state.get(wa_id, {
        "answers": {},
        "awaiting_field": None,
        "awaiting_origin": None,
        "completed": False,
        "has_welcomed": False,
        "history": [],
        "status": "active",
        "escalations": {},
        "has_recap_prompted": False,
        "awaiting_recap_choice": False,
        "last_recap_ts": 0.0,
        "phase": PHASE_DISCOVERY,
    })
    convo.setdefault("answers", {})
    convo.setdefault("history", [])
    convo.setdefault("escalations", {})
    convo.setdefault("status", "active")
    convo.setdefault("has_recap_prompted", False)
    convo.setdefault("awaiting_recap_choice", False)
    convo.setdefault("last_recap_ts", 0.0)
    convo.setdefault("phase", PHASE_DISCOVERY)
    convo.setdefault("awaiting_origin", None)

    _sanitize_answers(convo)

    previous_last_ts = convo.get("last_client_ts")
    now_ts = datetime.now(timezone.utc).timestamp()
    convo["last_client_ts"] = now_ts
    convo["inactivity_reminders_sent"] = 0
    convo["next_inactivity_ts"] = None
    convo["inactivity_paused"] = False
    convo["inactivity_soft_closed"] = False

    if contact_name and not convo.get("contact_name"):
        convo["contact_name"] = contact_name

    if convo.get("status") == "handoff":
        ack = (
            "Our lead designer is already reviewing your request. They’ll jump in shortly with the next update."
        )
        await _send_whatsapp_text(wa_id, ack, preview_url=False)
        _append_history(convo, "bot", ack)
        state[wa_id] = convo
        _save_state(state)
        return

    awaiting_field = convo.get("awaiting_field")
    awaiting_origin = convo.get("awaiting_origin")
    should_force_capture = (
        bool(awaiting_field)
        and bool(incoming_text)
        and (awaiting_origin == "legacy" or (awaiting_origin is None and not conversation_agent.is_ready))
    )
    if should_force_capture:
        cleaned_value = _normalize_text(incoming_text)
        captured = False
        if _is_valid_field_value(awaiting_field, cleaned_value):
            convo["answers"][awaiting_field] = cleaned_value
            captured = True
            if awaiting_field == "name" and not convo.get("contact_name"):
                convo["contact_name"] = cleaned_value
        convo["awaiting_field"] = None
        convo["awaiting_origin"] = None
        if not captured:
            answers = convo.setdefault("answers", {})
            answers.pop(awaiting_field, None)

    _hydrate_state_from_lead(wa_id, convo)

    if incoming_text:
        _append_history(convo, "client", incoming_text)

    if convo.get("awaiting_recap_choice"):
        handled = await _process_recap_choice(wa_id, convo, incoming_text)
        if handled:
            state[wa_id] = convo
            _save_state(state)
            return

    if _should_prompt_recap(convo, now_ts):
        await _send_recap_prompt(wa_id, convo, now_ts)
        state[wa_id] = convo
        _save_state(state)
        return

    if convo.get("completed"):
        ack = _build_followup_ack(convo)
        await _send_whatsapp_text(wa_id, ack, preview_url=False)
        _append_history(convo, "bot", ack)
        state[wa_id] = convo
        _save_state(state)
        _score_lead_from_conversation(wa_id, convo)
        return

    if not conversation_agent.is_ready:
        await _run_legacy_flow(wa_id, convo, state)
        return

    agent_result = await conversation_agent.generate_response(
        answers=convo.get("answers", {}),
        missing_fields=_missing_fields(convo),
        awaiting_field=convo.get("awaiting_field"),
        history=convo.get("history", []),
        message=incoming_text,
        contact_name=convo.get("contact_name"),
        portfolio_link=PORTFOLIO_LINK,
        phase=convo.get("phase"),
    )

    if not agent_result:
        await _run_legacy_flow(wa_id, convo, state)
        return

    handler = INTENT_ROUTER.get(agent_result.intent)
    if handler:
        handled = await handler(wa_id, convo)
        if handled:
            state[wa_id] = convo
            _save_state(state)
            _score_lead_from_conversation(wa_id, convo)
            return

    await _run_agent_flow(wa_id, convo, state, agent_result)


async def _run_legacy_flow(wa_id: str, convo: Dict[str, Any], state: Dict[str, Any]) -> None:
    next_field = _next_missing_field(convo)
    if not next_field:
        if not convo.get("has_welcomed"):
            welcome = _build_welcome_message(convo)
            if welcome:
                await _send_whatsapp_text(wa_id, welcome, preview_url=False)
                _append_history(convo, "bot", welcome)
            convo["has_welcomed"] = True
        await _send_meeting_prompt(wa_id, convo)
        convo["completed"] = True
        convo["status"] = "completed"
        convo["awaiting_field"] = None
        convo["awaiting_origin"] = None
    else:
        prompt = _build_question_prompt(next_field, convo)
        convo["awaiting_field"] = next_field
        convo["awaiting_origin"] = "legacy"
        if not convo.get("has_welcomed"):
            welcome = _build_welcome_message(convo)
            message = f"{welcome}\n\n{prompt}" if welcome else prompt
            convo["has_welcomed"] = True
        else:
            message = prompt
        await _send_whatsapp_text(wa_id, message, preview_url=False)
        _append_history(convo, "bot", message)
    state[wa_id] = convo
    _save_state(state)
    _score_lead_from_conversation(wa_id, convo)


async def _run_agent_flow(wa_id: str, convo: Dict[str, Any], state: Dict[str, Any], result: ConversationAgentResult) -> None:
    answers = convo.setdefault("answers", {})
    allowed_fields = set(QUESTION_FLOW)
    intent = (result.intent or "unknown").lower()
    confidence = result.confidence if result.confidence is not None else 1.0
    requires_clarification = (confidence < CONFIDENCE_MIN_SCORE and intent not in {"smalltalk"}) or intent == "unknown"

    if intent not in {"smalltalk", "objection", "confusion"} and not requires_clarification:
        for field, value in (result.fields_detected or {}).items():
            if field not in allowed_fields or value is None:
                continue
            cleaned = value.strip() if isinstance(value, str) else str(value).strip()
            if not cleaned:
                continue
            if not _is_valid_field_value(field, cleaned):
                continue
            answers[field] = cleaned
            if field == "name" and not convo.get("contact_name"):
                convo["contact_name"] = cleaned

    missing_fields = _missing_fields(convo)
    next_field = None
    if result.next_field and result.next_field in missing_fields:
        next_field = result.next_field

    needs_handoff = bool(result.needs_human)
    handoff_reason = result.handoff_reason
    skip_forced_follow = intent in INTENTS_THAT_SKIP_FORCED_PROMPT
    if skip_forced_follow:
        next_field = None

    if intent in ESCALATION_INTENTS and not requires_clarification:
        escalations = convo.setdefault("escalations", {})
        count = escalations.get(intent, 0) + 1
        escalations[intent] = count
        if count >= ESCALATION_THRESHOLD and not needs_handoff:
            needs_handoff = True
            handoff_reason = handoff_reason or f"{intent}_insistent"
    else:
        convo.setdefault("escalations", {}).pop(intent, None)

    if needs_handoff:
        convo["status"] = "handoff"
        convo["inactivity_paused"] = True
        convo["awaiting_field"] = None
        convo["awaiting_origin"] = None
        convo["handoff_reason"] = handoff_reason or intent
        state[wa_id] = convo
        _save_state(state)
        await _announce_handoff(wa_id, convo, convo.get("handoff_reason"))
        _score_lead_from_conversation(wa_id, convo)
        return

    reply_parts: List[str] = []
    follow_field = None
    reply_text = result.reply.strip() if result.reply else ""
    if intent == "ask_portfolio" and PORTFOLIO_LINK not in reply_text:
        portfolio_line = f"Here’s our latest work portfolio: {PORTFOLIO_LINK}"
        reply_text = f"{reply_text}\n\n{portfolio_line}".strip() if reply_text else portfolio_line
    if reply_text:
        reply_parts.append(reply_text)

    follow_up_prompt = result.follow_up_prompt.strip() if result.follow_up_prompt else None
    if follow_up_prompt and not skip_forced_follow:
        reply_parts.append(follow_up_prompt)
        if next_field:
            follow_field = next_field

    message = "\n\n".join(part for part in reply_parts if part)

    if message:
        await _send_whatsapp_text(wa_id, message, preview_url=False)
        _append_history(convo, "bot", message)
        convo["has_welcomed"] = True
    convo["awaiting_field"] = follow_field
    convo["awaiting_origin"] = "agent" if follow_field else None

    should_offer_meeting = (result.request_meeting and not requires_clarification) or (not _missing_fields(convo) and not requires_clarification)
    if should_offer_meeting and not convo.get("completed"):
        await _send_meeting_prompt(wa_id, convo)
        convo["completed"] = True
        convo["status"] = "completed"
    _update_convo_phase(convo)
    state[wa_id] = convo
    _save_state(state)
    _score_lead_from_conversation(wa_id, convo)


async def _send_recap_prompt(wa_id: str, convo: Dict[str, Any], now_ts: float) -> None:
    summary = _build_recap_summary(convo)
    recap_text = (
        f"Welcome back! {summary} Would you like to start a new project, make edits to the existing details, or continue from where we left off? "
        "Just reply with 'new', 'edit', or 'continue'."
    )
    await _send_whatsapp_text(wa_id, recap_text, preview_url=False)
    _append_history(convo, "bot", recap_text)
    convo["awaiting_recap_choice"] = True
    convo["has_recap_prompted"] = True
    convo["last_recap_ts"] = now_ts


async def _process_recap_choice(wa_id: str, convo: Dict[str, Any], incoming_text: str) -> bool:
    if not convo.get("awaiting_recap_choice"):
        return False
    choice = _classify_recap_choice(incoming_text or "")
    if not choice:
        prompt = "Just let me know if you'd like to start a new project, edit the existing plan, or continue where we paused."
        await _send_whatsapp_text(wa_id, prompt, preview_url=False)
        _append_history(convo, "bot", prompt)
        return True
    now = datetime.now(timezone.utc)
    convo["awaiting_recap_choice"] = False
    convo["has_recap_prompted"] = True
    convo["last_recap_ts"] = now.timestamp()

    if choice == "new":
        prev_answers = convo.get("answers", {})
        if prev_answers:
            convo.setdefault("previous_intakes", []).append(
                {"answers": prev_answers, "archived_at": now.isoformat()}
            )
        convo["answers"] = {}
        convo["completed"] = False
        convo["status"] = "active"
        convo["has_welcomed"] = False
        convo["awaiting_field"] = None
        convo["awaiting_origin"] = None
        next_field = _next_missing_field(convo) or "name"
        prompt = _build_question_prompt(next_field, convo)
        message = f"Fresh canvas — let’s capture this new project!\n\n{prompt}"
        convo["awaiting_field"] = next_field
        convo["awaiting_origin"] = "legacy"
        await _send_whatsapp_text(wa_id, message, preview_url=False)
        _append_history(convo, "bot", message)
        return True

    if choice == "edit":
        message = (
            "Perfect—tell me what detail you’d like to tweak and I’ll update it. Feel free to say something like ‘Change budget to 30L’."
        )
        await _send_whatsapp_text(wa_id, message, preview_url=False)
        _append_history(convo, "bot", message)
        return True

    next_field = _next_missing_field(convo)
    if next_field:
        prompt = _build_question_prompt(next_field, convo)
        message = f"Great, let’s continue from where we paused.\n\n{prompt}"
        convo["awaiting_field"] = next_field
        convo["awaiting_origin"] = "legacy"
        await _send_whatsapp_text(wa_id, message, preview_url=False)
        _append_history(convo, "bot", message)
        return True
    message = "We already have every detail saved. Let me resend the latest meeting slots."
    await _send_whatsapp_text(wa_id, message, preview_url=False)
    _append_history(convo, "bot", message)
    await _send_meeting_prompt(wa_id, convo)
    convo["status"] = "completed"
    convo["completed"] = True
    return True


async def _announce_handoff(wa_id: str, convo: Dict[str, Any], reason: str | None) -> None:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    client_msg = (
        f"{name}, I want a designer to weigh in directly so you get a precise answer. I’m looping them in now—expect a human follow-up shortly."
    )
    await _send_whatsapp_text(wa_id, client_msg, preview_url=False)
    _append_history(convo, "bot", client_msg)
    if not ADMIN_ALERT_NUMBERS:
        return
    summary = (
        f"Human takeover requested for {wa_id}. Reason: {reason or 'unspecified'}. Current answers: {json.dumps(convo.get('answers', {}), ensure_ascii=False)}"
    )
    for admin in ADMIN_ALERT_NUMBERS:
        try:
            await _send_whatsapp_text(admin, summary, preview_url=False)
        except HTTPException as exc:
            print(f"ADMIN ALERT SEND ERROR ({admin}): {exc.detail}")


def _should_prompt_recap(convo: Dict[str, Any], now_ts: float) -> bool:
    if not convo.get("history"):
        return False
    if convo.get("awaiting_recap_choice"):
        return False
    if convo.get("status") == "handoff":
        return False
    if not convo.get("has_recap_prompted"):
        return True
    last_recap = convo.get("last_recap_ts") or 0
    return (now_ts - last_recap) >= RECAP_COOLDOWN_SECONDS


def _build_recap_summary(convo: Dict[str, Any]) -> str:
    answers = convo.get("answers", {})
    snippets: List[str] = []
    if answers.get("service_type"):
        snippets.append(f"you were exploring {answers['service_type'].lower()} support")
    if answers.get("project_type"):
        snippets.append(f"for a {answers['project_type']}")
    if answers.get("location"):
        snippets.append(f"in {answers['location']}")
    if answers.get("area"):
        snippets.append(f"around {answers['area']}")
    if answers.get("budget"):
        snippets.append(f"with a budget of {answers['budget']}")
    if answers.get("timeline"):
        snippets.append(f"targeting {answers['timeline']}")
    if answers.get("finish"):
        snippets.append(f"preferring a {answers['finish']} finish")
    if snippets:
        return "We last noted " + ", ".join(snippets) + "."
    return "We still have your earlier project details on file."


def _classify_recap_choice(text: str) -> str | None:
    normalized = text.lower().strip()
    if not normalized:
        return None
    if "new" in normalized:
        return "new"
    if any(keyword in normalized for keyword in ["edit", "change", "tweak", "update"]):
        return "edit"
    if any(keyword in normalized for keyword in ["continue", "resume", "same", "carry on"]):
        return "continue"
    return None


def _build_question_prompt(field: str, convo: Dict[str, Any]) -> str:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    template = QUESTION_PROMPTS.get(field, "Could you share more details?")
    return template.format(name=name)


def _convo_display_name(convo: Dict[str, Any]) -> str:
    return convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"


def _build_gentle_project_prompt(convo: Dict[str, Any]) -> tuple[str | None, str | None]:
    answers = convo.get("answers", {})
    for field in GENTLE_RECOVERY_FIELDS:
        if not answers.get(field):
            return _build_question_prompt(field, convo), field
    return None, None


def _update_convo_phase(convo: Dict[str, Any]) -> str:
    phase = convo.get("phase") or PHASE_DISCOVERY
    answers = convo.get("answers", {})
    if answers.get("service_type") and answers.get("location"):
        phase = PHASE_QUALIFICATION
    if phase in {PHASE_DISCOVERY, PHASE_QUALIFICATION} and answers.get("project_type") and answers.get("area"):
        phase = PHASE_VALUE_BUILD
    if phase in {PHASE_DISCOVERY, PHASE_QUALIFICATION, PHASE_VALUE_BUILD} and answers.get("budget") and answers.get("timeline"):
        phase = PHASE_BOOKING
    if convo.get("completed"):
        phase = PHASE_POST_BOOKING
    convo["phase"] = phase
    return phase


def _build_followup_ack(convo: Dict[str, Any]) -> str:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    return (
        f"Thanks for the update, {name}. I’ve logged your details and will share them with our lead designer. "
        "If you’d like to tweak anything or book another slot, just let me know."
    )


def _build_welcome_message(convo: Dict[str, Any]) -> str:
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    answers = convo.get("answers", {})
    snippets: List[str] = []
    if answers.get("service_type"):
        snippets.append(f"you're looking for {answers['service_type'].lower()} support")
    if answers.get("project_type"):
        snippets.append(f"for a {answers['project_type']}")
    if answers.get("location"):
        snippets.append(f"in {answers['location']}")
    if answers.get("budget"):
        snippets.append(f"with a budget around {answers['budget']}")
    if answers.get("timeline"):
        snippets.append(f"and a timeline of {answers['timeline']}")
    known_text = ", ".join(snippets)
    if known_text:
        summary = f"I noted that {known_text}."
    else:
        summary = "I'll grab a few quick details so we can tailor everything perfectly."
    return (
        f"Hi {name}! 👋 Thanks for choosing Varush Architect & Interiors. {summary} "
        "I’ll ask only what’s needed and skip anything you’ve already shared."
    )


def _next_missing_field(convo: Dict[str, Any]) -> str | None:
    answers = convo.get("answers", {})
    for field in QUESTION_FLOW:
        value = answers.get(field)
        if field == "name" and (convo.get("contact_name") or value):
            continue
        if not value:
            return field
    return None


def _hydrate_state_from_lead(wa_id: str, convo: Dict[str, Any]) -> None:
    if convo.get("lead_prefill_done"):
        return
    index = _load_lead_index()
    key = _phone_key_from_wa(wa_id)
    record = index.get(key)
    if not record:
        return
    answers = convo.setdefault("answers", {})
    canonical = record.get("canonical", {})
    for canon_field, state_field in CANONICAL_TO_STATE_FIELD.items():
        value = canonical.get(canon_field)
        if value and not answers.get(state_field):
            answers[state_field] = value
            if state_field == "name" and not convo.get("contact_name"):
                convo["contact_name"] = value
    convo["lead_prefill_done"] = True


def _phone_key_from_wa(wa_id: str) -> str | None:
    key = _normalize_phone(wa_id)
    if key and len(key) > 10:
        return key[-10:]
    return key


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
    _append_history(convo, "bot", message)
    offer = convo.setdefault("meeting_offer", {})
    offer["sent_at"] = datetime.now(timezone.utc).isoformat()
    offer["slots"] = slots


async def _inactivity_watcher() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            await _process_inactivity_checks()
        except Exception:
            continue


async def _meeting_reminder_watcher() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            await _process_meeting_reminders()
        except Exception:
            continue


async def _process_inactivity_checks() -> None:
    state = _load_state()
    if not state:
        return
    now = datetime.now(timezone.utc).timestamp()
    updated = False
    for wa_id, convo in state.items():
        if convo.get("completed") or convo.get("inactivity_paused"):
            continue
        if not convo.get("has_welcomed"):
            continue
        last_ts = convo.get("last_client_ts")
        if not last_ts:
            continue
        reminders_sent = convo.get("inactivity_reminders_sent", 0)
        soft_closed = convo.get("inactivity_soft_closed", False)
        next_ts = convo.get("next_inactivity_ts")

        if reminders_sent >= len(INACTIVITY_MESSAGES):
            if soft_closed:
                continue
            trigger = next_ts or (last_ts + INACTIVITY_INITIAL_DELAY + reminders_sent * INACTIVITY_INTERVAL)
            if now < trigger:
                continue
            await _send_whatsapp_text(wa_id, INACTIVITY_SOFT_CLOSE, preview_url=False)
            convo["inactivity_soft_closed"] = True
            convo["inactivity_paused"] = True
            convo["next_inactivity_ts"] = None
            updated = True
            continue

        trigger = next_ts or (last_ts + INACTIVITY_INITIAL_DELAY)
        if reminders_sent > 0 and not next_ts:
            trigger = last_ts + INACTIVITY_INITIAL_DELAY + reminders_sent * INACTIVITY_INTERVAL
        if now < trigger:
            continue
        message = INACTIVITY_MESSAGES[reminders_sent]
        await _send_whatsapp_text(wa_id, message, preview_url=False)
        reminders_sent += 1
        convo["inactivity_reminders_sent"] = reminders_sent
        convo["next_inactivity_ts"] = now + INACTIVITY_INTERVAL
        updated = True
    if updated:
        _save_state(state)


async def _process_meeting_reminders() -> None:
    meetings = _load_meetings()
    if not meetings:
        return
    now = datetime.now(timezone.utc)
    state = _load_state()
    updated = False
    for meeting in meetings:
        if meeting.get("status") != "scheduled":
            continue
        scheduled_at = _parse_meeting_time(meeting.get("scheduled_at"))
        if not scheduled_at:
            continue
        seconds_until = (scheduled_at - now).total_seconds()
        if seconds_until <= -300:
            meeting["status"] = "completed"
            updated = True
            continue
        sent = set(meeting.get("reminders_sent", []))
        for threshold, label in MEETING_REMINDER_WINDOWS:
            if label in sent:
                continue
            if seconds_until <= threshold:
                message = _build_meeting_message(label, meeting, state)
                await _send_whatsapp_text(meeting.get("wa_id"), message, preview_url=False)
                meeting.setdefault("reminders_sent", []).append(label)
                updated = True
                break
    if updated:
        _save_meetings(meetings)


def _read_json_file(path: Path):
    if not path.exists():
        return None
    try:
        with path.open('r', encoding='utf-8') as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None


def _append_history(convo: Dict[str, Any], speaker: str, text: str) -> None:
    if not text:
        return
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "from": speaker, "text": text.strip()}
    history = convo.setdefault("history", [])
    history.append(entry)
    if len(history) > MAX_HISTORY_LENGTH:
        del history[:-MAX_HISTORY_LENGTH]


def _missing_fields(convo: Dict[str, Any]) -> List[str]:
    answers = convo.get("answers", {})
    missing: List[str] = []
    for field in QUESTION_FLOW:
        value = answers.get(field)
        if field == "name" and (convo.get("contact_name") or value):
            continue
        if not value:
            missing.append(field)
    return missing


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
    data = _read_json_file(STATE_PATH)
    if data is not None:
        return data
    if _r2_download_to_path(STATE_R2_KEY, STATE_PATH):
        restored = _read_json_file(STATE_PATH)
        if restored is not None:
            return restored
    return {}


def _save_state(state: Dict[str, Any]) -> None:
    tmp_path = STATE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh)
    tmp_path.replace(STATE_PATH)
    _r2_upload_path(STATE_PATH, STATE_R2_KEY)


def _load_lead_index() -> Dict[str, Any]:
    data = _read_json_file(LEAD_INDEX_PATH)
    if data is not None:
        return data
    if _r2_download_to_path(LEAD_INDEX_R2_KEY, LEAD_INDEX_PATH):
        restored = _read_json_file(LEAD_INDEX_PATH)
        if restored is not None:
            return restored
    return {}


def _save_lead_index(data: Dict[str, Any]) -> None:
    tmp_path = LEAD_INDEX_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh)
    tmp_path.replace(LEAD_INDEX_PATH)
    _r2_upload_path(LEAD_INDEX_PATH, LEAD_INDEX_R2_KEY)


def _store_lead_index(details: Dict[str, Any]) -> None:
    canonical = details.get("canonical") or {}
    phone = canonical.get("phone")
    key = _normalize_phone(phone)
    if not key:
        return
    if len(key) > 10:
        key = key[-10:]
    index = _load_lead_index()
    entry = index.get(key, {"canonical": {}})
    entry.update(
        {
            "leadgen_id": details.get("leadgen_id"),
            "canonical": canonical,
            "source": "meta_lead",
            "created_time": details.get("created_time"),
            "ad_name": details.get("ad_name"),
            "form_id": details.get("form_id"),
        }
    )
    if not entry.get("storage_prefix") and key:
        entry["storage_prefix"] = _r2_prefix_for_key(key)
    entry.pop("drive_folder_id", None)
    entry.pop("drive_folder_link", None)
    index[key] = entry
    _save_lead_index(index)


def _score_lead_from_canonical(details: Dict[str, Any]) -> None:
    canonical = details.get("canonical") or {}
    phone = canonical.get("phone")
    key = _normalize_phone(phone)
    if key and len(key) > 10:
        key = key[-10:]
    lead_input = LeadInput(
        timeline=canonical.get("timeline"),
        budget=canonical.get("budget_bracket"),
        property_type=canonical.get("project_type"),
        service_type=canonical.get("service_type"),
        assets_shared=False,
        answered_fields=0,
        total_fields=len(QUESTION_FLOW),
    )
    result = scoring_agent.score(lead_input)
    result.update({"source": "canonical", "leadgen_id": details.get("leadgen_id")})
    if key:
        _record_lead_score(key, result)


def _score_lead_from_conversation(wa_id: str, convo: Dict[str, Any]) -> None:
    key = _phone_key_from_wa(wa_id)
    index = _load_lead_index()
    canonical = index.get(key, {}).get("canonical", {}) if key else {}
    answers = convo.get("answers", {})
    timeline = answers.get("timeline") or canonical.get("timeline")
    budget = answers.get("budget") or canonical.get("budget_bracket")
    property_type = answers.get("project_type") or canonical.get("project_type")
    service_type = answers.get("service_type") or canonical.get("service_type")
    assets_shared = bool(answers.get("assets"))
    answered_fields = sum(1 for field in QUESTION_FLOW if answers.get(field))
    total_fields = len(QUESTION_FLOW)
    lead_input = LeadInput(
        timeline=timeline,
        budget=budget,
        property_type=property_type,
        service_type=service_type,
        assets_shared=assets_shared,
        answered_fields=answered_fields,
        total_fields=total_fields,
    )
    result = scoring_agent.score(lead_input)
    result.update({"source": "conversation", "wa_id": wa_id})
    _record_lead_score(key or wa_id, result)


def _normalize_phone(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


async def _download_whatsapp_media(media_id: str, media_info: Dict[str, Any] | None = None) -> tuple[bytes, str, str] | None:
    token = WHATSAPP_ACCESS_TOKEN or PAGE_ACCESS_TOKEN
    if not token:
        return None
    media_info = media_info or {}
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            meta_resp = await client.get(
                f"{GRAPH_API_BASE}/{media_id}",
                headers=headers,
            )
            meta_resp.raise_for_status()
            meta = meta_resp.json()
            url = meta.get("url")
            if not url:
                print(f"MEDIA META RESPONSE MISSING URL: {meta}")
                return None
            mime_type = meta.get("mime_type") or media_info.get("mime_type") or "application/octet-stream"
            download_resp = await client.get(url, headers=headers, follow_redirects=True)
            download_resp.raise_for_status()
            data = download_resp.content
            print(f"MEDIA SIZE: {len(data)}")
            if not data:
                return None
    except httpx.HTTPError as exc:
        print(f"MEDIA DOWNLOAD ERROR: {exc}")
        return None
    filename = media_info.get("filename")
    if not filename:
        ext = mimetypes.guess_extension(mime_type) or ""
        filename = f"{media_id}{ext}"
    return data, mime_type, filename



def _archive_media_locally(wa_id: str, filename: str, data: bytes) -> Path | None:
    try:
        key = _phone_key_from_wa(wa_id) or wa_id
        safe_name = _sanitize_filename(filename)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        folder = MEDIA_ARCHIVE_PATH / key
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{timestamp}_{safe_name}"
        path.write_bytes(data)
        return path
    except OSError:
        return None


def _sanitize_filename(filename: str) -> str:
    base = filename or "attachment"
    safe = []
    for ch in base:
        if ch.isalnum() or ch in {"-", "_", "."}:
            safe.append(ch)
        else:
            safe.append("_")
    cleaned = "".join(safe).strip("._")
    return cleaned or "file"


def _load_lead_scores() -> Dict[str, Any]:
    data = _read_json_file(LEAD_SCORE_PATH)
    if data is not None:
        return data
    if _r2_download_to_path(LEAD_SCORE_R2_KEY, LEAD_SCORE_PATH):
        restored = _read_json_file(LEAD_SCORE_PATH)
        if restored is not None:
            return restored
    return {}


def _save_lead_scores(data: Dict[str, Any]) -> None:
    tmp_path = LEAD_SCORE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh)
    tmp_path.replace(LEAD_SCORE_PATH)
    _r2_upload_path(LEAD_SCORE_PATH, LEAD_SCORE_R2_KEY)


def _record_lead_score(key: str, result: Dict[str, Any]) -> None:
    if not key:
        return
    scores = _load_lead_scores()
    scores[key] = result
    _save_lead_scores(scores)


def _load_meetings() -> List[Dict[str, Any]]:
    if not MEETINGS_PATH.exists():
        return []
    try:
        with MEETINGS_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        return []


def _save_meetings(meetings: List[Dict[str, Any]]) -> None:
    tmp_path = MEETINGS_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(meetings, fh)
    tmp_path.replace(MEETINGS_PATH)


def _register_meeting(body: ScheduleMeetingRequest) -> Dict[str, Any]:
    meeting_time = _parse_meeting_time(body.scheduled_at)
    if not meeting_time:
        raise HTTPException(status_code=400, detail="Invalid scheduled_at format")
    meetings = _load_meetings()
    record = {
        "wa_id": body.wa_id,
        "scheduled_at": meeting_time.astimezone(timezone.utc).isoformat(),
        "note": body.note,
        "status": "scheduled",
        "reminders_sent": [],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    meetings.append(record)
    _save_meetings(meetings)
    return record


def _parse_meeting_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(timezone.utc)


def _build_meeting_message(label: str, meeting: Dict[str, Any], state: Dict[str, Any]) -> str:
    template = MEETING_REMINDER_MESSAGES.get(label, "")
    wa_id = meeting.get("wa_id", "")
    convo = state.get(wa_id, {})
    name = convo.get("contact_name") or convo.get("answers", {}).get("name") or "there"
    return template.format(name=name, link=MEETING_LINK)


def _append_log(payload: Dict[str, Any]) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def _is_valid_verify_token(token: str | None) -> bool:
    if not token:
        return False
    if not VERIFY_TOKEN_VALUES:
        return False
    return token in VERIFY_TOKEN_VALUES


def _append_lead_log(payload: Dict[str, Any]) -> None:
    with LEAD_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def _append_lead_details(data: Dict[str, Any]) -> None:
    print("DEBUG: writing lead details")
    try:
        with LEAD_DETAILS_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(data) + "\n")
        print("DEBUG: lead details written successfully")
    except Exception as exc:
        print("LEAD DETAILS WRITE ERROR:", exc)


def _init_canonical_lead() -> Dict[str, Any]:
    return {field: None for field in CANONICAL_LEAD_FIELDS}


def _normalize_lead_fields(field_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    canonical = _init_canonical_lead()
    for item in field_data or []:
        raw_name = (item.get("name") or "").strip().lower()
        normalized_name = raw_name.replace("_", " ")
        key = LEAD_FIELD_MAP.get(raw_name) or LEAD_FIELD_MAP.get(normalized_name)
        if not key:
            continue
        values = item.get("values") or []
        value = values[0] if values else None
        if value:
            canonical[key] = value
    return canonical


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


async def _handle_leadgen_payload(payload: Dict[str, Any]) -> None:
    print("LEADGEN WEBHOOK RECEIVED")
    _append_lead_log(payload)
    await _process_leadgen_payload(payload)


async def _process_leadgen_payload(payload: Dict[str, Any]) -> None:
    index = _load_lead_index()
    existing_ids = {entry.get("leadgen_id") for entry in index.values() if entry.get("leadgen_id")}
    entries = payload.get("entry", [])
    for entry in entries:
        for change in entry.get("changes", []):
            if change.get("field") != "leadgen":
                continue
            value = change.get("value", {})
            lead_id = value.get("leadgen_id")
            if not lead_id:
                continue
            if lead_id in existing_ids:
                print(f"Duplicate lead skipped: {lead_id}")
                continue
            details = await _fetch_lead_details(lead_id)
            print("DEBUG: lead details fetched:", details)
            if details:
                existing_ids.add(lead_id)
                _append_lead_details(details)
                _store_lead_index(details)
                _score_lead_from_canonical(details)
                await _notify_admins_of_lead(details)


async def _fetch_lead_details(lead_id: str) -> Dict[str, Any] | None:
    token = LEAD_ACCESS_TOKEN
    if not token:
        print("LEAD FETCH SKIPPED: PAGE_ACCESS_TOKEN not configured")
        return None
    print(f"FETCHING LEAD DETAILS: {lead_id}")
    url = f"https://graph.facebook.com/v20.0/{lead_id}"
    params = {
        "access_token": token,
        "fields": "created_time,ad_id,ad_name,form_id,field_data,platform"
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
    print(f"LEAD FETCH STATUS: {resp.status_code}")
    if resp.status_code >= 400:
        print(f"LEAD FETCH ERROR: {resp.text}")
        _append_lead_details({"leadgen_id": lead_id, "error": resp.text})
        return None
    data = resp.json()
    data["leadgen_id"] = lead_id
    data["canonical"] = _normalize_lead_fields(data.get("field_data", []))
    print("LEAD FETCH SUCCESS")
    return data


async def _notify_admins_of_lead(details: Dict[str, Any]) -> None:
    if not ADMIN_ALERT_NUMBERS:
        return
    summary = _format_lead_summary(details)
    print(f"NOTIFYING ADMINS: {ADMIN_ALERT_NUMBERS}")
    for number in ADMIN_ALERT_NUMBERS:
        try:
            resp = await _send_whatsapp_text(number, summary, preview_url=False)
            print(f"WHATSAPP SENT SUCCESSFULLY TO {number}: {resp}")
        except HTTPException as exc:
            print(f"WHATSAPP SEND FAILED FOR {number}: {exc.detail}")


def _format_lead_summary(details: Dict[str, Any]) -> str:
    created = _format_timestamp(details.get("created_time"))
    ad_name = details.get("ad_name") or "Unknown Ad"
    form_id = details.get("form_id") or "Unknown Form"
    canonical = details.get("canonical") or {}
    highlights: List[str] = []
    if canonical.get("full_name"):
        highlights.append(f"Name: {canonical['full_name']}")
    if canonical.get("phone"):
        highlights.append(f"Phone: {canonical['phone']}")
    if canonical.get("project_location"):
        highlights.append(f"Location: {canonical['project_location']}")
    if canonical.get("project_type"):
        highlights.append(f"Property: {canonical['project_type']}")
    if canonical.get("budget_bracket"):
        highlights.append(f"Budget: {canonical['budget_bracket']}")
    if canonical.get("timeline"):
        highlights.append(f"Timeline: {canonical['timeline']}")
    highlight_text = "\n".join(highlights) if highlights else "(No structured details parsed yet.)"
    return (
        f"New Meta lead captured (Form {form_id}).\n"
        f"Ad: {ad_name}\n"
        f"Received: {created}\n\n"
        f"{highlight_text}"
    )


def _format_timestamp(ts: str | None) -> str:
    if not ts:
        return "Unknown time"
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
        return dt.astimezone(IST).strftime("%d %b %Y · %I:%M %p IST")
    except ValueError:
        return ts


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


async def _intent_ready_to_book(wa_id: str, convo: Dict[str, Any]) -> bool:
    await _send_meeting_prompt(wa_id, convo)
    convo["completed"] = True
    convo["status"] = "completed"
    _update_convo_phase(convo)
    return True


async def _intent_pricing_query(wa_id: str, convo: Dict[str, Any]) -> bool:
    name = _convo_display_name(convo)
    message = (
        f"Great question, {name}! Budgets flex with finishes and scale, but premium 3BHK design-build packages typically start ~₹15–20L when we handle design + execution end-to-end. Share whatever’s top of mind and we’ll tailor a tighter range or hop on a 10-min alignment call."
    )
    await _send_whatsapp_text(wa_id, message, preview_url=False)
    _append_history(convo, "bot", message)
    return True


async def _intent_smalltalk(wa_id: str, convo: Dict[str, Any]) -> bool:
    name = _convo_display_name(convo)
    message = (
        f"Hey {name}! I’m right here whenever you want to dive in—share whichever detail is easiest and I’ll take it from there."
    )
    await _send_whatsapp_text(wa_id, message, preview_url=False)
    _append_history(convo, "bot", message)
    return True


async def _intent_objection(wa_id: str, convo: Dict[str, Any]) -> bool:
    return await _send_gentle_reassurance(wa_id, convo)


async def _intent_confusion(wa_id: str, convo: Dict[str, Any]) -> bool:
    return await _send_gentle_reassurance(wa_id, convo)


async def _send_gentle_reassurance(wa_id: str, convo: Dict[str, Any]) -> bool:
    name = _convo_display_name(convo)
    explainer = (
        f"Makes sense, {name}! I only ask a couple of quick details so our lead designer can share precise ideas instead of generic advice.\n\n"
        "Usually we just note the location, property type, and approximate size, then tailor everything from there."
    )
    gentle_prompt, follow_field = _build_gentle_project_prompt(convo)
    if gentle_prompt:
        explainer = f"{explainer}\n\n{gentle_prompt}"
        convo["awaiting_field"] = follow_field
        convo["awaiting_origin"] = "agent" if follow_field else None
    await _send_whatsapp_text(wa_id, explainer, preview_url=False)
    _append_history(convo, "bot", explainer)
    return True


INTENT_ROUTER = {
    "ready_to_book": _intent_ready_to_book,
    "pricing_query": _intent_pricing_query,
    "smalltalk": _intent_smalltalk,
    "objection": _intent_objection,
    "confusion": _intent_confusion,
}


