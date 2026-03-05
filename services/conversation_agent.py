import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI

ALLOWED_INTENTS = [
    "new_info",
    "clarification",
    "edit_request",
    "ready_to_book",
    "ask_portfolio",
    "pricing_query",
    "objection",
    "confusion",
    "handoff",
    "smalltalk",
    "unknown",
]
INTAKE_FIELDS = ["name", "service_type", "location", "project_type", "area", "timeline", "finish", "budget"]
SMALLTALK_KEYWORDS = ["hi", "hello", "hey", "good morning", "good evening", "thanks", "thank you", "ok", "okay", "nice", "cool", "hmm", "great"]
FIELDS_SCHEMA_PROPERTIES = {field: {"type": "string"} for field in INTAKE_FIELDS}

SYSTEM_PROMPT = (
    'You are Kavya, the WhatsApp assistant for Varush Architect & Interiors. Speak warmly, stay focused on scheduling a design session, and return ONLY JSON that matches the schema. '
    'Allowed intents: new_info, clarification, edit_request, ready_to_book, ask_portfolio, pricing_query, objection, confusion, handoff, smalltalk, unknown. '
    'Treat greetings/polite chatter (hi, hello, hey, good morning, thanks, ok, nice, cool, hmm, great, etc.) as smalltalk: acknowledge briefly, do NOT update any fields, then guide the client back to the next best intake question. '
    'When basics are missing, feel free to bundle them (“Tell me the location, property type, and approximate size”) so the client can reply in one go. '
    'Ask at most one question per reply. Let the conversation flow naturally—never rapid-fire the full intake list. If the user expresses confusion or objects ("why so many questions?", "not hiring a designer"), set intent to objection or confusion, explain briefly why the detail helps, highlight the 45-day guarantee + direct factory-to-project delivery + our in-house expert designers, and only then offer a single gentle next step. '
    'If your confidence in the interpretation is below 0.6, leave fields_detected empty, keep the intent as clarification or unknown, and craft a clarifying follow-up prompt before logging anything. '
    'For pricing queries, share general guidance and recommend a 10-min alignment call first; if they still insist on an exact quote, set needs_human=true with a clear handoff_reason. '
    'For ask_portfolio, always include the provided portfolio link. When reconnecting with a returning client, start with a brief summary of the last discussion and ask whether they want to discuss a new project, edit the existing one, or continue from the last stage before proceeding. Keep replies under four sentences and avoid markdown.'
)

RESPONSE_SCHEMA = {
    "name": "conversation_response",
    "schema": {
        "type": "object",
        "properties": {
            "intent": {"type": "string", "enum": ALLOWED_INTENTS, "description": "Primary intent label."},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1, "description": "Model confidence for the selected intent."},
            "reply": {"type": "string", "description": "Natural-language reply text."},
            "fields_detected": {
                "type": "object",
                "properties": FIELDS_SCHEMA_PROPERTIES,
                "additionalProperties": False,
                "description": "Key/value pairs extracted from the client message."
            },
            "follow_up_prompt": {
                "type": ["string", "null"],
                "description": "Optional follow-up question copy."
            },
            "next_field": {
                "type": ["string", "null"],
                "description": "Name of the intake field the follow-up prompt addresses."
            },
            "request_meeting": {
                "type": "boolean",
                "description": "True if we should jump to meeting slots now."
            },
            "needs_human": {
                "type": "boolean",
                "description": "True if a designer should take over immediately."
            },
            "handoff_reason": {
                "type": ["string", "null"],
                "description": "Short reason for human takeover."
            }
        },
        "required": ["intent", "confidence", "reply", "fields_detected", "request_meeting", "needs_human"],
        "additionalProperties": False
    }
}

MAX_HISTORY_FOR_PROMPT = 10


@dataclass
class ConversationAgentResult:
    intent: str
    reply: str
    fields_detected: Dict[str, str]
    confidence: float = 1.0
    follow_up_prompt: Optional[str] = None
    next_field: Optional[str] = None
    request_meeting: bool = False
    needs_human: bool = False
    handoff_reason: Optional[str] = None


class ConversationAgent:
    def __init__(self) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
        self.client: Optional[AsyncOpenAI] = AsyncOpenAI(api_key=api_key) if api_key else None

    @property
    def is_ready(self) -> bool:
        return self.client is not None

    async def generate_response(
        self,
        *,
        answers: Dict[str, Any],
        missing_fields: List[str],
        awaiting_field: Optional[str],
        history: List[Dict[str, Any]],
        message: str,
        contact_name: Optional[str],
        portfolio_link: str,
    ) -> Optional[ConversationAgentResult]:
        if not self.client:
            return None
        history_text = self._format_history(history)
        answers_text = json.dumps(answers or {}, ensure_ascii=False)
        missing_text = ", ".join(missing_fields) if missing_fields else "none"
        awaiting_text = awaiting_field or "none"
        name = contact_name or "there"
        smalltalk_hint = ", ".join(SMALLTALK_KEYWORDS)
        user_prompt = (
            f"Client name: {name}\n"
            f"Known answers JSON: {answers_text}\n"
            f"Missing fields (in order): {missing_text}\n"
            f"Awaiting specific field: {awaiting_text}\n"
            f"Portfolio link: {portfolio_link}\n"
            f"Treat these keywords as pure smalltalk: {smalltalk_hint}.\n"
            f"Recent history (newest last):\n{history_text}\n\n"
            f"New client message: {message.strip()}"
        )
        try:
            response = await self.client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": [{"type": "text", "text": SYSTEM_PROMPT}],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": user_prompt}],
                    },
                ],
                response_format={"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
            )
        except Exception as exc:  # pylint: disable=broad-except
            print(f"LLM ERROR: {exc}")
            return None

        content = self._extract_text(response)
        if not content:
            return None
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            print("LLM JSON DECODE ERROR:", content)
            return None
        return ConversationAgentResult(
            intent=str(data.get("intent", "unknown")),
            reply=str(data.get("reply", "")).strip(),
            fields_detected={k: str(v).strip() for k, v in (data.get("fields_detected") or {}).items() if str(v).strip()},
            confidence=float(data.get("confidence", 1.0) or 0.0),
            follow_up_prompt=(data.get("follow_up_prompt") or None),
            next_field=(data.get("next_field") or None),
            request_meeting=bool(data.get("request_meeting")),
            needs_human=bool(data.get("needs_human")),
            handoff_reason=(data.get("handoff_reason") or None),
        )

    @staticmethod
    def _extract_text(response: Any) -> Optional[str]:
        for item in response.output or []:
            for content in getattr(item, "content", []) or []:
                text = getattr(content, "text", None)
                if text:
                    return text
        return None

    @staticmethod
    def _format_history(history: List[Dict[str, Any]]) -> str:
        if not history:
            return "(no prior messages)"
        trimmed = history[-MAX_HISTORY_FOR_PROMPT:]
        lines = []
        for entry in trimmed:
            speaker = "Client" if entry.get("from") == "client" else "Kavya"
            text = entry.get("text", "").strip()
            if not text:
                continue
            lines.append(f"{speaker}: {text}")
        return "\n".join(lines) if lines else "(no usable history)"
