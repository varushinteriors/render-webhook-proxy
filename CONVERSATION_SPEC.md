# Varush WhatsApp Agent – Intelligent Conversation Spec

_Last updated: 2026-03-05_

## 1. Goals & Principles
- **Feel human, stay precise.** Mirror Kavya’s warm, detail-obsessed tone while keeping replies concise and actionable.
- **Understand intent, not just order.** Parse what the client actually said before deciding whether to log an answer, clarify, or escalate.
- **Never lose context.** Every reply should reference what we already know (from Meta leads, prior answers, media uploads) so clients don’t repeat themselves.
- **Respect WhatsApp policy.** Free-form replies are only allowed inside the 24‑hour service window; otherwise we must send an approved template or escalate to a human.

## 2. Inbound Classification
Every inbound message is routed through a lightweight intent detector (LLM or rules) that returns:

| Intent | Description | Example triggers | Default action |
| --- | --- | --- | --- |
| `new_info` | Client provided a fresh answer to one of the intake fields. | “Budget is 25L.” | Log field, confirm briefly, ask next gap. |
| `clarification` | Client asks a question about the process or we’re unsure what they meant. | “What’s turnkey include?” | Answer directly or ask them to clarify before logging anything. |
| `edit_request` | Client wants to change a previous answer. | “Actually it’s 2200 sqft, not 1800.” | Update stored value, confirm change. |
| `ready_to_book` | Client wants to jump to meeting coordination. | “Send me the slots again.” | Share current slots/meeting link immediately. |
| `ask_portfolio` | Client explicitly requests our work samples/portfolio. | “Can you share your portfolio?” | Send the curated link + keep the flow moving. |
| `pricing_query` | Client wants costing/quotes. | “What’s your pricing?” | Share tier guidance, push for a 10‑min intro call, escalate if they still insist on a quote. |
| `objection` | Pushback to the offer/question (no calls, “designers are expensive”, “not hiring”). | “I don’t want a consultation”, “Designers overcharge.” | Acknowledge politely, reinforce the 45‑day guarantee + direct factory-to-project + in-house designers, then continue the flow. |
| `smalltalk` | Greetings/polite chatter with no project info. | “Hi”, “Thanks”, “Ok”, “Great.” | Acknowledge warmly, do **not** change lead data, immediately ask the next missing field. |
| `handoff` | They ask for a human / issue beyond automation. | “Need to talk to your designer.” | Pause automation, alert admins, reassure the client. |
| `unknown` | LLM isn’t confident (<0.6) or message is ambiguous. | “???” | Ask for clarification before logging anything. |

**Smalltalk handling.** Treat the greetings/polite fillers listed above strictly as `smalltalk`: acknowledge them, but never write to `answers`. Immediately follow with the highest-priority missing question so the flow keeps moving.

**Objection messaging.** When a client pushes back (no calls, doesn’t want to hire, says designers overcharge, refuses to answer), the agent must: (1) empathize, (2) remind them of Varush’s pillars—45-day guaranteed execution after design approval, direct factory-to-project modular delivery (zero middlemen, zero delays, consistent workmanship), and our in-house expert team—and (3) steer gently back to the flow.

The LLM returns a JSON object like:
```json
{
  "intent": "edit_request",
  "fields_detected": {"area": "2200 sqft"},
  "questions_to_answer": ["What is turnkey scope?"],
  "tone": "warm",
  "needs_human": false
}
```

## 3. Response Strategy
1. **Update state first.** Merge `fields_detected` into `convo["answers"]`, record edits with a timestamped audit trail (new `history` array).
2. **Decide next action** using a deterministic policy:
   - If `needs_human` or client explicitly requested, set `convo["status"] = "handoff"`, notify admins, stop auto-messaging.
   - Else if `ready_to_book`, send meeting slots (skip unanswered fields unless critical like `service_type`).
   - Else if unanswered fields remain, ask only the highest-priority missing field (preference order: service → location → project → area → timeline → finish → budget → assets → portfolio).
   - Else send a summary + meeting prompt if not already completed.
3. **Craft the reply.** Use the LLM to draft natural text, but require it to return a structured block:
```json
{
  "intent": "edit_request",
  "confidence": 0.91,
  "fields_detected": {"area": "2200 sqft"},
  "reply": "Noted the updated area, Varun!",
  "follow_up_prompt": "When are you hoping to kick off?",
  "next_field": "timeline",
  "request_meeting": false,
  "needs_human": false
}
```
4. **Validation + fallback.** If the LLM response is missing required keys or exceeds length, fall back to deterministic copy templates.

## 4. Data Model Updates (`STATE_PATH`)
```json
{
  "919873607248": {
    "answers": {
      "service_type": "Interior design",
      "location": "Gurugram"
    },
    "history": [
      {"ts": "2026-03-05T08:15:22Z", "from": "client", "text": "Hi"},
      {"ts": "2026-03-05T08:15:24Z", "from": "bot", "text": "Hi Varun!..."}
    ],
    "awaiting_field": "budget",
    "status": "active",  // active | completed | handoff | paused
    "has_recap_prompted": true,
    "awaiting_recap_choice": false,
    "last_recap_ts": 1772699000,
    "last_client_ts": 1772698179,
    "completed": false,
    "meeting_offer": {
      "sent_at": "2026-03-05T08:20:00Z",
      "slots": ["Thu, 06 Mar ..."],
      "confirmed": null
    }
  }
}
```
- `history` enables the LLM to reason over previous Q&A without hitting external logs.
- `edits` array (optional) tracks `{field, old_value, new_value, ts}` for audit.
- `handoff_reason` when `status="handoff"` helps route to the right human.

- `has_recap_prompted`, `awaiting_recap_choice`, and `last_recap_ts` drive the returning-client summary so we only ask “new vs edit vs continue” once per comeback (or after the cooldown).

## 5. LLM Integration Plan
- **Provider:** OpenAI Responses API (preferred) with JSON schema; fallback to local rules if credentials unset.
- **Prompt skeleton:**
  1. System: remind Kavya to use only the approved intents, keep replies ≤4 sentences, include the portfolio link when asked, and treat the `SMALLTALK_KEYWORDS` list as chatter that should not update state.
  2. Context: last 10 history messages + structured `answers`, `missing_fields`, `awaiting_field`, portfolio link, and the explicit smalltalk keyword list so the LLM can classify greetings correctly.
  3. User message.
- **Schema:** JSON schema enforces `{intent, confidence, fields_detected, reply, follow_up_prompt, next_field, request_meeting, needs_human, handoff_reason}` with `intent` limited to the final list and `confidence` clipped between 0–1.
- **Rate limiting:** cache the last LLM call hash to avoid duplicate generations if Meta retries the webhook.

## 6. WhatsApp Policy Handling
- Track `last_client_ts` and `last_bot_ts`. Before sending any free-form reply, check `now - last_client_ts`:
  - ≤ 23h 50m: proceed normally.
  - > 23h 50m: queue a `reengagement` action that sends a pre-approved template (e.g., `varush_followup_1`) summarizing the last context and inviting a reply.
  - If no template is available for the scenario, notify admins and pause automation (`status="paused"`).
- Template variables to request approval for:
  1. `varush_followup_1(name, summary)` – gentle nudge after 24 h.
  2. `varush_assets_reminder(name)` – reminds them to share drawings/photos.
  3. `varush_meeting_reminder(name, slot)` – to re-open conversation with a specific slot.

## 7. Error & Handoff Handling
- **Failed sends (131047, 131026):**
  - Log the error in `lead-engagement.json` with `status="send_failed"`.
  - Trigger admin alert with the phone number and reason so a human can intervene via official WhatsApp Business app if needed.
- **LLM failure / timeout:**
  - Use fallback template (“Got it! Could you confirm your project location?”) and mark `convo["llm_failures"] += 1` for observability.

## 8. QA checklist
1. **Greetings / smalltalk** – Send “Hi” then “Thanks”. Expect intent=`smalltalk`, no state edits, and Kavya immediately asks the next missing field.
2. **New info** – “Budget is 30L.” Should log `budget`, confirm it, and request the next field.
3. **Edit request** – “Actually make it 2200 sqft.” Should overwrite `area`, confirm the change, and continue.
4. **Portfolio ask** – “Can you share your portfolio?” Reply must contain the configured Drive link plus the next prompt.
5. **Pricing escalation** – Ask “What’s your pricing?” twice. First reply shares tier guidance + nudges for a call; second insistence should trigger `needs_human=true` and pause automation.
6. **Low-confidence / unknown** – Send a vague message (“Hmm maybe”) and ensure it returns intent=`clarification` or `unknown`, keeps `fields_detected` empty, and explicitly asks for clarification.
7. **Objection handling** – “Interior designers charge too much” → intent=`objection`, response references the guarantee + factory-to-project + in-house team, then resumes the intake.
8. **Returning client recap** – Reconnect after a prior intake. Expect the recap summary + “new / edit / continue” prompt. Reply “new” (state resets), “edit” (stays in edit mode), and “continue” (jumps to next missing question).

This doc now mirrors the production conversation engine (Step 3). Keep it updated as we add re-engagement templates or new intents.
