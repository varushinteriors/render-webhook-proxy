# Varush WhatsApp Webhook Proxy

FastAPI service that proxies WhatsApp Business webhook events to the existing Varush endpoint while logging every payload, so Bulk_message_sender can process replies/delivery statuses.

## Features
- Responds to Meta's GET challenge using `META_VERIFY_TOKEN`
- Accepts POST payloads, appends JSON to `logs/webhook-events.log` (or `/data/webhook-events.log` on Render)
- Forwards the same payload to `FORWARD_URL` so your current automation keeps working
- Simple Docker image for Render deployment

## Local Dev
```bash
pip install -r requirements.txt
META_VERIFY_TOKEN=testtoken FORWARD_URL=https://varush-webhook.onrender.com uvicorn app:app --reload --port 8080
```

## Deploy to Render
1. Create a new **Web Service** (Docker) and point it to this repo.
2. Set environment variables:
   - `META_VERIFY_TOKEN` (must match the token configured in WhatsApp Business Manager; you can provide multiple tokens separated by commas)
   - Optional: `PAGE_VERIFY_TOKEN` (use a different verify token for `page/leadgen` subscriptions if desired)
   - `FORWARD_URL` (e.g., `https://varush-webhook.onrender.com`)
   - Optional: `LOG_PATH=/data/webhook-events.log` (persistent disk)
   - Optional: `ADMIN_TOKEN` (required for the admin endpoints)
   - Optional: `WHATSAPP_PHONE_ID`, `WHATSAPP_ACCESS_TOKEN` (needed to send replies via the Cloud API)
   - Optional: `STATE_PATH` (defaults to `logs/conversations.json`) and `MEETING_LINK` for the auto-reply assistant
   - Optional: `PAGE_ACCESS_TOKEN`, `LEAD_ACCESS_TOKEN`, `LEAD_LOG_PATH`, `LEAD_DETAILS_PATH` if you enable Meta Lead Ads ingestion (the service uses `PAGE_ACCESS_TOKEN` first, then `LEAD_ACCESS_TOKEN`, then falls back to the WhatsApp token)
   - Optional: `LEAD_INDEX_PATH` (defaults to `logs/lead-index.json`) to store phone → lead-field mappings
   - Optional: `LEAD_SCORE_PATH` (defaults to `logs/lead-scores.json`) to persist cold/warm/hot ratings
   - Optional: `MEETINGS_PATH` (defaults to `logs/meetings.json`) to persist booked meetings for reminder scheduling
   - Optional: `LEAD_ENGAGEMENT_PATH` (defaults to `logs/lead-engagement.json`) for future template drips
   - Optional: `DRIVE_PARENT_FOLDER_ID` + (`GOOGLE_APPLICATION_CREDENTIALS` path or `GOOGLE_DRIVE_CREDENTIALS_JSON`) to enable Google Drive uploads (defaults prefilled for Varush)
   - Optional: `DRIVE_PORTFOLIO_LINK` to control the portfolio link shared in-chat
   - Optional: `ADMIN_ALERT_NUMBERS` (comma-separated WhatsApp numbers to receive lead notifications)
3. (Optional) Attach a persistent disk (1 GB) to `/data` for log retention.
4. Deploy. Once live, you'll get a URL like `https://varush-webhook-proxy.onrender.com/webhook`.

## Switching WhatsApp Webhook
After deployment, update WhatsApp Business Manager webhook to the new URL and verify using the same `META_VERIFY_TOKEN`. Test inbound messages; the service will log them and forward to the legacy endpoint.

## Meta Lead Ads Ingestion

Subscribe your Meta app to `page` → `leadgen` events and point it to `https://<service>/leadgen` (same verify token works). The service will:
1. Log the raw webhook payload to `LEAD_LOG_PATH` (default `logs/leadgen-events.log`).
2. Fetch the full lead record via Graph API `/{leadgen_id}?fields=field_data,...` using `LEAD_ACCESS_TOKEN` (falls back to `WHATSAPP_ACCESS_TOKEN` if unset).
3. Append the detailed record to `LEAD_DETAILS_PATH` for processing.
4. If `ADMIN_ALERT_NUMBERS` is set, WhatsApp a summary of each lead to those numbers via the built-in send-message helper.

## Lead Scoring Agent

- Implemented in `agents/lead_scoring.py` and instantiated by the FastAPI service.
- Every Meta lead (canonical data) and every WhatsApp conversation update is scored as **hot**, **warm**, or **cold** using the rules:
  - Hot if timeline ≤3 months with high/flexible budget or high-value property, or if the lead has shared layouts/photos and completed every intake answer.
  - Warm if budget/timeline are viable but missing hot criteria.
  - Cold otherwise.
- Scores + rationale are persisted to `LEAD_SCORE_PATH` for downstream CRM or prioritization.

## Meeting Warm-up Agent

- Meetings (stored in `MEETINGS_PATH`) receive reminders at T‑2h, T‑1h, and T‑10 min with on-brand hype copy.
- After booking a meeting, register it via `POST /admin/schedule-meeting`:
  ```json
  {
    "wa_id": "91987...",
    "scheduled_at": "2026-03-05T15:00:00+05:30",
    "note": "Intro call"
  }
  ```
- Reminder scheduler runs every ~30 s and will mark meetings complete once the start time passes.

## Asset & Portfolio Agent

- Uses the Google Drive service account at `GOOGLE_APPLICATION_CREDENTIALS` and `DRIVE_PARENT_FOLDER_ID` to auto-create `Lead - {Name}` folders.
- Any inbound WhatsApp media (images, documents, video, etc.) is downloaded via Meta’s Graph API and uploaded to the lead’s Drive folder.
- The agent acknowledges each upload in-chat so clients know their layouts/photos are saved securely.
- The intake flow’s portfolio question now shares the curated portfolio link (`DRIVE_PORTFOLIO_LINK`) automatically.

## Assistant Auto-Reply Flow

When `WHATSAPP_*` credentials are present, the webhook automatically:
1. Logs every inbound message.
2. Stores per-contact conversation state in `STATE_PATH` (default `logs/conversations.json`).
3. Looks up any Meta Lead data (via `LEAD_INDEX_PATH`) and pre-fills known answers so the welcome message can reference them and the intake flow skips redundant questions.
4. Sends guided replies that collect the remaining project details (service type, location, project type, area, timeline, finish, budget, assets, portfolio preference) and then offers meeting slots generated in IST. The meeting link defaults to `https://meet.varushinteriors.com/intro` but can be overridden via `MEETING_LINK`.

## Admin Endpoints

Set `ADMIN_TOKEN` to enable the following:

### Fetch latest events
```
GET /events/latest?limit=20
Headers: X-Admin-Token: <ADMIN_TOKEN>
```
Returns `{ "count": <n>, "events": [ ... ] }` with the newest WhatsApp payloads (limit default 20, max 200).

### Send a WhatsApp reply via Cloud API
```
POST /admin/send-message
Headers: X-Admin-Token: <ADMIN_TOKEN>
Body: { "to": "<wa_id>", "message": "text", "preview_url": false }
```
Requires `WHATSAPP_PHONE_ID` and `WHATSAPP_ACCESS_TOKEN` to be set. The endpoint relays the request to `https://graph.facebook.com/v20.0/{PHONE_ID}/messages`.

### Register a meeting (for warm-up reminders)
```
POST /admin/schedule-meeting
Headers: X-Admin-Token: <ADMIN_TOKEN>
Body: { "wa_id": "<contact wa_id>", "scheduled_at": "2026-03-05T15:00:00+05:30", "note": "optional" }
```
The service stores the meeting in `MEETINGS_PATH` and automatically sends reminders at 2 h, 1 h, and 10 min before start, then marks it complete.
