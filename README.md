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
   - `META_VERIFY_TOKEN` (must match the token configured in WhatsApp Business Manager)
   - `FORWARD_URL` (e.g., `https://varush-webhook.onrender.com`)
   - Optional: `LOG_PATH=/data/webhook-events.log` (persistent disk)
   - Optional: `ADMIN_TOKEN` (required for the admin endpoints)
   - Optional: `WHATSAPP_PHONE_ID`, `WHATSAPP_ACCESS_TOKEN` (needed to send replies via the Cloud API)
   - Optional: `STATE_PATH` (defaults to `logs/conversations.json`) and `MEETING_LINK` for the auto-reply assistant
   - Optional: `LEAD_ACCESS_TOKEN`, `LEAD_LOG_PATH`, `LEAD_DETAILS_PATH` if you enable Meta Lead Ads ingestion
3. (Optional) Attach a persistent disk (1 GB) to `/data` for log retention.
4. Deploy. Once live, you'll get a URL like `https://varush-webhook-proxy.onrender.com/webhook`.

## Switching WhatsApp Webhook
After deployment, update WhatsApp Business Manager webhook to the new URL and verify using the same `META_VERIFY_TOKEN`. Test inbound messages; the service will log them and forward to the legacy endpoint.

## Meta Lead Ads Ingestion

Subscribe your Meta app to `page` → `leadgen` events and point it to `https://<service>/leadgen` (same verify token works). The service will:
1. Log the raw webhook payload to `LEAD_LOG_PATH` (default `logs/leadgen-events.log`).
2. Fetch the full lead record via Graph API `/{leadgen_id}?fields=field_data,...` using `LEAD_ACCESS_TOKEN` (falls back to `WHATSAPP_ACCESS_TOKEN` if unset).
3. Append the detailed record to `LEAD_DETAILS_PATH` for processing.

## Assistant Auto-Reply Flow

When `WHATSAPP_*` credentials are present, the webhook automatically:
1. Logs every inbound message.
2. Stores per-contact conversation state in `STATE_PATH` (default `logs/conversations.json`).
3. Sends guided replies that collect project details (service type, location, project type, area, timeline, finish, budget, assets, portfolio preference) and then offers meeting slots generated in IST. The meeting link defaults to `https://meet.varushinteriors.com/intro` but can be overridden via `MEETING_LINK`.

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
