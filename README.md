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
3. (Optional) Attach a persistent disk (1 GB) to `/data` for log retention.
4. Deploy. Once live, you'll get a URL like `https://varush-webhook-proxy.onrender.com/webhook`.

## Switching WhatsApp Webhook
After deployment, update WhatsApp Business Manager webhook to the new URL and verify using the same `META_VERIFY_TOKEN`. Test inbound messages; the service will log them and forward to the legacy endpoint.
