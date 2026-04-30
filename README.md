# EduPing MVP Backend

This MVP turns the uploaded EduPing frontend into a basic backend connected product prototype.

## What is included

1. Node.js and Express backend
2. Secure AI API calls from the server, not from the browser
3. Web chat endpoint
4. Dashboard endpoint
5. Student data endpoint
6. Twilio WhatsApp sandbox webhook
7. Broadcast endpoint for test messages
8. JSON file database for MVP testing

## Setup

```bash
npm install
cp .env.example .env
npm start
```

Open:

```text
http://localhost:3000
```

## Environment variables

Edit `.env`.

For OpenAI:

```env
AI_PROVIDER=openai
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4o-mini
```

For Anthropic:

```env
AI_PROVIDER=anthropic
ANTHROPIC_API_KEY=your_key_here
ANTHROPIC_MODEL=claude-3-5-sonnet-latest
```

## API routes

```text
GET  /api/health
GET  /api/dashboard
GET  /api/students/:id
POST /api/chat
POST /api/broadcast
POST /webhooks/twilio/whatsapp
```

## Twilio WhatsApp sandbox setup

1. Create a Twilio account
2. Open Messaging, then WhatsApp Sandbox
3. Join the sandbox from your phone using the code Twilio gives you
4. Use ngrok while testing locally:

```bash
ngrok http 3000
```

5. Copy the HTTPS ngrok URL into Twilio sandbox webhook:

```text
https://your-ngrok-url.ngrok-free.app/webhooks/twilio/whatsapp
```

6. Set the webhook method to POST

## Testing the web chat

Run the app, open the browser, then ask:

```text
What is Emeka's attendance this week?
```

The frontend now calls:

```text
POST /api/chat
```

instead of calling the AI provider directly from the browser.

## Testing broadcast

The dashboard broadcast button calls:

```text
POST /api/broadcast
```

If Twilio credentials are missing, it simulates the broadcast and returns test mode. If Twilio credentials are present, it sends to the parent WhatsApp numbers in `data/db.json`.

## Important MVP limitations

This is not production ready yet.

Before using with a real school, add:

1. Admin login
2. Parent login or verified WhatsApp identity
3. Real database, preferably Supabase or Firebase
4. Proper consent for parent messaging
5. WhatsApp approved templates for outbound production messages
6. Audit logs
7. Data privacy controls

