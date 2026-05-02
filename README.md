# EduPing Multi Tenant

EduPing is a multi tenant WhatsApp AI assistant and school management dashboard for Nigerian schools.

## What changed

This rebuild replaces the old db.json storage with PostgreSQL. One deployment now supports many schools. Every tenant owned record includes `school_id`, and all admin queries filter by `school_id`.

## Stack

Node.js, Express, PostgreSQL through `pg`, Anthropic Claude, Twilio WhatsApp, node cron, Railway.

## Railway setup

1. Push this folder to GitHub.
2. Create a Railway project from the repo.
3. Add Railway PostgreSQL.
4. Add these variables:

```env
DATABASE_URL=Railway PostgreSQL connection string
ANTHROPIC_API_KEY=your Anthropic key
TWILIO_ACCOUNT_SID=your Twilio SID
TWILIO_AUTH_TOKEN=your Twilio token
SUPER_ADMIN_PASSWORD=your strong password
NODE_ENV=production
```

5. Deploy.

The server runs migrations automatically on startup.

## Routes

```txt
GET /health
GET /superadmin
GET /
POST /webhook/whatsapp
```

## Super Admin

Open `/superadmin`. Use `SUPER_ADMIN_PASSWORD`.

You can add, suspend, activate, and remove schools.

## School Admin

Open `/`. Use the school id and the school admin password.

Seeded test school password is:

```txt
admin123
```

Get the school id from the Super Admin dashboard.

## Twilio webhook

In Twilio WhatsApp Sandbox or WhatsApp sender settings, set incoming message webhook to:

```txt
https://your-railway-domain.up.railway.app/webhook/whatsapp
```

The system identifies the school by `req.body.To`, matching the school's `twilio_number`.

## Data isolation

All tenant tables include `school_id`. Every school admin endpoint uses the authenticated `school_id`. Super Admin is the only role that can see all schools.

## Demo mode

If `ANTHROPIC_API_KEY` is missing, EduPing returns demo replies instead of crashing. This lets you show the interface before funding AI credits.

## Production notes

This is a stronger MVP, not a finished enterprise SIS. Before large scale usage, add password hashing, audit trails, file storage for media, paid billing webhooks, and proper role based user accounts.
