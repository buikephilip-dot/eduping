# EduPing DeepSeek setup

This build uses DeepSeek for normal EduPing text replies.

## Railway variables

Add these in Railway → Project → Variables:

```env
DEEPSEEK_API_KEY=your_deepseek_key_here
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat
AI_TEMPERATURE=0.25
AI_TOP_P=0.85
AI_MAX_TOKENS=420
```

Keep these too:

```env
DATABASE_URL=your_railway_postgres_url
SUPER_ADMIN_PASSWORD=your_secure_password
TWILIO_ACCOUNT_SID=your_twilio_sid
TWILIO_AUTH_TOKEN=your_twilio_auth_token
TWILIO_DEFAULT_FROM=whatsapp:+14155238886
```

## Optional vision features

DeepSeek text mode does not handle the photo workflows in this build.
For photo register import, score sheet extraction, and photo sign in analysis, add:

```env
ANTHROPIC_API_KEY=your_anthropic_key
ANTHROPIC_MODEL=claude-sonnet-4-20250514
```

If you do not add Anthropic, normal parent and teacher text chat still works through DeepSeek.

## Health check

Open:

```txt
/health
```

You should see:

```json
{
  "ok": true,
  "db": true,
  "ai": true,
  "text_ai": true,
  "provider": "deepseek"
}
```
