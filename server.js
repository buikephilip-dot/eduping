require('dotenv').config();
const path = require('path');
const fs = require('fs/promises');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const crypto = require('crypto');
const twilio = require('twilio');
const OpenAI = require('openai');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'data', 'db.json');

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'public')));

async function readDb() {
  const raw = await fs.readFile(DB_PATH, 'utf8');
  return JSON.parse(raw);
}

async function writeDb(db) {
  await fs.writeFile(DB_PATH, JSON.stringify(db, null, 2));
}

function findStudentForMessage(db, text = '') {
  const lower = text.toLowerCase();
  return db.students.find(s => lower.includes(s.name.toLowerCase().split(' ')[0])) || db.students[0];
}

function buildSchoolContext(db, student) {
  return `
School: ${db.school.name}, ${db.school.city}
Current term: ${db.school.currentTerm}
School fees: ${db.school.fees}
Fee deadline: ${db.school.feeDeadline}
Events: ${db.school.events.map(e => `${e.title}: ${e.date}`).join('; ')}

Selected student:
Name: ${student.name}
Class: ${student.className}
Parent: ${student.parentName}
Attendance this week: ${student.attendanceThisWeek}
Attendance this term: ${student.attendanceTerm}
Weekly attendance: ${Object.entries(student.weeklyAttendance).map(([k, v]) => `${k}: ${v}`).join(', ')}
Scores: ${Object.entries(student.scores).map(([k, v]) => `${k}: ${v}`).join(', ')}
Fees: ${student.fees.status}, outstanding ${student.fees.outstanding}
`.trim();
}

function systemPrompt(context) {
  return `You are EduPing, a friendly and professional WhatsApp AI assistant for a Nigerian school.
Use only the school data provided below. Do not invent student records, fees, dates, or policies.
Keep replies short, clear, Nigerian friendly, and useful. Use emojis lightly.
For formal school information, end with: Greenfield Academy 🏫
If the parent asks for something outside the available data, say you will pass it to the school admin.

${context}`;
}

async function callOpenAI(prompt, messages) {
  if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY is missing');
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const response = await client.chat.completions.create({
    model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
    messages: [
      { role: 'system', content: prompt },
      ...messages
    ],
    temperature: 0.4,
    max_tokens: 500
  });
  return response.choices?.[0]?.message?.content || 'Sorry, I could not process that.';
}

async function callAnthropic(prompt, messages) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY is missing');
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  const response = await client.messages.create({
    model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514',
    max_tokens: 500,
    system: prompt,
    messages
  });
  return response.content?.[0]?.text || 'Sorry, I could not process that.';
}

async function askAI(userMessage, history = []) {
  const db = await readDb();
  const student = findStudentForMessage(db, userMessage);
  const prompt = systemPrompt(buildSchoolContext(db, student));
  const safeHistory = Array.isArray(history)
    ? history.filter(m => ['user', 'assistant'].includes(m.role) && typeof m.content === 'string').slice(-10)
    : [];
  const messages = [...safeHistory, { role: 'user', content: userMessage }];
  const provider = (process.env.AI_PROVIDER || 'openai').toLowerCase();
  const reply = provider === 'anthropic'
    ? await callAnthropic(prompt, messages)
    : await callOpenAI(prompt, messages);

  db.messages.push({
    id: crypto.randomUUID ? crypto.randomUUID() : String(Date.now()),
    channel: 'web',
    studentId: student.id,
    userMessage,
    assistantReply: reply,
    createdAt: new Date().toISOString()
  });
  await writeDb(db);
  return { reply, student };
}

// Admin auth middleware
function checkAdmin(req, res, next) {
  const token = req.headers['x-admin-token'] || req.query.token;
  const password = process.env.ADMIN_PASSWORD || 'admin';
  if (token !== password) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

app.get('/api/health', (req, res) => {
  res.json({ ok: true, service: 'EduPing MVP', time: new Date().toISOString() });
});

// Admin login
app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  const adminPassword = process.env.ADMIN_PASSWORD || 'admin';
  if (password === adminPassword) {
    res.json({ success: true, token: adminPassword });
  } else {
    res.status(401).json({ error: 'Wrong password' });
  }
});

// Protected dashboard
app.get('/api/dashboard', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const outstanding = db.students.filter(s => s.fees.outstanding !== '₦0').length;
    res.json({
      school: db.school,
      stats: {
        students: db.students.length,
        conversations: db.messages.length,
        feeIssues: outstanding,
        admissionInquiries: 8
      },
      students: db.students
    });
  } catch (err) { next(err); }
});

app.get('/api/students/:id', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const student = db.students.find(s => s.id === req.params.id);
    if (!student) return res.status(404).json({ error: 'Student not found' });
    res.json(student);
  } catch (err) { next(err); }
});

app.post('/api/chat', async (req, res, next) => {
  try {
    const { message, history } = req.body;
    if (!message || typeof message !== 'string') return res.status(400).json({ error: 'message is required' });
    const result = await askAI(message, history);
    res.json({ reply: result.reply, student: result.student });
  } catch (err) { next(err); }
});

app.post('/api/broadcast', checkAdmin, async (req, res, next) => {
  try {
    const { message, to } = req.body;
    if (!message) return res.status(400).json({ error: 'message is required' });
    const db = await readDb();
    const recipients = Array.isArray(to) && to.length ? to : db.students.map(s => s.parentPhone);

    if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) {
      return res.json({ queued: true, testMode: true, count: recipients.length, note: 'Twilio credentials missing, simulated only.' });
    }

    const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    const sent = [];
    for (const recipient of recipients) {
      const msg = await client.messages.create({
        from: process.env.TWILIO_WHATSAPP_FROM,
        to: recipient,
        body: message
      });
      sent.push({ to: recipient, sid: msg.sid });
    }
    res.json({ queued: true, count: sent.length, sent });
  } catch (err) { next(err); }
});

app.post('/webhooks/twilio/whatsapp', async (req, res) => {
  try {
    const incoming = req.body.Body || '';
    const from = req.body.From || '';
    const { reply } = await askAI(incoming, []);
    const twiml = new twilio.twiml.MessagingResponse();
    twiml.message(reply);

    const db = await readDb();
    db.messages.push({ channel: 'whatsapp', from, userMessage: incoming, assistantReply: reply, createdAt: new Date().toISOString() });
    await writeDb(db);

    res.type('text/xml').send(twiml.toString());
  } catch (err) {
    const twiml = new twilio.twiml.MessagingResponse();
    twiml.message('Sorry, EduPing is having trouble right now. The school admin has been notified.');
    res.type('text/xml').send(twiml.toString());
  }
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: err.message || 'Server error' });
});

app.listen(PORT, () => console.log(`EduPing MVP running on http://localhost:${PORT}`));
