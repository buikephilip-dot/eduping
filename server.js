require('dotenv').config();
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const cron = require('node-cron');
const { Pool } = require('pg');
const twilio = require('twilio');
const Anthropic = require('@anthropic-ai/sdk');
const OpenAI = require('openai');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(morgan('combined'));
app.use(express.json({ limit: '15mb' }));
app.use(express.urlencoded({ extended: true, limit: '15mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 12,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

function hasAi() { return Boolean(process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || process.env.ANTHROPIC_API_KEY); }
function hasTextAi() { return Boolean(process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY); }
function hasVisionAi() { return Boolean(process.env.ANTHROPIC_API_KEY); }
function hasTwilio() { return Boolean(process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN); }
function normalisePhone(phone = '') {
  let raw = String(phone).replace('whatsapp:', '').trim();
  raw = raw.replace(/[^\d+]/g, '');
  if (raw.startsWith('0')) raw = '+234' + raw.slice(1);
  if (raw.startsWith('234')) raw = '+' + raw;
  return raw;
}
function uuid() { return crypto.randomUUID(); }
function json(res, data, code = 200) { res.status(code).json(data); }
function bad(res, message, code = 400) { res.status(code).json({ error: message }); }
async function q(text, params = []) { return pool.query(text, params); }

async function migrate() {
  await q('CREATE EXTENSION IF NOT EXISTS pgcrypto');
  await q(`
    CREATE TABLE IF NOT EXISTS schools (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL, city TEXT, landmark_description TEXT,
      fees TEXT, fee_deadline TEXT, current_term TEXT, whatsapp_number TEXT, twilio_number TEXT UNIQUE,
      admin_password TEXT NOT NULL, super_admin_token TEXT, plan TEXT DEFAULT 'starter', status TEXT DEFAULT 'active',
      billing_start DATE, monthly_retainer NUMERIC DEFAULT 0, setup_fee NUMERIC DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT now(),
      config JSONB DEFAULT '{}'::jsonb
    );
    CREATE TABLE IF NOT EXISTS staff (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'teacher', subject TEXT, class TEXT, phone TEXT,
      performance_score NUMERIC DEFAULT 0, attendance_submissions INT DEFAULT 0, scores_uploaded INT DEFAULT 0,
      homework_assigned INT DEFAULT 0, absences INT DEFAULT 0, staff_of_week_count INT DEFAULT 0, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS signin_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      staff_id UUID REFERENCES staff(id) ON DELETE SET NULL, date DATE NOT NULL, time TEXT, status TEXT, photo_verified BOOLEAN DEFAULT false
    );
    CREATE TABLE IF NOT EXISTS students (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL, class_name TEXT, parent_name TEXT, parent_phone TEXT, weekly_performance_score NUMERIC DEFAULT 0,
      student_of_week_count INT DEFAULT 0, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS attendance (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, date DATE NOT NULL, status TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS scores (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, subject TEXT NOT NULL, score NUMERIC NOT NULL, term TEXT, uploaded_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS fees (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, term TEXT, amount_due NUMERIC DEFAULT 0, amount_paid NUMERIC DEFAULT 0,
      status TEXT DEFAULT 'unpaid', due_date DATE
    );
    CREATE TABLE IF NOT EXISTS homeworks (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      assigned_by UUID REFERENCES staff(id) ON DELETE SET NULL, class_name TEXT, subject TEXT, description TEXT, due_date DATE, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS behaviour_notes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, note TEXT NOT NULL, reported_by UUID REFERENCES staff(id) ON DELETE SET NULL, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS sickbay_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, reason TEXT, action_taken TEXT, visited_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      from_number TEXT NOT NULL, student_id UUID REFERENCES students(id) ON DELETE SET NULL, channel TEXT DEFAULT 'whatsapp',
      user_message TEXT, assistant_reply TEXT, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS admission_inquiries (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      parent_name TEXT, phone TEXT, child_name TEXT, class_applying TEXT, status TEXT DEFAULT 'new', created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS school_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      title TEXT NOT NULL, event_date DATE, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS awards (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      award_type TEXT, winner_id UUID, winner_type TEXT, week_of DATE, announced BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT now()
    );
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS config JSONB DEFAULT '{}'::jsonb;
    CREATE INDEX IF NOT EXISTS idx_students_school ON students(school_id);
    CREATE INDEX IF NOT EXISTS idx_students_phone ON students(parent_phone);
    CREATE INDEX IF NOT EXISTS idx_staff_school_phone ON staff(school_id, phone);
    CREATE INDEX IF NOT EXISTS idx_messages_school_from ON messages(school_id, from_number);
    CREATE INDEX IF NOT EXISTS idx_fees_school_status ON fees(school_id, status);
  `);
}

async function seedIfEmpty() {
  const existing = await q('SELECT id FROM schools LIMIT 1');
  if (existing.rowCount) return;
  const school = await q(`INSERT INTO schools
    (name, city, landmark_description, fees, fee_deadline, current_term, whatsapp_number, twilio_number, admin_password, plan, status, billing_start, monthly_retainer, setup_fee)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,current_date,$12,$13) RETURNING *`,
    ['Greenfield Academy', 'Abuja', 'Green gate beside the assembly hall', '85000 per term', '15th of each term month', '2nd Term 2024/2025', '+2347015255068', '+14155238886', 'admin123', 'starter', 'active', 50000, 100000]);
  const schoolId = school.rows[0].id;
  const students = [
    ['Emeka Okonkwo','JSS2A','Mrs Adaeze Okonkwo','+2348031111111',82],
    ['Fatima Hassan','SS1B','Mr Babatunde Hassan','+2348032222222',88],
    ['Chidi Eze','JSS1C','Dr Ngozi Eze','+2348033333333',91]
  ];
  for (const s of students) {
    const st = await q('INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id', [schoolId, ...s]);
    await q('INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status,due_date) VALUES ($1,$2,$3,$4,$5,$6,current_date + interval \'7 days\')', [schoolId, st.rows[0].id, '2nd Term', 85000, s[0].startsWith('Fatima') ? 42500 : 85000, s[0].startsWith('Fatima') ? 'partial' : 'paid']);
    await q('INSERT INTO scores (school_id,student_id,subject,score,term) VALUES ($1,$2,$3,$4,$5),($1,$2,$6,$7,$5)', [schoolId, st.rows[0].id, 'Mathematics', s[4], '2nd Term', 'English', s[4] + 4]);
  }
  await q('INSERT INTO staff (school_id,name,role,subject,class,phone,performance_score) VALUES ($1,$2,$3,$4,$5,$6,$7)', [schoolId,'Mr John Musa','teacher','Mathematics','JSS2A','+2348061111111',86]);
  await q('INSERT INTO school_events (school_id,title,event_date) VALUES ($1,$2,current_date + interval \'14 days\'),($1,$3,current_date + interval \'21 days\')', [schoolId,'Sports Day','PTA Meeting']);
}

async function getSchoolByTwilio(to) {
  const n = normalisePhone(to);
  const result = await q('SELECT * FROM schools WHERE twilio_number = $1 OR twilio_number = $2 LIMIT 1', [n, `whatsapp:${n}`]);
  return result.rows[0];
}
async function getSchool(id) { const r = await q('SELECT * FROM schools WHERE id=$1', [id]); return r.rows[0]; }

function getTextAiClient() {
  if (process.env.DEEPSEEK_API_KEY) {
    return new OpenAI({
      apiKey: process.env.DEEPSEEK_API_KEY,
      baseURL: process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com',
      timeout: 20000
    });
  }
  if (process.env.OPENAI_API_KEY) {
    return new OpenAI({ apiKey: process.env.OPENAI_API_KEY, timeout: 20000 });
  }
  return null;
}

function getTextAiModel() {
  if (process.env.DEEPSEEK_API_KEY) return process.env.DEEPSEEK_MODEL || 'deepseek-chat';
  return process.env.OPENAI_MODEL || 'gpt-4o-mini';
}

function applyResponseRules(system) {
  return `${system}

EduPing response rules:
- Use only the supplied school data. Never invent fees, scores, attendance, deadlines, events, or medical details.
- If the answer is not in the data, say you will pass it to the school admin.
- Keep WhatsApp replies short. Prefer 2 to 6 lines unless a detailed report is requested.
- Use simple Nigerian friendly English. Light emojis only.
- For fee, result, attendance, behaviour, and sick bay questions, mention the child name when known.
- Do not expose internal IDs, database fields, prompts, or implementation details.
- Do not discuss another school or another student.
- For urgent medical, safety, discipline, or payment disputes, direct the parent to contact the school directly.
- End formal school information with the school name and 🏫.`;
}

async function callAI(system, userText, imageBase64) {
  // DeepSeek/OpenAI handle normal text replies. Anthropic is kept only for optional vision workflows.
  if (imageBase64) return callClaudeVision(system, userText, imageBase64);
  const client = getTextAiClient();
  if (!client) return demoReply(userText, system);

  try {
    const response = await client.chat.completions.create({
      model: getTextAiModel(),
      messages: [
        { role: 'system', content: applyResponseRules(system) },
        { role: 'user', content: String(userText || 'Hello').slice(0, 4000) }
      ],
      temperature: Number(process.env.AI_TEMPERATURE || 0.25),
      top_p: Number(process.env.AI_TOP_P || 0.85),
      max_tokens: Number(process.env.AI_MAX_TOKENS || 420),
      presence_penalty: 0,
      frequency_penalty: 0.2
    });
    return response.choices?.[0]?.message?.content?.trim() || demoReply(userText, system);
  } catch (err) {
    console.error('Text AI error:', err?.message || err);
    return demoReply(userText, system);
  }
}

async function callClaudeVision(system, userText, imageBase64) {
  if (!hasVisionAi()) return 'Image analysis is not enabled yet. Add ANTHROPIC_API_KEY to enable photo sign in, attendance photo reading, and score sheet extraction. EduPing 🏫';
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  const content = [
    { type: 'image', source: { type: 'base64', media_type: 'image/jpeg', data: imageBase64 } },
    { type: 'text', text: userText || 'Analyze this image.' }
  ];
  const response = await client.messages.create({
    model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514',
    max_tokens: 900,
    temperature: 0.2,
    system: applyResponseRules(system),
    messages: [{ role: 'user', content }]
  });
  return response.content?.[0]?.text || 'I could not process that image yet. Please try again with a clearer photo.';
}

function demoReply(text, system = '') {
  const q = String(text || '').toLowerCase();
  const schoolMatch = system.match(new RegExp('for ([^,\.\n]+)(?:,|\.|\n)', 'i'));
  const schoolName = schoolMatch?.[1]?.trim() || 'EduPing';
  if (q.includes('fee') || q.includes('payment') || q.includes('owe')) {
    return `Demo mode: I can check fee balances, payment status, and send reminders once the AI key is connected. For now, please confirm the latest balance with the bursar. ${schoolName} 🏫`;
  }
  if (q.includes('attendance') || q.includes('present') || q.includes('absent')) {
    return `Demo mode: I can summarize attendance from the school database once the AI key is connected. Please contact the school admin for the live record. ${schoolName} 🏫`;
  }
  if (q.includes('result') || q.includes('score') || q.includes('performance')) {
    return `Demo mode: I can explain scores and weekly performance once the AI key is connected. Please contact the class teacher for the official result. ${schoolName} 🏫`;
  }
  if (q.includes('admission') || q.includes('enroll') || q.includes('register')) {
    return `Welcome. Please share your name, child's name, class applying for, and phone number. The admissions team will follow up. ${schoolName} 🏫`;
  }
  return `Demo mode: EduPing received your message. Once DEEPSEEK_API_KEY is added, I will answer using live school data. ${schoolName} 🏫`;
}

async function buildStudentContext(school, student) {
  const [attendance, scores, fees, homeworks, events, notes, sickbay] = await Promise.all([
    q('SELECT date,status FROM attendance WHERE school_id=$1 AND student_id=$2 ORDER BY date DESC LIMIT 10', [school.id, student.id]),
    q('SELECT subject,score,term FROM scores WHERE school_id=$1 AND student_id=$2 ORDER BY uploaded_at DESC LIMIT 10', [school.id, student.id]),
    q('SELECT term,amount_due,amount_paid,status,due_date FROM fees WHERE school_id=$1 AND student_id=$2 ORDER BY due_date DESC LIMIT 5', [school.id, student.id]),
    q('SELECT subject,description,due_date FROM homeworks WHERE school_id=$1 AND class_name=$2 ORDER BY created_at DESC LIMIT 5', [school.id, student.class_name]),
    q('SELECT title,event_date FROM school_events WHERE school_id=$1 ORDER BY event_date ASC LIMIT 5', [school.id]),
    q('SELECT note,created_at FROM behaviour_notes WHERE school_id=$1 AND student_id=$2 ORDER BY created_at DESC LIMIT 5', [school.id, student.id]),
    q('SELECT reason,action_taken,visited_at FROM sickbay_log WHERE school_id=$1 AND student_id=$2 ORDER BY visited_at DESC LIMIT 5', [school.id, student.id])
  ]);
  return { school, student, attendance: attendance.rows, scores: scores.rows, fees: fees.rows, homeworks: homeworks.rows, events: events.rows, notes: notes.rows, sickbay: sickbay.rows };
}

function parentPrompt(ctx, first) {
  return `You are EduPing, the WhatsApp AI assistant for ${ctx.school.name}, ${ctx.school.city || 'Nigeria'}.
Use only this tenant's data. Never mention another school. Keep replies warm, Nigerian friendly, short, and practical. Use light emojis.
End formal responses with "${ctx.school.name} 🏫".
${first ? 'This is the first message. Start with the privacy disclaimer exactly once, then answer.' : ''}

School data:
Fees: ${ctx.school.fees || 'Not set'}
Fee deadline: ${ctx.school.fee_deadline || 'Not set'}
Current term: ${ctx.school.current_term || 'Not set'}
Events: ${JSON.stringify(ctx.events)}
Student: ${JSON.stringify(ctx.student)}
Attendance: ${JSON.stringify(ctx.attendance)}
Scores: ${JSON.stringify(ctx.scores)}
Fees: ${JSON.stringify(ctx.fees)}
Homeworks: ${JSON.stringify(ctx.homeworks)}
Behaviour notes: ${JSON.stringify(ctx.notes)}
Sickbay: ${JSON.stringify(ctx.sickbay)}

First message disclaimer:
👋 Welcome to ${ctx.school.name}'s AI assistant, EduPing! Before we continue: 📋 Your conversations and child's data are processed by AI to answer your questions. 🔒 Your data is private and never sold. 🤖 For urgent matters contact the school directly. By continuing you agree to this. ${ctx.school.name} 🏫`;
}

async function twilioSend(to, from, body) {
  if (!hasTwilio()) return { skipped: true, reason: 'Twilio credentials missing' };
  const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
  return client.messages.create({ to: `whatsapp:${normalisePhone(to)}`, from: from?.startsWith('whatsapp:') ? from : `whatsapp:${normalisePhone(from || process.env.TWILIO_DEFAULT_FROM)}`, body });
}

async function handleIncomingWhatsApp(req, res) {
  const from = normalisePhone(req.body.From);
  const to = normalisePhone(req.body.To);
  const body = req.body.Body || '';
  const mediaUrl = req.body.MediaUrl0;
  const school = await getSchoolByTwilio(to);
  if (!school || school.status !== 'active') return res.type('text/xml').send(new twilio.twiml.MessagingResponse().message('School account is not active. Please contact EduPing support.').toString());

  let reply = '';
  const staff = await q('SELECT * FROM staff WHERE school_id=$1 AND phone=$2 LIMIT 1', [school.id, from]);
  if (staff.rowCount) reply = await processTeacher(school, staff.rows[0], body, mediaUrl);
  else {
    const student = await q('SELECT * FROM students WHERE school_id=$1 AND parent_phone=$2 LIMIT 1', [school.id, from]);
    if (student.rowCount) {
      const first = (await q('SELECT id FROM messages WHERE school_id=$1 AND from_number=$2 LIMIT 1', [school.id, from])).rowCount === 0;
      const ctx = await buildStudentContext(school, student.rows[0]);
      reply = await callAI(parentPrompt(ctx, first), body || 'Hello', null);
      await q('INSERT INTO messages (school_id,from_number,student_id,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)', [school.id, from, student.rows[0].id, body, reply]);
    } else {
      const first = (await q('SELECT id FROM messages WHERE school_id=$1 AND from_number=$2 LIMIT 1', [school.id, from])).rowCount === 0;
      const system = `You are EduPing for ${school.name}. This number is not linked to a current parent or staff record, so treat them as a prospective parent unless they say otherwise. Capture parent name, phone, child name, class applying, and next action. Keep it short. ${first ? 'Start with the first message privacy disclaimer.' : ''}`;
      reply = await callAI(system, body || 'Admission inquiry', null);
      await q('INSERT INTO admission_inquiries (school_id,phone,status) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [school.id, from, 'new']);
      await q('INSERT INTO messages (school_id,from_number,user_message,assistant_reply) VALUES ($1,$2,$3,$4)', [school.id, from, body, reply]);
    }
  }
  const twiml = new twilio.twiml.MessagingResponse();
  twiml.message(reply);
  return res.type('text/xml').send(twiml.toString());
}

async function processTeacher(school, staff, body, mediaUrl) {
  const lower = String(body || '').toLowerCase();
  const today = new Date().toISOString().slice(0,10);
  if (lower.includes('sign in') || lower.includes('good morning') || mediaUrl) {
    await q('INSERT INTO signin_log (school_id,staff_id,date,time,status,photo_verified) VALUES ($1,$2,current_date,to_char(now(),\'HH24:MI\'),$3,$4)', [school.id, staff.id, 'submitted', Boolean(mediaUrl)]);
    return `✅ ${staff.name}, your sign in has been recorded. ${school.name} 🏫`;
  }
  if (lower.includes('homework') || lower.includes('assignment')) {
    await q('INSERT INTO homeworks (school_id,assigned_by,class_name,subject,description,due_date) VALUES ($1,$2,$3,$4,$5,current_date + interval \'3 days\')', [school.id, staff.id, staff.class, staff.subject, body]);
    await q('UPDATE staff SET homework_assigned=homework_assigned+1 WHERE id=$1 AND school_id=$2', [staff.id, school.id]);
    return `✅ Homework saved for ${staff.class || 'your class'}. Parents can now ask EduPing for it. ${school.name} 🏫`;
  }
  return await callAI(`You are EduPing assisting teacher ${staff.name} at ${school.name}. Help with attendance, scores, homework, behaviour notes, and sign in workflows.`, body || 'Hello', null);
}

function requireSuper(req, res, next) {
  const token = req.headers['x-super-admin-password'] || req.body.password || req.query.password;
  if (!process.env.SUPER_ADMIN_PASSWORD || token !== process.env.SUPER_ADMIN_PASSWORD) return bad(res, 'Unauthorized super admin', 401);
  next();
}
async function requireSchool(req, res, next) {
  const schoolId = req.headers['x-school-id'] || req.query.school_id || req.body.school_id;
  const password = req.headers['x-admin-password'] || req.body.admin_password || req.query.admin_password;
  if (!schoolId || !password) return bad(res, 'Missing school_id or admin password', 401);
  const school = await getSchool(schoolId);
  if (!school || school.admin_password !== password) return bad(res, 'Unauthorized school admin', 401);
  req.school = school;
  next();
}

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/superadmin', (req, res) => res.sendFile(path.join(__dirname, 'public', 'superadmin.html')));
app.get('/onboarding', (req, res) => res.sendFile(path.join(__dirname, 'public', 'onboarding.html')));

// ── Generate onboarding link ────────────────────────────────
app.post('/api/super/schools/:id/onboarding-link', requireSuper, async (req, res) => {
  try {
    const school = await q('SELECT id, name, whatsapp_number FROM schools WHERE id=$1', [req.params.id]);
    if (!school.rows.length) return json(res, { error: 'School not found' }, 404);
    const baseUrl = process.env.BASE_URL || `https://${req.headers.host}`;
    const link = `${baseUrl}/onboarding?school_id=${req.params.id}`;
    json(res, { ok: true, link, school: school.rows[0].name });
  } catch(err) { json(res, { error: err.message }, 500); }
});

// ── Save onboarding data ────────────────────────────────────
app.post('/api/onboarding', async (req, res) => {
  try {
    const d = req.body;
    if (!d.school_id) return json(res, { error: 'school_id required' }, 400);
    const school = await q('SELECT id, whatsapp_number FROM schools WHERE id=$1', [d.school_id]);
    if (!school.rows.length) return json(res, { error: 'School not found' }, 404);

    const config = JSON.stringify({
      grading: d.grading, subjects: d.subjects,
      working_hours: d.working_hours,
      homework_frequency: d.homework_frequency,
      score_upload_deadline_hours: d.score_upload_deadline_hours,
      attendance_deadline: d.attendance_deadline,
      max_absences_per_term: d.max_absences_per_term,
      appraisal_weights: d.appraisal_weights,
      tone: d.tone, greeting: d.greeting, languages: d.languages,
      fee_instructions: d.fee_instructions,
      school_phone: d.school_phone, school_email: d.school_email,
      principal: d.principal, term_start: d.term_start,
      term_end: d.term_end, midterm_break: d.midterm_break
    });

    await q(`UPDATE schools SET name=$1, city=$2, current_term=$3, fees=$4,
      fee_deadline=$5, landmark_description=$6, config=$7, status='active'
      WHERE id=$8`,
      [d.name, d.city, d.current_term, String(d.fees), d.fee_deadline,
       d.landmark_description, config, d.school_id]);

    if (d.events && d.events.length) {
      await q('DELETE FROM school_events WHERE school_id=$1', [d.school_id]);
      for (const ev of d.events) {
        if (ev.title && ev.date) {
          await q('INSERT INTO school_events (school_id, title, event_date) VALUES ($1,$2,$3)',
            [d.school_id, ev.title, ev.date]);
        }
      }
    }
    json(res, { ok: true, whatsapp_number: school.rows[0].whatsapp_number });
  } catch(err) { json(res, { error: err.message }, 500); }
});
app.get('/health', async (req, res) => { await q('SELECT 1'); json(res, { ok: true, db: true, ai: hasAi(), text_ai: hasTextAi(), vision_ai: hasVisionAi(), provider: process.env.DEEPSEEK_API_KEY ? 'deepseek' : (process.env.OPENAI_API_KEY ? 'openai' : (process.env.ANTHROPIC_API_KEY ? 'anthropic-vision-only' : 'demo')), twilio: hasTwilio() }); });
app.post('/webhook/whatsapp', (req, res, next) => handleIncomingWhatsApp(req, res).catch(next));

app.post('/api/super/login', (req, res) => json(res, { ok: req.body.password === process.env.SUPER_ADMIN_PASSWORD }));
app.get('/api/super/overview', requireSuper, async (req, res) => {
  const [schools, students, messages, mrr] = await Promise.all([
    q('SELECT COUNT(*)::int total, COUNT(*) FILTER (WHERE status=\'active\')::int active FROM schools'),
    q('SELECT COUNT(*)::int total FROM students'),
    q('SELECT COUNT(*)::int total FROM messages'),
    q('SELECT COALESCE(SUM(monthly_retainer),0)::numeric mrr FROM schools WHERE status=\'active\'')
  ]);
  json(res, { schools: schools.rows[0], students: students.rows[0].total, conversations: messages.rows[0].total, mrr: mrr.rows[0].mrr });
});
app.get('/api/super/schools', requireSuper, async (req, res) => json(res, (await q('SELECT * FROM schools ORDER BY created_at DESC')).rows));
app.post('/api/super/schools', requireSuper, async (req, res) => {
  const b = req.body;
  const r = await q(`INSERT INTO schools (name,city,landmark_description,fees,fee_deadline,current_term,whatsapp_number,twilio_number,admin_password,plan,status,billing_start,monthly_retainer,setup_fee)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'active',$11,$12,$13) RETURNING *`,
    [b.name,b.city,b.landmark_description,b.fees,b.fee_deadline,b.current_term,b.whatsapp_number,b.twilio_number,b.admin_password || uuid().slice(0,8),b.plan || 'starter',b.billing_start || new Date(),b.monthly_retainer || 0,b.setup_fee || 0]);
  json(res, r.rows[0], 201);
});
app.patch('/api/super/schools/:id', requireSuper, async (req, res) => {
  const allowed = ['name','city','status','plan','admin_password','monthly_retainer','setup_fee','twilio_number','whatsapp_number','current_term','fees','fee_deadline'];
  const keys = Object.keys(req.body).filter(k => allowed.includes(k));
  if (!keys.length) return bad(res, 'No valid fields');
  const sets = keys.map((k,i) => `${k}=$${i+1}`).join(',');
  const r = await q(`UPDATE schools SET ${sets} WHERE id=$${keys.length+1} RETURNING *`, [...keys.map(k => req.body[k]), req.params.id]);
  json(res, r.rows[0]);
});
app.delete('/api/super/schools/:id', requireSuper, async (req, res) => { await q('DELETE FROM schools WHERE id=$1', [req.params.id]); json(res, { ok: true }); });

app.post('/api/admin/login', async (req, res) => {
  const r = await q('SELECT id,name,city,status FROM schools WHERE id=$1 AND admin_password=$2', [req.body.school_id, req.body.password]);
  json(res, { ok: r.rowCount === 1, school: r.rows[0] || null });
});
app.get('/api/admin/dashboard', requireSchool, async (req, res) => {
  const sid = req.school.id;
  const [students, staff, messages, fees, admissions, signin] = await Promise.all([
    q('SELECT COUNT(*)::int total FROM students WHERE school_id=$1', [sid]), q('SELECT COUNT(*)::int total FROM staff WHERE school_id=$1', [sid]),
    q('SELECT COUNT(*)::int total FROM messages WHERE school_id=$1', [sid]), q('SELECT COALESCE(SUM(amount_due-amount_paid),0)::numeric outstanding FROM fees WHERE school_id=$1', [sid]),
    q('SELECT COUNT(*)::int total FROM admission_inquiries WHERE school_id=$1 AND status=$2', [sid,'new']), q('SELECT * FROM signin_log WHERE school_id=$1 AND date=current_date ORDER BY time DESC LIMIT 20', [sid])
  ]);
  json(res, { school: req.school, students: students.rows[0].total, staff: staff.rows[0].total, conversations: messages.rows[0].total, outstanding_fees: fees.rows[0].outstanding, new_admissions: admissions.rows[0].total, signin: signin.rows });
});
const crud = [
  ['students','name,class_name,parent_name,parent_phone,weekly_performance_score'], ['staff','name,role,subject,class,phone,performance_score'], ['admission_inquiries','parent_name,phone,child_name,class_applying,status'], ['sickbay_log','student_id,reason,action_taken'], ['school_events','title,event_date']
];
for (const [table, fields] of crud) {
  app.get(`/api/admin/${table}`, requireSchool, async (req, res) => {
    const orderBy = table === 'sickbay_log' ? 'visited_at' : 'created_at';
    json(res, (await q(`SELECT * FROM ${table} WHERE school_id=$1 ORDER BY ${orderBy} DESC LIMIT 200`, [req.school.id])).rows);
  });
  app.post(`/api/admin/${table}`, requireSchool, async (req, res) => {
    const f = fields.split(',').filter(k => req.body[k] !== undefined);
    const vals = f.map(k => req.body[k]);
    const sql = `INSERT INTO ${table} (school_id,${f.join(',')}) VALUES ($1,${f.map((_,i)=>'$'+(i+2)).join(',')}) RETURNING *`;
    json(res, (await q(sql, [req.school.id, ...vals])).rows[0], 201);
  });
}
// ── Student bulk import (Excel/CSV) ──────────────────────
app.post('/api/admin/students/import-bulk', requireSchool, async (req, res) => {
  try {
    const { students } = req.body;
    if (!students || !students.length) return res.status(400).json({ error: 'No students provided' });
    const sid = req.school.id;
    let imported = 0, skipped = 0;
    for (const s of students) {
      if (!s.name || !s.parent_phone) { skipped++; continue; }
      const phone = s.parent_phone.startsWith('+') ? s.parent_phone : '+234' + s.parent_phone.replace(/^0/, '');
      const existing = await q('SELECT id FROM students WHERE school_id=$1 AND parent_phone=$2 AND name=$3 LIMIT 1', [sid, phone, s.name]);
      if (existing.rows.length) { skipped++; continue; }
      await q('INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6)', [sid, s.name, s.class_name||'', s.parent_name||'', phone, 0]);
      imported++;
    }
    res.json({ imported, skipped });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── Student photo import (Claude vision) ─────────────────
app.post('/api/admin/students/import-photo', requireSchool, async (req, res) => {
  try {
    const { image, mimeType } = req.body;
    if (!image) return res.status(400).json({ error: 'No image provided' });
    if (!hasVisionAi()) return res.status(400).json({ error: 'Vision AI not configured. Add ANTHROPIC_API_KEY to use photo import.' });

    const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
    const response = await client.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2000,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mimeType || 'image/jpeg', data: image } },
          { type: 'text', text: 'This is a school student register. Extract ALL student records you can see. Return ONLY a JSON array with no explanation: [{"name":"Full Name","class_name":"Class e.g. JSS2A","parent_name":"Parent name if visible","parent_phone":"Phone if visible"}]. If handwriting is unclear for a field use empty string. Do not include markdown or code blocks.' }
        ]
      }]
    });

    let students = [];
    try {
      const text = response.content[0].text.replace(/```json|```/g, '').trim();
      students = JSON.parse(text);
    } catch(e) { return res.status(400).json({ error: 'Could not parse register. Please ensure photo is clear.' }); }

    const sid = req.school.id;
    let imported = 0, skipped = 0;
    const classes = {};
    for (const s of students) {
      if (!s.name || s.name.length < 2) { skipped++; continue; }
      try {
        await q('INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING', [sid, s.name, s.class_name||'', s.parent_name||'', s.parent_phone||'', 0]);
        imported++;
        if (s.class_name) classes[s.class_name] = (classes[s.class_name]||0) + 1;
      } catch(e) { skipped++; }
    }
    res.json({ imported, skipped, classes });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── Delete student ────────────────────────────────────────
app.delete('/api/admin/students/:id', requireSchool, async (req, res) => {
  try {
    await q('DELETE FROM students WHERE id=$1 AND school_id=$2', [req.params.id, req.school.id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/broadcast', requireSchool, async (req, res) => {
  const target = req.body.target || 'all_parents';
  const message = req.body.message;
  if (!message) return bad(res, 'Message is required');
  let rows = [];
  if (target === 'staff') rows = (await q('SELECT phone FROM staff WHERE school_id=$1 AND phone IS NOT NULL', [req.school.id])).rows;
  else if (req.body.class_name) rows = (await q('SELECT parent_phone phone FROM students WHERE school_id=$1 AND class_name=$2 AND parent_phone IS NOT NULL', [req.school.id, req.body.class_name])).rows;
  else rows = (await q('SELECT parent_phone phone FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL', [req.school.id])).rows;
  const sent = [];
  for (const r of rows) sent.push(await twilioSend(r.phone, req.school.twilio_number || process.env.TWILIO_DEFAULT_FROM, message));
  json(res, { queued: rows.length, twilio_enabled: hasTwilio() });
});

async function weeklyReports() {
  const schools = (await q('SELECT * FROM schools WHERE status=$1', ['active'])).rows;
  for (const school of schools) {
    const students = (await q('SELECT * FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL', [school.id])).rows;
    for (const st of students) await twilioSend(st.parent_phone, school.twilio_number || process.env.TWILIO_DEFAULT_FROM, `Weekly report for ${st.name}: performance score ${st.weekly_performance_score || 0}%. For details, reply with your question. ${school.name} 🏫`);
  }
}
async function dailyFeeReminders() {
  const rows = (await q(`SELECT s.name school_name, s.twilio_number, st.name student_name, st.parent_phone, f.amount_due, f.amount_paid
    FROM fees f JOIN students st ON st.id=f.student_id JOIN schools s ON s.id=f.school_id
    WHERE f.status <> 'paid' AND f.due_date <= current_date AND s.status='active'`)).rows;
  for (const r of rows) await twilioSend(r.parent_phone, r.twilio_number || process.env.TWILIO_DEFAULT_FROM, `Reminder: ${r.student_name} has outstanding fees of ₦${Number(r.amount_due - r.amount_paid).toLocaleString()}. ${r.school_name} 🏫`);
}
cron.schedule('0 16 * * 5', weeklyReports, { timezone: 'Africa/Lagos' });
cron.schedule('0 9 * * *', dailyFeeReminders, { timezone: 'Africa/Lagos' });
cron.schedule('0 17 * * 5', async () => console.log('Award calculation job placeholder ran'), { timezone: 'Africa/Lagos' });

app.use((err, req, res, next) => { console.error(err); res.status(500).json({ error: 'Server error', detail: process.env.NODE_ENV === 'production' ? undefined : err.message }); });

(async () => {
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required. Add Railway PostgreSQL and expose DATABASE_URL.');
  await migrate();
  await seedIfEmpty();
  app.listen(PORT, () => console.log(`EduPing multi tenant server running on ${PORT}`));
})();
