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
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const { initReporting } = require('./reporting');
const Sentry = require('@sentry/node');
const Queue = require('bull');

const app = express();
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;

// Sentry error tracking
if (process.env.SENTRY_DSN) {
  Sentry.init({ dsn: process.env.SENTRY_DSN, environment: process.env.NODE_ENV || 'production', tracesSampleRate: 0.1 });
}

// Message queues (requires Redis — falls back gracefully if not available)
let messageQueue, reportQueue;
try {
  messageQueue = new Queue('whatsapp-messages', process.env.REDIS_URL || 'redis://localhost:6379');
  reportQueue = new Queue('weekly-reports', process.env.REDIS_URL || 'redis://localhost:6379');

  messageQueue.process(async (job) => {
    const { to, from, body } = job.data;
    if (!hasTwilio()) return { skipped: true };
    await new Promise(r => setTimeout(r, Math.random() * 2000));
    const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    return client.messages.create({ to: `whatsapp:${normalisePhone(to)}`, from: from?.startsWith('whatsapp:') ? from : `whatsapp:${normalisePhone(from)}`, body });
  });

  reportQueue.process(async (job) => {
    await generateWeeklyReportForSchool(job.data.schoolId);
  });
  console.log('✅ Message queues initialized');
} catch(e) {
  console.log('⚠️ Queue not available (Redis not connected) — using direct sends');
  messageQueue = null; reportQueue = null;
}

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(morgan('combined'));
app.use(express.json({ limit: '15mb', verify: (req, res, buf) => { req.rawBody = buf; } }));
app.use(express.urlencoded({ extended: true, limit: '15mb' }));

const apiLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 300 });
app.use('/api/', apiLimiter);
const webhookLimiter = rateLimit({ windowMs: 60 * 1000, max: 30 });
app.use('/webhooks/', webhookLimiter);
if (process.env.SENTRY_DSN) app.use(Sentry.Handlers.requestHandler());
// Serve landing page at root — before static middleware
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'landing.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.use(express.static(path.join(__dirname, 'public')));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 25,                       // up from 12 — Railway supports 100+
  min: 2,                        // keep 2 warm connections always ready
  idleTimeoutMillis: 30000,      // release idle connections after 30s
  connectionTimeoutMillis: 5000, // fail fast if pool exhausted
  statement_timeout: 8000,       // kill queries running over 8s
  query_timeout: 8000,           // client-side query timeout
  application_name: 'eduping',   // visible in pg_stat_activity for debugging
});

// ── Per-tenant query rate limiting ────────────────────────
const _schoolQueryCount = {};
function rateLimitDB(schoolId) {
  if (!schoolId) return;
  const key = schoolId + ':' + Math.floor(Date.now() / 60000);
  _schoolQueryCount[key] = (_schoolQueryCount[key] || 0) + 1;
  if (_schoolQueryCount[key] % 1000 === 0) {
    const now = Math.floor(Date.now() / 60000);
    Object.keys(_schoolQueryCount).forEach(k => {
      if (parseInt(k.split(':')[1]) < now - 2) delete _schoolQueryCount[k];
    });
  }
  if (_schoolQueryCount[key] > 150) {
    console.warn('[RateLimit] School ' + schoolId + ' exceeded 150 DB queries/min');
    throw new Error('Too many requests — please try again in a moment');
  }
}

// ── Pool health monitoring ────────────────────────────────
pool.on('error', (err) => {
  console.error('[Pool] Unexpected error on idle client:', err.message);
});

setInterval(() => {
  const { totalCount, idleCount, waitingCount } = pool;
  if (waitingCount > 0) {
    console.warn('[Pool] ' + waitingCount + ' queries waiting — total:' + totalCount + ' idle:' + idleCount);
  }
}, 30000);

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
function createToken(payload) {
  return jwt.sign(payload, process.env.JWT_SECRET || 'eduping-secret', { expiresIn: '7d' });
}
function authMiddleware(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    req.user = jwt.verify(token, process.env.JWT_SECRET || 'eduping-secret');
    next();
  } catch (e) {
    return res.status(401).json({ error: 'Invalid token' });
  }
}

function json(res, data, code = 200) { res.status(code).json(data); }
function bad(res, message, code = 400) { res.status(code).json({ error: message }); }
async function q(text, params = []) { return pool.query(text, params); }

async function migrate() {
  await q('CREATE EXTENSION IF NOT EXISTS pgcrypto');

  // Drop old unique constraint on twilio_number if it exists (allows empty values)
  await q(`ALTER TABLE schools DROP CONSTRAINT IF EXISTS schools_twilio_number_key`).catch(() => {});

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
    CREATE INDEX IF NOT EXISTS idx_messages_school_time ON messages(school_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_fees_school_status ON fees(school_id, status);

    -- Scores unique index — needed for ON CONFLICT upsert from teacher CA updates
    CREATE UNIQUE INDEX IF NOT EXISTS idx_scores_student_subject_term
      ON scores(school_id, student_id, subject, term);
    CREATE INDEX IF NOT EXISTS idx_scores_school_week
      ON scores(school_id, uploaded_at DESC);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_fees_student_term ON fees(school_id, student_id, term);

    -- Student promotion tracking
    ALTER TABLE students ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active';
    ALTER TABLE students ADD COLUMN IF NOT EXISTS graduation_year INTEGER;
    ALTER TABLE students ADD COLUMN IF NOT EXISTS previous_class TEXT;

    CREATE TABLE IF NOT EXISTS promotion_history (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      student_name TEXT,
      from_class TEXT NOT NULL,
      to_class TEXT NOT NULL,
      academic_session TEXT,
      promotion_type TEXT DEFAULT 'promoted',
      promoted_by TEXT DEFAULT 'admin',
      notes TEXT,
      notified_parent BOOLEAN DEFAULT false,
      promoted_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_promotion_school ON promotion_history(school_id, promoted_at DESC);
    CREATE INDEX IF NOT EXISTS idx_promotion_student ON promotion_history(student_id);

    -- Payment ledger (individual payment records, separate from fees balance)
    CREATE TABLE IF NOT EXISTS fee_payments (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      fee_id UUID REFERENCES fees(id) ON DELETE CASCADE,
      amount NUMERIC NOT NULL,
      payment_method TEXT DEFAULT 'cash',
      payment_date DATE DEFAULT current_date,
      recorded_by TEXT DEFAULT 'admin',
      note TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    );

    -- Scale-ready tables
    CREATE TABLE IF NOT EXISTS bulk_upload_errors (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      file_name TEXT, row_number INTEGER, error_message TEXT, row_data JSONB,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS global_announcements (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      title TEXT NOT NULL, message TEXT NOT NULL, audience TEXT DEFAULT 'all_admins',
      is_active BOOLEAN DEFAULT true, expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS message_queue_logs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID REFERENCES schools(id), job_id TEXT, status TEXT, recipient TEXT, error TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_bulk_errors_school ON bulk_upload_errors(school_id);
    CREATE INDEX IF NOT EXISTS idx_announcements_active ON global_announcements(is_active);
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS ai_training_paid BOOLEAN DEFAULT false;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS billing_cycle_start DATE DEFAULT CURRENT_DATE;

    CREATE TABLE IF NOT EXISTS student_risk_scores (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      risk_level TEXT NOT NULL DEFAULT 'low',
      academic_risk BOOLEAN DEFAULT false,
      attendance_risk BOOLEAN DEFAULT false,
      engagement_risk BOOLEAN DEFAULT false,
      trajectory TEXT DEFAULT 'stable',
      weak_subjects JSONB DEFAULT '[]'::jsonb,
      avg_score NUMERIC DEFAULT 0,
      attendance_pct NUMERIC DEFAULT 0,
      hw_completion_pct NUMERIC DEFAULT 0,
      assessed_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(student_id)
    );
    CREATE TABLE IF NOT EXISTS intervention_plans (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      risk_level TEXT,
      plan_text TEXT NOT NULL,
      weak_subjects JSONB DEFAULT '[]'::jsonb,
      sent_to_parent BOOLEAN DEFAULT false,
      parent_acknowledged BOOLEAN DEFAULT false,
      tutor_requested BOOLEAN DEFAULT false,
      created_at TIMESTAMPTZ DEFAULT now(),
      follow_up_date DATE
    );
    CREATE TABLE IF NOT EXISTS tutors (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name TEXT NOT NULL, phone TEXT NOT NULL, email TEXT,
      subjects JSONB DEFAULT '[]'::jsonb,
      cities JSONB DEFAULT '[]'::jsonb,
      rate_per_hour NUMERIC DEFAULT 0,
      bio TEXT, verified BOOLEAN DEFAULT false,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_risk_school ON student_risk_scores(school_id, risk_level);
    CREATE INDEX IF NOT EXISTS idx_intervention_student ON intervention_plans(student_id);

    -- Sickbay: track whether parent was notified
    ALTER TABLE sickbay_log ADD COLUMN IF NOT EXISTS parent_notified BOOLEAN DEFAULT false;
    ALTER TABLE sickbay_log ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ;

    -- Students: birthday for birthday notifications
    ALTER TABLE students ADD COLUMN IF NOT EXISTS date_of_birth DATE;

    -- Schools: term dates (used by events and term calendar)
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS term_start DATE;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS term_end DATE;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS events_enabled BOOLEAN DEFAULT true;

    -- School events: extended columns used by events hub
    ALTER TABLE school_events ADD COLUMN IF NOT EXISTS name TEXT;
    ALTER TABLE school_events ADD COLUMN IF NOT EXISTS date DATE;
    ALTER TABLE school_events ADD COLUMN IF NOT EXISTS time TEXT;
    ALTER TABLE school_events ADD COLUMN IF NOT EXISTS type TEXT DEFAULT 'social';
    ALTER TABLE school_events ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE school_events ADD COLUMN IF NOT EXISTS notify_parents BOOLEAN DEFAULT false;

    -- AI suppression: when admin replies, suppress AI for that thread for 30 mins
    CREATE TABLE IF NOT EXISTS ai_suppression (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      phone TEXT NOT NULL,
      suppressed_until TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(school_id, phone)
    );

    -- Results portal tables
    CREATE TABLE IF NOT EXISTS student_results (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      class_name TEXT, term TEXT NOT NULL,
      subjects JSONB DEFAULT '{}'::jsonb,
      position INTEGER, remark TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS result_pins (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      student_name TEXT, class_name TEXT, term TEXT NOT NULL,
      pin TEXT NOT NULL, access_token TEXT,
      print_limit INTEGER DEFAULT 3, print_count INTEGER DEFAULT 0,
      accessed BOOLEAN DEFAULT false,
      expires_at TIMESTAMPTZ, sent_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT now()
    );

    -- Event hub tables
    CREATE TABLE IF NOT EXISTS event_feeds (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      event_id UUID REFERENCES school_events(id) ON DELETE CASCADE,
      message TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS event_galleries (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      event_id UUID REFERENCES school_events(id) ON DELETE CASCADE,
      event_name TEXT, share_token TEXT UNIQUE,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS event_photos (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      gallery_id UUID REFERENCES event_galleries(id) ON DELETE CASCADE,
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      event_id UUID REFERENCES school_events(id) ON DELETE CASCADE,
      url TEXT NOT NULL, public_id TEXT, filename TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    );

    -- Waitlist for lead capture
    CREATE TABLE IF NOT EXISTS waitlist (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name TEXT, role TEXT, school TEXT, city TEXT,
      phone TEXT, email TEXT, students TEXT, timeline TEXT,
      features TEXT, challenge TEXT, submitted_at TIMESTAMPTZ,
      contacted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_ai_suppression ON ai_suppression(school_id, phone);
    CREATE INDEX IF NOT EXISTS idx_results_school ON student_results(school_id, term);
    CREATE INDEX IF NOT EXISTS idx_result_pins_school ON result_pins(school_id);
  `);
}

async function seedIfEmpty() {
  const existing = await q('SELECT id FROM schools LIMIT 1');
  if (existing.rowCount) return;

  // ── Cradle-style Nigerian school demo data ──────────────
  const school = await q(`INSERT INTO schools
    (name, city, landmark_description, fees, fee_deadline, current_term, whatsapp_number, twilio_number, admin_password, plan, status, billing_start, monthly_retainer, setup_fee)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,current_date,$12,$13) RETURNING *`,
    ['Greenfield Academy', 'Abuja', 'Green gate beside the assembly hall, Maitama', '120000 per term', '15th January', '2nd Term 2024/2025',
     '+2347015255068', null, 'admin123', 'pro', 'active', 80000, 150000]); // twilio_number intentionally null — real schools claim their own number

  const sid = school.rows[0].id;

  // Students — rich, realistic Nigerian names with varying performance
  const studentData = [
    // name, class, parent_name, parent_phone, dob, scores {subj:score}, fee_paid, fee_due
    ['Adaeze Josephine Obi',   'JSS3A', 'Mrs Josephine Obi',      '+2348069956420', '2012-03-14',
     {Mathematics:72, English:85, Science:68, Social_Studies:79, Basic_Technology:71}, 120000, 120000],
    ['Tobiloba Adeyemi',       'JSS2A', 'Mr Samuel Adeyemi',      '+2348031111111', '2013-07-22',
     {Mathematics:88, English:91, Science:84, Social_Studies:90, Basic_Technology:86}, 120000, 120000],
    ['Chukwuemeka Eze Jr',     'JSS2A', 'Chief Emmanuel Eze',     '+2348032222222', '2013-04-05',
     {Mathematics:55, English:61, Science:48, Social_Studies:57, Basic_Technology:43}, 120000, 60000],
    ['Fatima Bello',           'SS1B',  'Alhaji Musa Bello',      '+2348033333333', '2011-11-30',
     {Mathematics:41, English:55, Physics:38, Chemistry:44, Economics:50},             120000, 0],
    ['Oluwaseun Adesanya',     'JSS1C', 'Mrs Bunmi Adesanya',     '+2348034444444', '2014-01-18',
     {Mathematics:93, English:89, Science:91, Social_Studies:95, Basic_Technology:90}, 120000, 120000],
    ['Amara Okafor',           'JSS3A', 'Dr Patricia Okafor',     '+2348035555555', '2012-06-09',
     {Mathematics:78, English:82, Science:76, Social_Studies:80, Basic_Technology:74}, 120000, 120000],
    ['Yusuf Ibrahim Garba',    'SS2A',  'Alhaji Ibrahim Garba',   '+2348036666666', '2010-08-25',
     {Mathematics:65, English:70, Physics:60, Chemistry:58, Economics:72},             120000, 120000],
    ['Ngozi Adeyinka',         'JSS1C', 'Mrs Folake Adeyinka',    '+2348037777777', '2014-09-12',
     {Mathematics:36, English:48, Science:31, Social_Studies:44, Basic_Technology:38}, 120000, 0],
    ['David Okoye Nnamdi',     'SS1B',  'Dr Emeka Okoye',         '+2348038888888', '2011-12-03',
     {Mathematics:82, English:88, Physics:79, Chemistry:85, Economics:77},             120000, 120000],
    ['Blessing Adeyemi',       'JSS2A', 'Pastor Gbenga Adeyemi',  '+2348039999999', '2013-05-20',
     {Mathematics:47, English:59, Science:42, Social_Studies:51, Basic_Technology:45}, 120000, 40000],
  ];

  for (const [name, cls, parentName, parentPhone, dob, scores, amountDue, amountPaid] of studentData) {
    const status = amountPaid >= amountDue ? 'paid' : amountPaid > 0 ? 'partial' : 'unpaid';
    const st = await q(
      `INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,date_of_birth,weekly_performance_score)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [sid, name, cls, parentName, parentPhone, dob,
       Math.round(Object.values(scores).reduce((a,b)=>a+b,0)/Object.values(scores).length)]
    );
    const studentId = st.rows[0].id;
    await q(`INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status,due_date)
             VALUES ($1,$2,$3,$4,$5,$6,current_date + interval '30 days')`,
      [sid, studentId, '2nd Term 2024/2025', amountDue, amountPaid, status]);
    for (const [subject, score] of Object.entries(scores)) {
      await q(`INSERT INTO scores (school_id,student_id,subject,score,term) VALUES ($1,$2,$3,$4,$5)`,
        [sid, studentId, subject.replace(/_/g,' '), score, '2nd Term 2024/2025']);
    }
    // Attendance — realistic pattern
    for (let i = 1; i <= 20; i++) {
      const absent = (name.includes('Fatima') && i % 4 === 0) || (name.includes('Ngozi') && i % 3 === 0);
      const d = new Date(); d.setDate(d.getDate() - i);
      if (d.getDay() >= 1 && d.getDay() <= 5) {
        await q(`INSERT INTO attendance (school_id,student_id,date,status) VALUES ($1,$2,$3,$4)
                 ON CONFLICT DO NOTHING`,
          [sid, studentId, d.toISOString().slice(0,10), absent ? 'absent' : 'present']);
      }
    }
  }

  // Staff
  const staffData = [
    ['Mrs Grace Okonkwo',  'teacher',   'Mathematics',    'JSS2A',  '+2348061111111', 88],
    ['Mr Tunde Adeyemi',   'teacher',   'English',        'JSS3A',  '+2348062222222', 85],
    ['Mrs Chioma Eze',     'teacher',   'Science',        'JSS1C',  '+2348063333333', 90],
    ['Mr Bala Musa',       'teacher',   'Physics',        'SS1B',   '+2348064444444', 82],
    ['Mrs Folake Bakare',  'teacher',   'Chemistry',      'SS2A',   '+2348065555555', 87],
    ['Mr James Obi',       'principal', 'Administration', '',       '+2348066666666', 95],
  ];
  for (const [name, role, subject, cls, phone, score] of staffData) {
    await q(`INSERT INTO staff (school_id,name,role,subject,class,phone,performance_score) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [sid, name, role, subject, cls, phone, score]);
  }

  // Sickbay records — including Josephine's daughter (Adaeze Josephine Obi)
  const adaezeRes = await q(`SELECT id FROM students WHERE school_id=$1 AND name LIKE '%Adaeze%' LIMIT 1`, [sid]);
  if (adaezeRes.rows.length) {
    await q(`INSERT INTO sickbay_log (school_id,student_id,reason,action_taken,parent_notified,visited_at)
             VALUES ($1,$2,$3,$4,$5,now() - interval '2 hours')`,
      [sid, adaezeRes.rows[0].id, 'Headache and mild fever', 'Temperature taken (37.8°C). Paracetamol administered. Resting in sickbay.', false]);
    await q(`INSERT INTO sickbay_log (school_id,student_id,reason,action_taken,parent_notified,visited_at)
             VALUES ($1,$2,$3,$4,$5,current_date - interval '5 days')`,
      [sid, adaezeRes.rows[0].id, 'Stomach pain', 'Observed for 30 minutes. Felt better after rest. Returned to class.', true]);
  }
  // Sickbay for another student
  const ngoziRes = await q(`SELECT id FROM students WHERE school_id=$1 AND name LIKE '%Ngozi%' LIMIT 1`, [sid]);
  if (ngoziRes.rows.length) {
    await q(`INSERT INTO sickbay_log (school_id,student_id,reason,action_taken,parent_notified,visited_at)
             VALUES ($1,$2,$3,$4,$5,current_date - interval '2 days')`,
      [sid, ngoziRes.rows[0].id, 'Scraped knee from playground', 'Wound cleaned and dressed. No stitches needed.', true]);
  }

  // Events
  await q(`INSERT INTO school_events (school_id,title,name,event_date,date,type,description)
           VALUES ($1,$2,$2,$3,$3,'social',$4),($1,$5,$5,$6,$6,'academic',$7),($1,$8,$8,$9,$9,'social',$10)`,
    [sid,
     'Inter-House Sports Day', new Date(Date.now() + 7*86400000).toISOString().slice(0,10),
     'Annual inter-house sports competition. All parents welcome.',
     'PTA Meeting', new Date(Date.now() + 14*86400000).toISOString().slice(0,10),
     'Parent-Teacher Association meeting. Progress reports will be shared.',
     'Cultural Day & Prize Giving', new Date(Date.now() + 21*86400000).toISOString().slice(0,10),
     'End-of-term cultural celebration and prize giving ceremony.'
    ]);

  console.log('✅ Demo data seeded — school: Greenfield Academy');
}

async function getSchoolByTwilio(to) {
  const n = normalisePhone(to);
  // First: exact match on the incoming number
  const result = await q(
    `SELECT * FROM schools WHERE (twilio_number = $1 OR twilio_number = $2) AND status='active' LIMIT 1`,
    [n, `whatsapp:${n}`]
  );
  if (result.rows[0]) return result.rows[0];
  // Fallback: if only ONE active school exists and it has no dedicated number,
  // assume the message is for that school (safe for single-school or demo setups)
  const fallback = await q(
    `SELECT * FROM schools WHERE status='active' AND (twilio_number IS NULL OR twilio_number=$1) LIMIT 1`,
    [process.env.TWILIO_DEFAULT_FROM || '']
  );
  const fallbackSchool = fallback.rows[0];
  if (!fallbackSchool) return null;
  // Only use fallback if it's the ONLY active school with no dedicated number
  const otherSchools = await q(
    `SELECT COUNT(*) FROM schools WHERE status='active' AND (twilio_number IS NULL OR twilio_number=$1) AND id != $2`,
    [process.env.TWILIO_DEFAULT_FROM || '', fallbackSchool.id]
  );
  if (Number(otherSchools.rows[0].count) === 0) return fallbackSchool;
  return null; // Multiple schools, ambiguous — reject
}
async function getSchool(id) { const r = await q(`SELECT * FROM schools WHERE id=$1`, [id]); return r.rows[0]; }

function getDeepSeekClient() {
  if (!process.env.DEEPSEEK_API_KEY) return null;
  return new OpenAI({
    apiKey: process.env.DEEPSEEK_API_KEY,
    baseURL: process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com',
    timeout: 20000
  });
}

function getOpenAiClient() {
  if (!process.env.OPENAI_API_KEY) return null;
  return new OpenAI({ apiKey: process.env.OPENAI_API_KEY, timeout: 20000 });
}

// Legacy helper kept for hasTextAi() checks elsewhere
function getTextAiClient() {
  return getDeepSeekClient() || getOpenAiClient() || null;
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

async function callAiWithClient(client, model, system, userText, history = []) {
  // Build conversation memory from history so AI knows what was already discussed
  const historyMessages = history.flatMap(h => [
    { role: 'user', content: String(h.user_message || '').slice(0, 1000) },
    { role: 'assistant', content: String(h.assistant_reply || '').slice(0, 1000) }
  ]).filter(m => m.content.trim());

  const response = await client.chat.completions.create({
    model,
    messages: [
      { role: 'system', content: applyResponseRules(system) },
      ...historyMessages,
      { role: 'user', content: String(userText || 'Hello').slice(0, 4000) }
    ],
    temperature: Number(process.env.AI_TEMPERATURE || 0.7),
    top_p: Number(process.env.AI_TOP_P || 0.9),
    max_tokens: Number(process.env.AI_MAX_TOKENS || 420),
    presence_penalty: 0.1,
    frequency_penalty: 0.3
  });
  return response.choices?.[0]?.message?.content?.trim() || null;
}

async function callAI(system, userText, imageBase64, history = []) {
  if (imageBase64) return callClaudeVision(system, userText, imageBase64);

  const deepseek = getDeepSeekClient();
  const openai = getOpenAiClient();

  if (deepseek) {
    try {
      const result = await callAiWithClient(deepseek, process.env.DEEPSEEK_MODEL || 'deepseek-chat', system, userText, history);
      if (result) { console.log('AI via DeepSeek'); return result; }
    } catch (err) {
      console.warn('DeepSeek failed, trying OpenAI:', err?.message || err);
    }
    if (openai) {
      try {
        const result = await callAiWithClient(openai, process.env.OPENAI_MODEL || 'gpt-4o-mini', system, userText, history);
        if (result) { console.log('AI via OpenAI (DeepSeek fallback)'); return result; }
      } catch (err) {
        console.error('OpenAI fallback failed:', err?.message || err);
      }
    }
  } else if (openai) {
    try {
      const result = await callAiWithClient(openai, process.env.OPENAI_MODEL || 'gpt-4o-mini', system, userText, history);
      if (result) { console.log('AI via OpenAI'); return result; }
    } catch (err) {
      console.warn('OpenAI failed:', err?.message || err);
    }
    if (deepseek) {
      try {
        const result = await callAiWithClient(deepseek, process.env.DEEPSEEK_MODEL || 'deepseek-chat', system, userText, history);
        if (result) { console.log('AI via DeepSeek (OpenAI fallback)'); return result; }
      } catch (err) {
        console.error('DeepSeek fallback failed:', err?.message || err);
      }
    }
  }

  console.error('All AI providers failed or unavailable');
  return demoReply(userText, system);
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
  return `EduPing received your message. I will answer using live school data once fully configured. ${schoolName} 🏫`;
}

async function buildStudentContext(school, student, fromNumber) {
  // Each query is wrapped individually so a missing table/column never crashes the whole context
  const safe = async (fn) => { try { return await fn(); } catch(e) { console.warn('[buildStudentContext]', e.message); return { rows: [] }; } };
  const [attendance, scores, fees, homeworks, events, notes, sickbay, history] = await Promise.all([
    safe(() => q('SELECT date,status FROM attendance WHERE school_id=$1 AND student_id=$2 ORDER BY date DESC LIMIT 10', [school.id, student.id])),
    safe(() => q('SELECT subject,score,term FROM scores WHERE school_id=$1 AND student_id=$2 ORDER BY uploaded_at DESC LIMIT 10', [school.id, student.id])),
    safe(() => q('SELECT term,amount_due,amount_paid,status FROM fees WHERE school_id=$1 AND student_id=$2 LIMIT 5', [school.id, student.id])),
    safe(() => q('SELECT subject,description,due_date FROM homeworks WHERE school_id=$1 AND class_name=$2 ORDER BY created_at DESC LIMIT 5', [school.id, student.class_name])),
    safe(() => q('SELECT title,event_date FROM school_events WHERE school_id=$1 ORDER BY event_date ASC LIMIT 5', [school.id])),
    safe(() => q('SELECT note,created_at FROM behaviour_notes WHERE school_id=$1 AND student_id=$2 ORDER BY created_at DESC LIMIT 5', [school.id, student.id])),
    safe(() => q('SELECT reason,action_taken,visited_at FROM sickbay_log WHERE school_id=$1 AND student_id=$2 ORDER BY visited_at DESC LIMIT 5', [school.id, student.id])),
    // Pull last 6 messages (3 exchanges) to give AI conversation memory
    fromNumber
      ? safe(() => q('SELECT user_message,assistant_reply FROM messages WHERE school_id=$1 AND from_number=$2 ORDER BY created_at DESC LIMIT 6', [school.id, fromNumber]))
      : Promise.resolve({ rows: [] })
  ]);
  return { school, student, attendance: attendance.rows, scores: scores.rows, fees: fees.rows, homeworks: homeworks.rows, events: events.rows, notes: notes.rows, sickbay: sickbay.rows, history: history.rows.reverse() };
}

function parentPrompt(ctx, first) {
  // ── Humanise the data before passing to AI ──────────────
  const student = ctx.student;
  const school = ctx.school;

  // Attendance summary
  const totalDays = ctx.attendance.length;
  const presentDays = ctx.attendance.filter(a => a.status === 'present').length;
  const attendanceSummary = totalDays
    ? `${presentDays} out of ${totalDays} recent school days`
    : 'No attendance records yet';

  // Score summary — show subject and score as plain text
  const scoreSummary = ctx.scores.length
    ? ctx.scores.map(s => `${s.subject}: ${s.score}%`).join(', ')
    : 'No scores uploaded yet';

  // Score trend — is the child improving?
  const scoreTrend = (() => {
    if (ctx.scores.length < 2) return '';
    const recent = ctx.scores.slice(0, 3).map(s => s.score);
    const older = ctx.scores.slice(3, 6).map(s => s.score);
    if (!older.length) return '';
    const recentAvg = recent.reduce((a,b) => a+b,0)/recent.length;
    const olderAvg = older.reduce((a,b) => a+b,0)/older.length;
    if (recentAvg > olderAvg + 5) return 'improving recently';
    if (recentAvg < olderAvg - 5) return 'slipping recently — may need encouragement';
    return 'fairly consistent';
  })();

  // Fee summary
  const feeSummary = ctx.fees.length
    ? ctx.fees.map(f => `${f.term||'current term'}: ₦${Number(f.amount_paid||0).toLocaleString()} paid of ₦${Number(f.amount_due||0).toLocaleString()}`).join('; ')
    : 'No fee records on file';

  // Homework summary
  const homeworkSummary = ctx.homeworks.length
    ? ctx.homeworks.map(h => `${h.subject} — ${h.description}${h.due_date ? ', due ' + h.due_date : ''}`).join('; ')
    : 'No homework assigned recently';

  // Upcoming events
  const eventSummary = ctx.events.length
    ? ctx.events.map(e => `${e.title} on ${e.event_date}`).join(', ')
    : 'No upcoming events';

  // Sickbay
  const sickbaySummary = ctx.sickbay.length
    ? ctx.sickbay.map(s => `visited sickbay: ${s.reason||'unspecified'}, action: ${s.action_taken||'noted'}`).join('; ')
    : 'No sickbay visits on record';

  // Behaviour
  const behaviourSummary = ctx.notes.length
    ? ctx.notes.map(n => n.note).join('; ')
    : 'No behaviour notes';

  // ── Payment details from onboarding (bank + Paystack) ──
  const cfg = (typeof school.config === 'string' ? (() => { try { return JSON.parse(school.config); } catch(e) { return {}; } })() : (school.config || {}));
  const payParts = [];
  if (cfg.paystack_payment_link) payParts.push(`Pay online securely via Paystack: ${cfg.paystack_payment_link}`);
  if (cfg.bank_account_number) payParts.push(`Bank transfer: ${cfg.bank_name || 'Bank'} — Account ${cfg.bank_account_number} (${cfg.bank_account_name || school.name})`);
  if (cfg.fee_instructions) payParts.push(cfg.fee_instructions);
  const paymentInfo = payParts.length ? payParts.join('. ') : 'Payment details not yet on file — ask the parent to contact the school bursar';

  // Siblings (multi-child parents)
  const siblingNote = (ctx.siblings && ctx.siblings.length)
    ? `\nThis parent also has other children at the school: ${ctx.siblings.join(', ')}. You are currently answering about ${student.name}. If the parent asks about another child by name, tell them to mention that child's name and you will switch to them.`
    : '';

  return `You are a warm, caring school assistant for ${school.name}${school.city ? ', ' + school.city : ''}, powered by EduPing.

Your personality:
- You speak like a trusted, knowledgeable school staff member who genuinely knows this child — not a bot reading from a database
- You are warm, conversational and Nigerian-friendly in tone — the way a caring class teacher would speak to a parent on WhatsApp
- You give real, specific answers using the child's actual data — never generic, never vague
- When the news is good, celebrate it genuinely. When there is a concern, be honest but reassuring
- Keep responses short and natural — 2 to 4 sentences is ideal unless more detail is needed
- Never list raw data or use technical field names. Translate everything into natural human language
- Use light emojis naturally, not excessively
- End with "${school.name} 🏫" only when closing a topic, not after every single sentence
- If you do not have data to answer something, say so warmly and suggest they contact the school directly

About this child:
${student.name} is in ${student.class_name || 'their class'}. Parent/guardian name: ${student.parent_name || 'not on file'}.
Attendance: ${attendanceSummary}.
Academic performance: ${scoreSummary}${scoreTrend ? ' — ' + scoreTrend : ''}.
Fees: ${feeSummary}.
How parents can pay fees: ${paymentInfo}.
Recent homework: ${homeworkSummary}.
Health: ${sickbaySummary}.
Behaviour: ${behaviourSummary}.
Upcoming school events: ${eventSummary}.
Current term: ${school.current_term || 'not specified'}.${siblingNote}

Conversation rules:
- This is a WhatsApp conversation — keep it flowing and human
- If a parent asks how to pay fees, give them the exact payment details listed above (Paystack link and/or bank account) — never be vague about payment
- If a parent asks a follow-up question, answer it directly without repeating information already given
- If a parent seems worried, acknowledge their feeling before answering
- If a parent seems happy or proud, share in that moment genuinely
- Never say "based on the data" or "according to records" — you know this child, speak like it
- Never mention EduPing by name in the conversation — you are simply the school assistant`;
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
  const mediaType = req.body.MediaContentType0 || '';
  const school = await getSchoolByTwilio(to);
  if (!school || school.status !== 'active') return res.type('text/xml').send(new twilio.twiml.MessagingResponse().message('School account is not active. Please contact EduPing support.').toString());
  // Guard: check if multiple DIFFERENT schools share this exact incoming Twilio number
  // Only a problem if 2+ schools have the same twilio_number — one school using the default is fine
  if (school.twilio_number) {
    const sharedRisk = await q(
      `SELECT COUNT(*) FROM schools WHERE status='active' AND twilio_number=$1 AND id != $2`,
      [school.twilio_number, school.id]
    );
    if (Number(sharedRisk.rows[0].count) > 0) {
      console.error(`[MULTITENANCY] twilio_number ${school.twilio_number} shared by multiple schools — message routing ambiguous`);
      return res.type('text/xml').send(new twilio.twiml.MessagingResponse().toString());
    }
  }

  let reply = '';
  // Match staff by exact phone OR last 9 digits (handles +234 vs 0 format differences)
  const last9 = String(from).replace(/\D/g,'').slice(-9);
  const staffLookup = await q(
    `SELECT * FROM staff WHERE school_id=$1 AND (phone=$2 OR right(regexp_replace(phone,'[^0-9]','','g'),9)=$3) LIMIT 1`,
    [school.id, from, last9]
  );
  const staff = staffLookup;
  if (staff.rowCount) {
    // Admin takeover: if admin replied recently, AI stays silent — just log the teacher's message
    if (await isAiSuppressed(school.id, from)) {
      await q(`INSERT INTO messages (school_id,from_number,channel,user_message,assistant_reply) VALUES ($1,$2,'teacher',$3,$4)`,
        [school.id, from, body, '[AI suppressed — admin active in thread]']);
      return res.type('text/xml').send(new twilio.twiml.MessagingResponse().toString());
    }
    reply = await processTeacher(school, staff.rows[0], body, mediaUrl, mediaType);
    // Log teacher conversations so admins can monitor them in the dashboard
    await q(`INSERT INTO messages (school_id,from_number,channel,user_message,assistant_reply) VALUES ($1,$2,'teacher',$3,$4)`,
      [school.id, from, body, reply]).catch(e => console.warn('[teacher-log]', e.message));
  }
  else {
    // Match parent by exact phone OR last 9 digits (same fix as staff — handles +234 vs 0 formats)
    const student = await q(
      `SELECT * FROM students WHERE school_id=$1 AND (parent_phone=$2 OR right(regexp_replace(parent_phone,'[^0-9]','','g'),9)=$3) ORDER BY created_at ASC`,
      [school.id, from, last9]
    );
    // Multi-child parents: if a child's name is mentioned in the message, answer about THAT child
    let siblings = [];
    if (student.rows.length > 1) {
      const bodyLower = String(body || '').toLowerCase();
      const named = student.rows.find(s => String(s.name).toLowerCase().split(/\s+/).some(p => p.length > 2 && bodyLower.includes(p)));
      if (named) student.rows = [named, ...student.rows.filter(s => s.id !== named.id)];
      siblings = student.rows.slice(1).map(s => `${s.name}${s.class_name ? ' (' + s.class_name + ')' : ''}`);
    }
    if (student.rowCount) {
      const lower = body.toLowerCase().trim();
      const first = (await q(`SELECT id FROM messages WHERE school_id=$1 AND from_number=$2 LIMIT 1`, [school.id, from])).rowCount === 0;

      // ── First message — send disclaimer directly, no AI needed ──
      if (first) {
        reply = `👋 Welcome to ${school.name}'s AI assistant, powered by EduPing!

Before we continue:
📋 Your conversations and your child's data are processed by AI to answer your questions.
🔒 Your data is private and never sold to third parties.
🤖 For urgent matters, please contact the school directly.

By sending any message, you agree to this.

How can I help you today? You can ask about attendance, results, fees, homework, or school events. ${school.name} 🏫`;

        await q(`INSERT INTO messages (school_id,from_number,student_id,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)`, [school.id, from, student.rows[0].id, body, reply]);
        const twiml = new twilio.twiml.MessagingResponse();
        twiml.message(reply);
        return res.type('text/xml').send(twiml.toString());
      }

      // ── TUTOR keyword — parent wants tutor connection ────
      if (lower === 'tutor' || lower === 'i want a tutor' || lower === 'get tutor') {
        const riskRow = await q(`SELECT weak_subjects FROM student_risk_scores WHERE student_id=$1 AND school_id=$2`, [student.rows[0].id, school.id]);
        const enriched = { ...student.rows[0], weak_subjects: riskRow.rows[0]?.weak_subjects || [] };
        reply = await handleTutorRequest(school, enriched, from);
        await q(`UPDATE intervention_plans SET tutor_requested=true WHERE student_id=$1 AND tutor_requested=false`, [student.rows[0].id]);
      }
      // ── YES to intervention plan ─────────────────────────
      else if (lower === 'yes' || lower === 'ok' || lower === 'okay' || lower === 'sure') {
        const pending = await q(`SELECT ip.*, s.name student_name FROM intervention_plans ip JOIN students s ON s.id=ip.student_id WHERE ip.student_id=$1 AND s.school_id=$2 AND ip.parent_acknowledged=false ORDER BY ip.created_at DESC LIMIT 1`, [student.rows[0].id, school.id]);
        if (pending.rowCount) {
          await q(`UPDATE intervention_plans SET parent_acknowledged=true WHERE id=$1`, [pending.rows[0].id]);
          reply = `✅ Great! We've noted that you're on board with ${pending.rows[0].student_name}'s study plan.\n\nReply *TUTOR* anytime if you'd like us to connect you with a private tutor.\n\n${school.name} 🏫`;
        } else {
          const ctx = await buildStudentContext(school, student.rows[0], from);
          ctx.siblings = siblings;
          reply = await callAI(parentPrompt(ctx, first), body || 'Hello', null, ctx.history);
        }
      }
      // ── Normal parent query ──────────────────────────────
      else {
        // Check if admin has replied recently (AI suppressed for 30 mins)
        const suppressed = await isAiSuppressed(school.id, from);
        if (suppressed) {
          // Still log the message but don't send an AI reply
          await q(`INSERT INTO messages (school_id,from_number,student_id,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)`,
            [school.id, from, student.rows[0].id, body, '[AI suppressed — admin active in thread]']);
          const twiml = new twilio.twiml.MessagingResponse();
          return res.type('text/xml').send(twiml.toString()); // empty response — no AI reply
        }
        const ctx = await buildStudentContext(school, student.rows[0], from);
        ctx.siblings = siblings;
        reply = await callAI(parentPrompt(ctx, first), body || 'Hello', null, ctx.history);
      }

      await q(`INSERT INTO messages (school_id,from_number,student_id,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)`, [school.id, from, student.rows[0].id, body, reply]);

      // ── Escalation detection — notify admin if AI couldn't answer ──
      const escalationPhrases = ['pass your question', 'contact the school directly', 'reach out directly', 'speak to the school', 'please contact'];
      const isEscalation = escalationPhrases.some(p => (reply||'').toLowerCase().includes(p));
      if (isEscalation && school.admin_phone) {
        try {
          const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
          await twilioClient.messages.create({
            from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
            to: `whatsapp:${school.admin_phone}`,
            body: `⚠️ *EduPing Alert — Parent needs follow-up*

From: ${from}
Student: ${student.rows[0].name}
Message: "${body}"

EduPing could not fully answer this. Please follow up directly.

${school.name} 🏫`
          });
        } catch(e) { console.warn('Admin escalation notify failed:', e.message); }
      }
    } else {
      const first = (await q(`SELECT id FROM messages WHERE school_id=$1 AND from_number=$2 LIMIT 1`, [school.id, from])).rowCount === 0;
      const system = `You are EduPing for ${school.name}. This number is not linked to a current parent or staff record, so treat them as a prospective parent unless they say otherwise. Capture parent name, phone, child name, class applying, and next action. Keep it short. ${first ? 'Start with the first message privacy disclaimer.' : ''}`;
      reply = await callAI(system, body || 'Admission inquiry', null);
      await q(`INSERT INTO admission_inquiries (school_id,phone,status) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, [school.id, from, 'new']);
      await q(`INSERT INTO messages (school_id,from_number,user_message,assistant_reply) VALUES ($1,$2,$3,$4)`, [school.id, from, body, reply]);
    }
  }
  const twiml = new twilio.twiml.MessagingResponse();
  twiml.message(reply);
  return res.type('text/xml').send(twiml.toString());
}

async function processTeacher(school, staff, body, mediaUrl, mediaType) {
  const lower = String(body || '').toLowerCase();
  const today = new Date().toISOString().slice(0,10);

  // Voice notes arrive as mediaUrl with audio content type — reject before sign-in branch
  const isVoiceNote = mediaUrl && (mediaType || '').includes('audio');
  if (isVoiceNote) {
    return `🎤 Voice message received, ${staff.name}. Please *type* your message so EduPing can process it correctly.\n\nExamples:\n• "good morning" — sign in\n• "homework: Chapter 3 Q1-5 due Friday" — assign homework\n\n${school.name} 🏫`;
  }

  const isImage = mediaUrl && (mediaType || '').includes('image');
  if (lower.includes('sign in') || lower.includes('good morning') || isImage) {
    await q(`INSERT INTO signin_log (school_id,staff_id,date,time,status,photo_verified) VALUES ($1,$2,current_date,to_char(now(),'HH24:MI'),$3,$4)`, [school.id, staff.id, 'submitted', Boolean(isImage)]);
    return `✅ ${staff.name}, your sign in has been recorded. ${school.name} 🏫`;
  }
  if (lower.includes('homework') || lower.includes('assignment')) {
    await q(`INSERT INTO homeworks (school_id,assigned_by,class_name,subject,description,due_date) VALUES ($1,$2,$3,$4,$5,current_date + interval '3 days')`, [school.id, staff.id, staff.class, staff.subject, body]);
    await q(`UPDATE staff SET homework_assigned=homework_assigned+1 WHERE id=$1 AND school_id=$2`, [staff.id, school.id]);

    // Notify parents of students in this class
    if (staff.class) {
      const parents = await q(
        'SELECT name, parent_phone FROM students WHERE school_id=$1 AND class_name=$2 AND parent_phone IS NOT NULL AND parent_phone != \'\'',
        [school.id, staff.class]
      );
      const fromNumber = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
      const dueDate = new Date(Date.now() + 3 * 86400000).toLocaleDateString('en-NG', { weekday: 'long', day: 'numeric', month: 'long' });
      let notified = 0;
      for (const p of parents.rows) {
        try {
          const msg = `📚 *Homework Alert — ${school.name}*\n\nDear parent, ${staff.name} has assigned new ${staff.subject || 'class'} homework to *${staff.class}*:\n\n"${body}"\n\n📅 Due: ${dueDate}\n\nReply to this number to ask EduPing any questions.\n${school.name} 🏫`;
          await twilioSend(p.parent_phone, fromNumber, msg);
          notified++;
          console.log(`📱 Homework notification sent to parent of ${p.name} (${p.parent_phone})`);
        } catch(e) {
          console.warn(`⚠️ Failed to notify parent of ${p.name}: ${e.message}`);
        }
      }
      console.log(`📚 Homework saved for ${staff.class}. Notified ${notified}/${parents.rows.length} parents.`);
    }

    return `✅ Homework saved for ${staff.class || 'your class'}. ${staff.class ? 'Parents have been notified via WhatsApp.' : 'Parents can now ask EduPing for it.'} ${school.name} 🏫`;
  }
  // ── CA/Score update — writes to scores table so Friday report picks it up ──
  const isScoreMsg = lower.includes('scored') || lower.includes('ca update') ||
    lower.includes('test result') || lower.includes('exam result') ||
    lower.includes('ca:') || lower.includes('scores:') || /\d+\s*\/\s*\d+/.test(body);

  if (isScoreMsg) {
    const term = school.current_term || 'Current Term';
    const defaultSubject = staff.subject || 'General';
    let saved = 0;
    const failed = [];

    const singleRx = /([A-Za-z][\w\s]{0,25}?)\s+scored\s+(\d{1,3})(?:\s*\/\s*\d+)?(?:\s+in\s+([A-Za-z\s]+))?/i;
    const singleM = body.match(singleRx);

    const bulkRx = /([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(\d{1,3})(?:\s+([A-Za-z]+))?/g;
    const bulkMatches = [...body.matchAll(bulkRx)].filter(m => parseInt(m[2]) <= 100);

    const toProcess = [];
    if (singleM) {
      toProcess.push({ name: singleM[1].trim(), score: parseInt(singleM[2]), subject: (singleM[3] || defaultSubject).trim() });
    } else {
      for (const m of bulkMatches) {
        toProcess.push({ name: m[1].trim(), score: parseInt(m[2]), subject: (m[3] || defaultSubject).trim() });
      }
    }

    for (const entry of toProcess) {
      const studentRes = await q(
        `SELECT id, name FROM students
          WHERE school_id=$1
            AND (name ILIKE $2 OR split_part(name,' ',1) ILIKE $3)
            AND (class_name=$4 OR $4='')
          LIMIT 1`,
        [school.id, `%${entry.name}%`, entry.name, staff.class || '']
      );
      if (studentRes.rows.length) {
        try {
          await q(`
            INSERT INTO scores (school_id, student_id, subject, score, term, uploaded_at)
            VALUES ($1,$2,$3,$4,$5,now())
            ON CONFLICT (school_id, student_id, subject, term)
            DO UPDATE SET score=$4, uploaded_at=now()
          `, [school.id, studentRes.rows[0].id, entry.subject, entry.score, term]);
          await q(`UPDATE students SET weekly_performance_score=$1 WHERE id=$2`, [entry.score, studentRes.rows[0].id]);
          saved++;
        } catch(e) { failed.push(entry.name); }
      } else {
        failed.push(entry.name + ' (not found in class)');
      }
    }

    if (toProcess.length === 0) {
      return `📊 Got it, ${staff.name.split(' ')[0]}! To save scores for Friday's report, use:\n\n*"Amina scored 95 in Maths"*\nor bulk: *"Amina 95, Tobiloba 88, Chidi 72"*\n\n${school.name} 🏫`;
    }

    const failNote = failed.length ? `\n⚠️ Could not find: ${failed.join(', ')}` : '';
    return `✅ ${saved} score${saved !== 1 ? 's' : ''} saved for ${term}.${failNote}\n\n📊 These will appear in *Friday's weekly report* to parents.\n\n${school.name} 🏫`;
  }

  if (lower.includes('behaviour') || lower.includes('behavior') || lower.includes('disrupt') || lower.includes('absent') || lower.includes('late') || lower.includes('noted')) {
    return `📝 Noted, ${staff.name.split(' ')[0]}. Log detailed behaviour notes from the dashboard under the student's profile.

${school.name} 🏫`;
  }

  if (lower.includes('attendance') || lower.includes('present') || lower.includes('roll call')) {
    return `✅ Attendance noted, ${staff.name.split(' ')[0]}. Please update class attendance from the dashboard to keep records accurate.

${school.name} 🏫`;
  }

  const teacherSystem = `You are EduPing, a school management assistant for ${school.name}. 
You are speaking with ${staff.name}, a ${staff.role || 'teacher'}${staff.subject ? ' who teaches ' + staff.subject : ''}${staff.class ? ' for class ' + staff.class : ''}.
Help them with: logging scores, assigning homework, recording attendance, behaviour notes, or general school queries.
Keep responses SHORT (under 100 words), practical, and clearly directed at the teacher — not a parent.
Never send parent-style responses. End with ${school.name} 🏫`;

  return await callAI(teacherSystem, body || 'Hello', null);
}

function requireSuper(req, res, next) {
  const token = req.headers['x-super-admin-password'] || req.body.password || req.query.password;
  if (!process.env.SUPER_ADMIN_PASSWORD || token !== process.env.SUPER_ADMIN_PASSWORD) return bad(res, 'Unauthorized super admin', 401);
  next();
}
async function requireSchool(req, res, next) {
  try { rateLimitDB(req.headers['x-school-id']); } catch(e) { return res.status(429).json({ error: e.message }); }
  const schoolId = req.headers['x-school-id'] || req.query.school_id || req.body.school_id;
  const password = req.headers['x-admin-password'] || req.body.admin_password; // never from query params — would appear in logs
  if (!schoolId || !password) return bad(res, 'Missing school_id or admin password', 401);
  const school = await getSchool(schoolId);
  if (!school || school.admin_password !== password) return bad(res, 'Unauthorized school admin', 401);
  req.school = school;
  next();
}

// root route handled above by landing.html
app.get('/superadmin', (req, res) => res.sendFile(path.join(__dirname, 'public', 'superadmin.html')));
app.get('/onboarding', (req, res) => res.sendFile(path.join(__dirname, 'public', 'onboarding.html')));
app.get('/privacy', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang='en'>
<head>
<meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'>
<title>Privacy Policy — EduPing</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 760px; margin: 0 auto; padding: 2rem 1.5rem; color: #1a1a1a; line-height: 1.7; }
  h1 { font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; }
  .brand { color: #0099ee; }
  .meta { color: #666; font-size: 0.9rem; margin-bottom: 2rem; }
  h2 { font-size: 1.15rem; font-weight: 600; margin-top: 2rem; }
  p, li { font-size: 0.97rem; color: #333; }
  ul { padding-left: 1.25rem; }
  a { color: #0099ee; }
  footer { margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #eee; font-size: 0.85rem; color: #888; }
</style>
</head>
<body>
<h1>Edu<span class='brand'>Ping</span> Privacy Policy</h1>
<p class='meta'>Last updated: ${new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })}</p>

<p>EduPing (&quot;we&quot;, &quot;us&quot;, &quot;our&quot;) operates an AI-powered school communication platform accessible via WhatsApp and web. This policy explains how we collect, use, and protect your data.</p>

<h2>1. Data We Collect</h2>
<ul>
  <li>School information: name, location, contact details, term dates, and fee schedules</li>
  <li>Staff information: name, phone number, role, subject, and class assignment</li>
  <li>Student information: name, class, attendance records, academic scores, and behaviour notes</li>
  <li>Parent information: name, phone number, and WhatsApp messages sent to EduPing</li>
  <li>Usage data: message timestamps, feature usage, and system logs</li>
</ul>

<h2>2. How We Use Your Data</h2>
<ul>
  <li>To provide AI-powered responses to parent enquiries via WhatsApp</li>
  <li>To generate attendance records, score reports, and weekly student summaries</li>
  <li>To send automated school announcements and fee reminders</li>
  <li>To identify at-risk students and generate personalised learning support plans</li>
  <li>To improve platform performance and reliability</li>
</ul>

<h2>3. AI Processing</h2>
<p>Your school data and parent messages are processed by AI systems (including OpenAI and/or DeepSeek) to generate responses. Data sent to AI providers is subject to their respective privacy policies. We do not use your data to train AI models.</p>

<h2>4. Data Sharing</h2>
<p>We do not sell your data to third parties. Data is shared only with:</p>
<ul>
  <li>AI providers (OpenAI, DeepSeek) for message processing</li>
  <li>Twilio for WhatsApp message delivery</li>
  <li>Railway (our hosting provider) for infrastructure</li>
</ul>

<h2>5. Data Security</h2>
<p>All data is stored on encrypted PostgreSQL databases hosted on Railway. Passwords are hashed using bcrypt. All connections use HTTPS/TLS encryption. Access to school data is protected by JWT authentication and school-specific credentials.</p>

<h2>6. Data Retention</h2>
<p>We retain school and student data for the duration of the active subscription. Upon cancellation, data is retained for 90 days then permanently deleted upon written request.</p>

<h2>7. Your Rights</h2>
<p>You have the right to access, correct, or delete your data at any time. Contact us at <a href='mailto:buikephilip@gmail.com'>buikephilip@gmail.com</a> with any data requests.</p>

<h2>8. Children's Data</h2>
<p>EduPing processes student data on behalf of schools. Schools are responsible for obtaining appropriate consent from parents and guardians as required by applicable law.</p>

<h2>9. Changes to This Policy</h2>
<p>We may update this policy from time to time. Schools will be notified of significant changes via WhatsApp or email.</p>

<h2>10. Contact</h2>
<p>Philip Buike — EduPing<br>
Email: <a href='mailto:buikephilip@gmail.com'>buikephilip@gmail.com</a><br>
Phone: 07015255068<br>
Website: <a href='https://eduping.org'>eduping.org</a></p>

<footer>© ${new Date().getFullYear()} EduPing. All rights reserved.</footer>
</body>
</html>`);
});

// ── Generate onboarding link ────────────────────────────────
app.post('/api/super/schools/:id/onboarding-link', requireSuper, async (req, res) => {
  try {
    const school = await q(`SELECT id, name, whatsapp_number FROM schools WHERE id=$1`, [req.params.id]);
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
    const school = await q(`SELECT id, whatsapp_number FROM schools WHERE id=$1`, [d.school_id]);
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
      bank_name: d.bank_name, bank_account_number: d.bank_account_number,
      bank_account_name: d.bank_account_name, paystack_payment_link: d.paystack_payment_link,
      school_phone: d.school_phone, school_email: d.school_email,
      principal: d.principal, term_start: d.term_start,
      term_end: d.term_end, midterm_break: d.midterm_break
    });

    if (d.admin_password && d.admin_password.length >= 6) {
      await q(`UPDATE schools SET name=$1, city=$2, current_term=$3, fees=$4,
        fee_deadline=$5, landmark_description=$6, config=$7, admin_password=$8, status='active'
        WHERE id=$9`,
        [d.name, d.city, d.current_term, String(d.fees), d.fee_deadline,
         d.landmark_description, config, d.admin_password, d.school_id]);
    } else {
      await q(`UPDATE schools SET name=$1, city=$2, current_term=$3, fees=$4,
        fee_deadline=$5, landmark_description=$6, config=$7, status='active'
        WHERE id=$8`,
        [d.name, d.city, d.current_term, String(d.fees), d.fee_deadline,
         d.landmark_description, config, d.school_id]);
    }

    if (d.events && d.events.length) {
      await q(`DELETE FROM school_events WHERE school_id=$1`, [d.school_id]);
      for (const ev of d.events) {
        if (ev.title && ev.date) {
          await q(`INSERT INTO school_events (school_id, title, event_date) VALUES ($1,$2,$3)`,
            [d.school_id, ev.title, ev.date]);
        }
      }
    }
    json(res, { ok: true, whatsapp_number: school.rows[0].whatsapp_number });
  } catch(err) { json(res, { error: err.message }, 500); }
});
app.get('/health', async (req, res) => { await q('SELECT 1'); json(res, { ok: true, db: true, ai: hasAi(), text_ai: hasTextAi(), vision_ai: hasVisionAi(), provider: process.env.DEEPSEEK_API_KEY ? 'deepseek' : (process.env.OPENAI_API_KEY ? 'openai' : (process.env.ANTHROPIC_API_KEY ? 'anthropic-vision-only' : 'demo')), twilio: hasTwilio() }); });

// ── AI suppression helpers (FIX 4) ───────────────────────
async function isAiSuppressed(schoolId, phone) {
  try {
    const r = await q(`SELECT suppressed_until FROM ai_suppression WHERE school_id=$1 AND phone=$2`, [schoolId, phone]);
    if (!r.rows.length) return false;
    return new Date(r.rows[0].suppressed_until) > new Date();
  } catch(e) { return false; }
}
async function suppressAiForThread(schoolId, phone, minutes = 30) {
  const until = new Date(Date.now() + minutes * 60000).toISOString();
  await q(`INSERT INTO ai_suppression (school_id,phone,suppressed_until)
           VALUES ($1,$2,$3)
           ON CONFLICT (school_id,phone) DO UPDATE SET suppressed_until=$3`,
    [schoolId, phone, until]);
}

app.post('/api/chat', async (req, res) => {
  try {
    const message = String(req.body.message || req.body.user_message || '').trim();
    if (!message) return bad(res, 'Message is required', 400);

    let school = null;
    if (req.body.school_id) school = await getSchool(req.body.school_id);
    if (!school) {
      const fallback = await q("SELECT * FROM schools WHERE status='active' ORDER BY created_at DESC LIMIT 1");
      school = fallback.rows[0];
    }
    if (!school) return bad(res, 'No active school found. Add a school first from Super Admin.', 404);

    if (req.body.admin_mode) {
      const system = `You are EduPing's school AI assistant for ${school.name}. The school administrator is testing the AI. Answer school management questions, demo parent scenarios, explain features, or respond as if you are the parent-facing bot. Be helpful and specific. End with ${school.name} 🏫 when appropriate.`;
      const reply = await callAI(system, message, null);
      return json(res, { ok: true, reply, school_id: school.id, mode: 'admin', provider: process.env.DEEPSEEK_API_KEY ? 'deepseek' : 'demo' });
    }

    const fromNumber = normalisePhone(req.body.from_number || req.body.parent_phone || 'web-demo');
    let student = null;
    if (req.body.student_id) {
      const byId = await q(`SELECT * FROM students WHERE school_id=$1 AND id=$2 LIMIT 1`, [school.id, req.body.student_id]);
      student = byId.rows[0];
    }
    if (!student && req.body.parent_phone) {
      const byPhone = await q(`SELECT * FROM students WHERE school_id=$1 AND parent_phone=$2 LIMIT 1`, [school.id, normalisePhone(req.body.parent_phone)]);
      student = byPhone.rows[0];
    }
    if (!student) {
      const firstStudent = await q(`SELECT * FROM students WHERE school_id=$1 ORDER BY created_at ASC LIMIT 1`, [school.id]);
      student = firstStudent.rows[0];
    }

    let reply;
    if (student) {
      const first = (await q(`SELECT id FROM messages WHERE school_id=$1 AND from_number=$2 LIMIT 1`, [school.id, fromNumber])).rowCount === 0;
      const ctx = await buildStudentContext(school, student, fromNumber);
      reply = await callAI(parentPrompt(ctx, first), message, null, ctx.history);
      await q(`INSERT INTO messages (school_id,from_number,student_id,channel,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5,$6)`, [school.id, fromNumber, student.id, 'web', message, reply]);
    } else {
      const system = `You are EduPing for ${school.name}. No student has been imported yet for this school. Answer as a school AI demo assistant. If asked about a specific child, explain that the school must import students first. Keep it short and Nigerian friendly. End formal replies with ${school.name} 🏫.`;
      reply = await callAI(system, message, null);
      await q(`INSERT INTO messages (school_id,from_number,channel,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)`, [school.id, fromNumber, 'web', message, reply]);
    }

    json(res, { ok: true, reply, school_id: school.id, provider: process.env.DEEPSEEK_API_KEY ? 'deepseek' : 'demo' });
  } catch (err) {
    console.error('/api/chat error:', err);
    const errMsg = err?.message || String(err);
    json(res, { ok: false, reply: `EduPing: Chat hit an error — ${errMsg}. EduPing 🏫`, error: errMsg }, 500);
  }
});

app.post('/api/admin/reply', requireSchool, async (req, res) => {
  try {
    const { to, message } = req.body;
    if (!to || !message) return bad(res, 'to and message required');
    const school = req.school;
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    if (!from) return bad(res, 'Twilio not configured');
    const normTo = normalisePhone(to);
    await twilioSend(normTo, from, message);
    await suppressAiForThread(school.id, normTo, 30);
    await q(`INSERT INTO messages (school_id,from_number,channel,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)`,
      [school.id, normTo, 'admin-reply', '[Admin reply]', message]);
    json(res, { ok: true, suppressed_until: new Date(Date.now() + 30*60000).toISOString() });
  } catch(err) { bad(res, err.message, 500); }
});

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
  if (b.twilio_number) {
    const conflict = await q(`SELECT name FROM schools WHERE twilio_number=$1 AND status='active' LIMIT 1`, [b.twilio_number]);
    if (conflict.rows.length) console.warn(`[MULTITENANCY] twilio_number ${b.twilio_number} already assigned to ${conflict.rows[0].name} — WhatsApp routing will be ambiguous`);
  }
  const r = await q(`INSERT INTO schools (name,city,landmark_description,fees,fee_deadline,current_term,whatsapp_number,twilio_number,admin_password,plan,status,billing_start,monthly_retainer,setup_fee)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'active',$11,$12,$13) RETURNING *`,
    [b.name,b.city,b.landmark_description||'',b.fees||'',b.fee_deadline||'',b.current_term||'',b.whatsapp_number||'',b.twilio_number||null,b.admin_password || uuid().slice(0,8),b.plan || 'starter',b.billing_start || new Date(),b.monthly_retainer || 0,b.setup_fee || 0]);
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
app.delete('/api/super/schools/:id', requireSuper, async (req, res) => { await q(`DELETE FROM schools WHERE id=$1`, [req.params.id]); json(res, { ok: true }); });

app.post('/api/admin/login', async (req, res) => {
  const r = await q(`SELECT id,name,city,status FROM schools WHERE id=$1 AND admin_password=$2`, [req.body.school_id, req.body.password]);
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
  ['students','name,class_name,parent_name,parent_phone,weekly_performance_score'], ['staff','name,role,subject,class,phone,performance_score'], ['admission_inquiries','parent_name,phone,child_name,class_applying,status'], ['school_events','title,event_date']
];
app.get('/api/admin/sickbay_log', requireSchool, async (req, res) => {
  try {
    json(res, (await q(`SELECT * FROM sickbay_log WHERE school_id=$1 ORDER BY visited_at DESC LIMIT 200`, [req.school.id])).rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/sickbay_log', requireSchool, async (req, res) => {
  try {
    const { student_id, reason, action_taken } = req.body;
    if (!student_id) return bad(res, 'student_id required');
    const school = req.school;

    const r = await q(
      `INSERT INTO sickbay_log (school_id,student_id,reason,action_taken) VALUES ($1,$2,$3,$4) RETURNING *`,
      [school.id, student_id, reason || '', action_taken || '']
    );
    const entry = r.rows[0];

    const stuRes = await q(`SELECT name, class_name, parent_phone, parent_name FROM students WHERE id=$1 AND school_id=$2 LIMIT 1`, [student_id, school.id]);
    if (stuRes.rows.length && stuRes.rows[0].parent_phone && hasTwilio()) {
      const s = stuRes.rows[0];
      const fromNumber = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
      const time = new Date().toLocaleTimeString('en-NG', { hour: '2-digit', minute: '2-digit', hour12: true });
      const msg =
        `🏥 *Sickbay Visit — ${school.name}*\n\n` +
        `Dear ${s.parent_name || 'Parent'}, we want to let you know that *${s.name}* visited the sickbay today at *${time}*.\n\n` +
        `📋 *Reason:* ${reason || 'Not specified'}\n` +
        `✅ *Action taken:* ${action_taken || 'Child is being cared for'}\n\n` +
        `${s.name} is currently in the care of our school nurse. ` +
        `Please reply to this message or call the school if you have any concerns.\n\n` +
        `${school.name} 🏫`;

      try {
        await twilioSend(s.parent_phone, fromNumber, msg);
        await q(`UPDATE sickbay_log SET parent_notified=true, notified_at=now() WHERE id=$1`, [entry.id]);
        console.log(`🏥 Sickbay notification sent to parent of ${s.name} (${s.parent_phone})`);
      } catch(e) {
        console.warn(`⚠️ Sickbay WhatsApp failed for ${s.name}: ${e.message}`);
      }
    }

    json(res, entry, 201);
  } catch(err) { bad(res, err.message, 500); }
});


app.get('/api/admin/messages', requireSchool, async (req, res) => {
  try {
    const rows = await q(`SELECT * FROM messages WHERE school_id=$1 ORDER BY created_at DESC LIMIT 100`, [req.school.id]);
    json(res, rows.rows);
  } catch(err) { json(res, { error: err.message }, 500); }
});
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
app.post('/api/admin/students', requireSchool, async (req, res) => {
  try {
    const { name, class_name, parent_name, parent_phone,
            fee_amount, fee_paid, fee_term, fee_status, date_of_birth } = req.body;
    if (!name) return bad(res, 'name required');
    const sid = req.school.id;
    const phone = parent_phone ? (parent_phone.startsWith('+') ? parent_phone : '+234' + String(parent_phone).replace(/^0/, '')) : '';
    const st = await q(
      `INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,date_of_birth,weekly_performance_score)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [sid, name.trim(), class_name||'', parent_name||'', phone, date_of_birth||null, 0]
    );
    const student = st.rows[0];
    if (fee_amount && Number(fee_amount) > 0) {
      const term = fee_term || req.school.current_term || 'Current Term';
      await createFeeRecord(sid, student.id, { amount_due: Number(fee_amount), term }, {
        amount_paid: fee_paid ? Number(fee_paid) : 0,
        fee_status: fee_status || null
      });
    }
    json(res, student, 201);
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/students/list', requireSchool, async (req, res) => {
  try {
    const rows = await q(
      `SELECT s.*,
        COALESCE(f.status, 'no record') as fee_status,
        COALESCE(f.amount_due, 0) as amount_due,
        COALESCE(f.amount_paid, 0) as amount_paid,
        GREATEST(COALESCE(f.amount_due,0) - COALESCE(f.amount_paid,0), 0) as balance,
        f.term as fee_term
       FROM students s
       LEFT JOIN fees f ON f.student_id = s.id AND f.school_id = s.school_id
       WHERE s.school_id = $1
       ORDER BY s.class_name, s.name LIMIT 500`,
      [req.school.id]
    );
    json(res, rows.rows);
  } catch(err) { json(res, { error: err.message }, 500); }
});

async function createFeeRecord(schoolId, studentId, feeSettings, perStudentFee) {
  const term = (perStudentFee?.term) || (feeSettings?.term) || null;
  const amountDue = Number(perStudentFee?.amount_due || feeSettings?.amount_due || 0);
  if (amountDue <= 0 || !term) return;
  const amountPaid = Number(perStudentFee?.amount_paid || 0);
  const status = perStudentFee?.fee_status ||
    (amountPaid >= amountDue ? 'paid' : amountPaid > 0 ? 'partial' : 'unpaid');
  const dueDate = feeSettings?.due_date || null;
  await q(`INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status,due_date)
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT (school_id,student_id,term)
           DO UPDATE SET amount_due=$4, amount_paid=$5, status=$6, due_date=$7, updated_at=now()`,
    [schoolId, studentId, term, amountDue, amountPaid, status, dueDate]);
}

app.post('/api/admin/students/import-bulk', requireSchool, async (req, res) => {
  try {
    const { students, fee_settings } = req.body;
    if (!students || !students.length) return res.status(400).json({ error: 'No students provided' });
    const sid = req.school.id;
    const term = fee_settings?.term || req.school.current_term;
    const feeSettingsWithTerm = fee_settings ? { ...fee_settings, term } : null;
    let imported = 0, skipped = 0;
    for (const s of students) {
      if (!s.name) { skipped++; continue; }
      const phone = s.parent_phone ? (s.parent_phone.startsWith('+') ? s.parent_phone : '+234' + String(s.parent_phone).replace(/^0/, '')) : '';
      const existing = await q(`SELECT id FROM students WHERE school_id=$1 AND name=$2 AND class_name=$3 LIMIT 1`, [sid, s.name, s.class_name||'']);
      if (existing.rows.length) {
        if (s.amount_due) await createFeeRecord(sid, existing.rows[0].id, feeSettingsWithTerm, s);
        skipped++; continue;
      }
      const dob = s.date_of_birth || null;
      const st = await q(`INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,date_of_birth,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
        [sid, s.name, s.class_name||'', s.parent_name||'', phone, dob, 0]);
      await createFeeRecord(sid, st.rows[0].id, feeSettingsWithTerm, s);
      imported++;
    }
    res.json({ imported, skipped });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

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
        await q(`INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, [sid, s.name, s.class_name||'', s.parent_name||'', s.parent_phone||'', 0]);
        imported++;
        if (s.class_name) classes[s.class_name] = (classes[s.class_name]||0) + 1;
      } catch(e) { skipped++; }
    }
    res.json({ imported, skipped, classes });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/students/import-pdf', requireSchool, async (req, res) => {
  try {
    const { pdfText } = req.body;
    if (!pdfText || pdfText.trim().length < 10) return res.status(400).json({ error: 'No PDF text provided.' });
    const prompt = 'Extract ALL student records from this school register text. Return ONLY a JSON array, no explanation, no markdown: [{"name":"Full Name","class_name":"Class","parent_name":"Parent name if visible","parent_phone":"Phone if visible"}]. Text: ' + pdfText.slice(0, 8000);
    const result = await callAI('You are a data extraction assistant. Extract student records and return only valid JSON array.', prompt, null);
    let students = [];
    try {
      const clean = result.replace(/```json|```/g, '').trim();
      const match = clean.match(/\[.*\]/s);
      students = JSON.parse(match ? match[0] : clean);
    } catch(e) {
      return res.status(400).json({ error: 'Could not parse student data from PDF. Try manual entry instead.' });
    }
    const { fee_settings } = req.body;
    const sid = req.school.id;
    const term = fee_settings?.term || req.school.current_term;
    const feeSettingsWithTerm = fee_settings ? { ...fee_settings, term } : null;
    let imported = 0, skipped = 0;
    for (const s of students) {
      if (!s.name || s.name.length < 2) { skipped++; continue; }
      try {
        let phone = s.parent_phone ? String(s.parent_phone).trim() : '';
        if (phone.startsWith('0')) phone = '+234' + phone.slice(1);
        else if (phone.startsWith('234')) phone = '+' + phone;
        const existing = await q(`SELECT id FROM students WHERE school_id=$1 AND name=$2 LIMIT 1`, [sid, s.name.trim()]);
        if (existing.rows.length) { skipped++; continue; }
        const st = await q(`INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
          [sid, s.name.trim(), s.class_name||'', s.parent_name||'', phone, 0]);
        await createFeeRecord(sid, st.rows[0].id, feeSettingsWithTerm, null);
        imported++;
      } catch(e) { skipped++; }
    }
    res.json({ ok: true, imported, skipped, total: students.length });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/students/add-manual', requireSchool, async (req, res) => {
  try {
    const { name, class_name, parent_name, parent_phone } = req.body;
    if (!name || name.trim().length < 2) return res.status(400).json({ error: 'Student name is required' });
    const sid = req.school.id;
    let phone = '';
    if (parent_phone) {
      phone = String(parent_phone).trim();
      if (phone.startsWith('0')) phone = '+234' + phone.slice(1);
      else if (phone.startsWith('234')) phone = '+' + phone;
      else if (!phone.startsWith('+')) phone = '+234' + phone;
    }
    const existing = await q(`SELECT id FROM students WHERE school_id=$1 AND name=$2 AND class_name=$3 LIMIT 1`, [sid, name.trim(), class_name||'']);
    if (existing.rows.length) return res.status(409).json({ error: 'A student with this name and class already exists' });
    const result = await q(
      'INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id,name,class_name,parent_name,parent_phone',
      [sid, name.trim(), class_name||'', parent_name||'', phone, 0]
    );
    res.json({ ok: true, student: result.rows[0] });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/students/import-text', requireSchool, async (req, res) => {
  try {
    const { text, class_name, fee_settings } = req.body;
    if (!text || text.trim().length < 2) return res.status(400).json({ error: 'No text provided' });
    const lines = text.split('\n').map(l => l.trim()).filter(l => l.length > 1);
    const sid = req.school.id;
    let imported = 0, skipped = 0, errors = [];
    for (const line of lines) {
      try {
        const parts = line.split(/[,\t|]/).map(p => p.trim());
        const name = parts[0];
        if (!name || name.length < 2) { skipped++; continue; }
        const parent_name = parts[1] || '';
        let parent_phone = parts[2] || '';
        if (parent_phone.startsWith('0')) parent_phone = '+234' + parent_phone.slice(1);
        else if (parent_phone.startsWith('234')) parent_phone = '+' + parent_phone;
        else if (parent_phone && !parent_phone.startsWith('+')) parent_phone = '+234' + parent_phone;
        const existing = await q(`SELECT id FROM students WHERE school_id=$1 AND name=$2 LIMIT 1`, [sid, name]);
        if (existing.rows.length) { skipped++; continue; }
        const st2 = await q(`INSERT INTO students (school_id,name,class_name,parent_name,parent_phone,weekly_performance_score) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
          [sid, name, class_name||'', parent_name, parent_phone, 0]);
        const fsTerm = fee_settings?.term || req.school.current_term;
        await createFeeRecord(sid, st2.rows[0].id, fee_settings ? {...fee_settings, term: fsTerm} : null, null);
        imported++;
      } catch(e) { errors.push(line); skipped++; }
    }
    res.json({ ok: true, imported, skipped, errors: errors.slice(0, 10) });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/send-message', requireSchool, async (req, res) => {
  try {
    const { to, message } = req.body;
    if (!to || !message) return res.status(400).json({ error: 'to and message are required' });
    const school = req.school;
    const fromNumber = school.twilio_number || process.env.TWILIO_DEFAULT_FROM || process.env.TWILIO_WHATSAPP_NUMBER;
    if (!fromNumber) return res.status(400).json({ error: 'No WhatsApp number configured for this school' });
    await twilioSend(normalisePhone(to), fromNumber, message);
    // Admin has taken over — silence the AI on this thread for 30 minutes
    await suppressAiForThread(school.id, normalisePhone(to), 30).catch(e => console.warn('[suppress]', e.message));
    await q(`INSERT INTO messages (school_id,from_number,channel,user_message,assistant_reply) VALUES ($1,$2,$3,$4,$5)`,
      [school.id, normalisePhone(to), 'admin', '[Admin reply]', message]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/admin/students/:id', requireSchool, async (req, res) => {
  try {
    await q(`DELETE FROM students WHERE id=$1 AND school_id=$2`, [req.params.id, req.school.id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── FIX 1: PATCH /api/admin/students/:id ─────────────────
app.patch('/api/admin/students/:id', requireSchool, async (req, res) => {
  try {
    const allowed = ['name', 'class_name', 'parent_name', 'parent_phone', 'date_of_birth'];
    const updates = [], params = [];
    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        let val = req.body[key];
        if (key === 'parent_phone' && val) {
          val = String(val).trim();
          if (val.startsWith('0')) val = '+234' + val.slice(1);
          else if (val.startsWith('234')) val = '+' + val;
          else if (!val.startsWith('+')) val = '+234' + val;
        }
        params.push(val);
        updates.push(`${key}=$${params.length}`);
      }
    }
    if (!updates.length) return bad(res, 'Nothing to update');
    params.push(req.params.id, req.school.id);
    await q(`UPDATE students SET ${updates.join(',')} WHERE id=$${params.length-1} AND school_id=$${params.length}`, params);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/refine-message', requireSchool, async (req, res) => {
  try {
    const { message, phone } = req.body;
    if (!message) return bad(res, 'message required');
    const school = req.school;

    let context = '';
    if (phone) {
      const student = await q(`SELECT name, class_name FROM students WHERE school_id=$1 AND parent_phone=$2 LIMIT 1`, [school.id, normalisePhone(phone)]);
      if (student.rowCount) {
        context = `You are helping the admin of ${school.name} communicate with the parent of ${student.rows[0].name} (${student.rows[0].class_name}).`;
      }
    }

    const system = `${context || 'You are helping a school admin communicate professionally with a parent.'}

Your job: take the admin's draft message and rewrite it to sound warm, professional and caring — the way a senior teacher or school director would write it on WhatsApp.

Rules:
- Keep the same meaning and intent — do NOT add or remove information
- Make it sound human, warm and personal — not stiff or corporate
- Keep it concise — WhatsApp messages should be short
- Do NOT add emojis unless the draft already has them
- Do NOT add a school name sign-off — the system adds that automatically
- Return ONLY the refined message text, nothing else`;

    const refined = await callAI(system, `Refine this message: "${message}"`, null);
    json(res, { refined: refined || message });
  } catch(err) { bad(res, err.message, 500); }
});

async function calculateStudentRisk(student, schoolId) {
  const today = new Date().toISOString().slice(0, 10);
  const threeWeeksAgo = new Date(Date.now() - 21 * 86400000).toISOString().slice(0, 10);
  const twoWeeksAgo = new Date(Date.now() - 14 * 86400000).toISOString().slice(0, 10);

  const [scoresRes, attendRes, hwRes, recentScoresRes] = await Promise.all([
    q('SELECT subject, score FROM scores WHERE student_id=$1 AND school_id=$2 ORDER BY uploaded_at DESC LIMIT 20', [student.id, schoolId]),
    q('SELECT status FROM attendance WHERE student_id=$1 AND date >= $2', [student.id, twoWeeksAgo]),
    q('SELECT id FROM homeworks WHERE school_id=$1 AND class_name=$2 AND created_at >= $3', [schoolId, student.class_name, threeWeeksAgo]),
    q('SELECT score, uploaded_at FROM scores WHERE student_id=$1 AND school_id=$2 ORDER BY uploaded_at DESC LIMIT 6', [student.id, schoolId])
  ]);

  const scores = scoresRes.rows;
  const attendance = attendRes.rows;
  const homeworksAssigned = hwRes.rows.length;

  const subjectMap = {};
  for (const s of scores) {
    if (!subjectMap[s.subject]) subjectMap[s.subject] = [];
    subjectMap[s.subject].push(Number(s.score));
  }
  const subjectAvgs = Object.entries(subjectMap).map(([subject, vals]) => ({
    subject,
    avg: vals.reduce((a, b) => a + b, 0) / vals.length
  }));
  const weakSubjects = subjectAvgs.filter(s => s.avg < 50).map(s => s.subject);
  const avgScore = subjectAvgs.length
    ? subjectAvgs.reduce((a, b) => a + b.avg, 0) / subjectAvgs.length
    : null;

  const totalDays = attendance.length;
  const presentDays = attendance.filter(a => a.status === 'present').length;
  const attendancePct = totalDays > 0 ? (presentDays / totalDays) * 100 : 100;

  const hwCompletionPct = homeworksAssigned > 0
    ? Math.min(100, (scores.length / Math.max(homeworksAssigned, 1)) * 100)
    : 100;

  let trajectory = 'stable';
  if (recentScoresRes.rows.length >= 4) {
    const recent = recentScoresRes.rows.map(r => Number(r.score));
    const older = recent.slice(Math.floor(recent.length / 2));
    const newer = recent.slice(0, Math.floor(recent.length / 2));
    const olderAvg = older.reduce((a, b) => a + b, 0) / older.length;
    const newerAvg = newer.reduce((a, b) => a + b, 0) / newer.length;
    if (newerAvg - olderAvg < -8) trajectory = 'declining';
    else if (newerAvg - olderAvg > 8) trajectory = 'improving';
  }

  const academicRisk = (avgScore !== null && avgScore < 50) || weakSubjects.length >= 2;
  const attendanceRisk = attendancePct < 75;
  const engagementRisk = hwCompletionPct < 50;
  const trajectoryRisk = trajectory === 'declining';

  const riskCount = [academicRisk, attendanceRisk, engagementRisk, trajectoryRisk].filter(Boolean).length;
  let riskLevel = 'low';
  if (riskCount === 1) riskLevel = 'medium';
  if (riskCount === 2) riskLevel = 'high';
  if (riskCount >= 3) riskLevel = 'critical';

  return {
    riskLevel, academicRisk, attendanceRisk, engagementRisk, trajectory,
    weakSubjects, avgScore: avgScore || 0, attendancePct, hwCompletionPct
  };
}

async function generateInterventionPlan(student, school, risk) {
  if (!hasTextAi()) {
    return buildFallbackPlan(student, school, risk);
  }

  const prompt = `You are EduPing, an educational intervention AI for Nigerian schools.

Student Profile:
- Name: ${student.name}
- Class: ${student.class_name}
- School: ${school.name}, ${school.city}
- Average score: ${Math.round(risk.avgScore)}%
- Weak subjects: ${risk.weakSubjects.join(', ') || 'none identified'}
- Attendance rate: ${Math.round(risk.attendancePct)}% (last 2 weeks)
- Score trend: ${risk.trajectory}
- Risk areas: ${[risk.academicRisk && 'academic performance', risk.attendanceRisk && 'attendance', risk.engagementRisk && 'homework engagement'].filter(Boolean).join(', ')}

Generate a warm, practical 4-week intervention plan for the parent. The plan must:
1. Open with an encouraging, non-alarming message about the child
2. Include a 20-minute daily study routine (realistic for Nigerian households)
3. Suggest 2-3 FREE online resources (Khan Academy, YouTube — available in Nigeria)
4. Give specific weekly focus topics for the weak subjects
5. Include one teacher support action (notify their teacher)
6. Include one behavioural/motivation tip
7. End with whether a private tutor is recommended (yes/no and why)
8. Be formatted for WhatsApp — use emojis, short lines, clear sections
9. Close with the school name and a check-in date (7 days from now)

Keep tone warm, encouraging, and specific. Written for a Nigerian parent. Not alarming.`;

  try {
    const client = getTextAiClient();
    const res = await client.chat.completions.create({
      model: getTextAiModel(),
      max_tokens: 1000,
      messages: [{ role: 'user', content: prompt }]
    });
    return res.choices[0].message.content;
  } catch (e) {
    console.error('Intervention AI error:', e.message);
    return buildFallbackPlan(student, school, risk);
  }
}

function buildFallbackPlan(student, school, risk) {
  const subjects = risk.weakSubjects.length ? risk.weakSubjects.join(' and ') : 'some subjects';
  const followUp = new Date(Date.now() + 7 * 86400000).toLocaleDateString('en-NG', { weekday: 'long', day: 'numeric', month: 'long' });
  return `📋 *${student.name}'s Study Support Plan*\n\nDear parent, we noticed ${student.name} may benefit from some extra support in ${subjects} this term. Here's a simple plan to help:\n\n📅 *Daily Routine (20 mins)*\n• Morning: Review notes from previous day\n• Evening: 10 practice questions in weak subject\n\n🎥 *Free Resources*\n• Khan Academy (khanacademy.org) — search "${subjects}"\n• YouTube: search "${subjects} for beginners"\n\n👨‍🏫 *Teacher Support*\nWe have notified ${student.name}'s class teacher to give extra attention this week.\n\n💡 *Motivation Tip*\nCelebrate small wins — praise effort, not just scores.\n\n📞 *Need a tutor?*\nReply TUTOR and we'll connect you with a vetted tutor near you.\n\nNext check-in: ${followUp}\n${school.name} 🏫`;
}

async function runRiskAssessmentForSchool(school) {
  const students = (await q(`SELECT * FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL`, [school.id])).rows;
  let flagged = 0;

  for (const student of students) {
    try {
      const risk = await calculateStudentRisk(student, school.id);

      await q(`INSERT INTO student_risk_scores
        (school_id, student_id, risk_level, academic_risk, attendance_risk, engagement_risk, trajectory, weak_subjects, avg_score, attendance_pct, hw_completion_pct, assessed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
        ON CONFLICT (student_id) DO UPDATE SET
          risk_level=$3, academic_risk=$4, attendance_risk=$5, engagement_risk=$6,
          trajectory=$7, weak_subjects=$8, avg_score=$9, attendance_pct=$10,
          hw_completion_pct=$11, assessed_at=now()`,
        [school.id, student.id, risk.riskLevel, risk.academicRisk, risk.attendanceRisk,
         risk.engagementRisk, risk.trajectory, JSON.stringify(risk.weakSubjects),
         risk.avgScore, risk.attendancePct, risk.hwCompletionPct]);

      if (['medium', 'high', 'critical'].includes(risk.riskLevel)) {
        const recent = await q(`SELECT id FROM intervention_plans WHERE student_id=$1 AND created_at > now() - interval '14 days' LIMIT 1`, [student.id]);
        if (recent.rowCount) continue;

        const planText = await generateInterventionPlan(student, school, risk);
        const followUpDate = new Date(Date.now() + 7 * 86400000).toISOString().slice(0, 10);

        const plan = await q(`INSERT INTO intervention_plans
          (school_id, student_id, risk_level, plan_text, weak_subjects, follow_up_date)
          VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
          [school.id, student.id, risk.riskLevel, planText, JSON.stringify(risk.weakSubjects), followUpDate]);

        if (hasTwilio() && student.parent_phone) {
          await twilioSend(student.parent_phone, school.twilio_number || process.env.TWILIO_DEFAULT_FROM, planText);
          await q(`UPDATE intervention_plans SET sent_to_parent=true WHERE id=$1`, [plan.rows[0].id]);
          flagged++;
        }

        if (hasTwilio() && risk.weakSubjects.length) {
          for (const subject of risk.weakSubjects) {
            const teacherRes = await q(
              `SELECT * FROM staff WHERE school_id=$1 AND phone IS NOT NULL
               AND (LOWER(subject) LIKE $2 OR LOWER(class) = $3)
               LIMIT 1`,
              [school.id, `%${subject.toLowerCase()}%`, (student.class_name || '').toLowerCase()]
            );

            if (teacherRes.rowCount) {
              const teacher = teacherRes.rows[0];
              const urgency = risk.riskLevel === 'critical' ? '🚨 URGENT' : '📋';
              const teacherMsg =
                `${urgency} *Student Support Needed*\n\n` +
                `Hi ${teacher.name}, EduPing has flagged *${student.name}* (${student.class_name}) ` +
                `as needing extra support in *${subject}*.\n\n` +
                `📊 Current average: ${Math.round(risk.avgScore)}%\n` +
                `📉 Trend: ${risk.trajectory}\n` +
                (risk.attendanceRisk ? `⚠️ Also missing classes frequently\n` : '') +
                `\n🙏 Could you give them 10 minutes of extra attention this week?\n\n` +
                `The parent has been notified and is on board.\n` +
                `${school.name} 🏫`;

              await twilioSend(
                teacher.phone,
                school.twilio_number || process.env.TWILIO_DEFAULT_FROM,
                teacherMsg
              );
              console.log(`👨‍🏫 Teacher notified: ${teacher.name} about ${student.name} (${subject})`);
            }
          }
        }

        console.log(`🚨 Intervention sent: ${student.name} (${risk.riskLevel} risk) — ${school.name}`);
      }
    } catch (e) {
      console.error(`Risk assessment failed for student ${student.id}:`, e.message);
    }
  }
  return flagged;
}

async function findTutors(subjects, city) {
  const res = await q(`SELECT * FROM tutors WHERE verified=true AND cities @> $1::jsonb ORDER BY rate_per_hour ASC LIMIT 3`, [JSON.stringify([city])]);
  if (res.rows.length) return res.rows;
  const res2 = await q(`SELECT * FROM tutors WHERE verified=true AND subjects @> $1::jsonb ORDER BY rate_per_hour ASC LIMIT 3`, [JSON.stringify([subjects[0]])]);
  return res2.rows;
}

async function handleTutorRequest(school, student, parentPhone) {
  const subjects = student.weak_subjects || [];
  const tutors = await findTutors(subjects, school.city || '');
  if (!tutors.length) {
    return `📞 *Tutor Request Received*\n\nThank you! We're building our tutor network in ${school.city}. We'll contact you within 24 hours with available tutors.\n\nFor urgent support call Philip: 07015255068\n\n${school.name} 🏫`;
  }
  let msg = `👨‍🏫 *Verified Tutors Near You*\n\nHere are tutors available for ${subjects.join(', ')}:\n\n`;
  for (const t of tutors) {
    msg += `*${t.name}*\n📚 ${(t.subjects || []).join(', ')}\n💰 ₦${Number(t.rate_per_hour).toLocaleString()}/hour\n📞 ${t.phone}\n\n`;
  }
  msg += `Contact them directly to book a session.\n${school.name} 🏫`;
  return msg;
}

app.get('/api/admin/at-risk', requireSchool, async (req, res) => {
  const rows = await q(`
    SELECT s.name, s.class_name, s.parent_phone,
           r.risk_level, r.avg_score, r.attendance_pct, r.weak_subjects, r.trajectory, r.assessed_at,
           ip.sent_to_parent, ip.created_at as plan_sent_at, ip.tutor_requested
    FROM student_risk_scores r
    JOIN students s ON s.id = r.student_id
    LEFT JOIN intervention_plans ip ON ip.student_id = r.student_id
    WHERE r.school_id = $1 AND r.risk_level != 'low'
    ORDER BY CASE r.risk_level WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END
  `, [req.school.id]);
  json(res, rows.rows);
});

app.post('/api/admin/run-risk-assessment', requireSchool, async (req, res) => {
  const flagged = await runRiskAssessmentForSchool(req.school);
  json(res, { ok: true, flagged, message: `Risk assessment complete. ${flagged} parent(s) notified.` });
});

app.get('/api/admin/interventions', requireSchool, async (req, res) => {
  const rows = await q(`
    SELECT ip.*, s.name student_name, s.class_name, s.parent_phone
    FROM intervention_plans ip JOIN students s ON s.id = ip.student_id
    WHERE ip.school_id = $1 ORDER BY ip.created_at DESC LIMIT 50
  `, [req.school.id]);
  json(res, rows.rows);
});

app.post('/api/tutors/register', async (req, res) => {
  const { name, phone, email, subjects, cities, rate_per_hour, bio } = req.body;
  if (!name || !phone) return bad(res, 'Name and phone are required');
  const r = await q(`INSERT INTO tutors (name,phone,email,subjects,cities,rate_per_hour,bio) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
    [name, phone, email || '', JSON.stringify(subjects || []), JSON.stringify(cities || []), rate_per_hour || 0, bio || '']);
  json(res, { ok: true, id: r.rows[0].id, message: 'Application received. We will verify and activate your profile within 24 hours.' }, 201);
});

app.patch('/api/super/tutors/:id/verify', requireSuper, async (req, res) => {
  await q(`UPDATE tutors SET verified=true WHERE id=$1`, [req.params.id]);
  json(res, { ok: true });
});
app.get('/api/super/tutors', requireSuper, async (req, res) => {
  json(res, (await q('SELECT * FROM tutors ORDER BY created_at DESC')).rows);
});

app.get('/api/admin/documents', requireSchool, async (req, res) => {
  try {
    const rows = await q(
      `SELECT d.*, s.name as student_name, s.class_name
       FROM documents d LEFT JOIN students s ON s.id=d.student_id
       WHERE d.school_id=$1 ORDER BY d.created_at DESC LIMIT 200`,
      [req.school.id]
    ).catch(() => ({ rows: [] }));
    json(res, rows.rows || []);
  } catch(err) { json(res, []); }
});

app.post('/api/admin/documents', requireSchool, async (req, res) => {
  try {
    const { student_id, type, title, file_url, notes } = req.body;
    await q(`CREATE TABLE IF NOT EXISTS documents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE,
      type TEXT, title TEXT, file_url TEXT, notes TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    )`);
    const r = await q(
      `INSERT INTO documents (school_id,student_id,type,title,file_url,notes) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [req.school.id, student_id||null, type||'general', title||'', file_url||'', notes||'']
    );
    json(res, r.rows[0]);
  } catch(err) { bad(res, err.message, 500); }
});

// ── FIX 4: GET /api/admin/appraisal — corrected response shape ──
app.get('/api/admin/appraisal', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const weights = req.school.config?.appraisal_weights || { punctuality: 30, attendance: 25, scores: 25, homework: 20 };

    const staffRows = await q(`SELECT * FROM staff WHERE school_id=$1 AND status != 'inactive' ORDER BY name`, [sid]);

    const staff = await Promise.all(staffRows.rows.map(async (st) => {
      const signins = await q(
        `SELECT COUNT(*)::int c FROM signin_log WHERE staff_id=$1 AND date >= current_date - interval '30 days'`,
        [st.id]
      );
      const workingDays = 22; // approx weekdays in 30 days
      const onTime = signins.rows[0].c;
      const punctualityPct = Math.min(100, Math.round((onTime / workingDays) * 100));

      const attendanceScore = Math.min(100, Math.round((st.attendance_submissions || 0) / workingDays * 100));
      const scoresScore = Math.min(100, Math.round((st.scores_uploaded || 0) / 20 * 100));
      const homeworkScore = Math.min(100, Math.round((st.homework_assigned || 0) / 4 * 100));

      const overall = Math.round(
        (punctualityPct * weights.punctuality +
         attendanceScore * weights.attendance +
         scoresScore * weights.scores +
         homeworkScore * weights.homework) / 100
      );

      const grade = overall >= 85 ? 'Excellent' : overall >= 70 ? 'Good' : overall >= 55 ? 'Satisfactory' : 'Needs Improvement';

      return {
        id: st.id, name: st.name, role: st.role, subject: st.subject,
        punctuality: { on_time: onTime, total: workingDays, percent: punctualityPct },
        submissions: { attendance: st.attendance_submissions || 0, scores: st.scores_uploaded || 0, homework: st.homework_assigned || 0 },
        overall_score: overall, grade,
      };
    }));

    json(res, { staff, weights, generated_at: new Date().toISOString() });
  } catch(err) { bad(res, err.message, 500); }
});

app.patch('/api/admin/appraisal/:staff_id', requireSchool, async (req, res) => {
  try {
    const { performance_score, notes } = req.body;
    await q(`UPDATE staff SET performance_score=$1 WHERE id=$2 AND school_id=$3`,
      [performance_score, req.params.staff_id, req.school.id]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/usage', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const [msgs, students, staff, signins, homeworks, attendance] = await Promise.all([
      q(`SELECT DATE_TRUNC('day', created_at) as day, COUNT(*) as count FROM messages WHERE school_id=$1 AND created_at >= now() - interval '30 days' GROUP BY 1 ORDER BY 1`, [sid]),
      q(`SELECT COUNT(*) as total FROM students WHERE school_id=$1`, [sid]),
      q(`SELECT COUNT(*) as total FROM staff WHERE school_id=$1 AND status='active'`, [sid]),
      q(`SELECT COUNT(*) as total FROM signin_log WHERE school_id=$1 AND date >= current_date - interval '30 days'`, [sid]),
      q(`SELECT COUNT(*) as total FROM homeworks WHERE school_id=$1 AND created_at >= now() - interval '30 days'`, [sid]),
      q(`SELECT COUNT(*) as total FROM attendance WHERE school_id=$1 AND date >= current_date - interval '30 days'`, [sid]),
    ]);
    json(res, {
      messages_chart: msgs.rows,
      totals: {
        students: Number(students.rows[0]?.total||0),
        staff: Number(staff.rows[0]?.total||0),
        signins_30d: Number(signins.rows[0]?.total||0),
        homeworks_30d: Number(homeworks.rows[0]?.total||0),
        attendance_30d: Number(attendance.rows[0]?.total||0),
      }
    });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/admission_inquiries', requireSchool, async (req, res) => {
  try {
    const rows = await q(
      `SELECT * FROM admission_inquiries WHERE school_id=$1 ORDER BY created_at DESC LIMIT 200`,
      [req.school.id]
    );
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.patch('/api/admin/admission_inquiries/:id', requireSchool, async (req, res) => {
  try {
    const { status } = req.body;
    await q(`UPDATE admission_inquiries SET status=$1 WHERE id=$2 AND school_id=$3`, [status, req.params.id, req.school.id]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/class-summary/:class_name', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const cls = decodeURIComponent(req.params.class_name);
    const [students, attendance, scores, homeworks] = await Promise.all([
      q(`SELECT id, name, parent_name, parent_phone FROM students WHERE school_id=$1 AND class_name=$2 ORDER BY name`, [sid, cls]),
      q(`SELECT student_id, status, date FROM attendance WHERE school_id=$1 AND date >= current_date - interval '14 days'
         AND student_id IN (SELECT id FROM students WHERE school_id=$1 AND class_name=$2)`, [sid, cls]),
      q(`SELECT student_id, subject, score FROM scores WHERE school_id=$1
         AND student_id IN (SELECT id FROM students WHERE school_id=$1 AND class_name=$2) ORDER BY uploaded_at DESC`, [sid, cls]),
      q(`SELECT subject, description, due_date, created_at FROM homeworks WHERE school_id=$1 AND class_name=$2 ORDER BY created_at DESC LIMIT 5`, [sid, cls]),
    ]);
    json(res, {
      class_name: cls,
      students: students.rows,
      attendance: attendance.rows,
      scores: scores.rows,
      homeworks: homeworks.rows,
    });
  } catch(err) { bad(res, err.message, 500); }
});

const CLASS_PROGRESSION = {
  'Nursery 1': 'Nursery 2', 'Nursery 2': 'Nursery 3',
  'Nursery 3': 'Primary 1', 'Primary 1': 'Primary 2',
  'Primary 2': 'Primary 3', 'Primary 3': 'Primary 4',
  'Primary 4': 'Primary 5', 'Primary 5': 'Primary 6',
  'Primary 6': 'JSS1',
  'JSS1': 'JSS2', 'JSS1A': 'JSS2A', 'JSS1B': 'JSS2B', 'JSS1C': 'JSS2C',
  'JSS2': 'JSS3', 'JSS2A': 'JSS3A', 'JSS2B': 'JSS3B', 'JSS2C': 'JSS3C',
  'JSS3': 'SS1',  'JSS3A': 'SS1A',  'JSS3B': 'SS1B',  'JSS3C': 'SS1C',
  'SS1':  'SS2',  'SS1A':  'SS2A',  'SS1B':  'SS2B',  'SS1C':  'SS2C',
  'SS2':  'SS3',  'SS2A':  'SS3A',  'SS2B':  'SS3B',  'SS2C':  'SS3C',
  'SS3': 'GRADUATED', 'SS3A': 'GRADUATED', 'SS3B': 'GRADUATED', 'SS3C': 'GRADUATED',
  'SSS1': 'SSS2', 'SSS1A': 'SSS2A', 'SSS1B': 'SSS2B',
  'SSS2': 'SSS3', 'SSS2A': 'SSS3A', 'SSS2B': 'SSS3B',
  'SSS3': 'GRADUATED', 'SSS3A': 'GRADUATED', 'SSS3B': 'GRADUATED',
};

function getNextClass(currentClass) {
  if (!currentClass) return null;
  const trimmed = currentClass.trim();
  return CLASS_PROGRESSION[trimmed] || null;
}

// ── FIX 2: GET /api/admin/classes ────────────────────────
app.get('/api/admin/classes', requireSchool, async (req, res) => {
  try {
    const rows = await q(
      `SELECT DISTINCT class_name FROM students WHERE school_id=$1 AND class_name IS NOT NULL AND class_name != '' ORDER BY class_name`,
      [req.school.id]
    );
    json(res, rows.rows.map(r => r.class_name));
  } catch(err) { bad(res, err.message, 500); }
});

// ── FIX 3: GET /api/admin/absentees/today ────────────────
app.get('/api/admin/absentees/today', requireSchool, async (req, res) => {
  try {
    const rows = await q(`
      SELECT s.name, s.class_name, s.parent_phone, a.status, a.date
      FROM attendance a
      JOIN students s ON s.id = a.student_id
      WHERE a.school_id=$1 AND a.date=current_date AND a.status='absent'
      ORDER BY s.class_name, s.name
    `, [req.school.id]);
    json(res, rows.rows.map(r => ({ ...r, marked_by: 'Teacher' })));
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/students/promotion-preview', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const students = await q(`
      SELECT s.id, s.name, s.class_name, s.status, s.parent_phone, s.parent_name,
             COALESCE(f.status,'no record') as fee_status,
             COALESCE(f.amount_due,0)::numeric as amount_due,
             COALESCE(f.amount_paid,0)::numeric as amount_paid
      FROM students s
      LEFT JOIN fees f ON f.student_id = s.id AND f.school_id = s.school_id
      WHERE s.school_id = $1 AND COALESCE(s.status,'active') = 'active'
      ORDER BY s.class_name, s.name
    `, [sid]);

    const preview = students.rows.map(s => {
      const nextClass = getNextClass(s.class_name);
      return {
        ...s,
        next_class: nextClass,
        action: nextClass === 'GRADUATED' ? 'graduate'
               : nextClass ? 'promote'
               : 'unknown',
        can_promote: nextClass !== null,
      };
    });

    const counts = {
      total: preview.length,
      promote: preview.filter(s => s.action === 'promote').length,
      graduate: preview.filter(s => s.action === 'graduate').length,
      unknown: preview.filter(s => s.action === 'unknown').length,
      unpaid_fees: preview.filter(s => s.fee_status === 'unpaid' || s.fee_status === 'partial').length,
    };

    json(res, { students: preview, counts });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/students/promote', requireSchool, async (req, res) => {
  try {
    const { overrides = {}, academic_session, new_term, fee_amount, notify_parents } = req.body;
    if (!academic_session) return bad(res, 'academic_session is required (e.g. 2025/2026)');

    const sid = req.school.id;
    const school = req.school;

    const students = await q(`
      SELECT id, name, class_name, parent_phone, parent_name
      FROM students WHERE school_id=$1 AND COALESCE(status,'active')='active'
    `, [sid]);

    let promoted = 0, graduated = 0, repeated = 0, withdrawn = 0, skipped = 0;
    const errors = [];

    for (const s of students.rows) {
      const override = overrides[s.id] || {};
      const action = override.action || (getNextClass(s.class_name) === 'GRADUATED' ? 'graduate' : 'promote');
      const nextClass = override.next_class || getNextClass(s.class_name);

      try {
        if (action === 'withdraw') {
          await q(`UPDATE students SET status='withdrawn', previous_class=$1 WHERE id=$2`, [s.class_name, s.id]);
          await q(`INSERT INTO promotion_history (school_id,student_id,student_name,from_class,to_class,academic_session,promotion_type,notes)
                   VALUES ($1,$2,$3,$4,$5,$6,'withdrawn',$7)`,
            [sid, s.id, s.name, s.class_name, s.class_name, academic_session, override.notes||null]);
          withdrawn++;

        } else if (action === 'graduate') {
          const yr = new Date().getFullYear();
          await q(`UPDATE students SET status='graduated', graduation_year=$1, previous_class=$2, class_name='Graduated' WHERE id=$3`,
            [yr, s.class_name, s.id]);
          await q(`INSERT INTO promotion_history (school_id,student_id,student_name,from_class,to_class,academic_session,promotion_type,notes)
                   VALUES ($1,$2,$3,$4,$5,$6,'graduated',$7)`,
            [sid, s.id, s.name, s.class_name, 'GRADUATED', academic_session, override.notes||null]);
          graduated++;

        } else if (action === 'repeat') {
          await q(`INSERT INTO promotion_history (school_id,student_id,student_name,from_class,to_class,academic_session,promotion_type,notes)
                   VALUES ($1,$2,$3,$4,$5,$6,'repeated',$7)`,
            [sid, s.id, s.name, s.class_name, s.class_name, academic_session, override.notes||null]);
          repeated++;

        } else if (action === 'promote' && nextClass && nextClass !== 'GRADUATED') {
          await q(`UPDATE students SET class_name=$1, previous_class=$2 WHERE id=$3`,
            [nextClass, s.class_name, s.id]);
          await q(`INSERT INTO promotion_history (school_id,student_id,student_name,from_class,to_class,academic_session,promotion_type,notes)
                   VALUES ($1,$2,$3,$4,$5,$6,'promoted',$7)`,
            [sid, s.id, s.name, s.class_name, nextClass, academic_session, override.notes||null]);

          if (fee_amount && Number(fee_amount) > 0 && new_term) {
            await createFeeRecord(sid, s.id, { amount_due: Number(fee_amount), term: new_term }, null);
          }

          if (notify_parents && s.parent_phone && hasTwilio()) {
            const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
            const msg =
              `🎉 *Congratulations, ${s.parent_name || 'Parent'}!*\n\n` +
              `We are pleased to inform you that *${s.name}* has been promoted to *${nextClass}* ` +
              `for the *${academic_session} Academic Session*.\n\n` +
              `We look forward to another great year with ${s.name.split(' ')[0]}! ` +
              `Please ensure all ${new_term || 'new term'} fees are settled before resumption.\n\n` +
              `${school.name} 🏫`;
            try {
              await twilioSend(s.parent_phone, from, msg);
              await q(`UPDATE promotion_history SET notified_parent=true WHERE student_id=$1 AND academic_session=$2`, [s.id, academic_session]);
            } catch(e) { console.warn(`Promotion WhatsApp failed for ${s.name}: ${e.message}`); }
          }
          promoted++;

        } else {
          skipped++;
        }
      } catch(e) {
        errors.push(`${s.name}: ${e.message}`);
      }
    }

    json(res, { ok: true, promoted, graduated, repeated, withdrawn, skipped, errors });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/students/promotion-history', requireSchool, async (req, res) => {
  try {
    const rows = await q(`
      SELECT ph.*, s.parent_name, s.parent_phone
      FROM promotion_history ph
      LEFT JOIN students s ON s.id = ph.student_id
      WHERE ph.school_id = $1
      ORDER BY ph.promoted_at DESC LIMIT 200
    `, [req.school.id]);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/students/alumni', requireSchool, async (req, res) => {
  try {
    const rows = await q(`
      SELECT s.*, ph.academic_session as graduation_session, ph.promoted_at as graduated_at
      FROM students s
      LEFT JOIN promotion_history ph ON ph.student_id = s.id AND ph.promotion_type = 'graduated'
      WHERE s.school_id = $1 AND s.status = 'graduated'
      ORDER BY s.graduation_year DESC, s.name
    `, [req.school.id]);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

async function dailyFeeReminders() {
  const rows = (await q(`SELECT s.name school_name, s.twilio_number, st.name student_name, st.parent_phone, f.amount_due, f.amount_paid
    FROM fees f JOIN students st ON st.id=f.student_id JOIN schools s ON s.id=f.school_id
    WHERE f.status <> 'paid' AND f.due_date <= current_date AND s.status='active'`)).rows;
  for (const r of rows) await twilioSend(r.parent_phone, r.twilio_number || process.env.TWILIO_DEFAULT_FROM, `Reminder: ${r.student_name} has outstanding fees of ₦${Number(r.amount_due - r.amount_paid).toLocaleString()}. ${r.school_name} 🏫`);
}

async function dailyBirthdayNotifications() {
  try {
    const today = new Date();
    const mm = String(today.getMonth() + 1).padStart(2, '0');
    const dd = String(today.getDate()).padStart(2, '0');
    const rows = (await q(`
      SELECT st.name, st.parent_phone, st.parent_name, s.name school_name, s.twilio_number
      FROM students st JOIN schools s ON s.id=st.school_id
      WHERE s.status='active'
        AND st.date_of_birth IS NOT NULL
        AND to_char(st.date_of_birth,'MM-DD') = $1
        AND st.parent_phone IS NOT NULL AND st.parent_phone != ''
    `, [`${mm}-${dd}`])).rows;
    for (const r of rows) {
      const from = r.twilio_number || process.env.TWILIO_DEFAULT_FROM;
      if (!from) continue;
      const msg =
        `🎂 *Happy Birthday, ${r.name}!*\n\n` +
        `Dear ${r.parent_name || 'Parent'}, today is ${r.name}'s special day! 🎉\n\n` +
        `The entire ${r.school_name} family wishes ${r.name} a wonderful birthday filled with joy and success.\n\n` +
        `May this new year of life bring great achievement! 🌟\n\n${r.school_name} 🏫`;
      try { await twilioSend(r.parent_phone, from, msg); } catch(e) { console.warn('Birthday msg failed:', e.message); }
    }
    if (rows.length) console.log(`🎂 Birthday messages sent: ${rows.length}`);
  } catch(e) { console.error('Birthday notification error:', e.message); }
}

cron.schedule('0 9 * * *', dailyFeeReminders, { timezone: 'Africa/Lagos' });
cron.schedule('0 7 * * *', dailyBirthdayNotifications, { timezone: 'Africa/Lagos' });
cron.schedule('0 17 * * 5', async () => console.log('Award calculation job placeholder ran'), { timezone: 'Africa/Lagos' });

app.post('/api/admin/notify/fee-receipt', requireSchool, async (req, res) => {
  try {
    const { student_id, amount_paid, payment_method, receipt_number } = req.body;
    if (!student_id || !amount_paid) return bad(res, 'student_id and amount_paid required');
    const school = req.school;
    const stuRes = await q(`SELECT name, parent_name, parent_phone FROM students WHERE id=$1 AND school_id=$2 LIMIT 1`, [student_id, school.id]);
    if (!stuRes.rows.length) return bad(res, 'Student not found', 404);
    const s = stuRes.rows[0];
    if (!s.parent_phone) return bad(res, 'No parent phone on file for this student');
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    if (!from) return bad(res, 'Twilio not configured');
    const date = new Date().toLocaleDateString('en-NG', { day: 'numeric', month: 'long', year: 'numeric' });
    const msg =
      `✅ *Payment Received — ${school.name}*\n\n` +
      `Dear ${s.parent_name || 'Parent'},\n\n` +
      `We confirm receipt of your payment for *${s.name}*.\n\n` +
      `💰 *Amount:* ₦${Number(amount_paid).toLocaleString()}\n` +
      `📅 *Date:* ${date}\n` +
      (payment_method ? `💳 *Method:* ${payment_method}\n` : '') +
      (receipt_number ? `🧾 *Receipt No:* ${receipt_number}\n` : '') +
      `\nThank you for your prompt payment.\n\n${school.name} 🏫`;
    await twilioSend(s.parent_phone, from, msg);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

cron.schedule('0 6 * * 1', async () => {
  console.log('🔍 Running weekly risk assessment for all schools...');
  const schools = (await q("SELECT * FROM schools WHERE status='active'")).rows;
  for (const school of schools) {
    try {
      const flagged = await runRiskAssessmentForSchool(school);
      console.log(`✅ ${school.name}: ${flagged} students flagged`);
    } catch (e) {
      console.error(`Risk assessment failed for ${school.name}:`, e.message);
    }
  }
}, { timezone: 'Africa/Lagos' });

app.get('/api/admin/homeworks', requireSchool, async (req, res) => {
  try {
    const rows = await q(`
      SELECT h.*, st.name as assigned_by_name
      FROM homeworks h
      LEFT JOIN staff st ON st.id = h.assigned_by
      WHERE h.school_id=$1
      ORDER BY h.created_at DESC LIMIT 100
    `, [req.school.id]);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/message/send', requireSchool, async (req, res) => {
  try {
    const { to, message } = req.body;
    if (!to || !message) return bad(res, 'to and message are required');
    const school = req.school;
    const fromNumber = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    if (!fromNumber) return bad(res, 'No Twilio number configured');
    await twilioSend(to, fromNumber, message);
    await suppressAiForThread(school.id, normalisePhone(to), 30);
    await q(`INSERT INTO messages (school_id,from_number,channel,user_message,assistant_reply)
             VALUES ($1,$2,$3,$4,$5)`,
      [school.id, normalisePhone(to), 'admin', '[Admin direct message]', message]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/students/:id/profile', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const studentId = req.params.id;
    const stuRes = await q(`SELECT * FROM students WHERE id=$1 AND school_id=$2 LIMIT 1`, [studentId, sid]);
    if (!stuRes.rows.length) return bad(res, 'Student not found', 404);
    const s = stuRes.rows[0];

    const [scores, fees, attendance, sickbay, homeworks, behaviour, risk, results] = await Promise.all([
      q(`SELECT subject, score, term, uploaded_at FROM scores WHERE student_id=$1 AND school_id=$2 ORDER BY uploaded_at DESC LIMIT 20`, [studentId, sid]),
      q(`SELECT * FROM fees WHERE student_id=$1 AND school_id=$2 ORDER BY created_at DESC LIMIT 5`, [studentId, sid]),
      q(`SELECT date, status FROM attendance WHERE student_id=$1 AND school_id=$2 ORDER BY date DESC LIMIT 30`, [studentId, sid]),
      q(`SELECT reason, action_taken, parent_notified, visited_at FROM sickbay_log WHERE student_id=$1 AND school_id=$2 ORDER BY visited_at DESC LIMIT 10`, [studentId, sid]),
      q(`SELECT subject, description, due_date FROM homeworks WHERE school_id=$1 AND class_name=$2 ORDER BY created_at DESC LIMIT 5`, [sid, s.class_name]),
      q(`SELECT note, created_at FROM behaviour_notes WHERE student_id=$1 AND school_id=$2 ORDER BY created_at DESC LIMIT 5`, [studentId, sid]),
      q(`SELECT risk_level, avg_score, attendance_pct, weak_subjects, trajectory, assessed_at FROM student_risk_scores WHERE student_id=$1 AND school_id=$2 LIMIT 1`, [studentId, sid]),
      q(`SELECT term, subjects, position, remark FROM student_results WHERE student_id=$1 AND school_id=$2 ORDER BY created_at DESC LIMIT 3`, [studentId, sid]),
    ]);

    const attendanceArr = attendance.rows;
    const presentCount = attendanceArr.filter(a => a.status === 'present').length;
    const attendancePct = attendanceArr.length ? Math.round((presentCount / attendanceArr.length) * 100) : null;

    json(res, {
      ...s,
      scores: scores.rows,
      fees: fees.rows,
      attendance: attendanceArr,
      attendance_pct: attendancePct,
      sickbay: sickbay.rows,
      homeworks: homeworks.rows,
      behaviour: behaviour.rows,
      risk: risk.rows[0] || null,
      results: results.rows,
    });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/attendance', requireSchool, async (req, res) => {
  try {
    const { from, to, student_id } = req.query;
    let sql = 'SELECT a.date, a.status, a.student_id, s.name, s.class_name FROM attendance a JOIN students s ON s.id=a.student_id WHERE a.school_id=$1';
    const params = [req.school.id];
    if (from) { params.push(from); sql += ` AND a.date >= $${params.length}`; }
    if (to) { params.push(to); sql += ` AND a.date <= $${params.length}`; }
    if (student_id) { params.push(student_id); sql += ` AND a.student_id = $${params.length}`; }
    sql += ' ORDER BY a.date DESC LIMIT 2000';
    json(res, (await q(sql, params)).rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/school', requireSchool, async (req, res) => {
  try {
    const r = await q(`SELECT id, name, city, plan, status, twilio_number, events_enabled, current_term FROM schools WHERE id=$1`, [req.school.id]);
    json(res, r.rows[0] || {});
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/events', requireSchool, async (req, res) => {
  try {
    const rows = await q(`SELECT * FROM school_events WHERE school_id=$1 ORDER BY date ASC`, [req.school.id]);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/events', requireSchool, async (req, res) => {
  try {
    const { name, date, time, type, description, notify_parents } = req.body;
    if (!name || !date) return bad(res, 'name and date required');
    const r = await q(
      'INSERT INTO school_events (school_id,name,date,time,type,description,notify_parents) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *',
      [req.school.id, name, date, time||null, type||'social', description||null, notify_parents||false]
    );
    json(res, r.rows[0]);
  } catch(err) { bad(res, err.message, 500); }
});

app.delete('/api/admin/events/:id', requireSchool, async (req, res) => {
  try {
    await q(`DELETE FROM school_events WHERE id=$1 AND school_id=$2`, [req.params.id, req.school.id]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/events/:id/notify', requireSchool, async (req, res) => {
  try {
    const school = req.school;
    const evtRes = await q(`SELECT * FROM school_events WHERE id=$1 AND school_id=$2`, [req.params.id, school.id]);
    if (!evtRes.rows.length) return bad(res, 'Event not found', 404);
    const evt = evtRes.rows[0];
    const parents = await q(`SELECT DISTINCT parent_phone, name FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL AND parent_phone != ''`, [school.id]);
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    const dateStr = new Date(evt.date).toLocaleDateString('en-NG', { weekday:'long', day:'numeric', month:'long', year:'numeric' });
    const msg = `📅 *Event Reminder — ${school.name}*\n\n*${evt.name}*\n📆 ${dateStr}${evt.time ? ' · ' + evt.time : ''}\n${evt.description ? '\n' + evt.description + '\n' : ''}\n${school.name} 🏫`;
    let sent = 0;
    for (const p of parents.rows) {
      try { await twilioSend(p.parent_phone, from, msg); sent++; } catch(e) { console.warn('Failed to notify:', p.parent_phone); }
    }
    json(res, { ok: true, sent });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/events/notify-all', requireSchool, async (req, res) => {
  try {
    const school = req.school;
    const events = await q(`SELECT * FROM school_events WHERE school_id=$1 AND date >= current_date ORDER BY date ASC LIMIT 5`, [school.id]);
    if (!events.rows.length) return json(res, { ok: true, sent: 0 });
    const parents = await q(`SELECT DISTINCT parent_phone FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL AND parent_phone != ''`, [school.id]);
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    const eventList = events.rows.map(e => {
      const d = new Date(e.date).toLocaleDateString('en-NG', { day:'numeric', month:'short' });
      return '📅 *' + e.name + '* — ' + d;
    }).join('\n');
    const msg = `📅 *Upcoming Events — ${school.name}*\n\n${eventList}\n\nStay updated with EduPing!\n${school.name} 🏫`;
    let sent = 0;
    for (const p of parents.rows) {
      try { await twilioSend(p.parent_phone, from, msg); sent++; } catch(e) {}
    }
    json(res, { ok: true, sent });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/events/:id/feed', requireSchool, async (req, res) => {
  try {
    const { message } = req.body;
    if (!message) return bad(res, 'message required');
    await q(`INSERT INTO event_feeds (school_id,event_id,message) VALUES ($1,$2,$3)`, [req.school.id, req.params.id, message]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/events/:id/photos', requireSchool, async (req, res) => {
  try {
    const { image_data, filename, mime_type } = req.body;
    if (!image_data) return bad(res, 'image_data required');

    const cloudName = process.env.CLOUDINARY_CLOUD_NAME;
    const apiKey = process.env.CLOUDINARY_API_KEY;
    const apiSecret = process.env.CLOUDINARY_API_SECRET;
    if (!cloudName || !apiKey || !apiSecret) return bad(res, 'Cloudinary not configured');

    const timestamp = Math.round(Date.now() / 1000);
    const folder = 'eduping/' + req.school.id + '/events/' + req.params.id;
    const sig = crypto.createHash('sha1').update('folder=' + folder + '&timestamp=' + timestamp + apiSecret).digest('hex');

    const formData = new URLSearchParams();
    formData.append('file', 'data:' + (mime_type||'image/jpeg') + ';base64,' + image_data);
    formData.append('api_key', apiKey);
    formData.append('timestamp', timestamp);
    formData.append('folder', folder);
    formData.append('signature', sig);

    const uploadRes = await fetch('https://api.cloudinary.com/v1_1/' + cloudName + '/image/upload', {
      method: 'POST',
      body: formData
    });
    const uploadData = await uploadRes.json();
    if (uploadData.error) return bad(res, uploadData.error.message);

    let gallery = await q(`SELECT * FROM event_galleries WHERE event_id=$1 AND school_id=$2`, [req.params.id, req.school.id]);
    if (!gallery.rows.length) {
      const evtName = (await q(`SELECT name FROM school_events WHERE id=$1`, [req.params.id])).rows[0]?.name || 'Event';
      const token = crypto.randomBytes(16).toString('hex');
      gallery = await q(`INSERT INTO event_galleries (school_id,event_id,event_name,share_token) VALUES ($1,$2,$3,$4) RETURNING *`,
        [req.school.id, req.params.id, evtName, token]);
    }
    const galleryId = gallery.rows[0].id;

    await q(`INSERT INTO event_photos (gallery_id,school_id,event_id,url,public_id,filename) VALUES ($1,$2,$3,$4,$5,$6)`,
      [galleryId, req.school.id, req.params.id, uploadData.secure_url, uploadData.public_id, filename||'photo']);

    json(res, { ok: true, url: uploadData.secure_url });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/events/galleries', requireSchool, async (req, res) => {
  try {
    const galleries = await q(`SELECT * FROM event_galleries WHERE school_id=$1 ORDER BY created_at DESC`, [req.school.id]);
    const result = [];
    for (const g of galleries.rows) {
      const photos = await q(`SELECT url, public_id FROM event_photos WHERE gallery_id=$1 ORDER BY created_at DESC`, [g.id]);
      result.push({ ...g, photos: photos.rows, photo_count: photos.rows.length });
    }
    json(res, result);
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/events/:id/blast-gallery', requireSchool, async (req, res) => {
  try {
    const { share_url } = req.body;
    const school = req.school;
    const evtRes = await q(`SELECT name FROM school_events WHERE id=$1 AND school_id=$2`, [req.params.id, school.id]);
    const evtName = evtRes.rows[0]?.name || 'Event';
    const parents = await q(`SELECT DISTINCT parent_phone FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL AND parent_phone != ''`, [school.id]);
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    const msg = `📸 *${evtName} — Photo Gallery*\n\nDear parent, photos from *${evtName}* are now available!\n\nView and download your child's photos here:\n${share_url}\n\n${school.name} 🏫`;
    let sent = 0;
    for (const p of parents.rows) {
      try { await twilioSend(p.parent_phone, from, msg); sent++; } catch(e) {}
    }
    json(res, { ok: true, sent });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/gallery/:token', async (req, res) => {
  try {
    const gallery = await q(`SELECT eg.*, s.name as school_name FROM event_galleries eg JOIN schools s ON s.id=eg.school_id WHERE eg.share_token=$1`, [req.params.token]);
    if (!gallery.rows.length) return res.status(404).send('Gallery not found');
    const g = gallery.rows[0];
    const photos = await q(`SELECT url FROM event_photos WHERE gallery_id=$1 ORDER BY created_at ASC`, [g.id]);
    const html = `<!DOCTYPE html><html><head>
      <title>${g.event_name} — ${g.school_name}</title>
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <style>
        *{box-sizing:border-box;margin:0;padding:0;}
        body{font-family:'DM Sans',Arial,sans-serif;background:#f0f2f5;min-height:100vh;}
        .header{background:#1a7a4a;color:white;padding:20px;text-align:center;}
        .header h1{font-size:22px;font-weight:700;}
        .header p{font-size:13px;opacity:.8;margin-top:4px;}
        .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;padding:20px;}
        .photo-wrap{position:relative;aspect-ratio:1;overflow:hidden;border-radius:10px;background:#ddd;}
        .photo-wrap img{width:100%;height:100%;object-fit:cover;display:block;}
        .download-btn{position:absolute;bottom:6px;right:6px;background:rgba(0,0,0,.6);color:white;border:none;border-radius:6px;padding:4px 8px;font-size:11px;cursor:pointer;text-decoration:none;}
        .footer{text-align:center;padding:20px;font-size:12px;color:#667781;}
        .empty{text-align:center;padding:60px;color:#667781;}
      </style>
    </head><body>
      <div class="header">
        <h1>📸 ${g.event_name}</h1>
        <p>${g.school_name} · ${photos.rows.length} photos</p>
      </div>
      ${photos.rows.length
        ? '<div class="grid">' + photos.rows.map(p =>
            '<div class="photo-wrap"><img src="' + p.url + '" loading="lazy"><a class="download-btn" href="' + p.url + '?fl_attachment=1" download>⬇ Save</a></div>'
          ).join('') + '</div>'
        : '<div class="empty">No photos uploaded yet</div>'
      }
      <div class="footer">Powered by EduPing · eduping.org</div>
    </body></html>`;
    res.send(html);
  } catch(err) { res.status(500).send('Error loading gallery'); }
});

app.post('/api/waitlist', async (req, res) => {
  try {
    const { name, role, school, city, phone, email, students, timeline, features, challenge, submitted_at } = req.body;
    if (!name || !school || !phone) return bad(res, 'name, school and phone are required');

    await q(`INSERT INTO waitlist (name,role,school,city,phone,email,students,timeline,features,challenge,submitted_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [name, role||null, school, city||null, phone, email||null, students||null, timeline||null, features||null, challenge||null, submitted_at||new Date().toISOString()]);

    const notifyPhone = process.env.ADMIN_PHONE || '+2347015255068';
    const fromNumber = process.env.TWILIO_DEFAULT_FROM;
    if (fromNumber) {
      const msg = `🎉 *New EduPing Waitlist Lead!*

` +
        `👤 *Name:* ${name}
` +
        `🏫 *School:* ${school}
` +
        `📍 *City:* ${city||'—'}
` +
        `💼 *Role:* ${role||'—'}
` +
        `📱 *Phone:* ${phone}
` +
        `📧 *Email:* ${email||'—'}
` +
        `👥 *Students:* ${students||'—'}
` +
        `⏱ *Timeline:* ${timeline||'—'}
` +
        `✨ *Features wanted:* ${features||'—'}
` +
        `💬 *Challenge:* ${challenge||'—'}`;
      try { await twilioSend(notifyPhone, fromNumber, msg); } catch(e) { console.warn('Could not notify admin of lead:', e.message); }
    }

    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/waitlist', requireSchool, async (req, res) => {
  try {
    const rows = await q(`SELECT * FROM waitlist ORDER BY created_at DESC`, []);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.patch('/api/admin/waitlist/:id/contacted', requireSchool, async (req, res) => {
  try {
    await q(`UPDATE waitlist SET contacted=true WHERE id=$1`, [req.params.id]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/chats', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const type = req.query.type || 'parent';
    if (type === 'staff') {
      const staff = await q(`SELECT phone, name FROM staff WHERE school_id=$1 AND phone IS NOT NULL AND phone != ''`, [sid]);
      const result = [];
      for (const s of staff.rows) {
        const last = await q(`SELECT user_message, created_at FROM messages WHERE school_id=$1 AND from_number LIKE $2 ORDER BY created_at DESC LIMIT 1`,
          [sid, '%' + s.phone.replace('+','').slice(-9) + '%']);
        result.push({ phone: s.phone, name: s.name, last_message: last.rows[0]?.user_message?.slice(0,60) || '—', last_time: last.rows[0]?.created_at });
      }
      return json(res, result);
    }
    const rows = await q(`
      SELECT DISTINCT ON (m.from_number)
        m.from_number as phone,
        m.user_message as last_message,
        m.assistant_reply as last_reply,
        m.created_at as last_time,
        COALESCE(s.parent_name, m.from_number) as parent_name,
        s.name as student_name,
        s.class_name
      FROM messages m
      LEFT JOIN students s ON s.school_id = m.school_id AND s.parent_phone = m.from_number
      WHERE m.school_id=$1
        AND COALESCE(m.channel,'') != 'teacher'
        AND m.from_number NOT IN (SELECT COALESCE(phone,'') FROM staff WHERE school_id=$1)
      ORDER BY m.from_number, m.created_at DESC
    `, [sid]);
    const escalationPhrases = ['pass your question', 'contact the school', 'reach out directly'];
    const out = rows.rows.map(r => ({
      ...r,
      needs_attention: escalationPhrases.some(p => (r.last_reply || '').toLowerCase().includes(p))
    })).sort((a, b) => new Date(b.last_time) - new Date(a.last_time));
    json(res, out);
  } catch(err) { bad(res, err.message, 500); }
});

// ── FIX (was bug 4, already correct here vs columns) ─────
app.get('/api/admin/chats/:phone/messages', requireSchool, async (req, res) => {
  try {
    const phone = decodeURIComponent(req.params.phone).replace(/\D/g,'').slice(-10);
    const rows = await q(`SELECT user_message, assistant_reply, channel, created_at FROM messages
      WHERE school_id=$1 AND from_number LIKE $2
      ORDER BY created_at ASC LIMIT 100`, [req.school.id, '%' + phone + '%']);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/school/term', requireSchool, async (req, res) => {
  try {
    const { current_term, term_start, term_end, fee_deadline } = req.body;
    await q(`UPDATE schools SET current_term=$1, term_start=$2, term_end=$3, fee_deadline=$4 WHERE id=$5`,
      [current_term||null, term_start||null, term_end||null, fee_deadline||null, req.school.id]);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.patch('/api/admin/staff/:id', requireSchool, async (req, res) => {
  try {
    const allowed = ['phone', 'class', 'subject', 'status', 'email'];
    const updates = []; const params = [];
    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        params.push(req.body[key]);
        updates.push(key + '=$' + params.length);
      }
    }
    if (!updates.length) return bad(res, 'Nothing to update');
    params.push(req.params.id, req.school.id);
    await q('UPDATE staff SET ' + updates.join(',') + ' WHERE id=$' + (params.length-1) + ' AND school_id=$' + params.length, params);
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/broadcast', requireSchool, async (req, res) => {
  try {
    const { message, target, class_name } = req.body;
    if (!message) return bad(res, 'message required');
    const school = req.school;
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    if (!from) return bad(res, 'Twilio not configured');
    let phones = [];

    if (target === 'all_parents') {
      const rows = await q(`SELECT DISTINCT parent_phone FROM students WHERE school_id=$1 AND parent_phone IS NOT NULL AND parent_phone != ''`, [school.id]);
      phones = rows.rows.map(r => r.parent_phone);
    } else if (target === 'staff') {
      const rows = await q(`SELECT phone FROM staff WHERE school_id=$1 AND phone IS NOT NULL AND phone != '' AND status != 'inactive'`, [school.id]);
      phones = rows.rows.map(r => r.phone);
    } else if (target === 'class' && class_name) {
      const rows = await q(`SELECT DISTINCT parent_phone FROM students WHERE school_id=$1 AND class_name=$2 AND parent_phone IS NOT NULL AND parent_phone != ''`, [school.id, class_name]);
      phones = rows.rows.map(r => r.parent_phone);
    } else if (target === 'fee_defaulters') {
      const rows = await q(`SELECT s.parent_phone FROM students s JOIN fees f ON f.student_id=s.id WHERE s.school_id=$1 AND f.status='unpaid' AND s.parent_phone IS NOT NULL AND s.parent_phone != ''`, [school.id]);
      phones = rows.rows.map(r => r.parent_phone);
    } else if (target === 'absent_today') {
      const rows = await q(`SELECT s.parent_phone FROM attendance a JOIN students s ON s.id=a.student_id WHERE a.school_id=$1 AND a.date=current_date AND a.status='absent' AND s.parent_phone IS NOT NULL`, [school.id]);
      phones = rows.rows.map(r => r.parent_phone);
    }

    let sent = 0;
    for (const phone of phones) {
      try { await twilioSend(phone, from, message); sent++; } catch(e) { console.warn('Broadcast failed to:', phone); }
    }
    json(res, { ok: true, sent, total: phones.length });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/broadcast/progress-reports', requireSchool, async (req, res) => {
  try {
    const { class_name } = req.body;
    if (!class_name) return bad(res, 'class_name required');
    const school = req.school;
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    const students = await q(`SELECT * FROM students WHERE school_id=$1 AND class_name=$2 AND parent_phone IS NOT NULL`, [school.id, class_name]);
    let sent = 0;
    for (const s of students.rows) {
      try {
        const scores = await q(`SELECT subject, score FROM scores WHERE student_id=$1 AND school_id=$2 ORDER BY uploaded_at DESC LIMIT 10`, [s.id, school.id]);
        const attRes = await q(`SELECT status FROM attendance WHERE student_id=$1 AND school_id=$2 AND date >= current_date - 30`, [s.id, school.id]);
        const present = attRes.rows.filter(a => a.status === 'present').length;
        const attPct = attRes.rows.length ? Math.round((present/attRes.rows.length)*100) : null;
        let scoreText = '';
        if (scores.rows.length) {
          scoreText = '\n\n*📊 Recent Scores:*\n' + scores.rows.map(sc => sc.subject + ': ' + sc.score + '%').join('\n');
        }
        const attLine = attPct !== null ? '\n✅ Attendance (last 30 days): *' + attPct + '%*' : '';
        const msg = '📄 *Progress Report — ' + school.name + '*\n\nDear Parent of *' + s.name + '* (' + class_name + '),' + attLine + scoreText + '\n\nFor a full report or to ask questions, reply to this message.\n\n' + school.name + ' 🏫';
        await twilioSend(s.parent_phone, from, msg);
        sent++;
      } catch(e) { console.warn('Report failed for', s.name); }
    }
    json(res, { ok: true, sent });
  } catch(err) { bad(res, err.message, 500); }
});

function generatePIN() {
  const seg = () => Math.floor(1000 + Math.random() * 9000);
  return `EPG-${seg()}-${seg()}`;
}

app.post('/api/admin/results', requireSchool, async (req, res) => {
  try {
    const { student_id, class_name, term, subjects, position, remark } = req.body;
    if (!student_id) return bad(res, 'student_id required');
    const existing = await q('SELECT id FROM student_results WHERE student_id=$1 AND school_id=$2 AND term=$3', [student_id, req.school.id, term]);
    if (existing.rows.length) {
      await q('UPDATE student_results SET subjects=$1, position=$2, remark=$3, class_name=$4, updated_at=NOW() WHERE id=$5',
        [JSON.stringify(subjects||{}), position||null, remark||null, class_name, existing.rows[0].id]);
    } else {
      await q('INSERT INTO student_results (school_id,student_id,class_name,term,subjects,position,remark) VALUES ($1,$2,$3,$4,$5,$6,$7)',
        [req.school.id, student_id, class_name, term, JSON.stringify(subjects||{}), position||null, remark||null]);
    }
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/results/stats', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const [uploaded, pins, accessed, pending] = await Promise.all([
      q('SELECT COUNT(*)::int c FROM student_results WHERE school_id=$1', [sid]),
      q('SELECT COUNT(*)::int c FROM result_pins WHERE school_id=$1', [sid]),
      q('SELECT COUNT(*)::int c FROM result_pins WHERE school_id=$1 AND accessed=true', [sid]),
      q(`SELECT COUNT(*)::int c FROM students s 
         LEFT JOIN fees f ON f.student_id=s.id AND f.school_id=s.school_id
         WHERE s.school_id=$1 AND COALESCE(f.status,'unpaid') != 'paid'`, [sid])
    ]);
    json(res, { uploaded: uploaded.rows[0].c, pins: pins.rows[0].c, accessed: accessed.rows[0].c, fee_pending: pending.rows[0].c });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/results/release', requireSchool, async (req, res) => {
  try {
    const { model, term, class_name, print_limit, expiry_days } = req.body;
    const school = req.school;
    const from = school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    const expiresAt = new Date(Date.now() + (expiry_days || 30) * 86400000);
    const baseUrl = process.env.BASE_URL || 'https://eduping.org';

    let sql = `SELECT sr.*, s.name as student_name, s.parent_phone, s.class_name,
               COALESCE(f.status,'unpaid') as fee_status
               FROM student_results sr
               JOIN students s ON s.id=sr.student_id
               LEFT JOIN fees f ON f.student_id=s.id AND f.school_id=s.school_id
               WHERE sr.school_id=$1 AND sr.term=$2`;
    const params = [school.id, term];
    if (class_name) { params.push(class_name); sql += ` AND sr.class_name=$${params.length}`; }

    const results = await q(sql, params);
    let sent = 0, pending = 0;

    for (const r of results.rows) {
      if (!r.parent_phone) continue;
      const hasPaidFees = r.fee_status === 'paid';
      const canAccess = model === 'free' || (model === 'bundled' && hasPaidFees) || model === 'paid';

      if (canAccess) {
        let pinRecord = await q('SELECT * FROM result_pins WHERE student_id=$1 AND school_id=$2 AND term=$3', [r.student_id, school.id, term]);
        let pin;
        if (pinRecord.rows.length) {
          pin = pinRecord.rows[0].pin;
        } else {
          pin = generatePIN();
          await q(`INSERT INTO result_pins (school_id,student_id,student_name,class_name,term,pin,print_limit,expires_at,sent_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())`,
            [school.id, r.student_id, r.student_name, r.class_name, term, pin, print_limit||3, expiresAt]);
        }
        const msg = `📄 *Result Ready — ${school.name}*

Dear Parent, *${r.student_name}*'s ${term} result is now available.

🔑 Your result PIN: *${pin}*

📱 View result here:
${baseUrl}/results

Enter your child's Student ID and PIN to view and print.

⏳ PIN valid for ${expiry_days||30} days · ${print_limit||3} prints allowed
${school.name} 🏫`;
        try { await twilioSend(r.parent_phone, from, msg); sent++; } catch(e) { console.warn('Failed to send PIN to', r.parent_phone); }
      } else {
        const msg = `⏳ *Result Pending — ${school.name}*

Dear Parent, *${r.student_name}*'s ${term} result is ready.

However, there are outstanding school fees on your child's account.

Kindly visit the school bursar to clear the balance and receive your result PIN.

${school.name} 🏫`;
        try { await twilioSend(r.parent_phone, from, msg); pending++; } catch(e) {}
      }
    }
    json(res, { ok: true, sent, pending, total: results.rows.length });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/results/pins', requireSchool, async (req, res) => {
  try {
    const rows = await q('SELECT * FROM result_pins WHERE school_id=$1 ORDER BY sent_at DESC LIMIT 500', [req.school.id]);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/results', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EduPing — View Result</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'DM Sans',sans-serif;background:#f0f2f5;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:flex-start;padding:40px 16px;}
.card{background:white;border-radius:16px;padding:32px;width:100%;max-width:440px;box-shadow:0 4px 24px rgba(0,0,0,.08);}
.logo{font-size:22px;font-weight:700;color:#0f1a14;text-align:center;margin-bottom:8px;}
.logo span{color:#0055CC;}
.subtitle{font-size:14px;color:#667781;text-align:center;margin-bottom:28px;}
.form-group{margin-bottom:16px;}
label{font-size:12px;font-weight:600;color:#667781;letter-spacing:.5px;text-transform:uppercase;display:block;margin-bottom:6px;}
input{width:100%;padding:12px 14px;border:1.5px solid #e9edef;border-radius:8px;font-size:14px;font-family:'DM Sans',sans-serif;outline:none;transition:border-color .2s;}
input:focus{border-color:#1a7a4a;}
.btn{width:100%;background:#1a7a4a;color:white;border:none;padding:14px;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;font-family:'DM Sans',sans-serif;margin-top:4px;}
.btn:hover{background:#0d4a2c;}
.error{background:#fee2e2;color:#991b1b;padding:10px 14px;border-radius:8px;font-size:13px;margin-bottom:16px;display:none;}
.footer{font-size:12px;color:#aab;text-align:center;margin-top:16px;}
</style>
</head>
<body>
<div class="card">
  <div class="logo">Edu<span>Ping</span></div>
  <div class="subtitle">Enter your details to view your child's result</div>
  <div class="error" id="error-msg"></div>
  <div class="form-group">
    <label>Student ID or Full Name</label>
    <input type="text" id="student-id" placeholder="e.g. Amara Okafor or STU-001">
  </div>
  <div class="form-group">
    <label>Result PIN</label>
    <input type="text" id="result-pin" placeholder="EPG-XXXX-XXXX" style="letter-spacing:2px;font-weight:600;">
  </div>
  <button class="btn" onclick="viewResult()">View Result →</button>
  <div class="footer">Powered by EduPing · eduping.org</div>
</div>
<script>
async function viewResult() {
  const studentId = document.getElementById('student-id').value.trim();
  const pin = document.getElementById('result-pin').value.trim().toUpperCase();
  const errEl = document.getElementById('error-msg');
  errEl.style.display = 'none';
  if (!studentId || !pin) { errEl.textContent = 'Please enter both your child\\'s name and the PIN.'; errEl.style.display = 'block'; return; }
  try {
    const res = await fetch('/api/results/verify', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ student_id: studentId, pin }) });
    const data = await res.json();
    if (!data.ok) { errEl.textContent = data.error || 'Invalid PIN or student details. Please check and try again.'; errEl.style.display = 'block'; return; }
    window.location.href = '/results/' + data.token;
  } catch(e) { errEl.textContent = 'Something went wrong. Please try again.'; errEl.style.display = 'block'; }
}
document.getElementById('result-pin').addEventListener('keydown', e => { if(e.key==='Enter') viewResult(); });
</script>
</body>
</html>`);
});

app.post('/api/results/verify', async (req, res) => {
  try {
    const { student_id, pin } = req.body;
    if (!student_id || !pin) return bad(res, 'student_id and pin required');
    const pinRecord = await q(`SELECT rp.*, sr.subjects, sr.position, sr.remark, sr.term, sr.class_name,
                                s.name as student_name, s.id as sid, sch.name as school_name
                                FROM result_pins rp
                                JOIN student_results sr ON sr.student_id=rp.student_id AND sr.term=rp.term
                                JOIN students s ON s.id=rp.student_id
                                JOIN schools sch ON sch.id=rp.school_id
                                WHERE rp.pin=$1 AND (s.name ILIKE $2 OR s.id::text=$3)`,
      [pin.toUpperCase(), `%${student_id}%`, student_id]);
    if (!pinRecord.rows.length) return json(res, { ok: false, error: 'Invalid PIN or student name. Please check and try again.' });
    const record = pinRecord.rows[0];
    if (record.expires_at && new Date(record.expires_at) < new Date()) return json(res, { ok: false, error: 'This PIN has expired. Please contact your school.' });
    if (record.print_limit > 0 && record.print_count >= record.print_limit) return json(res, { ok: false, error: 'This PIN has reached its print limit. Please contact your school.' });
    const token = crypto.randomBytes(20).toString('hex');
    await q('UPDATE result_pins SET access_token=$1, accessed=true, print_count=print_count+1 WHERE id=$2', [token, record.id]);
    json(res, { ok: true, token });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/results/:token', async (req, res) => {
  try {
    const record = await q(`SELECT rp.*, sr.subjects, sr.position, sr.remark, sr.term, sr.class_name,
                             s.name as student_name, sch.name as school_name, sch.city
                             FROM result_pins rp
                             JOIN student_results sr ON sr.student_id=rp.student_id AND sr.term=rp.term
                             JOIN students s ON s.id=rp.student_id
                             JOIN schools sch ON sch.id=rp.school_id
                             WHERE rp.access_token=$1`, [req.params.token]);
    if (!record.rows.length) return res.status(404).send('Result not found or link has expired.');
    const r = record.rows[0];
    const subjects = typeof r.subjects === 'string' ? JSON.parse(r.subjects) : r.subjects || {};
    const subjectRows = Object.entries(subjects).map(([subj, score]) => {
      const s = Number(score);
      const grade = s>=70?'A':s>=60?'B':s>=50?'C':s>=40?'D':'F';
      const gc = s>=70?'#1a7a4a':s>=60?'#0055cc':s>=50?'#f59e0b':s>=40?'#f97316':'#e53935';
      const remark = s>=70?'Excellent':s>=60?'Very Good':s>=50?'Good':s>=40?'Average':'Needs Improvement';
      return `<tr><td>${subj}</td><td style="text-align:center;">${score}</td><td style="text-align:center;font-weight:700;color:${gc};">${grade}</td><td>${remark}</td></tr>`;
    }).join('');
    const avg = Object.values(subjects).length ? Math.round(Object.values(subjects).reduce((a,b)=>a+Number(b),0)/Object.values(subjects).length) : 0;

    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${r.student_name} — ${r.term} Result</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'DM Sans',sans-serif;background:#f0f2f5;padding:24px 16px;}
.result-card{background:white;max-width:700px;margin:0 auto;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.08);}
.result-header{background:#0d4a2c;color:white;padding:28px 32px;text-align:center;}
.school-name{font-size:22px;font-weight:700;margin-bottom:4px;}
.result-title{font-size:13px;opacity:.7;letter-spacing:1px;text-transform:uppercase;}
.student-info{display:grid;grid-template-columns:repeat(3,1fr);gap:0;background:#f7f9f7;border-bottom:1px solid #e9edef;}
.info-item{padding:16px 20px;border-right:1px solid #e9edef;}
.info-item:last-child{border-right:none;}
.info-label{font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:#667781;margin-bottom:4px;}
.info-value{font-size:15px;font-weight:600;color:#0f1a14;}
.scores{padding:24px 28px;}
table{width:100%;border-collapse:collapse;font-size:14px;}
th{font-size:11px;font-weight:600;color:#667781;text-align:left;padding:10px 12px;border-bottom:2px solid #e9edef;text-transform:uppercase;letter-spacing:.5px;}
td{padding:12px 12px;border-bottom:1px solid #f0f2f5;color:#0f1a14;}
.summary{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:0 28px 24px;}
.sum-box{background:#f7f9f7;border-radius:8px;padding:14px;text-align:center;}
.sum-val{font-size:24px;font-weight:700;color:#1a7a4a;}
.sum-label{font-size:11px;color:#667781;margin-top:3px;}
.remarks-section{padding:0 28px 24px;}
.remarks-title{font-size:12px;font-weight:600;color:#667781;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;}
.remarks-box{background:#f7f9f7;border-radius:8px;padding:14px;font-size:14px;color:#374151;line-height:1.6;}
.result-footer{background:#f7f9f7;padding:16px 28px;display:flex;align-items:center;justify-content:space-between;border-top:1px solid #e9edef;}
.footer-brand{font-size:13px;color:#aab;}
.footer-brand strong{color:#1a7a4a;}
.print-btn{background:#1a7a4a;color:white;border:none;padding:10px 24px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;font-family:'DM Sans',sans-serif;}
@media print{
  body{background:white;padding:0;}
  .print-btn{display:none;}
  .result-card{box-shadow:none;border-radius:0;}
}
</style>
</head>
<body>
<div class="result-card">
  <div class="result-header">
    <div class="school-name">${r.school_name}</div>
    <div class="result-title">${r.term} — Academic Result</div>
  </div>
  <div class="student-info">
    <div class="info-item"><div class="info-label">Student Name</div><div class="info-value">${r.student_name}</div></div>
    <div class="info-item"><div class="info-label">Class</div><div class="info-value">${r.class_name||'—'}</div></div>
    <div class="info-item"><div class="info-label">Position</div><div class="info-value">${r.position ? r.position + getOrdinal(r.position) : '—'}</div></div>
  </div>
  <div class="scores">
    <table>
      <thead><tr><th>Subject</th><th style="text-align:center;">Score</th><th style="text-align:center;">Grade</th><th>Remark</th></tr></thead>
      <tbody>${subjectRows}</tbody>
    </table>
  </div>
  <div class="summary">
    <div class="sum-box"><div class="sum-val">${avg}%</div><div class="sum-label">Average Score</div></div>
    <div class="sum-box"><div class="sum-val">${r.position||'—'}</div><div class="sum-label">Class Position</div></div>
    <div class="sum-box"><div class="sum-val">${Object.keys(subjects).length}</div><div class="sum-label">Subjects</div></div>
  </div>
  ${r.remark ? `<div class="remarks-section"><div class="remarks-title">Class Teacher's Remark</div><div class="remarks-box">${r.remark}</div></div>` : ''}
  <div class="result-footer">
    <div class="footer-brand">Powered by <strong>EduPing</strong> · eduping.org</div>
    <button class="print-btn" onclick="window.print()">🖨 Print Result</button>
  </div>
</div>
</body>
</html>`);
  } catch(err) { res.status(500).send('Error loading result: ' + err.message); }
});

function getOrdinal(n) {
  const s = ['th','st','nd','rd'], v = n%100;
  return s[(v-20)%10]||s[v]||s[0];
}

app.use((err, req, res, next) => { console.error(err); res.status(500).json({ error: 'Server error', detail: process.env.NODE_ENV === 'production' ? undefined : err.message }); });

(async () => {
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required. Add Railway PostgreSQL and expose DATABASE_URL.');
  await migrate();
  await migrateCBT().catch(e => console.error('[migrateCBT]', e.message));
  await migratePaystack().catch(e => console.error('[migratePaystack]', e.message));
  await seedIfEmpty();
  await initReporting({ app, requireSchool, q, callAI, twilioSend, cron });
  app.listen(PORT, () => {
    console.log(`EduPing multi tenant server running on ${PORT}`);
    console.log(`🤖 AI providers: DeepSeek=${Boolean(process.env.DEEPSEEK_API_KEY)} | OpenAI=${Boolean(process.env.OPENAI_API_KEY)} | Anthropic=${Boolean(process.env.ANTHROPIC_API_KEY)}`);
    console.log(`📱 Twilio=${Boolean(process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN)} | DB=${Boolean(process.env.DATABASE_URL)}`);
    if (!process.env.DEEPSEEK_API_KEY && !process.env.OPENAI_API_KEY) console.warn(`⚠️  WARNING: No AI key found — running in demo mode`);
  });
})();

app.get('/api/admin/fees/summary', requireSchool, async (req, res) => {
  try {
    const sid = req.school.id;
    const rows = await q(`
      SELECT s.id, s.name, s.class_name,
             COALESCE(f.id, NULL) as fee_id,
             COALESCE(f.term, $2) as term,
             COALESCE(f.amount_due, 0)::numeric as amount_due,
             COALESCE(f.amount_paid, 0)::numeric as amount_paid,
             COALESCE(f.amount_due, 0) - COALESCE(f.amount_paid, 0) as balance,
             COALESCE(f.status, 'unpaid') as fee_status,
             f.due_date
      FROM students s
      LEFT JOIN fees f ON f.student_id = s.id AND f.school_id = s.school_id
      WHERE s.school_id = $1
      ORDER BY s.class_name, s.name
    `, [sid, req.school.current_term || 'Current Term']);

    const students = rows.rows;
    const totals = {
      expected:    students.reduce((a, r) => a + Number(r.amount_due  || 0), 0),
      collected:   students.reduce((a, r) => a + Number(r.amount_paid || 0), 0),
      outstanding: students.reduce((a, r) => a + Number(r.balance     || 0), 0),
    };
    json(res, { students, totals });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/fees/payment', requireSchool, async (req, res) => {
  try {
    const { student_id, amount, payment_method, payment_date, note } = req.body;
    if (!student_id) return bad(res, 'student_id required');
    if (!amount || Number(amount) <= 0) return bad(res, 'Valid amount required');
    const sid = req.school.id;
    const term = req.school.current_term || 'Current Term';

    await q(`INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status)
             VALUES ($1,$2,$3,0,0,'unpaid')
             ON CONFLICT (school_id,student_id,term) DO NOTHING`,
      [sid, student_id, term]).catch(() => {});

    const feeRec = await q(`SELECT * FROM fees WHERE school_id=$1 AND student_id=$2 AND term=$3 LIMIT 1`,
      [sid, student_id, term]);

    let fee = feeRec.rows[0];
    if (!fee) {
      await q(`INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status) VALUES ($1,$2,$3,0,0,'unpaid')`,
        [sid, student_id, term]);
      fee = (await q(`SELECT * FROM fees WHERE school_id=$1 AND student_id=$2 AND term=$3 LIMIT 1`, [sid, student_id, term])).rows[0];
    }

    const newPaid = Number(fee.amount_paid || 0) + Number(amount);
    const newStatus = newPaid <= 0 ? 'unpaid'
      : newPaid >= Number(fee.amount_due || 0) && Number(fee.amount_due || 0) > 0 ? 'paid'
      : 'partial';

    await q(`UPDATE fees SET amount_paid=$1, status=$2, updated_at=now() WHERE id=$3`,
      [newPaid, newStatus, fee.id]);

    await q(`INSERT INTO fee_payments (school_id,student_id,fee_id,amount,payment_method,payment_date,note)
             VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [sid, student_id, fee.id, Number(amount), payment_method || 'cash',
       payment_date || new Date().toISOString().slice(0,10), note || null]);

    json(res, { ok: true, amount_paid: newPaid, status: newStatus });
  } catch(err) { bad(res, err.message, 500); }
});

app.post('/api/admin/fees/setup', requireSchool, async (req, res) => {
  try {
    const { class_name, term, amount_due, due_date } = req.body;
    if (!amount_due || Number(amount_due) <= 0) return bad(res, 'amount_due required');
    const sid = req.school.id;
    const feeTerm = term || req.school.current_term || 'Current Term';
    const feeDate = due_date || null;

    const studentQ = class_name && class_name !== 'all'
      ? await q('SELECT id FROM students WHERE school_id=$1 AND class_name=$2', [sid, class_name])
      : await q('SELECT id FROM students WHERE school_id=$1', [sid]);

    let created = 0, updated = 0;
    for (const s of studentQ.rows) {
      const existing = await q('SELECT id, amount_paid FROM fees WHERE school_id=$1 AND student_id=$2 AND term=$3', [sid, s.id, feeTerm]);
      if (existing.rows.length) {
        const paid = Number(existing.rows[0].amount_paid || 0);
        const newStatus = paid <= 0 ? 'unpaid' : paid >= Number(amount_due) ? 'paid' : 'partial';
        await q('UPDATE fees SET amount_due=$1, due_date=$2, status=$3, updated_at=now() WHERE id=$4',
          [Number(amount_due), feeDate, newStatus, existing.rows[0].id]);
        updated++;
      } else {
        await q(`INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status,due_date)
                 VALUES ($1,$2,$3,$4,0,'unpaid',$5)`,
          [sid, s.id, feeTerm, Number(amount_due), feeDate]);
        created++;
      }
    }
    json(res, { ok: true, created, updated, total: studentQ.rows.length });
  } catch(err) { bad(res, err.message, 500); }
});

app.get('/api/admin/fees/payments/:student_id', requireSchool, async (req, res) => {
  try {
    const rows = await q(`SELECT fp.*, s.name as student_name FROM fee_payments fp
                          JOIN students s ON s.id=fp.student_id
                          WHERE fp.school_id=$1 AND fp.student_id=$2
                          ORDER BY fp.payment_date DESC LIMIT 50`,
      [req.school.id, req.params.student_id]);
    json(res, rows.rows);
  } catch(err) { bad(res, err.message, 500); }
});

app.patch('/api/admin/fees/:student_id', requireSchool, async (req, res) => {
  try {
    const { amount_due, term, due_date } = req.body;
    const sid = req.school.id;
    const feeTerm = term || req.school.current_term || 'Current Term';
    const existing = await q('SELECT id, amount_paid FROM fees WHERE school_id=$1 AND student_id=$2 AND term=$3', [sid, req.params.student_id, feeTerm]);
    if (existing.rows.length) {
      const paid = Number(existing.rows[0].amount_paid || 0);
      const newStatus = paid <= 0 ? 'unpaid' : paid >= Number(amount_due) ? 'paid' : 'partial';
      await q('UPDATE fees SET amount_due=$1, due_date=$2, status=$3, updated_at=now() WHERE id=$4',
        [Number(amount_due), due_date||null, newStatus, existing.rows[0].id]);
    } else {
      await q(`INSERT INTO fees (school_id,student_id,term,amount_due,amount_paid,status,due_date) VALUES ($1,$2,$3,$4,0,'unpaid',$5)`,
        [sid, req.params.student_id, feeTerm, Number(amount_due), due_date||null]);
    }
    json(res, { ok: true });
  } catch(err) { bad(res, err.message, 500); }
});

// ═══════════════════════════════════════════════════════════════
// CBT MODULE — Computer-Based Testing (ported from eduping v2)
// ═══════════════════════════════════════════════════════════════
const CBT_BASE_URL = process.env.BASE_URL || 'https://eduping.org';

// ─── MIGRATION — call from migrate.js ─────────────────────────────────────────
async function migrateCBT() {
  await q(`
    CREATE TABLE IF NOT EXISTS assessments (
      id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id             UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      title                 TEXT NOT NULL,
      subject               TEXT NOT NULL,
      class_name            TEXT NOT NULL,
      term                  TEXT NOT NULL,
      time_limit_minutes    INTEGER NOT NULL DEFAULT 30,
      shuffle_questions     BOOLEAN DEFAULT false,
      shuffle_options       BOOLEAN DEFAULT false,
      show_score_immediately BOOLEAN DEFAULT true,
      status                TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','closed')),
      start_time            TIMESTAMPTZ,
      end_time              TIMESTAMPTZ,
      created_by_staff      UUID REFERENCES staff(id) ON DELETE SET NULL,
      created_at            TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS questions (
      id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      assessment_id  UUID NOT NULL REFERENCES assessments(id) ON DELETE CASCADE,
      school_id      UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      question_text  TEXT NOT NULL,
      question_type  TEXT NOT NULL DEFAULT 'mcq' CHECK (question_type IN ('mcq','truefalse','fillin')),
      marks          INTEGER NOT NULL DEFAULT 1,
      image_url      TEXT,
      order_index    INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS options (
      id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      question_id  UUID NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
      option_text  TEXT NOT NULL,
      is_correct   BOOLEAN DEFAULT false,
      order_index  INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS student_sessions (
      id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      assessment_id          UUID NOT NULL REFERENCES assessments(id) ON DELETE CASCADE,
      student_id             UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      school_id              UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      access_token           UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
      started_at             TIMESTAMPTZ,
      submitted_at           TIMESTAMPTZ,
      time_remaining_seconds INTEGER,
      score                  NUMERIC,
      total_marks            NUMERIC,
      percentage             NUMERIC,
      answers_json           JSONB DEFAULT '{}'::jsonb,
      status                 TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','in_progress','submitted'))
    );

    CREATE TABLE IF NOT EXISTS cbt_results (
      id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      assessment_id     UUID NOT NULL REFERENCES assessments(id) ON DELETE CASCADE,
      student_id        UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      school_id         UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      score             NUMERIC,
      total_marks       NUMERIC,
      percentage        NUMERIC,
      grade             TEXT,
      time_taken_seconds INTEGER,
      parent_notified   BOOLEAN DEFAULT false,
      created_at        TIMESTAMPTZ DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_student_sessions_token      ON student_sessions(access_token);
    CREATE INDEX IF NOT EXISTS idx_student_sessions_assessment ON student_sessions(assessment_id);
    CREATE INDEX IF NOT EXISTS idx_assessments_school          ON assessments(school_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_questions_assessment        ON questions(assessment_id);
    CREATE INDEX IF NOT EXISTS idx_cbt_results_assessment      ON cbt_results(assessment_id);
  `);
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function calcGrade(pct) {
  if (pct >= 70) return 'A';
  if (pct >= 60) return 'B';
  if (pct >= 50) return 'C';
  if (pct >= 45) return 'D';
  return 'F';
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function ordinal(n) {
  const s = ['th','st','nd','rd'], v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

// ─── AUTO-MARK ────────────────────────────────────────────────────────────────

async function autoMark(answers, assessmentId) {
  const { rows: questions } = await q(
    `SELECT q.id, q.question_type, q.marks,
       json_agg(json_build_object('id',o.id,'option_text',o.option_text,'is_correct',o.is_correct)
                ORDER BY o.order_index) AS options
     FROM questions q
     LEFT JOIN options o ON o.question_id = q.id
     WHERE q.assessment_id = $1
     GROUP BY q.id`, [assessmentId]
  );

  let score = 0, totalMarks = 0;
  const breakdown = {};

  for (const q of questions) {
    totalMarks += q.marks;
    const given = answers[q.id];
    let correct = false;

    if (q.question_type === 'mcq' || q.question_type === 'truefalse') {
      const correctOpt = q.options.find(o => o.is_correct);
      correct = correctOpt && String(given) === String(correctOpt.id);
    } else if (q.question_type === 'fillin') {
      const correctOpt = q.options.find(o => o.is_correct);
      correct = correctOpt &&
        String(given || '').trim().toLowerCase() === correctOpt.option_text.trim().toLowerCase();
    }

    if (correct) score += q.marks;
    breakdown[q.id] = { correct, given, marks: q.marks, earned: correct ? q.marks : 0 };
  }

  const percentage = totalMarks > 0 ? Math.round((score / totalMarks) * 100) : 0;
  return { score, totalMarks, percentage, grade: calcGrade(percentage), breakdown };
}

// ═══════════════════════════════════════════════════════════════════════════════
// ADMIN ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

// Create assessment
app.post('/api/admin/cbt/assessments', requireSchool, async (req, res) => {
  try {
    const {
      title, subject, class_name, term, time_limit_minutes,
      shuffle_questions, shuffle_options, show_score_immediately,
      start_time, end_time, created_by_staff
    } = req.body;

    if (!title || !subject || !class_name) return bad(res, 'title, subject and class_name required', 400);

    const { rows: [a] } = await q(
      `INSERT INTO assessments
         (school_id, title, subject, class_name, term, time_limit_minutes,
          shuffle_questions, shuffle_options, show_score_immediately,
          start_time, end_time, created_by_staff)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [req.school.id, title, subject, class_name,
       term || req.school.current_term,
       time_limit_minutes || 30,
       !!shuffle_questions, !!shuffle_options,
       show_score_immediately !== false,
       start_time || null, end_time || null,
       created_by_staff || null]
    );
    json(res, a);
  } catch (e) { console.error(e); bad(res, e.message); }
});

// List assessments
app.get('/api/admin/cbt/assessments', requireSchool, async (req, res) => {
  try {
    const { rows } = await q(
      `SELECT a.*,
         COUNT(DISTINCT qu.id)::int AS question_count,
         COUNT(DISTINCT ss.id)::int AS session_count,
         COUNT(DISTINCT CASE WHEN ss.status='submitted' THEN ss.id END)::int AS submitted_count
       FROM assessments a
       LEFT JOIN questions qu ON qu.assessment_id = a.id
       LEFT JOIN student_sessions ss ON ss.assessment_id = a.id
       WHERE a.school_id = $1
       GROUP BY a.id ORDER BY a.created_at DESC`,
      [req.school.id]
    );
    json(res, rows);
  } catch (e) { bad(res, e.message); }
});

// Get single assessment with questions + options
app.get('/api/admin/cbt/assessments/:id', requireSchool, async (req, res) => {
  try {
    const { rows: [assessment] } = await q(
      'SELECT * FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!assessment) return bad(res, 'Not found', 404);

    const { rows: questions } = await q(
      'SELECT * FROM questions WHERE assessment_id=$1 ORDER BY order_index, id',
      [req.params.id]
    );
    for (const question of questions) {
      const { rows: opts } = await q(
        'SELECT * FROM options WHERE question_id=$1 ORDER BY order_index, id',
        [question.id]
      );
      question.options = opts;
    }
    assessment.questions = questions;
    json(res, assessment);
  } catch (e) { bad(res, e.message); }
});

// Update assessment
app.patch('/api/admin/cbt/assessments/:id', requireSchool, async (req, res) => {
  try {
    const fields = [
      'title','subject','class_name','term','time_limit_minutes',
      'shuffle_questions','shuffle_options','show_score_immediately',
      'status','start_time','end_time'
    ];
    const updates = [], vals = [];
    let i = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) { updates.push(`${f}=$${i++}`); vals.push(req.body[f]); }
    }
    if (!updates.length) return bad(res, 'Nothing to update', 400);
    vals.push(req.params.id, req.school.id);
    const { rows: [a] } = await q(
      `UPDATE assessments SET ${updates.join(',')} WHERE id=$${i++} AND school_id=$${i} RETURNING *`,
      vals
    );
    if (!a) return bad(res, 'Not found', 404);
    json(res, a);
  } catch (e) { bad(res, e.message); }
});

// Delete assessment
app.delete('/api/admin/cbt/assessments/:id', requireSchool, async (req, res) => {
  try {
    const { rowCount } = await q(
      'DELETE FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!rowCount) return bad(res, 'Not found', 404);
    json(res, { deleted: true });
  } catch (e) { bad(res, e.message); }
});

// Add question with options
app.post('/api/admin/cbt/assessments/:id/questions', requireSchool, async (req, res) => {
  try {
    const { rows: [a] } = await q(
      'SELECT id FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!a) return bad(res, 'Assessment not found', 404);

    const { question_text, question_type, marks, image_url, order_index, options } = req.body;
    if (!question_text) return bad(res, 'question_text required', 400);

    const { rows: [newQ] } = await q(
      `INSERT INTO questions (assessment_id, school_id, question_text, question_type, marks, image_url, order_index)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [req.params.id, req.school.id, question_text,
       question_type || 'mcq', marks || 1, image_url || null, order_index || 0]
    );

    newQ.options = [];
    if (Array.isArray(options)) {
      for (let idx = 0; idx < options.length; idx++) {
        const o = options[idx];
        const { rows: [opt] } = await q(
          'INSERT INTO options (question_id, option_text, is_correct, order_index) VALUES ($1,$2,$3,$4) RETURNING *',
          [newQ.id, o.option_text, !!o.is_correct, idx]
        );
        newQ.options.push(opt);
      }
    }
    json(res, newQ);
  } catch (e) { bad(res, e.message); }
});

// Edit question
app.patch('/api/admin/cbt/questions/:id', requireSchool, async (req, res) => {
  try {
    const { rows: [existing] } = await q(
      `SELECT qu.* FROM questions qu
       JOIN assessments a ON a.id = qu.assessment_id
       WHERE qu.id=$1 AND a.school_id=$2`,
      [req.params.id, req.school.id]
    );
    if (!existing) return bad(res, 'Not found', 404);

    const fields = ['question_text','question_type','marks','image_url','order_index'];
    const updates = [], vals = [];
    let i = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) { updates.push(`${f}=$${i++}`); vals.push(req.body[f]); }
    }

    let updated = existing;
    if (updates.length) {
      vals.push(req.params.id);
      const { rows: [u] } = await q(
        `UPDATE questions SET ${updates.join(',')} WHERE id=$${i} RETURNING *`, vals
      );
      updated = u;
    }

    if (Array.isArray(req.body.options)) {
      await q('DELETE FROM options WHERE question_id=$1', [req.params.id]);
      updated.options = [];
      for (let idx = 0; idx < req.body.options.length; idx++) {
        const o = req.body.options[idx];
        const { rows: [opt] } = await q(
          'INSERT INTO options (question_id, option_text, is_correct, order_index) VALUES ($1,$2,$3,$4) RETURNING *',
          [req.params.id, o.option_text, !!o.is_correct, idx]
        );
        updated.options.push(opt);
      }
    }
    json(res, updated);
  } catch (e) { bad(res, e.message); }
});

// Delete question
app.delete('/api/admin/cbt/questions/:id', requireSchool, async (req, res) => {
  try {
    const { rowCount } = await q(
      `DELETE FROM questions
       WHERE id=$1 AND assessment_id IN (
         SELECT id FROM assessments WHERE school_id=$2
       )`,
      [req.params.id, req.school.id]
    );
    if (!rowCount) return bad(res, 'Not found', 404);
    json(res, { deleted: true });
  } catch (e) { bad(res, e.message); }
});

// Publish — generate per-student sessions
app.post('/api/admin/cbt/assessments/:id/publish', requireSchool, async (req, res) => {
  try {
    const { rows: [assessment] } = await q(
      'SELECT * FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!assessment) return bad(res, 'Not found', 404);

    const { rows: students } = await q(
      'SELECT * FROM students WHERE school_id=$1 AND class_name=$2 AND status=$3',
      [req.school.id, assessment.class_name, 'active']
    );
    if (!students.length) return bad(res, `No active students found in ${assessment.class_name}`, 400);

    const sessions = [];
    for (const s of students) {
      const { rows: [existing] } = await q(
        'SELECT access_token FROM student_sessions WHERE assessment_id=$1 AND student_id=$2',
        [assessment.id, s.id]
      );
      const token = existing?.access_token || (await q(
        `INSERT INTO student_sessions (assessment_id, student_id, school_id)
         VALUES ($1,$2,$3) RETURNING access_token`,
        [assessment.id, s.id, req.school.id]
      )).rows[0].access_token;

      sessions.push({
        student_id: s.id,
        student_name: s.name,
        parent_phone: s.parent_phone,
        token,
        url: `${CBT_BASE_URL}/cbt/${token}`
      });
    }

    await q("UPDATE assessments SET status='active' WHERE id=$1", [assessment.id]);
    json(res, { published: true, sessions });
  } catch (e) { bad(res, e.message); }
});

// Notify parents via WhatsApp
app.post('/api/admin/cbt/assessments/:id/notify', requireSchool, async (req, res) => {
  try {
    const { rows: [assessment] } = await q(
      'SELECT * FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!assessment) return bad(res, 'Not found', 404);

    const { rows: sessions } = await q(
      `SELECT ss.access_token, s.name AS student_name, s.parent_phone, s.parent_name
       FROM student_sessions ss JOIN students s ON s.id = ss.student_id
       WHERE ss.assessment_id=$1`, [assessment.id]
    );

    const school = req.school;
    const timingNote = assessment.start_time
      ? `Opens: ${new Date(assessment.start_time).toLocaleString('en-NG', { weekday:'long', hour:'2-digit', minute:'2-digit' })}.`
      : '';

    let sent = 0;
    for (const sess of sessions) {
      if (!sess.parent_phone) continue;
      const link = `${CBT_BASE_URL}/cbt/${sess.access_token}`;
      const msg =
        `📝 ${assessment.subject} ${assessment.title} — ${school.name}\n` +
        `Dear ${sess.parent_name || 'Parent'}, ${sess.student_name} has a ${assessment.subject} test scheduled.\n` +
        `Access their exam here: ${link}\n` +
        `Time limit: ${assessment.time_limit_minutes} minutes. ${timingNote}\n` +
        `${school.name} 🏫`;
      try {
        await twilioSend(sess.parent_phone, school.twilio_number, msg);
        sent++;
      } catch (e) { console.error('[cbt] notify error:', e.message); }
    }
    json(res, { sent, total: sessions.length });
  } catch (e) { bad(res, e.message); }
});

// Results
app.get('/api/admin/cbt/assessments/:id/results', requireSchool, async (req, res) => {
  try {
    const { rows: [assessment] } = await q(
      'SELECT * FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!assessment) return bad(res, 'Not found', 404);

    const { rows } = await q(
      `SELECT ss.id, ss.status, ss.score, ss.total_marks, ss.percentage,
         ss.started_at, ss.submitted_at,
         s.name AS student_name, s.class_name,
         r.grade, r.time_taken_seconds, r.parent_notified,
         RANK() OVER (ORDER BY ss.percentage DESC NULLS LAST)::int AS class_position,
         COUNT(*) OVER ()::int AS total_students
       FROM student_sessions ss
       JOIN students s ON s.id = ss.student_id
       LEFT JOIN cbt_results r ON r.assessment_id = ss.assessment_id AND r.student_id = ss.student_id
       WHERE ss.assessment_id=$1 AND ss.school_id=$2
       ORDER BY ss.percentage DESC NULLS LAST`,
      [req.params.id, req.school.id]
    );

    const submitted = rows.filter(r => r.status === 'submitted');
    const avg = submitted.length
      ? Math.round(submitted.reduce((s, r) => s + (r.percentage || 0), 0) / submitted.length)
      : null;

    json(res, {
      assessment,
      results: rows,
      stats: {
        total: rows.length,
        submitted: submitted.length,
        average: avg,
        highest: submitted[0]?.percentage ?? null,
        lowest: submitted[submitted.length - 1]?.percentage ?? null
      }
    });
  } catch (e) { bad(res, e.message); }
});

// Live monitor
app.get('/api/admin/cbt/assessments/:id/monitor', requireSchool, async (req, res) => {
  try {
    const { rows } = await q(
      `SELECT ss.id, ss.status, ss.started_at, ss.submitted_at,
         ss.score, ss.total_marks, ss.percentage, ss.time_remaining_seconds,
         s.name AS student_name
       FROM student_sessions ss
       JOIN students s ON s.id = ss.student_id
       WHERE ss.assessment_id=$1 AND ss.school_id=$2
       ORDER BY s.name`,
      [req.params.id, req.school.id]
    );
    json(res, rows);
  } catch (e) { bad(res, e.message); }
});

// Release results — WhatsApp scores to parents
app.post('/api/admin/cbt/assessments/:id/release', requireSchool, async (req, res) => {
  try {
    const { rows: [assessment] } = await q(
      'SELECT * FROM assessments WHERE id=$1 AND school_id=$2',
      [req.params.id, req.school.id]
    );
    if (!assessment) return bad(res, 'Not found', 404);

    const { rows } = await q(
      `SELECT ss.score, ss.total_marks, ss.percentage, ss.student_id,
         s.name AS student_name, s.parent_phone, s.parent_name,
         r.grade,
         RANK() OVER (ORDER BY ss.percentage DESC NULLS LAST)::int AS class_position,
         COUNT(*) OVER ()::int AS total_students
       FROM student_sessions ss
       JOIN students s ON s.id = ss.student_id
       LEFT JOIN cbt_results r ON r.assessment_id = ss.assessment_id AND r.student_id = ss.student_id
       WHERE ss.assessment_id=$1 AND ss.school_id=$2 AND ss.status='submitted'`,
      [req.params.id, req.school.id]
    );

    let sent = 0;
    for (const r of rows) {
      if (!r.parent_phone) continue;
      const grade = r.grade || calcGrade(r.percentage);
      const msg =
        `📊 Results Released — ${req.school.name}\n` +
        `Dear ${r.parent_name || 'Parent'}, ${r.student_name}'s ${assessment.subject} ${assessment.title} results are now available.\n` +
        `Score: ${r.score}/${r.total_marks} | Grade: ${grade} | Class position: ${ordinal(r.class_position)} of ${r.total_students}\n` +
        `${req.school.name} 🏫`;
      try {
        await twilioSend(r.parent_phone, req.school.twilio_number, msg);
        await q(
          'UPDATE cbt_results SET parent_notified=true WHERE assessment_id=$1 AND student_id=$2',
          [assessment.id, r.student_id]
        );
        sent++;
      } catch (e) { console.error('[cbt] release error:', e.message); }
    }
    json(res, { sent, total: rows.length });
  } catch (e) { bad(res, e.message); }
});

// Bulk import questions
app.post('/api/admin/cbt/bulk-import-questions', requireSchool, async (req, res) => {
  try {
    const { assessment_id, questions: raw, text_format } = req.body;

    const { rows: [assessment] } = await q(
      'SELECT id FROM assessments WHERE id=$1 AND school_id=$2',
      [assessment_id, req.school.id]
    );
    if (!assessment) return bad(res, 'Assessment not found', 404);

    let questions = [];

    if (text_format && typeof raw === 'string') {
      // Format: Q: text | A: opt | B: opt | ANS: B  (one per line)
      const lines = raw.trim().split('\n').filter(l => l.trim());
      for (const line of lines) {
        const parts = line.split('|').map(p => p.trim());
        const qObj = {}, opts = [];
        let correctLetter = null;
        for (const part of parts) {
          if (/^Q:/i.test(part))   qObj.text = part.replace(/^Q:\s*/i, '');
          else if (/^ANS:/i.test(part)) correctLetter = part.replace(/^ANS:\s*/i, '').toUpperCase().trim();
          else {
            const m = part.match(/^([A-F]):\s*(.*)/i);
            if (m) opts.push({ letter: m[1].toUpperCase(), text: m[2] });
          }
        }
        if (qObj.text && opts.length) {
          questions.push({
            question_text: qObj.text,
            question_type: 'mcq',
            marks: 1,
            options: opts.map(o => ({ option_text: o.text, is_correct: o.letter === correctLetter }))
          });
        }
      }
    } else if (Array.isArray(raw)) {
      questions = raw;
    }

    const created = [];
    for (let idx = 0; idx < questions.length; idx++) {
      const qd = questions[idx];
      const { rows: [newQ] } = await q(
        `INSERT INTO questions (assessment_id, school_id, question_text, question_type, marks, order_index)
         VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
        [assessment_id, req.school.id, qd.question_text, qd.question_type || 'mcq', qd.marks || 1, idx]
      );
      newQ.options = [];
      if (Array.isArray(qd.options)) {
        for (let oi = 0; oi < qd.options.length; oi++) {
          const o = qd.options[oi];
          const { rows: [opt] } = await q(
            'INSERT INTO options (question_id, option_text, is_correct, order_index) VALUES ($1,$2,$3,$4) RETURNING *',
            [newQ.id, o.option_text, !!o.is_correct, oi]
          );
          newQ.options.push(opt);
        }
      }
      created.push(newQ);
    }
    json(res, { imported: created.length, questions: created });
  } catch (e) { bad(res, e.message); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// STUDENT-FACING ROUTES  (no auth — token-based)
// ═══════════════════════════════════════════════════════════════════════════════

// Verify token — return exam info
app.get('/api/cbt/:token', async (req, res) => {
  try {
    const { rows: [session] } = await q(
      `SELECT ss.*, ss.access_token,
         s.name AS student_name, s.class_name AS student_class,
         a.title, a.subject, a.time_limit_minutes,
         a.status AS assessment_status, a.start_time, a.end_time,
         a.show_score_immediately,
         sc.name AS school_name
       FROM student_sessions ss
       JOIN students s  ON s.id  = ss.student_id
       JOIN assessments a ON a.id = ss.assessment_id
       JOIN schools sc    ON sc.id = ss.school_id
       WHERE ss.access_token = $1`, [req.params.token]
    );
    if (!session) return bad(res, 'Invalid or expired exam link', 404);

    if (session.status === 'submitted') {
      return json(res, {
        already_submitted: true,
        student_name: session.student_name,
        score: session.score,
        total_marks: session.total_marks,
        percentage: session.percentage,
        show_score: session.show_score_immediately
      });
    }
    if (session.assessment_status !== 'active') {
      return bad(res, 'This exam is not currently active', 403);
    }
    json(res, {
      status: session.status,
      student_name: session.student_name,
      student_class: session.student_class,
      school_name: session.school_name,
      title: session.title,
      subject: session.subject,
      time_limit_minutes: session.time_limit_minutes,
      time_remaining_seconds: session.time_remaining_seconds || session.time_limit_minutes * 60,
      start_time: session.start_time,
      end_time: session.end_time
    });
  } catch (e) { bad(res, e.message); }
});

// Start session — return shuffled questions (no is_correct)
app.post('/api/cbt/:token/start', async (req, res) => {
  try {
    const { rows: [session] } = await q(
      `SELECT ss.*, a.shuffle_questions, a.shuffle_options, a.time_limit_minutes,
         a.status AS assessment_status
       FROM student_sessions ss
       JOIN assessments a ON a.id = ss.assessment_id
       WHERE ss.access_token = $1`, [req.params.token]
    );
    if (!session) return bad(res, 'Invalid token', 404);
    if (session.status === 'submitted') return bad(res, 'Already submitted', 400);
    if (session.assessment_status !== 'active') return bad(res, 'Exam not active', 403);

    if (session.status === 'pending') {
      await q(
        `UPDATE student_sessions SET status='in_progress', started_at=now(),
           time_remaining_seconds=$1 WHERE access_token=$2`,
        [session.time_limit_minutes * 60, req.params.token]
      );
    }

    const { rows: questions } = await q(
      `SELECT id, question_text, question_type, marks, image_url, order_index
       FROM questions WHERE assessment_id=$1`, [session.assessment_id]
    );

    let qs = session.shuffle_questions
      ? shuffle(questions)
      : questions.sort((a, b) => a.order_index - b.order_index);

    for (const question of qs) {
      const { rows: opts } = await q(
        'SELECT id, option_text, order_index FROM options WHERE question_id=$1',
        [question.id]
      );
      // Never send is_correct to the client
      question.options = session.shuffle_options
        ? shuffle(opts)
        : opts.sort((a, b) => a.order_index - b.order_index);
    }

    json(res, {
      questions: qs,
      time_remaining_seconds: session.time_remaining_seconds || session.time_limit_minutes * 60,
      answers: session.answers_json || {}
    });
  } catch (e) { bad(res, e.message); }
});

// Autosave
app.post('/api/cbt/:token/save', async (req, res) => {
  try {
    const { answers, time_remaining_seconds } = req.body;
    const { rows: [session] } = await q(
      'SELECT id, status FROM student_sessions WHERE access_token=$1', [req.params.token]
    );
    if (!session) return bad(res, 'Invalid token', 404);
    if (session.status === 'submitted') return bad(res, 'Already submitted', 400);

    await q(
      `UPDATE student_sessions SET answers_json=$1, time_remaining_seconds=$2
       WHERE access_token=$3`,
      [JSON.stringify(answers || {}), time_remaining_seconds || 0, req.params.token]
    );
    json(res, { saved: true });
  } catch (e) { bad(res, e.message); }
});

// Submit
app.post('/api/cbt/:token/submit', async (req, res) => {
  try {
    const { answers, time_remaining_seconds } = req.body;

    const { rows: [session] } = await q(
      `SELECT ss.*, a.show_score_immediately, a.time_limit_minutes, a.subject, a.title, a.term,
         a.status AS assessment_status,
         s.parent_phone, s.parent_name, s.name AS student_name,
         sc.name AS school_name, sc.twilio_number
       FROM student_sessions ss
       JOIN assessments a ON a.id = ss.assessment_id
       JOIN students s    ON s.id = ss.student_id
       JOIN schools sc    ON sc.id = ss.school_id
       WHERE ss.access_token = $1`, [req.params.token]
    );
    if (!session) return bad(res, 'Invalid token', 404);
    if (session.status === 'submitted') return bad(res, 'Already submitted', 400);

    const finalAnswers = answers || session.answers_json || {};
    const timeTaken = session.started_at
      ? Math.round((Date.now() - new Date(session.started_at).getTime()) / 1000)
      : session.time_limit_minutes * 60 - (time_remaining_seconds || 0);

    const { score, totalMarks, percentage, grade, breakdown } =
      await autoMark(finalAnswers, session.assessment_id);

    // Update session
    await q(
      `UPDATE student_sessions
       SET status='submitted', submitted_at=now(),
           answers_json=$1, time_remaining_seconds=$2,
           score=$3, total_marks=$4, percentage=$5
       WHERE access_token=$6`,
      [JSON.stringify(finalAnswers), time_remaining_seconds || 0,
       score, totalMarks, percentage, req.params.token]
    );

    // Insert cbt_results
    await q(
      `INSERT INTO cbt_results
         (assessment_id, student_id, school_id, score, total_marks, percentage, grade, time_taken_seconds)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT DO NOTHING`,
      [session.assessment_id, session.student_id, session.school_id,
       score, totalMarks, percentage, grade, timeTaken]
    );

    // Feed into existing scores table for Friday AI reports
    await q(
      `INSERT INTO scores (school_id, student_id, subject, score, term, uploaded_at)
       VALUES ($1,$2,$3,$4,$5,now())
       ON CONFLICT (school_id, student_id, subject, term)
       DO UPDATE SET score=$4, uploaded_at=now()`,
      [session.school_id, session.student_id, session.subject, percentage, session.term]
    );

    // WhatsApp parent notification
    if (session.parent_phone && session.twilio_number) {
      const mins = Math.round(timeTaken / 60);
      const msg =
        `✅ Exam Submitted — ${session.school_name}\n` +
        `Dear ${session.parent_name || 'Parent'}, ${session.student_name} has just submitted their ${session.subject} ${session.title}.\n` +
        `Score: ${score}/${totalMarks} (${percentage}%) — Grade: ${grade}\n` +
        `Time taken: ${mins} minute${mins !== 1 ? 's' : ''}.\n` +
        `${session.school_name} 🏫`;
      try {
        await twilioSend(session.parent_phone, session.twilio_number, msg);
      } catch (e) { console.error('[cbt] submit WhatsApp error:', e.message); }
    }

    const result = {
      submitted: true,
      show_score: session.show_score_immediately,
      message: session.show_score_immediately
        ? null
        : 'Your answers have been submitted. Results will be released by your school.'
    };
    if (session.show_score_immediately) {
      Object.assign(result, { score, total_marks: totalMarks, percentage, grade, breakdown });
    }
    json(res, result);
  } catch (e) { bad(res, e.message); }
});




// ═══════════════════════════════════════════════════════════════
// PAYSTACK MODULE — online fee collection (missing payments.js, rebuilt)
// Works when PAYSTACK_SECRET_KEY env var is set on Railway.
// Until then, endpoints return a clear "not configured" message and
// schools use the bank/Paystack-page details collected at onboarding.
// ═══════════════════════════════════════════════════════════════

function hasPaystack() { return Boolean(process.env.PAYSTACK_SECRET_KEY); }

async function migratePaystack() {
  await q(`
    CREATE TABLE IF NOT EXISTS paystack_transactions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE SET NULL,
      fee_id UUID,
      reference TEXT UNIQUE NOT NULL,
      amount NUMERIC NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      authorization_url TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      paid_at TIMESTAMPTZ
    );
  `);
}

// ── Admin: generate a Paystack payment link for a student's balance ──
app.post('/api/admin/fees/paystack-link', requireSchool, async (req, res) => {
  try {
    if (!hasPaystack()) return bad(res, 'Paystack is not configured yet. Add PAYSTACK_SECRET_KEY in Railway settings, or use the bank details from onboarding.', 400);
    const { student_id, amount } = req.body;
    if (!student_id) return bad(res, 'student_id required');
    const sid = req.school.id;

    const st = await q(`SELECT s.id, s.name, s.class_name, s.parent_phone, s.parent_name,
                               f.id AS fee_id, COALESCE(f.amount_due,0)-COALESCE(f.amount_paid,0) AS balance
                        FROM students s
                        LEFT JOIN fees f ON f.student_id=s.id AND f.school_id=s.school_id
                        WHERE s.school_id=$1 AND s.id=$2 LIMIT 1`, [sid, student_id]);
    if (!st.rows.length) return bad(res, 'Student not found', 404);
    const stu = st.rows[0];

    const naira = Number(amount || stu.balance || 0);
    if (!naira || naira <= 0) return bad(res, 'No outstanding balance for this student. Pass an amount to charge a custom figure.');

    // Paystack requires an email — derive a routing address from the parent phone
    const digits = String(stu.parent_phone || '0000000000').replace(/\D/g, '');
    const email = `p${digits}@eduping.org`;
    const reference = `EDU-${sid.slice(0,8)}-${Date.now()}`;

    const psRes = await fetch('https://api.paystack.co/transaction/initialize', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${process.env.PAYSTACK_SECRET_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email,
        amount: Math.round(naira * 100), // kobo
        reference,
        currency: 'NGN',
        metadata: {
          school_id: sid, student_id: stu.id, fee_id: stu.fee_id,
          student_name: stu.name, school_name: req.school.name,
          custom_fields: [
            { display_name: 'Student', variable_name: 'student', value: `${stu.name} (${stu.class_name || ''})` },
            { display_name: 'School', variable_name: 'school', value: req.school.name }
          ]
        }
      })
    });
    const ps = await psRes.json();
    if (!ps.status) return bad(res, `Paystack error: ${ps.message || 'could not create link'}`, 502);

    await q(`INSERT INTO paystack_transactions (school_id, student_id, fee_id, reference, amount, authorization_url)
             VALUES ($1,$2,$3,$4,$5,$6)`,
      [sid, stu.id, stu.fee_id, reference, naira, ps.data.authorization_url]);

    json(res, { ok: true, url: ps.data.authorization_url, reference, amount: naira, student: stu.name, parent_phone: stu.parent_phone });
  } catch (err) { bad(res, err.message, 500); }
});

// ── Admin: send the payment link to the parent on WhatsApp ──
app.post('/api/admin/fees/paystack-send', requireSchool, async (req, res) => {
  try {
    const { reference } = req.body;
    if (!reference) return bad(res, 'reference required');
    const tx = await q(`SELECT pt.*, s.name AS student_name, s.parent_phone
                        FROM paystack_transactions pt JOIN students s ON s.id=pt.student_id
                        WHERE pt.reference=$1 AND pt.school_id=$2`, [reference, req.school.id]);
    if (!tx.rows.length) return bad(res, 'Transaction not found', 404);
    const t = tx.rows[0];
    if (!t.parent_phone) return bad(res, 'No parent phone on file for this student');
    const from = req.school.twilio_number || process.env.TWILIO_DEFAULT_FROM;
    const msg = `💳 *School Fees Payment — ${req.school.name}*\n\nDear parent, you can now pay *${t.student_name}*'s school fees of *₦${Number(t.amount).toLocaleString()}* securely online (card, bank transfer or USSD):\n\n${t.authorization_url}\n\nYou will receive an instant confirmation once payment is complete.\n\n${req.school.name} 🏫`;
    await twilioSend(t.parent_phone, from, msg);
    json(res, { ok: true, sent_to: t.parent_phone });
  } catch (err) { bad(res, err.message, 500); }
});

// ── Paystack webhook — verifies signature, records payment, sends receipt ──
app.post('/webhook/paystack', async (req, res) => {
  try {
    if (!hasPaystack()) return res.sendStatus(200);
    const signature = req.headers['x-paystack-signature'];
    const raw = req.rawBody || Buffer.from(JSON.stringify(req.body));
    const expected = crypto.createHmac('sha512', process.env.PAYSTACK_SECRET_KEY).update(raw).digest('hex');
    if (signature !== expected) {
      console.warn('[paystack] invalid webhook signature');
      return res.sendStatus(401);
    }

    const event = req.body;
    if (event.event !== 'charge.success') return res.sendStatus(200);
    const reference = event.data?.reference;
    if (!reference) return res.sendStatus(200);

    const tx = await q(`SELECT * FROM paystack_transactions WHERE reference=$1`, [reference]);
    if (!tx.rows.length) return res.sendStatus(200);
    const t = tx.rows[0];
    if (t.status === 'success') return res.sendStatus(200); // idempotent — already processed

    const paidNaira = Number(event.data.amount || 0) / 100;
    await q(`UPDATE paystack_transactions SET status='success', paid_at=now() WHERE id=$1`, [t.id]);

    // Update the fee record
    if (t.fee_id) {
      await q(`UPDATE fees SET amount_paid = COALESCE(amount_paid,0) + $1,
               status = CASE WHEN COALESCE(amount_paid,0) + $1 >= amount_due THEN 'paid' ELSE 'partial' END
               WHERE id=$2`, [paidNaira, t.fee_id]);
    }
    await q(`INSERT INTO fee_payments (school_id, student_id, fee_id, amount, payment_method, payment_date, note)
             VALUES ($1,$2,$3,$4,'paystack',current_date,$5)`,
      [t.school_id, t.student_id, t.fee_id, paidNaira, `Paystack ref ${reference}`]).catch(e => console.warn('[paystack] ledger insert:', e.message));

    // WhatsApp receipt to parent + alert to school admin
    const info = await q(`SELECT s.name AS student_name, s.parent_phone, sc.name AS school_name, sc.twilio_number, sc.admin_phone
                          FROM students s JOIN schools sc ON sc.id=s.school_id WHERE s.id=$1`, [t.student_id]);
    if (info.rows.length) {
      const i = info.rows[0];
      const from = i.twilio_number || process.env.TWILIO_DEFAULT_FROM;
      if (i.parent_phone) {
        await twilioSend(i.parent_phone, from,
          `✅ *Payment Received — ${i.school_name}*\n\nWe confirm receipt of your online payment for *${i.student_name}*.\n\n💰 *Amount:* ₦${paidNaira.toLocaleString()}\n💳 *Method:* Paystack (online)\n🧾 *Reference:* ${reference}\n\nThank you for your prompt payment.\n\n${i.school_name} 🏫`
        ).catch(e => console.warn('[paystack] parent receipt:', e.message));
      }
      if (i.admin_phone) {
        await twilioSend(i.admin_phone, from,
          `💰 *Fee Payment Alert — ${i.school_name}*\n\n*${i.student_name}* — ₦${paidNaira.toLocaleString()} paid online via Paystack.\nRef: ${reference}\n\nThe fee record has been updated automatically. ${i.school_name} 🏫`
        ).catch(e => console.warn('[paystack] admin alert:', e.message));
      }
    }
    res.sendStatus(200);
  } catch (err) {
    console.error('[paystack webhook]', err.message);
    res.sendStatus(200); // always 200 so Paystack doesn't retry forever
  }
});

// ── Student CBT exam page ──
app.get('/cbt/:token', (req, res) => res.sendFile(path.join(__dirname, 'public', 'cbt.html')));
