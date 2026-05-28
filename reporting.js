// ══════════════════════════════════════════════════════════════════
// EduPing — Weekly Reporting Engine (production-safe, 100+ schools)
// Drop this file into your project root and require it in server.js:
//   const { initReporting } = require('./reporting');
//   initReporting({ app, requireSchool, q, callAI, twilioSend, cron, school: { hasTwilio } });
// ══════════════════════════════════════════════════════════════════

'use strict';

// ── Helpers ───────────────────────────────────────────────────────

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function getMondayOfWeek(date) {
  const d = new Date(date);
  const day = d.getDay();
  d.setDate(d.getDate() - day + (day === 0 ? -6 : 1));
  d.setHours(0, 0, 0, 0);
  return d;
}

// Run up to `limit` promises concurrently — prevents DB/AI overload
async function withConcurrency(items, fn, limit = 5) {
  const results = [];
  const executing = new Set();
  for (const item of items) {
    const p = Promise.resolve().then(() => fn(item)).catch(e => ({ __error: e.message, item }));
    results.push(p);
    executing.add(p);
    p.finally(() => executing.delete(p));
    if (executing.size >= limit) await Promise.race(executing);
  }
  return Promise.all(results);
}

// Wrap a promise with a hard timeout
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Timeout after ${ms}ms: ${label}`)), ms)
    )
  ]);
}

// Delay helper for Twilio rate-limiting
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── DB migration — call inside your existing migrate() function ───

async function migrateReportingTables(q) {
  await q(`
    CREATE TABLE IF NOT EXISTS weekly_reports (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id   UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      week_number INT  NOT NULL,
      year        INT  NOT NULL DEFAULT EXTRACT(YEAR FROM now()),
      class_id    TEXT NOT NULL,
      status      TEXT NOT NULL DEFAULT 'pending',
      report_json JSONB,
      error_log   TEXT,
      generated_at      TIMESTAMPTZ DEFAULT now(),
      sent_to_parents   BOOLEAN DEFAULT false,
      UNIQUE(school_id, week_number, year, class_id)
    );
    CREATE INDEX IF NOT EXISTS idx_weekly_reports_school
      ON weekly_reports(school_id, week_number, year);
    CREATE INDEX IF NOT EXISTS idx_weekly_reports_status
      ON weekly_reports(status);
  `);
}

// ── SYSTEM PROMPT ─────────────────────────────────────────────────

const REPORT_SYSTEM_PROMPT = `You are an AI reporting engine for a school intelligence system. Your job is to convert structured student data into consistent, multi-audience weekly school reports.

You do not ask questions. You infer from context. You prioritize accuracy and clear separation of facts from interpretation.

EVENT CATEGORIES (classify every data point into one):
- Academic positive performance
- Academic struggle or difficulty
- Homework completion or missing work
- Behavioral issue or positive behavior
- Participation and engagement
- Attendance-related note
- Assessment score or result
- Class activity or curriculum coverage
- Emotional or wellbeing signal

DEFAULT RULE: If a student has no recorded events, generate a positive baseline report. Assume normal attendance, acceptable behavior, and steady progress. Never mark negative states without evidence. Default scores to 70.

PARENT REPORT STYLE:
- Short (3–5 sentences), clear, warm
- Positive in tone unless evidence shows struggle
- No educational jargon
- Include at least one specific strength
- End with one practical next step for the parent

SCORING: Return three integers 0–100:
- academic: based on scores data
- behavior: based on behaviour_notes
- engagement: based on attendance + homework patterns
- If no data for a dimension, return 70

Return ONLY valid JSON. No preamble, no markdown, no explanation. Exact format:
{
  "week_number": "",
  "class_id": "",
  "students": [
    {
      "student_id": "",
      "parent_report": "",
      "ai_summary_score": {
        "academic": 70,
        "behavior": 70,
        "engagement": 70
      },
      "key_events": [
        {
          "type": "",
          "subject": "",
          "message": "",
          "severity": "low|medium|high"
        }
      ]
    }
  ],
  "class_insights": {
    "overall_performance": "",
    "risk_students": [],
    "top_performers": [],
    "behavior_trends": "",
    "curriculum_completion_status": ""
  },
  "teacher_log": [
    {
      "teacher_id": "auto",
      "summary": "",
      "coverage_status": ""
    }
  ]
}`;

// ── CORE: Generate report for one school ──────────────────────────

async function generateWeeklyReportForSchool(schoolId, { q, callAI, twilioSend }) {
  // Load school
  const schoolRes = await q(`SELECT * FROM schools WHERE id=$1`, [schoolId]);
  const school = schoolRes.rows[0];
  if (!school) throw new Error('School not found: ' + schoolId);

  const weekNumber = getISOWeek(new Date());
  const year = new Date().getFullYear();
  const weekStart = getMondayOfWeek(new Date()).toISOString().slice(0, 10);

  // Load all students for this school
  const students = (await q(`SELECT * FROM students WHERE school_id=$1 ORDER BY class_name, name`, [schoolId])).rows;
  if (!students.length) {
    console.log(`[Reports] No students for school ${school.name} — skipping`);
    return { school: school.name, classes: 0, students: 0 };
  }

  // Group by class
  const byClass = {};
  for (const s of students) {
    const cls = (s.class_name || 'General').trim();
    if (!byClass[cls]) byClass[cls] = [];
    byClass[cls].push(s);
  }

  const classEntries = Object.entries(byClass);
  let totalReports = 0;

  // Process each class — sequential within a school to respect DB pool
  for (const [classId, classStudents] of classEntries) {
    // Mark as pending
    await q(`
      INSERT INTO weekly_reports (school_id, week_number, year, class_id, status)
      VALUES ($1, $2, $3, $4, 'pending')
      ON CONFLICT (school_id, week_number, year, class_id)
      DO UPDATE SET status='pending', error_log=null, generated_at=now()
    `, [schoolId, weekNumber, year, classId]);

    try {
      // Gather all student data for this class in parallel
      const studentInputs = await Promise.all(classStudents.map(async student => {
        const [scores, attendance, behaviour, homeworks] = await Promise.all([
          q(`SELECT subject, score, uploaded_at FROM scores
             WHERE student_id=$1 AND school_id=$2 AND uploaded_at >= $3
             ORDER BY uploaded_at DESC LIMIT 20`,
            [student.id, schoolId, weekStart]),
          q(`SELECT date, status FROM attendance
             WHERE student_id=$1 AND date >= $2
             ORDER BY date DESC`,
            [student.id, weekStart]),
          q(`SELECT note, created_at FROM behaviour_notes
             WHERE student_id=$1 AND created_at >= $2
             ORDER BY created_at DESC`,
            [student.id, weekStart]),
          q(`SELECT subject, description, due_date FROM homeworks
             WHERE school_id=$1 AND class_name=$2 AND created_at >= $3
             ORDER BY created_at DESC LIMIT 10`,
            [schoolId, student.class_name, weekStart])
        ]);

        return {
          student_id: student.id,
          student_name: student.name,
          class: student.class_name,
          scores: scores.rows,
          attendance: attendance.rows,
          behaviour_notes: behaviour.rows,
          homeworks_assigned: homeworks.rows
        };
      }));

      // Build the user message — one AI call covers the whole class
      const userPrompt = `School: ${school.name}, ${school.city || 'Nigeria'}
Term: ${school.current_term || 'Current Term'}
Week number: ${weekNumber}
Week starting: ${weekStart}
Class: ${classId}
Total students: ${classStudents.length}

Student data:
${JSON.stringify(studentInputs, null, 2)}`;

      // Call AI with a hard 90s timeout
      const raw = await withTimeout(
        callAI(REPORT_SYSTEM_PROMPT, userPrompt, null),
        90000,
        `${school.name} — ${classId}`
      );

      // Parse and validate
      const clean = raw.replace(/```json|```/g, '').trim();
      let report;
      try {
        report = JSON.parse(clean);
      } catch (parseErr) {
        // Try to extract JSON object from response
        const match = clean.match(/\{[\s\S]*\}/);
        if (match) report = JSON.parse(match[0]);
        else throw new Error('Could not parse AI response as JSON');
      }

      // Enforce required shape
      report.week_number = String(weekNumber);
      report.class_id = classId;
      if (!Array.isArray(report.students)) report.students = [];

      // Save report
      await q(`
        UPDATE weekly_reports
        SET status='generated', report_json=$1, generated_at=now(), error_log=null
        WHERE school_id=$2 AND week_number=$3 AND year=$4 AND class_id=$5
      `, [JSON.stringify(report), schoolId, weekNumber, year, classId]);

      totalReports++;
      console.log(`[Reports] ✅ ${school.name} — ${classId} — Week ${weekNumber} (${classStudents.length} students)`);

      // ── Send WhatsApp reports to parents ──────────────────────
      // Decouple: send after all classes are generated, rate-limited
      // We queue here and send in a separate sweep below
      // For now, mark them ready for sending
    } catch (err) {
      // Log failure but don't crash — other classes continue
      console.error(`[Reports] ❌ ${school.name} — ${classId}:`, err.message);
      await q(`
        UPDATE weekly_reports
        SET status='failed', error_log=$1, generated_at=now()
        WHERE school_id=$2 AND week_number=$3 AND year=$4 AND class_id=$5
      `, [err.message, schoolId, weekNumber, year, classId]);
    }
  }

  return { school: school.name, classes: classEntries.length, generated: totalReports, students: students.length };
}

// ── SEND: Deliver parent reports via WhatsApp ─────────────────────
// Called separately from generation — rate-limited at ~8 msg/sec

async function sendWeeklyReportMessages(schoolId, weekNumber, { q, twilioSend }) {
  const year = new Date().getFullYear();
  const schoolRes = await q(`SELECT * FROM schools WHERE id=$1`, [schoolId]);
  const school = schoolRes.rows[0];
  if (!school) return;

  const reports = await q(`
    SELECT * FROM weekly_reports
    WHERE school_id=$1 AND week_number=$2 AND year=$3
      AND status='generated' AND sent_to_parents=false
  `, [schoolId, weekNumber, year]);

  let sent = 0, failed = 0;

  for (const reportRow of reports.rows) {
    const report = reportRow.report_json;
    if (!report?.students?.length) continue;

    for (const sr of report.students) {
      if (!sr.parent_report) continue;

      // Look up parent phone
      const studentRes = await q(`SELECT parent_phone, name FROM students WHERE id=$1`, [sr.student_id]);
      const student = studentRes.rows[0];
      if (!student?.parent_phone) continue;

      const msg =
        `📊 *Weekly Report — ${school.name}*\n\n` +
        `${sr.parent_report}\n\n` +
        `📚 Week ${weekNumber} · ${school.current_term || 'Current Term'}\n` +
        `Reply to ask EduPing any questions.\n${school.name} 🏫`;

      try {
        await twilioSend(student.parent_phone, school.twilio_number || process.env.TWILIO_DEFAULT_FROM, msg);
        sent++;
      } catch (e) {
        console.warn(`[Reports] Send failed — ${student.name}:`, e.message);
        failed++;
      }

      // Rate limit: ~8 messages/sec — stays under Twilio WhatsApp limits
      await sleep(125);
    }

    // Mark this class report as sent
    await q(`
      UPDATE weekly_reports SET sent_to_parents=true
      WHERE id=$1
    `, [reportRow.id]);
  }

  console.log(`[Reports] 📱 ${school.name} — Sent: ${sent}, Failed: ${failed}`);
  return { sent, failed };
}

// ── CRON: Full weekly sweep across all schools ────────────────────

function scheduleReportingCron(cronLib, deps) {
  const { q, callAI, twilioSend } = deps;

  // ── STEP 1: Generate reports — Friday 3pm ─────────────────────
  cronLib.schedule('0 15 * * 5', async () => {
    console.log('[Reports] 🚀 Starting weekly report generation run...');
    const schools = (await q(`SELECT id, name FROM schools WHERE status='active'`)).rows;
    console.log(`[Reports] Processing ${schools.length} schools`);

    const results = await withConcurrency(
      schools,
      school => withTimeout(
        generateWeeklyReportForSchool(school.id, { q, callAI, twilioSend }),
        120000,
        school.name
      ),
      5  // max 5 schools simultaneously
    );

    const succeeded = results.filter(r => !r?.__error).length;
    const failed = results.filter(r => r?.__error).length;
    console.log(`[Reports] Generation complete — ✅ ${succeeded} schools done, ❌ ${failed} failed`);
  });

  // ── STEP 2: Send messages — Friday 4pm (1hr after generation) ─
  cronLib.schedule('0 16 * * 5', async () => {
    console.log('[Reports] 📱 Starting weekly report delivery run...');
    const weekNumber = getISOWeek(new Date());
    const schools = (await q(`SELECT id FROM schools WHERE status='active'`)).rows;

    // Sequential sends — don't blast Twilio with 100 parallel loops
    for (const school of schools) {
      try {
        await sendWeeklyReportMessages(school.id, weekNumber, { q, twilioSend });
      } catch(e) {
        console.error(`[Reports] Send sweep failed for ${school.id}:`, e.message);
      }
      // Brief pause between schools
      await sleep(2000);
    }

    console.log('[Reports] 📱 Delivery run complete');
  });

  // ── STEP 3: Retry failed reports — Saturday 8am ───────────────
  cronLib.schedule('0 8 * * 6', async () => {
    const weekNumber = getISOWeek(new Date());
    const year = new Date().getFullYear();
    const failed = await q(`
      SELECT DISTINCT school_id FROM weekly_reports
      WHERE status='failed' AND week_number=$1 AND year=$2
    `, [weekNumber, year]);

    if (!failed.rows.length) return;
    console.log(`[Reports] 🔁 Retrying ${failed.rows.length} failed schools...`);

    await withConcurrency(
      failed.rows,
      row => generateWeeklyReportForSchool(row.school_id, { q, callAI, twilioSend }),
      3
    );
  });
}

// ── API ROUTES ────────────────────────────────────────────────────

function registerReportingRoutes(app, { requireSchool, q, callAI, twilioSend }) {

  // GET /api/admin/reports — list reports for this school
  app.get('/api/admin/reports', requireSchool, async (req, res) => {
    try {
      const { week_number, year, class_id } = req.query;
      const currentYear = year || new Date().getFullYear();
      let sql = `SELECT id, week_number, year, class_id, status, sent_to_parents, generated_at,
                        CASE WHEN status='generated' THEN report_json ELSE null END as report_json,
                        error_log
                 FROM weekly_reports WHERE school_id=$1 AND year=$2`;
      const params = [req.school.id, currentYear];
      if (week_number) { sql += ` AND week_number=$${params.length+1}`; params.push(week_number); }
      if (class_id)    { sql += ` AND class_id=$${params.length+1}`;    params.push(class_id); }
      sql += ` ORDER BY generated_at DESC LIMIT 100`;
      res.json((await q(sql, params)).rows);
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/admin/reports/classes — distinct class list for dropdown
  app.get('/api/admin/reports/classes', requireSchool, async (req, res) => {
    try {
      const rows = await q(`SELECT DISTINCT class_id FROM weekly_reports WHERE school_id=$1 ORDER BY class_id`, [req.school.id]);
      res.json(rows.rows.map(r => r.class_id));
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/admin/reports/trigger — manually trigger generation for this school
  app.post('/api/admin/reports/trigger', requireSchool, async (req, res) => {
    try {
      // Run async — don't block the HTTP response
      generateWeeklyReportForSchool(req.school.id, { q, callAI, twilioSend })
        .then(result => console.log('[Reports] Manual trigger done:', result))
        .catch(err  => console.error('[Reports] Manual trigger failed:', err.message));
      res.json({ ok: true, message: 'Report generation started. Check back in a few minutes.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/admin/reports/send — manually trigger delivery for this school
  app.post('/api/admin/reports/send', requireSchool, async (req, res) => {
    try {
      const weekNumber = req.body.week_number || getISOWeek(new Date());
      sendWeeklyReportMessages(req.school.id, weekNumber, { q, twilioSend })
        .then(r => console.log('[Reports] Manual send done:', r))
        .catch(e => console.error('[Reports] Manual send failed:', e.message));
      res.json({ ok: true, message: 'Sending started. Parents will receive WhatsApp messages shortly.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/admin/reports/student/:studentId — get all reports for one student
  app.get('/api/admin/reports/student/:studentId', requireSchool, async (req, res) => {
    try {
      // Pull student's class, then fetch relevant weekly_reports
      const studentRes = await q(`SELECT class_name FROM students WHERE id=$1 AND school_id=$2`, [req.params.studentId, req.school.id]);
      if (!studentRes.rows.length) return res.status(404).json({ error: 'Student not found' });
      const classId = studentRes.rows[0].class_name || 'General';

      const rows = await q(`
        SELECT week_number, year, generated_at, report_json
        FROM weekly_reports
        WHERE school_id=$1 AND class_id=$2 AND status='generated'
        ORDER BY year DESC, week_number DESC LIMIT 20
      `, [req.school.id, classId]);

      // Extract only this student's slice from each week's report
      const studentReports = rows.rows.map(row => {
        const report = row.report_json;
        const sr = (report?.students || []).find(s => s.student_id === req.params.studentId);
        return {
          week_number: row.week_number,
          year: row.year,
          generated_at: row.generated_at,
          student_report: sr || null,
          class_insights: report?.class_insights || null
        };
      }).filter(r => r.student_report);

      res.json(studentReports);
    } catch(err) { res.status(500).json({ error: err.message }); }
  });
}

// ── INIT: Wire everything up ──────────────────────────────────────

async function initReporting({ app, requireSchool, q, callAI, twilioSend, cron: cronLib }) {
  // Run DB migration
  await migrateReportingTables(q);
  console.log('✅ Reporting tables ready');

  // Register API routes
  registerReportingRoutes(app, { requireSchool, q, callAI, twilioSend });
  console.log('✅ Reporting routes registered');

  // Schedule crons
  scheduleReportingCron(cronLib, { q, callAI, twilioSend });
  console.log('✅ Reporting crons scheduled (Gen: Fri 3pm | Send: Fri 4pm | Retry: Sat 8am)');
}

module.exports = {
  initReporting,
  generateWeeklyReportForSchool,
  sendWeeklyReportMessages,
  getISOWeek,
  migrateReportingTables
};
