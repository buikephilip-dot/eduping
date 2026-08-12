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
      reviewed_by_staff_id UUID REFERENCES staff(id) ON DELETE SET NULL,
      reviewed_at TIMESTAMPTZ,
      UNIQUE(school_id, week_number, year, class_id)
    );
    CREATE INDEX IF NOT EXISTS idx_weekly_reports_school
      ON weekly_reports(school_id, week_number, year);
    CREATE INDEX IF NOT EXISTS idx_weekly_reports_status
      ON weekly_reports(status);
  `);
  // For deployments that already had this table before review columns existed
  await q(`ALTER TABLE weekly_reports ADD COLUMN IF NOT EXISTS reviewed_by_staff_id UUID REFERENCES staff(id) ON DELETE SET NULL`).catch(() => {});
  await q(`ALTER TABLE weekly_reports ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ`).catch(() => {});
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

async function generateWeeklyReportForSchool(schoolId, { q, callAI, twilioSend, issueTeacherPortalToken }) {
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
        const [scores, attendance, behaviour, homeworks, sickbay] = await Promise.all([
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
            [schoolId, student.class_name, weekStart]),
          q(`SELECT reason, action_taken, visited_at FROM sickbay_log
             WHERE student_id=$1 AND school_id=$2 AND visited_at >= $3
             ORDER BY visited_at DESC`,
            [student.id, schoolId, weekStart])
        ]);

        return {
          student_id: student.id,
          student_name: student.name,
          class: student.class_name,
          scores: scores.rows,
          attendance: attendance.rows,
          behaviour_notes: behaviour.rows,
          homeworks_assigned: homeworks.rows,
          sickbay_visits: sickbay.rows
        };
      }));

      // ── Baseline + exceptions model ────────────────────────
      // Count how many students have actual data this week
      const studentsWithData = studentInputs.filter(s =>
        s.scores.length > 0 ||
        s.behaviour_notes.length > 0 ||
        s.sickbay_visits.length > 0 ||
        s.attendance.some(a => a.status === 'absent')
      ).length;

      const isBaselineWeek = studentsWithData === 0;
      const baselineNote = isBaselineWeek
        ? 'NOTE: No exceptions were logged this week. Generate positive baseline reports for all students — assume normal attendance, steady progress, and good behaviour unless data says otherwise.'
        : `${studentsWithData} of ${classStudents.length} students have logged data this week. For students with no data, generate a positive baseline report.`;

      // Build the user message — one AI call covers the whole class
      const userPrompt = `School: ${school.name}, ${school.city || 'Nigeria'}
Term: ${school.current_term || 'Current Term'}
Week number: ${weekNumber}
Week starting: ${weekStart}
Class: ${classId}
Total students: ${classStudents.length}
${baselineNote}

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

      // Every student starts unreviewed — teacher approves all or edits/excludes individuals
      report.students = report.students.map(s => ({ ...s, approved: false, excluded: false }));

      // Save report — awaiting teacher review before anything is sent
      await q(`
        UPDATE weekly_reports
        SET status='generated', report_json=$1, generated_at=now(), error_log=null
        WHERE school_id=$2 AND week_number=$3 AND year=$4 AND class_id=$5
      `, [JSON.stringify(report), schoolId, weekNumber, year, classId]);

      totalReports++;
      console.log(`[Reports] ✅ ${school.name} — ${classId} — Week ${weekNumber} (${classStudents.length} students) — awaiting teacher review`);

      // ── Notify the class teacher their report is ready to review ──
      // Reports never auto-send; a teacher must approve (or edit/exclude students) first.
      if (issueTeacherPortalToken) {
        try {
          const { rows: teachers } = await q(
            `SELECT * FROM staff WHERE school_id=$1 AND class=$2 AND phone IS NOT NULL`,
            [schoolId, classId]
          );
          for (const teacher of teachers) {
            await issueTeacherPortalToken(teacher, school, {
              intro: `📋 Hi ${teacher.name}, this week's AI-drafted report for ${classId} is ready for your review. Approve, edit, or exclude any student before it's sent to parents:`
            });
          }
        } catch (notifyErr) {
          console.warn(`[Reports] Could not notify teacher for ${classId}:`, notifyErr.message);
        }
      }
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

// ── SEND: Deliver one already-approved report's messages via WhatsApp ─────
// Never call this on an unreviewed ('generated') report — only on 'approved'.
// Skips any student the teacher excluded during review.
async function sendReportRow(reportRow, school, { q, twilioSend }) {
  const report = typeof reportRow.report_json === 'string' ? JSON.parse(reportRow.report_json) : reportRow.report_json;
  if (!report?.students?.length) return { sent: 0, failed: 0, excluded: 0 };

  let sent = 0, failed = 0, excluded = 0;
  for (const sr of report.students) {
    if (sr.excluded) { excluded++; continue; }
    if (!sr.parent_report) continue;

    const studentRes = await q(`SELECT parent_phone, name FROM students WHERE id=$1`, [sr.student_id]);
    const student = studentRes.rows[0];
    if (!student?.parent_phone) continue;

    const msg =
      `📊 *Weekly Report — ${school.name}*\n\n` +
      `${sr.parent_report}\n\n` +
      `📚 Week ${reportRow.week_number} · ${school.current_term || 'Current Term'}\n` +
      `Reply to ask EduPing any questions.\n${school.name} 🏫`;

    try {
      await twilioSend(student.parent_phone, school.twilio_number || process.env.TWILIO_DEFAULT_FROM, msg);
      sent++;
    } catch (e) {
      console.warn(`[Reports] Send failed — ${student.name}:`, e.message);
      failed++;
    }
    await sleep(125); // ~8 msg/sec — stays under Twilio WhatsApp limits
  }

  await q(`UPDATE weekly_reports SET sent_to_parents=true WHERE id=$1`, [reportRow.id]);
  console.log(`[Reports] 📱 ${school.name} — ${reportRow.class_id} — Sent: ${sent}, Excluded: ${excluded}, Failed: ${failed}`);
  return { sent, failed, excluded };
}

// School/week sweep — sends any already-approved reports that haven't gone out yet
// (e.g. a retry after a prior send attempt partially failed). Never touches unreviewed reports.
async function sendWeeklyReportMessages(schoolId, weekNumber, { q, twilioSend }) {
  const year = new Date().getFullYear();
  const school = (await q(`SELECT * FROM schools WHERE id=$1`, [schoolId])).rows[0];
  if (!school) return { sent: 0, failed: 0 };

  const reports = await q(`
    SELECT * FROM weekly_reports
    WHERE school_id=$1 AND week_number=$2 AND year=$3
      AND status='approved' AND sent_to_parents=false
  `, [schoolId, weekNumber, year]);

  let totals = { sent: 0, failed: 0, excluded: 0 };
  for (const reportRow of reports.rows) {
    const r = await sendReportRow(reportRow, school, { q, twilioSend });
    totals.sent += r.sent; totals.failed += r.failed; totals.excluded += r.excluded;
  }
  return totals;
}

// ── CRON: Full weekly sweep across all schools ────────────────────

function scheduleReportingCron(cronLib, deps) {
  const { q, callAI, twilioSend, issueTeacherPortalToken } = deps;

  // ── STEP 1: Generate reports — Friday 3pm ─────────────────────
  // Generation notifies each class teacher automatically (inside generateWeeklyReportForSchool)
  // so there is no separate "send" step here — reports never go to parents until a teacher approves.
  cronLib.schedule('0 15 * * 5', async () => {
    console.log('[Reports] 🚀 Starting weekly report generation run...');
    const schools = (await q(`SELECT id, name FROM schools WHERE status='active'`)).rows;
    console.log(`[Reports] Processing ${schools.length} schools`);

    const results = await withConcurrency(
      schools,
      school => withTimeout(
        generateWeeklyReportForSchool(school.id, { q, callAI, twilioSend, issueTeacherPortalToken }),
        120000,
        school.name
      ),
      5  // max 5 schools simultaneously
    );

    const succeeded = results.filter(r => !r?.__error).length;
    const failed = results.filter(r => r?.__error).length;
    console.log(`[Reports] Generation complete — ✅ ${succeeded} schools done, ❌ ${failed} failed`);
  });

  // ── STEP 2: Reminder for un-reviewed reports — Saturday 10am ──
  // Reports still sitting at status='generated' a day later haven't been approved yet.
  // Nudge the class teacher again rather than silently sending unreviewed content.
  cronLib.schedule('0 10 * * 6', async () => {
    console.log('[Reports] 🔔 Checking for un-reviewed reports...');
    const weekNumber = getISOWeek(new Date());
    const year = new Date().getFullYear();
    const pending = await q(
      `SELECT DISTINCT school_id, class_id FROM weekly_reports
       WHERE status='generated' AND week_number=$1 AND year=$2`,
      [weekNumber, year]
    );
    if (!pending.rows.length) { console.log('[Reports] Nothing pending review'); return; }

    for (const row of pending.rows) {
      try {
        const school = (await q(`SELECT * FROM schools WHERE id=$1`, [row.school_id])).rows[0];
        if (!school) continue;
        const teachers = (await q(
          `SELECT * FROM staff WHERE school_id=$1 AND class=$2 AND phone IS NOT NULL`,
          [row.school_id, row.class_id]
        )).rows;
        for (const teacher of teachers) {
          await issueTeacherPortalToken(teacher, school, {
            intro: `🔔 Reminder: ${row.class_id}'s weekly report is still awaiting your review before it can go to parents:`
          });
        }
      } catch (e) {
        console.error(`[Reports] Reminder failed for ${row.school_id}/${row.class_id}:`, e.message);
      }
      await sleep(500);
    }
    console.log(`[Reports] 🔔 Reminders sent for ${pending.rows.length} un-reviewed reports`);
  });

  // ── STEP 3: Retry failed report generation — Saturday 8am ─────
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
      row => generateWeeklyReportForSchool(row.school_id, { q, callAI, twilioSend, issueTeacherPortalToken }),
      3
    );
  });
}

// ── API ROUTES ────────────────────────────────────────────────────

function registerReportingRoutes(app, { requireSchool, q, callAI, twilioSend, issueTeacherPortalToken, requireUnlockedTeacherToken, getSchool }) {

  // GET /api/admin/reports — list reports for this school
  // report_json is always visible to admin regardless of status — approval doesn't
  // hide the audit trail of what was actually sent to parents.
  app.get('/api/admin/reports', requireSchool, async (req, res) => {
    try {
      const { week_number, year, class_id } = req.query;
      const currentYear = year || new Date().getFullYear();
      let sql = `SELECT wr.id, wr.week_number, wr.year, wr.class_id, wr.status, wr.sent_to_parents, wr.generated_at,
                        wr.report_json, wr.error_log, wr.reviewed_at,
                        st.name AS reviewed_by_name
                 FROM weekly_reports wr
                 LEFT JOIN staff st ON st.id = wr.reviewed_by_staff_id
                 WHERE wr.school_id=$1 AND wr.year=$2`;
      const params = [req.school.id, currentYear];
      if (week_number) { sql += ` AND wr.week_number=$${params.length+1}`; params.push(week_number); }
      if (class_id)    { sql += ` AND wr.class_id=$${params.length+1}`;    params.push(class_id); }
      sql += ` ORDER BY wr.generated_at DESC LIMIT 100`;
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
      generateWeeklyReportForSchool(req.school.id, { q, callAI, twilioSend, issueTeacherPortalToken })
        .then(result => console.log('[Reports] Manual trigger done:', result))
        .catch(err  => console.error('[Reports] Manual trigger failed:', err.message));
      res.json({ ok: true, message: 'Report generation started. Class teachers will be notified to review once ready.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/admin/reports/send — resend already-approved reports that haven't gone out yet
  // (e.g. a prior send attempt partially failed). Cannot bypass teacher review —
  // sendWeeklyReportMessages only ever touches status='approved' rows.
  app.post('/api/admin/reports/send', requireSchool, async (req, res) => {
    try {
      const weekNumber = req.body.week_number || getISOWeek(new Date());
      sendWeeklyReportMessages(req.school.id, weekNumber, { q, twilioSend })
        .then(r => console.log('[Reports] Manual send done:', r))
        .catch(e => console.error('[Reports] Manual send failed:', e.message));
      res.json({ ok: true, message: 'Sending started for approved reports. Anything still awaiting teacher review will not be sent.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/admin/reports/student/:studentId — full report history for one student, any status
  app.get('/api/admin/reports/student/:studentId', requireSchool, async (req, res) => {
    try {
      const studentRes = await q(`SELECT class_name FROM students WHERE id=$1 AND school_id=$2`, [req.params.studentId, req.school.id]);
      if (!studentRes.rows.length) return res.status(404).json({ error: 'Student not found' });
      const classId = studentRes.rows[0].class_name || 'General';

      const rows = await q(`
        SELECT week_number, year, status, sent_to_parents, generated_at, report_json
        FROM weekly_reports
        WHERE school_id=$1 AND class_id=$2
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

  // ── Teacher-facing review (via the same tokenized portal link) ──
  // All guarded by requireUnlockedTeacherToken — same PIN + device-binding as
  // the question/score submission flow. A teacher can only ever see/act on
  // their own class's report.

  app.get('/api/teacher/:token/weekly-report/:reportId', requireUnlockedTeacherToken, async (req, res) => {
    try {
      const t = req.teacherToken;
      const { rows: [staff] } = await q(`SELECT class FROM staff WHERE id=$1`, [t.staff_id]);
      const { rows: [report] } = await q(
        `SELECT * FROM weekly_reports WHERE id=$1 AND school_id=$2 AND class_id=$3`,
        [req.params.reportId, t.school_id, staff?.class]
      );
      if (!report) return res.status(404).json({ error: 'Report not found' });

      // The AI output only carries student_id — attach names here so the review UI
      // doesn't need a separate round trip or show anonymous IDs.
      const data = typeof report.report_json === 'string' ? JSON.parse(report.report_json) : report.report_json;
      if (data?.students?.length) {
        const ids = data.students.map(s => s.student_id).filter(Boolean);
        const { rows: names } = ids.length
          ? await q(`SELECT id, name FROM students WHERE id = ANY($1)`, [ids])
          : { rows: [] };
        const nameMap = Object.fromEntries(names.map(n => [n.id, n.name]));
        data.students = data.students.map(s => ({ ...s, student_name: nameMap[s.student_id] || 'Unknown student' }));
      }

      res.json({ ...report, report_json: data });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Edit one student's summary text and/or toggle whether they're excluded from this week's send
  app.post('/api/teacher/:token/weekly-report/:reportId/student/:studentId', requireUnlockedTeacherToken, async (req, res) => {
    try {
      const t = req.teacherToken;
      const { parent_report, excluded } = req.body;
      const { rows: [staff] } = await q(`SELECT class FROM staff WHERE id=$1`, [t.staff_id]);
      const { rows: [report] } = await q(
        `SELECT * FROM weekly_reports WHERE id=$1 AND school_id=$2 AND class_id=$3 AND status='generated'`,
        [req.params.reportId, t.school_id, staff?.class]
      );
      if (!report) return res.status(404).json({ error: 'Report not found or already sent' });

      const data = typeof report.report_json === 'string' ? JSON.parse(report.report_json) : report.report_json;
      const idx = (data.students || []).findIndex(s => s.student_id === req.params.studentId);
      if (idx === -1) return res.status(404).json({ error: 'Student not found in this report' });

      if (typeof parent_report === 'string' && parent_report.trim()) data.students[idx].parent_report = parent_report.trim();
      if (typeof excluded === 'boolean') data.students[idx].excluded = excluded;

      await q(`UPDATE weekly_reports SET report_json=$1 WHERE id=$2`, [JSON.stringify(data), report.id]);
      res.json({ ok: true, student: data.students[idx] });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Approve the whole report (minus any excluded students) — sends immediately
  app.post('/api/teacher/:token/weekly-report/:reportId/approve', requireUnlockedTeacherToken, async (req, res) => {
    try {
      const t = req.teacherToken;
      const { rows: [staff] } = await q(`SELECT class FROM staff WHERE id=$1`, [t.staff_id]);
      const { rows: [report] } = await q(
        `SELECT * FROM weekly_reports WHERE id=$1 AND school_id=$2 AND class_id=$3 AND status='generated'`,
        [req.params.reportId, t.school_id, staff?.class]
      );
      if (!report) return res.status(404).json({ error: 'Report not found or already reviewed' });

      await q(
        `UPDATE weekly_reports SET status='approved', reviewed_by_staff_id=$1, reviewed_at=now() WHERE id=$2`,
        [t.staff_id, report.id]
      );

      const school = await getSchool(t.school_id);
      const updated = (await q(`SELECT * FROM weekly_reports WHERE id=$1`, [report.id])).rows[0];
      const result = await sendReportRow(updated, school, { q, twilioSend });

      res.json({ ok: true, message: `Approved. Sent to ${result.sent} parent${result.sent === 1 ? '' : 's'}${result.excluded ? `, ${result.excluded} excluded` : ''}.`, ...result });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });
}

// ── INIT: Wire everything up ──────────────────────────────────────

async function initReporting({ app, requireSchool, q, callAI, twilioSend, cron: cronLib, issueTeacherPortalToken, requireUnlockedTeacherToken, getSchool }) {
  // Run DB migration
  await migrateReportingTables(q);
  console.log('✅ Reporting tables ready');

  // Register API routes
  registerReportingRoutes(app, { requireSchool, q, callAI, twilioSend, issueTeacherPortalToken, requireUnlockedTeacherToken, getSchool });
  console.log('✅ Reporting routes registered');

  // Schedule crons
  scheduleReportingCron(cronLib, { q, callAI, twilioSend, issueTeacherPortalToken });
  console.log('✅ Reporting crons scheduled (Gen: Fri 3pm | Review reminder: Sat 10am | Retry: Sat 8am)');
}

module.exports = {
  initReporting,
  generateWeeklyReportForSchool,
  sendWeeklyReportMessages,
  getISOWeek,
  migrateReportingTables
};
