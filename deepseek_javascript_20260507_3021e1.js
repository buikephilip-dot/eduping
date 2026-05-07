// Add these new tables to the migrate() function
async function migrate() {
  await q('CREATE EXTENSION IF NOT EXISTS pgcrypto');

  // Drop old unique constraint on twilio_number if it exists
  await q(`ALTER TABLE schools DROP CONSTRAINT IF EXISTS schools_twilio_number_key`).catch(() => {});

  await q(`
    CREATE TABLE IF NOT EXISTS schools (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      name TEXT NOT NULL, 
      city TEXT, 
      landmark_description TEXT,
      fees TEXT, 
      fee_deadline TEXT, 
      current_term TEXT, 
      whatsapp_number TEXT, 
      twilio_number TEXT,
      admin_password TEXT NOT NULL, 
      super_admin_token TEXT, 
      plan TEXT DEFAULT 'starter', 
      status TEXT DEFAULT 'active',
      billing_start DATE, 
      monthly_retainer NUMERIC DEFAULT 0, 
      setup_fee NUMERIC DEFAULT 0,
      ai_training_paid BOOLEAN DEFAULT false,
      message_limit INTEGER DEFAULT 0,
      messages_used INTEGER DEFAULT 0,
      billing_cycle_start DATE DEFAULT CURRENT_DATE,
      created_at TIMESTAMPTZ DEFAULT now(),
      config JSONB DEFAULT '{}'::jsonb
    );
    
    CREATE TABLE IF NOT EXISTS staff (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL, 
      role TEXT NOT NULL DEFAULT 'teacher', 
      subject TEXT, 
      class TEXT, 
      phone TEXT,
      performance_score NUMERIC DEFAULT 0, 
      attendance_submissions INT DEFAULT 0, 
      scores_uploaded INT DEFAULT 0,
      homework_assigned INT DEFAULT 0, 
      absences INT DEFAULT 0, 
      staff_of_week_count INT DEFAULT 0, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS signin_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      staff_id UUID REFERENCES staff(id) ON DELETE SET NULL, 
      date DATE NOT NULL, 
      time TEXT, 
      status TEXT, 
      photo_verified BOOLEAN DEFAULT false
    );
    
    CREATE TABLE IF NOT EXISTS students (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL, 
      class_name TEXT, 
      parent_name TEXT, 
      parent_phone TEXT,
      weekly_performance_score NUMERIC DEFAULT 0,
      student_of_week_count INT DEFAULT 0, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS attendance (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, 
      date DATE NOT NULL, 
      status TEXT NOT NULL,
      subject TEXT,
      teacher_id UUID REFERENCES staff(id) ON DELETE SET NULL
    );
    
    CREATE TABLE IF NOT EXISTS scores (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, 
      subject TEXT NOT NULL, 
      score NUMERIC NOT NULL, 
      term TEXT, 
      teacher_id UUID REFERENCES staff(id) ON DELETE SET NULL,
      uploaded_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS fees (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, 
      term TEXT, 
      amount_due NUMERIC DEFAULT 0, 
      amount_paid NUMERIC DEFAULT 0,
      status TEXT DEFAULT 'unpaid', 
      due_date DATE
    );
    
    CREATE TABLE IF NOT EXISTS homeworks (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      assigned_by UUID REFERENCES staff(id) ON DELETE SET NULL, 
      class_name TEXT, 
      subject TEXT, 
      description TEXT, 
      due_date DATE, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS behaviour_notes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, 
      note TEXT NOT NULL, 
      reported_by UUID REFERENCES staff(id) ON DELETE SET NULL, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS sickbay_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID REFERENCES students(id) ON DELETE CASCADE, 
      reason TEXT, 
      action_taken TEXT, 
      visited_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS messages (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      from_number TEXT NOT NULL, 
      student_id UUID REFERENCES students(id) ON DELETE SET NULL, 
      channel TEXT DEFAULT 'whatsapp',
      direction TEXT DEFAULT 'inbound',
      user_message TEXT, 
      assistant_reply TEXT, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS admission_inquiries (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      parent_name TEXT, 
      phone TEXT, 
      child_name TEXT, 
      class_applying TEXT, 
      status TEXT DEFAULT 'new', 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS school_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      title TEXT NOT NULL, 
      event_date DATE, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    CREATE TABLE IF NOT EXISTS awards (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      award_type TEXT, 
      winner_id UUID, 
      winner_type TEXT, 
      week_of DATE, 
      announced BOOLEAN DEFAULT false, 
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    -- NEW: Document ingestion for AI training
    CREATE TABLE IF NOT EXISTS school_documents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      title TEXT NOT NULL,
      document_type TEXT CHECK (document_type IN ('handbook', 'calendar', 'fee_structure', 'admission_requirements', 'policy', 'other')),
      content TEXT NOT NULL,
      file_name TEXT,
      file_size INTEGER,
      uploaded_by UUID REFERENCES staff(id) ON DELETE SET NULL,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
    
    -- NEW: Multi-admin seats (separate from staff/teachers)
    CREATE TABLE IF NOT EXISTS school_admins (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      email TEXT,
      phone TEXT,
      role TEXT CHECK (role IN ('owner', 'admin', 'bursar', 'teacher_admin', 'viewer')) DEFAULT 'admin',
      password TEXT NOT NULL,
      is_active BOOLEAN DEFAULT true,
      last_login TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    
    -- NEW: Message usage tracking per month
    CREATE TABLE IF NOT EXISTS message_usage (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      month DATE NOT NULL,
      messages_sent INTEGER DEFAULT 0,
      messages_received INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(school_id, month)
    );
    
    -- NEW: Premium modules system
    CREATE TABLE IF NOT EXISTS school_modules (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      module_key TEXT NOT NULL CHECK (module_key IN ('report_cards', 'fee_payments', 'digital_forms', 'ai_training')),
      is_enabled BOOLEAN DEFAULT false,
      enabled_at TIMESTAMPTZ,
      settings JSONB DEFAULT '{}'::jsonb,
      UNIQUE(school_id, module_key)
    );
    
    -- NEW: Teacher appraisal summary (end of term)
    CREATE TABLE IF NOT EXISTS teacher_appraisals (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      teacher_id UUID NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
      term TEXT NOT NULL,
      punctuality_score NUMERIC DEFAULT 0,
      attendance_rate NUMERIC DEFAULT 0,
      submissions_on_time INTEGER DEFAULT 0,
      homework_assigned_total INTEGER DEFAULT 0,
      average_class_performance NUMERIC DEFAULT 0,
      feedback TEXT,
      completed_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(school_id, teacher_id, term)
    );
    
    -- NEW: Weekly report logs
    CREATE TABLE IF NOT EXISTS weekly_reports (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      school_id UUID NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      week_start DATE NOT NULL,
      report_content TEXT,
      sent_at TIMESTAMPTZ,
      UNIQUE(school_id, student_id, week_start)
    );
    
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS config JSONB DEFAULT '{}'::jsonb;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS ai_training_paid BOOLEAN DEFAULT false;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS message_limit INTEGER DEFAULT 500;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS messages_used INTEGER DEFAULT 0;
    ALTER TABLE schools ADD COLUMN IF NOT EXISTS billing_cycle_start DATE DEFAULT CURRENT_DATE;
    
    CREATE INDEX IF NOT EXISTS idx_students_school ON students(school_id);
    CREATE INDEX IF NOT EXISTS idx_students_phone ON students(parent_phone);
    CREATE INDEX IF NOT EXISTS idx_staff_school_phone ON staff(school_id, phone);
    CREATE INDEX IF NOT EXISTS idx_messages_school_from ON messages(school_id, from_number);
    CREATE INDEX IF NOT EXISTS idx_fees_school_status ON fees(school_id, status);
    CREATE INDEX IF NOT EXISTS idx_school_documents_school ON school_documents(school_id);
    CREATE INDEX IF NOT EXISTS idx_school_admins_school ON school_admins(school_id);
    CREATE INDEX IF NOT EXISTS idx_message_usage_school_month ON message_usage(school_id, month);
  `);
  
  // Initialize modules for existing schools
  const schools = await q('SELECT id FROM schools');
  for (const school of schools.rows) {
    const modules = ['report_cards', 'fee_payments', 'digital_forms', 'ai_training'];
    for (const module_key of modules) {
      await q(`INSERT INTO school_modules (school_id, module_key, is_enabled) 
               VALUES ($1, $2, $3) 
               ON CONFLICT (school_id, module_key) DO NOTHING`, 
               [school.id, module_key, module_key === 'report_cards' ? true : false]);
    }
  }
}

// Track message usage function
async function trackMessageUsage(schoolId, direction = 'outbound') {
  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  
  // Update message_usage table
  if (direction === 'outbound') {
    await q(`INSERT INTO message_usage (school_id, month, messages_sent) 
             VALUES ($1, $2, 1) 
             ON CONFLICT (school_id, month) 
             DO UPDATE SET messages_sent = message_usage.messages_sent + 1, 
                           updated_at = NOW()`, 
             [schoolId, monthStart]);
  } else {
    await q(`INSERT INTO message_usage (school_id, month, messages_received) 
             VALUES ($1, $2, 1) 
             ON CONFLICT (school_id, month) 
             DO UPDATE SET messages_received = message_usage.messages_received + 1, 
                           updated_at = NOW()`, 
             [schoolId, monthStart]);
  }
  
  // Update school total
  await q(`UPDATE schools SET messages_used = messages_used + 1 WHERE id = $1`, [schoolId]);
  
  // Check if approaching limit
  const school = await q('SELECT message_limit, messages_used FROM schools WHERE id = $1', [schoolId]);
  if (school.rows[0] && school.rows[0].message_limit > 0) {
    const usagePercent = (school.rows[0].messages_used / school.rows[0].message_limit) * 100;
    if (usagePercent >= 80 && usagePercent < 90) {
      // Alert at 80%
      console.log(`School ${schoolId} at ${usagePercent}% of message limit`);
    } else if (usagePercent >= 90) {
      console.log(`School ${schoolId} approaching message limit - 90% used`);
    }
  }
}

// Document ingestion endpoint
app.post('/api/admin/documents/upload', requireSchool, async (req, res) => {
  try {
    const { title, document_type, content, file_name, file_size } = req.body;
    if (!title || !document_type || !content) {
      return bad(res, 'Title, document type, and content are required');
    }
    
    const result = await q(`INSERT INTO school_documents 
      (school_id, title, document_type, content, file_name, file_size) 
      VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [req.school.id, title, document_type, content, file_name || null, file_size || null]);
    
    // If AI training module is enabled, this document will be used in context
    res.json({ ok: true, document: result.rows[0] });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/documents', requireSchool, async (req, res) => {
  try {
    const docs = await q('SELECT * FROM school_documents WHERE school_id = $1 AND is_active = true ORDER BY created_at DESC', [req.school.id]);
    res.json(docs.rows);
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/documents/:id', requireSchool, async (req, res) => {
  try {
    await q('UPDATE school_documents SET is_active = false WHERE id = $1 AND school_id = $2', [req.params.id, req.school.id]);
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Enhanced AI call with document context
async function callAIWithContext(schoolId, system, userText, imageBase64) {
  // Fetch relevant documents for this school
  const documents = await q(`SELECT title, content, document_type 
    FROM school_documents 
    WHERE school_id = $1 AND is_active = true 
    ORDER BY created_at DESC LIMIT 10`, [schoolId]);
  
  let enhancedSystem = system;
  if (documents.rows.length > 0) {
    const docContext = documents.rows.map(doc => 
      `[${doc.title}]: ${doc.content.substring(0, 1000)}`
    ).join('\n\n');
    enhancedSystem = `${system}\n\nSchool documents for reference:\n${docContext}`;
  }
  
  return callAI(enhancedSystem, userText, imageBase64);
}

// Multi-admin endpoints
app.post('/api/admin/admins', requireSchool, async (req, res) => {
  try {
    const { name, email, phone, role, password } = req.body;
    
    // Check seat limit based on plan
    const seatLimits = { starter: 1, growth: 3, scale: 10 };
    const currentAdmins = await q('SELECT COUNT(*) FROM school_admins WHERE school_id = $1 AND is_active = true', [req.school.id]);
    const limit = seatLimits[req.school.plan] || 1;
    
    if (parseInt(currentAdmins.rows[0].count) >= limit) {
      return bad(res, `Your ${req.school.plan} plan only allows ${limit} admin seat(s). Upgrade to add more.`);
    }
    
    const result = await q(`INSERT INTO school_admins (school_id, name, email, phone, role, password) 
      VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, name, email, phone, role`,
      [req.school.id, name, email, phone, role, password]);
    
    res.json({ ok: true, admin: result.rows[0] });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/admins', requireSchool, async (req, res) => {
  try {
    const admins = await q('SELECT id, name, email, phone, role, is_active, last_login, created_at FROM school_admins WHERE school_id = $1', [req.school.id]);
    res.json(admins.rows);
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/api/admin/admins/:id', requireSchool, async (req, res) => {
  try {
    const { role, is_active, password } = req.body;
    const updates = [];
    const values = [];
    let idx = 1;
    
    if (role) { updates.push(`role = $${idx++}`); values.push(role); }
    if (is_active !== undefined) { updates.push(`is_active = $${idx++}`); values.push(is_active); }
    if (password) { updates.push(`password = $${idx++}`); values.push(password); }
    
    if (updates.length === 0) return bad(res, 'No fields to update');
    
    values.push(req.params.id, req.school.id);
    await q(`UPDATE school_admins SET ${updates.join(', ')} WHERE id = $${idx} AND school_id = $${idx+1}`, values);
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Message usage endpoint
app.get('/api/admin/usage', requireSchool, async (req, res) => {
  try {
    const school = await q('SELECT message_limit, messages_used FROM schools WHERE id = $1', [req.school.id]);
    const monthlyUsage = await q(`SELECT 
      EXTRACT(YEAR FROM month) as year,
      EXTRACT(MONTH FROM month) as month,
      messages_sent,
      messages_received
      FROM message_usage 
      WHERE school_id = $1 
      ORDER BY month DESC LIMIT 6`, [req.school.id]);
    
    const usagePercent = school.rows[0].message_limit > 0 
      ? (school.rows[0].messages_used / school.rows[0].message_limit) * 100 
      : 0;
    
    res.json({
      limit: school.rows[0].message_limit,
      used: school.rows[0].messages_used,
      percent: Math.round(usagePercent),
      is_over_limit: usagePercent >= 100,
      monthly: monthlyUsage.rows
    });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Premium modules endpoints
app.get('/api/admin/modules', requireSchool, async (req, res) => {
  try {
    const modules = await q('SELECT * FROM school_modules WHERE school_id = $1', [req.school.id]);
    res.json(modules.rows);
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/api/admin/modules/:module_key', requireSchool, async (req, res) => {
  try {
    const { is_enabled, settings } = req.body;
    const school = await q('SELECT plan, ai_training_paid FROM schools WHERE id = $1', [req.school.id]);
    
    // Check if module is available for this plan
    const planModules = {
      starter: ['report_cards'],
      growth: ['report_cards', 'fee_payments', 'digital_forms'],
      scale: ['report_cards', 'fee_payments', 'digital_forms', 'ai_training']
    };
    
    if (!planModules[school.rows[0].plan].includes(req.params.module_key)) {
      return bad(res, `${req.params.module_key} module is not available on your ${school.rows[0].plan} plan`);
    }
    
    if (req.params.module_key === 'ai_training' && is_enabled && !school.rows[0].ai_training_paid) {
      return bad(res, 'AI Training fee must be paid first. Contact your account manager.');
    }
    
    await q(`INSERT INTO school_modules (school_id, module_key, is_enabled, settings, enabled_at)
      VALUES ($1, $2, $3, $4, CASE WHEN $3 THEN NOW() ELSE NULL END)
      ON CONFLICT (school_id, module_key) 
      DO UPDATE SET is_enabled = EXCLUDED.is_enabled, 
                    settings = EXCLUDED.settings,
                    enabled_at = CASE WHEN EXCLUDED.is_enabled THEN NOW() ELSE NULL END`,
      [req.school.id, req.params.module_key, is_enabled, settings || {}]);
    
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Report card generation (PDF)
app.post('/api/admin/report-cards/:student_id', requireSchool, async (req, res) => {
  try {
    const module = await q('SELECT is_enabled FROM school_modules WHERE school_id = $1 AND module_key = $2', [req.school.id, 'report_cards']);
    if (!module.rows[0]?.is_enabled) {
      return bad(res, 'Report cards module is not enabled for your school');
    }
    
    const { term } = req.body;
    const student = await q('SELECT * FROM students WHERE id = $1 AND school_id = $2', [req.params.student_id, req.school.id]);
    const scores = await q('SELECT subject, score FROM scores WHERE student_id = $1 AND term = $2', [req.params.student_id, term || req.school.current_term]);
    const attendance = await q('SELECT COUNT(*) as total, SUM(CASE WHEN status = \'present\' THEN 1 ELSE 0 END) as present FROM attendance WHERE student_id = $1', [req.params.student_id]);
    
    // Calculate average
    const avgScore = scores.rows.length > 0 
      ? scores.rows.reduce((sum, s) => sum + s.score, 0) / scores.rows.length 
      : 0;
    
    const attendanceRate = attendance.rows[0].total > 0 
      ? (attendance.rows[0].present / attendance.rows[0].total) * 100 
      : 0;
    
    const report = {
      student: student.rows[0],
      term: term || req.school.current_term,
      school: { name: req.school.name, city: req.school.city, landmark: req.school.landmark_description },
      scores: scores.rows,
      average_score: avgScore,
      attendance_rate: attendanceRate,
      generated_at: new Date().toISOString()
    };
    
    // In production: generate actual PDF and send via WhatsApp
    // For now, return JSON that can be used by frontend
    res.json({ ok: true, report });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Enhanced weekly reports with proper templates
async function weeklyReports() {
  const schools = (await q('SELECT * FROM schools WHERE status = $1', ['active'])).rows;
  const weekStart = new Date();
  weekStart.setDate(weekStart.getDate() - weekStart.getDay()); // Start of week (Sunday)
  
  for (const school of schools) {
    const students = (await q('SELECT * FROM students WHERE school_id = $1 AND parent_phone IS NOT NULL', [school.id])).rows;
    
    for (const student of students) {
      // Get weekly performance data
      const scores = await q(`SELECT subject, score FROM scores 
        WHERE student_id = $1 AND uploaded_at >= $2 
        ORDER BY uploaded_at DESC LIMIT 5`, 
        [student.id, weekStart]);
      
      const attendance = await q(`SELECT COUNT(*) as days_present FROM attendance 
        WHERE student_id = $1 AND date >= $2 AND status = 'present'`, 
        [student.id, weekStart]);
      
      const behaviour = await q(`SELECT note FROM behaviour_notes 
        WHERE student_id = $1 AND created_at >= $2 
        ORDER BY created_at DESC LIMIT 3`, 
        [student.id, weekStart]);
      
      // Build report message
      let reportMessage = `📊 *Weekly Report* - ${school.name}\n`;
      reportMessage += `Student: ${student.name}\n`;
      reportMessage += `Class: ${student.class_name || 'Not specified'}\n`;
      reportMessage += `Week of: ${weekStart.toLocaleDateString('en-NG')}\n\n`;
      
      if (scores.rows.length > 0) {
        reportMessage += `*Recent Scores:*\n`;
        scores.rows.forEach(s => {
          reportMessage += `• ${s.subject}: ${s.score}%\n`;
        });
        reportMessage += `\n`;
      }
      
      if (attendance.rows[0]) {
        reportMessage += `*Attendance:* ${attendance.rows[0].days_present} days present this week\n\n`;
      }
      
      if (behaviour.rows.length > 0) {
        reportMessage += `*Behaviour Notes:*\n`;
        behaviour.rows.slice(0, 2).forEach(n => {
          reportMessage += `• ${n.note.substring(0, 100)}${n.note.length > 100 ? '...' : ''}\n`;
        });
        reportMessage += `\n`;
      }
      
      reportMessage += `Performance Score: ${student.weekly_performance_score || 0}%\n`;
      reportMessage += `\nReply with questions about your child's progress.\n`;
      reportMessage += `${school.name} 🏫`;
      
      // Send report
      if (hasTwilio()) {
        await twilioSend(student.parent_phone, school.twilio_number || process.env.TWILIO_DEFAULT_FROM, reportMessage);
      }
      
      // Log report
      await q(`INSERT INTO weekly_reports (school_id, student_id, week_start, report_content, sent_at) 
        VALUES ($1, $2, $3, $4, NOW()) 
        ON CONFLICT (school_id, student_id, week_start) 
        DO UPDATE SET report_content = EXCLUDED.report_content, sent_at = NOW()`,
        [school.id, student.id, weekStart, reportMessage]);
    }
  }
}

// Teacher appraisal end of term report
app.get('/api/admin/teacher-appraisal/:teacher_id', requireSchool, async (req, res) => {
  try {
    const { term } = req.query;
    const targetTerm = term || req.school.current_term;
    
    const teacher = await q('SELECT * FROM staff WHERE id = $1 AND school_id = $2', [req.params.teacher_id, req.school.id]);
    if (!teacher.rows[0]) return bad(res, 'Teacher not found', 404);
    
    // Calculate appraisal metrics
    const signins = await q(`SELECT COUNT(*) as total, 
      SUM(CASE WHEN time <= '08:30' THEN 1 ELSE 0 END) as punctual 
      FROM signin_log WHERE staff_id = $1 AND date >= date_trunc('term', NOW())`, 
      [req.params.teacher_id]);
    
    const submissions = await q(`SELECT COUNT(*) as total, 
      SUM(CASE WHEN uploaded_at <= NOW() THEN 1 ELSE 0 END) as on_time 
      FROM scores WHERE teacher_id = $1`, 
      [req.params.teacher_id]);
    
    const homework = await q('SELECT COUNT(*) FROM homeworks WHERE assigned_by = $1', [req.params.teacher_id]);
    
    const classPerformance = await q(`SELECT AVG(score) as average 
      FROM scores s 
      JOIN students st ON st.id = s.student_id 
      WHERE s.teacher_id = $1 AND st.class_name = $2`,
      [req.params.teacher_id, teacher.rows[0].class]);
    
    const punctualityScore = signins.rows[0].total > 0 
      ? (signins.rows[0].punctual / signins.rows[0].total) * 100 
      : 0;
    
    const attendanceRate = teacher.rows[0].attendance_submissions > 0 
      ? 100 : 0;
    
    const submissionsOnTime = submissions.rows[0].total > 0 
      ? (submissions.rows[0].on_time / submissions.rows[0].total) * 100 
      : 0;
    
    const appraisal = {
      teacher: teacher.rows[0],
      term: targetTerm,
      metrics: {
        punctuality_score: punctualityScore,
        attendance_rate: attendanceRate,
        submissions_on_time: submissionsOnTime,
        homework_assigned_total: parseInt(homework.rows[0].count),
        average_class_performance: classPerformance.rows[0].average || 0
      },
      overall_score: (punctualityScore + attendanceRate + submissionsOnTime + (classPerformance.rows[0].average || 0)) / 4,
      generated_at: new Date().toISOString()
    };
    
    // Save appraisal
    await q(`INSERT INTO teacher_appraisals (school_id, teacher_id, term, 
      punctuality_score, attendance_rate, submissions_on_time, 
      homework_assigned_total, average_class_performance)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (school_id, teacher_id, term) 
      DO UPDATE SET punctuality_score = EXCLUDED.punctuality_score,
                    attendance_rate = EXCLUDED.attendance_rate,
                    submissions_on_time = EXCLUDED.submissions_on_time,
                    homework_assigned_total = EXCLUDED.homework_assigned_total,
                    average_class_performance = EXCLUDED.average_class_performance,
                    completed_at = NOW()`,
      [req.school.id, req.params.teacher_id, targetTerm, 
       appraisal.metrics.punctuality_score, 
       appraisal.metrics.attendance_rate,
       appraisal.metrics.submissions_on_time,
       appraisal.metrics.homework_assigned_total,
       appraisal.metrics.average_class_performance]);
    
    res.json(appraisal);
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/teacher-appraisals', requireSchool, async (req, res) => {
  try {
    const appraisals = await q(`SELECT ta.*, s.name as teacher_name, s.class, s.subject
      FROM teacher_appraisals ta
      JOIN staff s ON s.id = ta.teacher_id
      WHERE ta.school_id = $1
      ORDER BY ta.completed_at DESC`, 
      [req.school.id]);
    res.json(appraisals.rows);
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Update plan limits in super admin
app.patch('/api/super/schools/:id/plan', requireSuper, async (req, res) => {
  try {
    const { plan, ai_training_paid, message_limit } = req.body;
    const planLimits = { starter: 500, growth: 2000, scale: 10000 };
    
    const updates = [];
    const values = [];
    let idx = 1;
    
    if (plan) {
      updates.push(`plan = $${idx++}`);
      values.push(plan);
      updates.push(`message_limit = $${idx++}`);
      values.push(message_limit || planLimits[plan] || 500);
    }
    if (ai_training_paid !== undefined) {
      updates.push(`ai_training_paid = $${idx++}`);
      values.push(ai_training_paid);
    }
    if (message_limit !== undefined && !plan) {
      updates.push(`message_limit = $${idx++}`);
      values.push(message_limit);
    }
    
    if (updates.length === 0) return bad(res, 'No fields to update');
    
    values.push(req.params.id);
    const result = await q(`UPDATE schools SET ${updates.join(', ')} WHERE id = $${idx} RETURNING *`, values);
    res.json(result.rows[0]);
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Update message tracking in webhook handler - modify handleIncomingWhatsApp
// Add tracking call inside the function after identifying school
// Also track in /api/chat endpoint

// Update seedIfEmpty to include new fields
async function seedIfEmpty() {
  const existing = await q('SELECT id FROM schools LIMIT 1');
  if (existing.rowCount) return;
  const school = await q(`INSERT INTO schools
    (name, city, landmark_description, fees, fee_deadline, current_term, whatsapp_number, twilio_number, admin_password, plan, status, billing_start, monthly_retainer, setup_fee, message_limit, messages_used)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,current_date,$12,$13,$14,$15) RETURNING *`,
    ['Greenfield Academy', 'Abuja', 'Green gate beside the assembly hall', '85000 per term', '15th of each term month', '2nd Term 2024/2025', '+2347015255068', '+14155238886', 'admin123', 'starter', 'active', 50000, 100000, 500, 0]);
  // ... rest of seed data remains same
}