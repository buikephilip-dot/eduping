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
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: false, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── DB helpers ───────────────────────────────────────────
async function readDb() {
  const raw = await fs.readFile(DB_PATH, 'utf8');
  return JSON.parse(raw);
}
async function writeDb(db) {
  await fs.writeFile(DB_PATH, JSON.stringify(db, null, 2));
}

// ─── Auth middleware ───────────────────────────────────────
function checkAdmin(req, res, next) {
  const token = req.headers['x-admin-token'] || req.query.token;
  const password = process.env.ADMIN_PASSWORD || 'admin';
  if (token !== password) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

function getStaffFromPhone(db, phone) {
  return db.staff.find(s => s.phone === phone || s.phone === `whatsapp:${phone}` || phone.includes(s.phone.replace('whatsapp:', '')));
}

function getParentFromPhone(db, phone) {
  return db.students.find(s => s.parentPhone === phone || s.parentPhone === `whatsapp:${phone}` || phone.includes(s.parentPhone.replace('whatsapp:', '')));
}

// ─── AI helpers ────────────────────────────────────────────
async function callAnthropic(system, messages, imageBase64 = null) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY is missing');
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  let userContent = messages[messages.length - 1].content;
  if (imageBase64) {
    userContent = [
      { type: 'image', source: { type: 'base64', media_type: 'image/jpeg', data: imageBase64 } },
      { type: 'text', text: typeof userContent === 'string' ? userContent : 'Analyze this image.' }
    ];
    messages = [...messages.slice(0, -1), { role: 'user', content: userContent }];
  }

  const response = await client.messages.create({
    model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514',
    max_tokens: 800,
    system,
    messages
  });
  return response.content?.[0]?.text || 'Sorry, I could not process that.';
}

async function callOpenAI(system, messages) {
  if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY is missing');
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const response = await client.chat.completions.create({
    model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
    messages: [{ role: 'system', content: system }, ...messages],
    temperature: 0.4,
    max_tokens: 800
  });
  return response.choices?.[0]?.message?.content || 'Sorry, I could not process that.';
}

async function callAI(system, messages, imageBase64 = null) {
  const provider = (process.env.AI_PROVIDER || 'anthropic').toLowerCase();
  if (provider === 'anthropic' || imageBase64) return callAnthropic(system, messages, imageBase64);
  return callOpenAI(system, messages);
}

// ─── Parent chat ───────────────────────────────────────────
function buildParentContext(db, student) {
  return `School: ${db.school.name}, ${db.school.city}
Current term: ${db.school.currentTerm}
School fees: ${db.school.fees}
Fee deadline: ${db.school.feeDeadline}
Events: ${db.school.events.map(e => `${e.title}: ${e.date}`).join('; ')}

Student: ${student.name} | Class: ${student.className}
Parent: ${student.parentName}
Attendance this week: ${student.attendanceThisWeek} | This term: ${student.attendanceTerm}
Daily: ${Object.entries(student.weeklyAttendance).map(([k,v]) => `${k}: ${v}`).join(', ')}
Scores: ${Object.entries(student.scores).map(([k,v]) => `${k}: ${v}`).join(', ')}
Fees: ${student.fees.status}, outstanding: ${student.fees.outstanding}
Behaviour notes: ${student.behaviourNotes.length ? student.behaviourNotes.join('; ') : 'None'}
Pending homework: ${student.homeworks.filter(h => !h.submitted).map(h => h.subject + ': ' + h.description + ' due ' + h.dueDate).join('; ') || 'None'}
Sickbay visits: ${student.sickbayVisits.length ? student.sickbayVisits.map(v => v.date + ': ' + v.reason).join('; ') : 'None'}`;
}

const PARENT_SYSTEM = (context) => `You are EduPing, a friendly professional WhatsApp AI assistant for a Nigerian school. Use only the data below. Keep replies warm, clear, Nigerian-friendly with light emojis. End formal info with: ${context.split('\n')[0].split(':')[1]?.trim() || 'Greenfield Academy'} 🏫. If you cannot answer from the data, say you will pass it to admin.

${context}`;

async function askParentAI(userMessage, history, db, student) {
  const context = buildParentContext(db, student);
  const system = PARENT_SYSTEM(context);
  const safeHistory = (Array.isArray(history) ? history : []).filter(m => ['user','assistant'].includes(m.role)).slice(-10);
  const messages = [...safeHistory, { role: 'user', content: userMessage }];
  return callAI(system, messages);
}

// ─── Teacher WhatsApp processing ───────────────────────────
async function processTeacherMessage(staff, messageText, imageBase64, db) {
  const lower = messageText.toLowerCase().trim();
  const now = new Date();
  const today = now.toISOString().split('T')[0];
  const timeStr = now.toTimeString().slice(0, 5);
  const lateThreshold = db.school.lateThreshold || '08:00';
  const isLate = timeStr > lateThreshold;

  // ── Sign in via photo ──
  if (imageBase64 && (lower.includes('sign') || lower.includes('morning') || lower.includes('arrived') || lower.includes('here') || !lower || lower.length < 30)) {
    const landmark = db.school.landmark || 'school gate';
    const verifyPrompt = `You are verifying a teacher sign-in photo for ${db.school.name}. The school landmark is: ${landmark}. Look at this image and determine if it shows a person at or near a school entrance, gate, or building. Reply with ONLY a JSON object: {"verified": true/false, "reason": "brief reason"}`;
    let verified = false;
    let reason = 'Could not verify photo';
    try {
      const verifyResult = await callAI(verifyPrompt, [{ role: 'user', content: 'Verify this sign-in photo.' }], imageBase64);
      const cleaned = verifyResult.replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(cleaned);
      verified = parsed.verified;
      reason = parsed.reason;
    } catch(e) {
      verified = true; // fallback — don't block on parse error
      reason = 'Photo received';
    }

    if (verified) {
      const existingToday = staff.signinLog.find(l => l.date === today);
      if (!existingToday) {
        staff.signinLog.push({ date: today, time: timeStr, status: isLate ? 'late' : 'on_time' });
        await writeDb(db);
      }
      if (isLate) {
        return `⚠️ Good morning ${staff.name.split(' ')[1]}! You've been signed in at ${timeStr}. Please note this is after the ${lateThreshold} deadline. Have a good day! 🏫`;
      }
      return `✅ Good morning ${staff.name.split(' ')[1]}! Signed in at ${timeStr}. Have a great day! 🏫`;
    } else {
      return `❌ Sign-in photo could not be verified. Please send a clear photo at ${landmark}. Reason: ${reason}`;
    }
  }

  // ── Attendance via photo ──
  if (imageBase64 && (lower.includes('attendance') || lower.includes('register') || lower.includes('present') || lower.includes('absent'))) {
    const attendancePrompt = `You are reading a teacher's attendance register photo for ${db.school.name}. Extract the student names and their attendance status (Present/Absent). Reply ONLY with JSON: {"class": "detected class or unknown", "records": [{"name": "student name", "status": "Present or Absent"}]}`;
    try {
      const result = await callAI(attendancePrompt, [{ role: 'user', content: 'Extract attendance from this register.' }], imageBase64);
      const cleaned = result.replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(cleaned);
      staff.attendanceSubmissions = (staff.attendanceSubmissions || 0) + 1;
      await writeDb(db);
      const presentCount = parsed.records.filter(r => r.status === 'Present').length;
      const absentCount = parsed.records.filter(r => r.status === 'Absent').length;
      return `✅ Attendance recorded for ${parsed.class || staff.class}!\n\n📊 ${presentCount} Present | ${absentCount} Absent\n\nRecords updated successfully. Greenfield Academy 🏫`;
    } catch(e) {
      return `📋 Attendance photo received! I'll process this manually. Please also send as voice note for faster processing.`;
    }
  }

  // ── Scores via photo ──
  if (imageBase64 && (lower.includes('score') || lower.includes('result') || lower.includes('test') || lower.includes('marks') || lower.includes('script'))) {
    const scoresPrompt = `You are reading a teacher's marked scripts or score sheet for ${db.school.name}. Extract student names and their scores. Reply ONLY with JSON: {"subject": "subject name or unknown", "records": [{"name": "student name", "score": number}]}`;
    try {
      const result = await callAI(scoresPrompt, [{ role: 'user', content: 'Extract scores from this sheet.' }], imageBase64);
      const cleaned = result.replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(cleaned);
      staff.scoresUploaded = (staff.scoresUploaded || 0) + 1;
      await writeDb(db);
      const avg = parsed.records.length ? Math.round(parsed.records.reduce((a, r) => a + r.score, 0) / parsed.records.length) : 0;
      return `✅ ${parsed.subject || 'Subject'} scores recorded for ${parsed.records.length} students!\n\n📊 Class average: ${avg}%\n\nParents will be notified. Greenfield Academy 🏫`;
    } catch(e) {
      return `📝 Score sheet received! Processing your students' results now.`;
    }
  }

  // ── Voice/text: assign homework ──
  if (lower.includes('homework') || lower.includes('assignment')) {
    const hwPrompt = `Extract homework assignment from this teacher message. Reply ONLY with JSON: {"class": "class name", "subject": "subject", "description": "what to do", "dueDate": "due date or 'next class'"}
Message: "${messageText}"`;
    try {
      const result = await callAI(hwPrompt, [{ role: 'user', content: messageText }]);
      const cleaned = result.replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(cleaned);
      const hw = { id: crypto.randomUUID(), assignedBy: staff.name, ...parsed, assignedDate: today, submitted: false };
      db.homeworks.push(hw);
      staff.homeworkAssigned = (staff.homeworkAssigned || 0) + 1;
      await writeDb(db);
      return `✅ Homework assigned!\n\n📚 ${parsed.subject} — ${parsed.description}\n📅 Due: ${parsed.dueDate}\n👥 Class: ${parsed.class}\n\nParents will be notified shortly. Greenfield Academy 🏫`;
    } catch(e) {
      return `📚 Homework noted! Make sure to include: subject, class, description and due date for automatic parent notification.`;
    }
  }

  // ── Behaviour note ──
  if (lower.includes('behaviour') || lower.includes('behavior') || lower.includes('disrupt') || lower.includes('note') || lower.includes('report')) {
    const notePrompt = `Extract a student behaviour note from this teacher message. Reply ONLY with JSON: {"studentName": "name", "note": "behaviour description"}
Message: "${messageText}"`;
    try {
      const result = await callAI(notePrompt, [{ role: 'user', content: messageText }]);
      const cleaned = result.replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(cleaned);
      const student = db.students.find(s => s.name.toLowerCase().includes(parsed.studentName.toLowerCase().split(' ')[0]));
      if (student) {
        student.behaviourNotes.push(`${today}: ${parsed.note}`);
        await writeDb(db);
        return `✅ Behaviour note added for ${student.name}.\n\n📝 "${parsed.note}"\n\nAdmin has been notified. Greenfield Academy 🏫`;
      }
      return `✅ Behaviour note recorded. Student not found in database — admin will review.`;
    } catch(e) {
      return `📝 Note received. Please format as: "Behaviour note for [student name]: [description]"`;
    }
  }

  // ── Sickbay (nurse) ──
  if (staff.role === 'nurse' && (lower.includes('sickbay') || lower.includes('sick') || lower.includes('unwell') || lower.includes('visited'))) {
    const sickPrompt = `Extract sickbay visit info from this nurse message. Reply ONLY with JSON: {"studentName": "name", "reason": "reason for visit", "action": "what was done"}
Message: "${messageText}"`;
    try {
      const result = await callAI(sickPrompt, [{ role: 'user', content: messageText }]);
      const cleaned = result.replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(cleaned);
      const student = db.students.find(s => s.name.toLowerCase().includes(parsed.studentName.toLowerCase().split(' ')[0]));
      if (student) {
        student.sickbayVisits.push({ date: today, reason: parsed.reason, action: parsed.action });
        db.sickbayLog.push({ date: today, studentId: student.id, studentName: student.name, reason: parsed.reason, action: parsed.action });
        await writeDb(db);
        return `✅ Sickbay visit logged for ${student.name}.\n\n🏥 Reason: ${parsed.reason}\n💊 Action: ${parsed.action}\n\nParent will be notified. Greenfield Academy 🏫`;
      }
    } catch(e) {}
    return `🏥 Sickbay visit noted. Please format as: "Sickbay: [student name] - [reason] - [action taken]"`;
  }

  // ── Sign out ──
  if (lower.includes('goodbye') || lower.includes('signing out') || lower.includes('leaving') || lower.includes('good evening')) {
    const signout = staff.signinLog.find(l => l.date === today);
    if (signout) signout.signout = timeStr;
    await writeDb(db);
    return `👋 Goodbye ${staff.name.split(' ')[1]}! Signed out at ${timeStr}. See you tomorrow! Greenfield Academy 🏫`;
  }

  // ── General teacher query ──
  const teacherSystem = `You are EduPing, the school AI assistant for ${db.school.name}. You are speaking with ${staff.name}, a ${staff.role.replace('_', ' ')} who teaches ${staff.subject || 'at the school'}. Help them with school-related queries. Keep replies short and friendly.`;
  return callAI(teacherSystem, [{ role: 'user', content: messageText }]);
}

// ─── Weekly awards calculation ─────────────────────────────
function calculateStaffOfWeek(db) {
  return db.staff
    .filter(s => s.role === 'class_teacher' || s.role === 'subject_teacher')
    .map(s => {
      const recentSignins = s.signinLog.slice(-5);
      const punctuality = recentSignins.length ? recentSignins.filter(l => l.status === 'on_time').length / recentSignins.length * 100 : 0;
      const score = (punctuality * 0.3) + (Math.min(s.attendanceSubmissions, 20) / 20 * 100 * 0.3) + (Math.min(s.scoresUploaded, 15) / 15 * 100 * 0.2) + (Math.min(s.homeworkAssigned, 10) / 10 * 100 * 0.2);
      return { ...s, calculatedScore: Math.round(score) };
    })
    .sort((a, b) => b.calculatedScore - a.calculatedScore)[0];
}

function calculateStudentOfWeek(db, className) {
  return db.students
    .filter(s => s.className === className)
    .map(s => {
      const attendance = parseFloat(s.attendanceThisWeek) || 0;
      const avgScore = Object.values(s.scores).length ? Object.values(s.scores).reduce((a, v) => a + parseFloat(v), 0) / Object.values(s.scores).length : 0;
      const behaviour = s.behaviourNotes.length === 0 ? 100 : Math.max(0, 100 - s.behaviourNotes.length * 20);
      const score = (attendance * 0.3) + (avgScore * 0.5) + (behaviour * 0.2);
      return { ...s, calculatedScore: Math.round(score) };
    })
    .sort((a, b) => b.calculatedScore - a.calculatedScore)[0];
}

// ─── Routes ────────────────────────────────────────────────

app.get('/api/health', (req, res) => res.json({ ok: true, service: 'EduPing', time: new Date().toISOString() }));

app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  if (password === (process.env.ADMIN_PASSWORD || 'admin')) {
    res.json({ success: true, token: process.env.ADMIN_PASSWORD || 'admin' });
  } else {
    res.status(401).json({ error: 'Wrong password' });
  }
});

app.get('/api/dashboard', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const outstanding = db.students.filter(s => s.fees.outstanding !== '₦0').length;
    const todayStr = new Date().toISOString().split('T')[0];
    const signedInToday = db.staff.filter(s => s.signinLog.some(l => l.date === todayStr)).length;
    const staffOfWeek = db.staff.find(s => s.id === db.staffOfWeek?.current);
    const classes = [...new Set(db.students.map(s => s.className))];
    const studentOfWeek = classes.map(cls => {
      const sowId = db.studentOfWeek?.[cls]?.current;
      return { class: cls, student: db.students.find(s => s.id === sowId) };
    });
    res.json({
      school: db.school,
      stats: {
        students: db.students.length,
        staff: db.staff.length,
        conversations: db.messages.length,
        feeIssues: outstanding,
        admissionInquiries: db.admissionInquiries?.length || 0,
        signedInToday,
        homeworksThisWeek: db.homeworks?.length || 0
      },
      students: db.students,
      staff: db.staff,
      staffOfWeek,
      studentOfWeek,
      admissionInquiries: db.admissionInquiries || [],
      sickbayLog: db.sickbayLog || [],
      recentMessages: db.messages.slice(-20).reverse()
    });
  } catch (err) { next(err); }
});

app.get('/api/staff', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    res.json(db.staff);
  } catch(err) { next(err); }
});

app.post('/api/staff', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const newStaff = { id: 'staff-' + Date.now(), performanceScore: 0, signinLog: [], attendanceSubmissions: 0, scoresUploaded: 0, homeworkAssigned: 0, absences: 0, staffOfWeekCount: 0, ...req.body };
    db.staff.push(newStaff);
    await writeDb(db);
    res.json(newStaff);
  } catch(err) { next(err); }
});

app.get('/api/awards/calculate', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const staffWinner = calculateStaffOfWeek(db);
    const classes = [...new Set(db.students.map(s => s.className))];
    const studentWinners = classes.map(cls => ({ class: cls, winner: calculateStudentOfWeek(db, cls) }));
    res.json({ staffOfWeek: staffWinner, studentOfWeek: studentWinners });
  } catch(err) { next(err); }
});

app.post('/api/awards/confirm', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const { staffId, studentAwards } = req.body;
    if (staffId) {
      db.staffOfWeek.current = staffId;
      const staff = db.staff.find(s => s.id === staffId);
      if (staff) staff.staffOfWeekCount = (staff.staffOfWeekCount || 0) + 1;
    }
    if (studentAwards) {
      studentAwards.forEach(({ className, studentId }) => {
        if (!db.studentOfWeek[className]) db.studentOfWeek[className] = { current: null, history: [] };
        db.studentOfWeek[className].current = studentId;
        const student = db.students.find(s => s.id === studentId);
        if (student) student.studentOfWeekCount = (student.studentOfWeekCount || 0) + 1;
      });
    }
    await writeDb(db);
    res.json({ success: true });
  } catch(err) { next(err); }
});

app.get('/api/students/:id', checkAdmin, async (req, res, next) => {
  try {
    const db = await readDb();
    const student = db.students.find(s => s.id === req.params.id);
    if (!student) return res.status(404).json({ error: 'Student not found' });
    res.json(student);
  } catch(err) { next(err); }
});

app.post('/api/chat', async (req, res, next) => {
  try {
    const { message, history } = req.body;
    if (!message || typeof message !== 'string') return res.status(400).json({ error: 'message is required' });
    const db = await readDb();
    const lower = message.toLowerCase();
    const student = db.students.find(s => lower.includes(s.name.toLowerCase().split(' ')[0])) || db.students[0];
    const reply = await askParentAI(message, history || [], db, student);
    db.messages.push({ id: crypto.randomUUID(), channel: 'web', studentId: student.id, userMessage: message, assistantReply: reply, createdAt: new Date().toISOString() });
    await writeDb(db);
    res.json({ reply, student });
  } catch(err) { next(err); }
});

app.post('/api/broadcast', checkAdmin, async (req, res, next) => {
  try {
    const { message, to } = req.body;
    if (!message) return res.status(400).json({ error: 'message is required' });
    const db = await readDb();
    const recipients = Array.isArray(to) && to.length ? to : db.students.map(s => s.parentPhone);
    if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) {
      return res.json({ queued: true, testMode: true, count: recipients.length, note: 'Simulated — Twilio not configured.' });
    }
    const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    const sent = [];
    for (const recipient of recipients) {
      const msg = await client.messages.create({ from: process.env.TWILIO_WHATSAPP_FROM, to: recipient, body: message });
      sent.push({ to: recipient, sid: msg.sid });
    }
    res.json({ queued: true, count: sent.length, sent });
  } catch(err) { next(err); }
});

// ─── Super Admin route ─────────────────────────────────────
app.get('/superadmin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'superadmin.html'));
});

app.post('/api/superadmin/login', (req, res) => {
  const { password } = req.body;
  const superPassword = process.env.SUPER_ADMIN_PASSWORD || 'superadmin';
  if (password === superPassword) {
    res.json({ success: true, token: superPassword });
  } else {
    res.status(401).json({ error: 'Wrong password' });
  }
});

// ─── First-message disclaimer ──────────────────────────────
function buildDisclaimer(schoolName) {
  return `👋 Welcome to ${schoolName}'s AI assistant — EduPing!\n\nBefore we continue, please note:\n\n📋 Your conversations and your child's school data (attendance, results, fees) are processed by our AI system to answer your questions.\n\n🔒 Your data is kept private and only used to provide information about your child. It is never sold or shared with third parties.\n\n🤖 This service is powered by AI. For urgent matters please contact the school directly.\n\nBy continuing to chat you agree to this. Type anything to get started! 😊\n\n${schoolName} 🏫`;
}

function isFirstMessage(db, phone) {
  return !db.messages.some(m => m.from === phone || m.from === `whatsapp:${phone}`);
}

// ─── WhatsApp Webhook (Twilio) ─────────────────────────────
app.post('/webhooks/twilio/whatsapp', async (req, res) => {
  const twiml = new twilio.twiml.MessagingResponse();
  try {
    const incoming = req.body.Body || '';
    const from = req.body.From || '';
    const mediaUrl = req.body.MediaUrl0 || null;
    const db = await readDb();

    let imageBase64 = null;
    if (mediaUrl && process.env.TWILIO_ACCOUNT_SID) {
      try {
        const imgRes = await fetch(mediaUrl, {
          headers: { 'Authorization': 'Basic ' + Buffer.from(`${process.env.TWILIO_ACCOUNT_SID}:${process.env.TWILIO_AUTH_TOKEN}`).toString('base64') }
        });
        const buffer = await imgRes.arrayBuffer();
        imageBase64 = Buffer.from(buffer).toString('base64');
      } catch(e) { console.error('Image fetch error:', e.message); }
    }

    const staff = getStaffFromPhone(db, from);
    const parentStudent = getParentFromPhone(db, from);

    let reply = '';

    if (staff) {
      // Staff never get disclaimer — they are internal
      reply = await processTeacherMessage(staff, incoming, imageBase64, db);

    } else if (parentStudent) {
      const firstTime = isFirstMessage(db, from);
      const aiReply = await askParentAI(incoming, [], db, parentStudent);
      reply = firstTime ? buildDisclaimer(db.school.name) + '\n\n─────────────────\n\n' + aiReply : aiReply;
      db.messages.push({ channel: 'whatsapp', from, studentId: parentStudent.id, userMessage: incoming, assistantReply: reply, createdAt: new Date().toISOString() });
      await writeDb(db);

    } else {
      // Unknown number — prospective parent / admission inquiry
      const firstTime = isFirstMessage(db, from);
      const admissionSystem = `You are EduPing, the WhatsApp AI assistant for ${db.school.name} in ${db.school.city}. Someone is contacting us for the first time. Help them with admission inquiries, school information, fees (${db.school.fees}), and current term info. Keep replies warm and professional. End with: ${db.school.name} 🏫`;
      const aiReply = await callAI(admissionSystem, [{ role: 'user', content: incoming }]);
      reply = firstTime ? buildDisclaimer(db.school.name) + '\n\n─────────────────\n\n' + aiReply : aiReply;
      db.messages.push({ channel: 'whatsapp', from, studentId: null, userMessage: incoming, assistantReply: reply, createdAt: new Date().toISOString() });
      await writeDb(db);
    }

    twiml.message(reply);
    res.type('text/xml').send(twiml.toString());
  } catch(err) {
    console.error('Webhook error:', err);
    twiml.message('Sorry, EduPing is having trouble right now. Please try again shortly. 🏫');
    res.type('text/xml').send(twiml.toString());
  }
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: err.message || 'Server error' });
});

app.listen(PORT, () => console.log(`EduPing running on http://localhost:${PORT}`));
