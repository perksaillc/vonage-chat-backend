const express = require('express');
const cors = require('cors');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const { Vonage } = require('@vonage/server-sdk');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

const VONAGE_API_KEY = process.env.VONAGE_API_KEY;
const VONAGE_API_SECRET = process.env.VONAGE_API_SECRET;
const VONAGE_NUMBER = process.env.VONAGE_NUMBER;
const AGENT_PHONES = (process.env.AGENT_PHONES || process.env.AGENT_PHONE || '')
  .split(',').map(s => s.trim()).filter(Boolean); // supports multiple employee numbers
const DASHBOARD_URL = process.env.DASHBOARD_URL;
const DATABASE_URL = process.env.DATABASE_URL;
const SEED_AGENT_USERNAME = process.env.SEED_AGENT_USERNAME;
const SEED_AGENT_PASSWORD = process.env.SEED_AGENT_PASSWORD;
const SEED_AGENT_NAME = process.env.SEED_AGENT_NAME || SEED_AGENT_USERNAME;

const SESSION_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes of inactivity auto-closes a chat

const vonage = new Vonage({ apiKey: VONAGE_API_KEY, apiSecret: VONAGE_API_SECRET });
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });

// sessionId -> { ws, name, messages, lastActivity, unread, closed }
const sessions = new Map();
const agentSockets = new Map();   // ws -> { username, name }
const tokens = new Map();         // token -> { username, name }

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id TEXT PRIMARY KEY,
      name TEXT,
      last_activity BIGINT,
      closed BOOLEAN DEFAULT FALSE
    );
  `);
  // in case this table pre-dates the "closed" column
  await pool.query(`ALTER TABLE sessions ADD COLUMN IF NOT EXISTS closed BOOLEAN DEFAULT FALSE;`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      session_id TEXT REFERENCES sessions(session_id),
      from_role TEXT,
      agent_name TEXT,
      text TEXT,
      ts BIGINT
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS agents (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      display_name TEXT NOT NULL
    );
  `);

  const { rows: sessionRows } = await pool.query('SELECT * FROM sessions');
  for (const row of sessionRows) {
    sessions.set(row.session_id, {
      ws: null,
      name: row.name,
      messages: [],
      lastActivity: Number(row.last_activity),
      unread: 0,
      closed: !!row.closed,
    });
  }
  const { rows: messageRows } = await pool.query('SELECT * FROM messages ORDER BY id ASC');
  for (const row of messageRows) {
    const s = sessions.get(row.session_id);
    if (s) s.messages.push({ from: row.from_role, agentName: row.agent_name, text: row.text, ts: Number(row.ts) });
  }
  console.log(`Loaded ${sessions.size} sessions from database`);

  if (SEED_AGENT_USERNAME && SEED_AGENT_PASSWORD) {
    const { rows } = await pool.query('SELECT id FROM agents WHERE username = $1', [SEED_AGENT_USERNAME]);
    if (rows.length === 0) {
      const hash = await bcrypt.hash(SEED_AGENT_PASSWORD, 10);
      await pool.query('INSERT INTO agents (username, password_hash, display_name) VALUES ($1, $2, $3)', [SEED_AGENT_USERNAME, hash, SEED_AGENT_NAME]);
      console.log(`Seeded agent account: ${SEED_AGENT_USERNAME}`);
    }
  }
}

async function persistSession(sessionId, name, lastActivity, closed) {
  await pool.query(
    `INSERT INTO sessions (session_id, name, last_activity, closed) VALUES ($1, $2, $3, $4)
     ON CONFLICT (session_id) DO UPDATE SET name = COALESCE($2, sessions.name), last_activity = $3, closed = $4`,
    [sessionId, name, lastActivity, !!closed]
  );
}
async function persistMessage(sessionId, from, text, ts, agentName) {
  await pool.query(
    'INSERT INTO messages (session_id, from_role, agent_name, text, ts) VALUES ($1, $2, $3, $4, $5)',
    [sessionId, from, agentName || null, text, ts]
  );
}

function broadcastToAgents(payload) {
  const data = JSON.stringify(payload);
  for (const ws of agentSockets.keys()) {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}
function sessionSummary(sessionId) {
  const s = sessions.get(sessionId);
  const last = s.messages[s.messages.length - 1];
  return {
    sessionId,
    name: s.name || null,
    lastMessage: last ? last.text : '',
    lastFrom: last ? last.from : null,
    lastActivity: s.lastActivity,
    unread: s.unread || 0,
    closed: !!s.closed,
  };
}
function allSessionSummaries() {
  return Array.from(sessions.keys()).map(sessionSummary);
}

async function closeSession(sessionId, reason) {
  const s = sessions.get(sessionId);
  if (!s || s.closed) return;
  s.closed = true;
  const entry = { from: 'system', text: reason, ts: Date.now() };
  s.messages.push(entry);
  s.lastActivity = entry.ts;
  try {
    await persistMessage(sessionId, 'system', reason, entry.ts, null);
    await persistSession(sessionId, s.name, entry.ts, true);
  } catch (err) { console.error('DB write failed:', err); }
  if (s.ws && s.ws.readyState === WebSocket.OPEN) {
    s.ws.send(JSON.stringify({ from: 'system', text: reason, closed: true }));
  }
  broadcastToAgents({ type: 'message', sessionId, ...entry });
  broadcastToAgents({ type: 'session_update', session: sessionSummary(sessionId) });
}

// Sweep for inactive sessions every minute
setInterval(() => {
  const now = Date.now();
  for (const [sessionId, s] of sessions.entries()) {
    if (!s.closed && now - s.lastActivity > SESSION_TIMEOUT_MS) {
      closeSession(sessionId, 'Conversation closed after 15 minutes of inactivity').catch(console.error);
    }
  }
}, 60 * 1000);

// ---- Agent auth ----
app.post('/agent/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'username and password required' });
  const { rows } = await pool.query('SELECT * FROM agents WHERE username = $1', [username]);
  if (rows.length === 0) return res.status(401).json({ error: 'invalid credentials' });
  const ok = await bcrypt.compare(password, rows[0].password_hash);
  if (!ok) return res.status(401).json({ error: 'invalid credentials' });
  const token = crypto.randomBytes(24).toString('hex');
  tokens.set(token, { username, name: rows[0].display_name });
  res.json({ token, name: rows[0].display_name });
});

function requireAgent(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '');
  const agent = tokens.get(token);
  if (!agent) return res.status(401).json({ error: 'unauthorized' });
  req.agent = agent;
  req.token = token;
  next();
}

app.post('/agent/logout', requireAgent, (req, res) => {
  tokens.delete(req.token);
  res.json({ ok: true });
});

app.post('/agent/register', requireAgent, async (req, res) => {
  const { username, password, displayName } = req.body;
  if (!username || !password || !displayName) return res.status(400).json({ error: 'missing fields' });
  const hash = await bcrypt.hash(password, 10);
  try {
    await pool.query('INSERT INTO agents (username, password_hash, display_name) VALUES ($1, $2, $3)', [username, hash, displayName]);
    res.json({ ok: true });
  } catch (err) {
    res.status(400).json({ error: 'username already exists' });
  }
});

app.get('/agent/search', requireAgent, async (req, res) => {
  const q = req.query.q || '';
  const { rows } = await pool.query(
    `SELECT m.session_id, m.text, m.ts, s.name FROM messages m
     JOIN sessions s ON s.session_id = m.session_id
     WHERE m.text ILIKE $1 ORDER BY m.ts DESC LIMIT 30`,
    [`%${q}%`]
  );
  res.json(rows.map(r => ({ sessionId: r.session_id, text: r.text, ts: Number(r.ts), name: r.name })));
});

// ---- WebSocket routing ----
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/ws') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      const sessionId = url.searchParams.get('sessionId');
      const name = url.searchParams.get('name');
      if (!sessionId) { ws.close(); return; }

      if (!sessions.has(sessionId)) {
        sessions.set(sessionId, { ws, name: name || null, messages: [], lastActivity: Date.now(), unread: 0, closed: false });
        persistSession(sessionId, name || null, Date.now(), false).catch(console.error);
      } else {
        const s = sessions.get(sessionId);
        s.ws = ws;
        if (name) s.name = name;
      }

      broadcastToAgents({ type: 'session_update', session: sessionSummary(sessionId) });

      ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }
        if (msg.type === 'end_chat') {
          closeSession(sessionId, `${sessions.get(sessionId)?.name || 'Visitor'} ended the conversation`).catch(console.error);
        }
      });

      ws.on('close', () => {
        if (sessions.has(sessionId)) sessions.get(sessionId).ws = null;
      });
    });
  } else if (url.pathname === '/agent-ws') {
    const token = url.searchParams.get('token');
    const agent = tokens.get(token);
    if (!agent) { socket.destroy(); return; }

    wss.handleUpgrade(req, socket, head, (ws) => {
      agentSockets.set(ws, agent);
      ws.send(JSON.stringify({ type: 'init', sessions: allSessionSummaries(), you: agent.name }));
      broadcastToAgents({ type: 'presence', agents: Array.from(agentSockets.values()).map(a => a.name) });

      ws.on('message', async (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }

        if (msg.type === 'get_history') {
          const s = sessions.get(msg.sessionId);
          if (s) { s.unread = 0; broadcastToAgents({ type: 'session_update', session: sessionSummary(msg.sessionId) }); }
          ws.send(JSON.stringify({ type: 'history', sessionId: msg.sessionId, messages: s ? s.messages : [] }));
          broadcastToAgents({ type: 'viewing', sessionId: msg.sessionId, agentName: agent.name });
        }

        if (msg.type === 'join') {
          const s = sessions.get(msg.sessionId);
          if (!s) return;
          const entry = { from: 'system', text: `${agent.name} joined the chat`, ts: Date.now() };
          s.messages.push(entry);
          s.lastActivity = entry.ts;
          try {
            await persistMessage(msg.sessionId, 'system', entry.text, entry.ts, null);
            await persistSession(msg.sessionId, s.name, entry.ts, s.closed);
          } catch (err) { console.error('DB write failed:', err); }
          if (s.ws && s.ws.readyState === WebSocket.OPEN) {
            s.ws.send(JSON.stringify({ from: 'system', text: entry.text }));
          }
          broadcastToAgents({ type: 'message', sessionId: msg.sessionId, ...entry });
        }

        if (msg.type === 'reply') {
          const s = sessions.get(msg.sessionId);
          if (!s) return;
          const entry = { from: 'agent', agentName: agent.name, text: msg.text, ts: Date.now() };
          s.messages.push(entry);
          s.lastActivity = entry.ts;
          try {
            await persistMessage(msg.sessionId, 'agent', msg.text, entry.ts, agent.name);
            await persistSession(msg.sessionId, s.name, entry.ts, s.closed);
          } catch (err) { console.error('DB write failed:', err); }
          if (s.ws && s.ws.readyState === WebSocket.OPEN) {
            s.ws.send(JSON.stringify({ from: 'agent', agentName: agent.name, text: msg.text }));
          }
          broadcastToAgents({ type: 'message', sessionId: msg.sessionId, ...entry });
        }
      });

      ws.on('close', () => {
        agentSockets.delete(ws);
        broadcastToAgents({ type: 'presence', agents: Array.from(agentSockets.values()).map(a => a.name) });
      });
    });
  } else {
    socket.destroy();
  }
});

app.post('/send', async (req, res) => {
  const { sessionId, name, message } = req.body;
  if (!sessionId || !message) return res.status(400).json({ error: 'sessionId and message are required' });

  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, { ws: null, name: name || null, messages: [], lastActivity: Date.now(), unread: 0, closed: false });
  }
  const s = sessions.get(sessionId);
  if (name) s.name = name;
  s.closed = false; // a new message reopens a timed-out conversation
  s.unread = (s.unread || 0) + 1;
  const entry = { from: 'visitor', text: message, ts: Date.now() };
  s.messages.push(entry);
  s.lastActivity = entry.ts;

  try {
    await persistSession(sessionId, s.name, entry.ts, false);
    await persistMessage(sessionId, 'visitor', message, entry.ts, null);
  } catch (err) { console.error('DB write failed:', err); }

  broadcastToAgents({ type: 'message', sessionId, ...entry, name: s.name });
  broadcastToAgents({ type: 'session_update', session: sessionSummary(sessionId) });

  if (agentSockets.size === 0 && AGENT_PHONES.length > 0) {
    const text = `New chat message${s.name ? ' from ' + s.name : ''}: ${message}${DASHBOARD_URL ? ' — reply at ' + DASHBOARD_URL : ''}`;
    for (const phone of AGENT_PHONES) {
      try {
        await vonage.sms.send({ to: phone, from: VONAGE_NUMBER, text });
      } catch (err) {
        console.error(`Vonage notification to ${phone} failed:`, err);
      }
    }
  }

  res.json({ ok: true });
});

app.post('/webhook/inbound', (req, res) => res.status(200).send('ok'));
app.post('/webhook/status', (req, res) => res.status(200).send('ok'));
app.get('/dashboard', (req, res) => res.sendFile(path.join(__dirname, 'public', 'dashboard.html')));
app.get('/', (req, res) => res.send('Vonage live chat backend is running'));

const PORT = process.env.PORT || 3000;
initDb()
  .then(() => server.listen(PORT, () => console.log(`Server running on port ${PORT}`)))
  .catch((err) => { console.error('Failed to initialize database:', err); process.exit(1); });
