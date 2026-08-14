const express = require('express');
const cors = require('cors');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const { Vonage } = require('@vonage/server-sdk');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

const VONAGE_API_KEY = process.env.VONAGE_API_KEY;
const VONAGE_API_SECRET = process.env.VONAGE_API_SECRET;
const VONAGE_NUMBER = process.env.VONAGE_NUMBER;   // your Vonage virtual number
const AGENT_PHONE = process.env.AGENT_PHONE;       // phone that gets the "new message" ping
const AGENT_KEY = process.env.AGENT_KEY;           // shared secret for dashboard access
const DASHBOARD_URL = process.env.DASHBOARD_URL;   // e.g. https://your-app.up.railway.app/dashboard

const vonage = new Vonage({ apiKey: VONAGE_API_KEY, apiSecret: VONAGE_API_SECRET });

const sessions = new Map();
const agentSockets = new Set();

function broadcastToAgents(payload) {
  const data = JSON.stringify(payload);
  for (const s of agentSockets) {
    if (s.readyState === WebSocket.OPEN) s.send(data);
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
  };
}

function allSessionSummaries() {
  return Array.from(sessions.keys()).map(sessionSummary);
}

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/ws') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      const sessionId = url.searchParams.get('sessionId');
      const name = url.searchParams.get('name');
      if (!sessionId) { ws.close(); return; }

      if (!sessions.has(sessionId)) {
        sessions.set(sessionId, { ws, name: name || null, messages: [], lastActivity: Date.now() });
      } else {
        sessions.get(sessionId).ws = ws;
        if (name) sessions.get(sessionId).name = name;
      }

      broadcastToAgents({ type: 'session_update', session: sessionSummary(sessionId) });

      ws.on('close', () => {
        if (sessions.has(sessionId)) sessions.get(sessionId).ws = null;
      });
    });
  } else if (url.pathname === '/agent-ws') {
    if (url.searchParams.get('key') !== AGENT_KEY) {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      agentSockets.add(ws);
      ws.send(JSON.stringify({ type: 'init', sessions: allSessionSummaries() }));

      ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }

        if (msg.type === 'get_history') {
          const s = sessions.get(msg.sessionId);
          ws.send(JSON.stringify({ type: 'history', sessionId: msg.sessionId, messages: s ? s.messages : [] }));
        }

        if (msg.type === 'reply') {
          const s = sessions.get(msg.sessionId);
          if (!s) return;
          const entry = { from: 'agent', text: msg.text, ts: Date.now() };
          s.messages.push(entry);
          s.lastActivity = entry.ts;
          if (s.ws && s.ws.readyState === WebSocket.OPEN) {
            s.ws.send(JSON.stringify({ from: 'agent', text: msg.text }));
          }
          broadcastToAgents({ type: 'message', sessionId: msg.sessionId, ...entry });
        }
      });

      ws.on('close', () => agentSockets.delete(ws));
    });
  } else {
    socket.destroy();
  }
});

app.post('/send', async (req, res) => {
  const { sessionId, name, message } = req.body;
  if (!sessionId || !message) {
    return res.status(400).json({ error: 'sessionId and message are required' });
  }

  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, { ws: null, name: name || null, messages: [], lastActivity: Date.now() });
  }
  const s = sessions.get(sessionId);
  if (name) s.name = name;
  const entry = { from: 'visitor', text: message, ts: Date.now() };
  s.messages.push(entry);
  s.lastActivity = entry.ts;

  broadcastToAgents({ type: 'message', sessionId, ...entry, name: s.name });

  if (agentSockets.size === 0) {
    try {
      await vonage.sms.send({
        to: AGENT_PHONE,
        from: VONAGE_NUMBER,
        text: `New chat message${s.name ? ' from ' + s.name : ''}: ${message}${DASHBOARD_URL ? ' — reply at ' + DASHBOARD_URL : ''}`,
      });
    } catch (err) {
      console.error('Vonage notification failed:', err);
    }
  }

  res.json({ ok: true });
});

app.post('/webhook/inbound', (req, res) => res.status(200).send('ok'));
app.post('/webhook/status', (req, res) => res.status(200).send('ok'));

app.get('/dashboard', (req, res) => res.sendFile(path.join(__dirname, 'public', 'dashboard.html')));

app.get('/', (req, res) => res.send('Vonage live chat backend is running'));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
