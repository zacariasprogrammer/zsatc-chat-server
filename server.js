/* ============================================================
   ZSATC Chat Server v6 — private rooms + ACCOUNTS + voice
   + avatars + images + reactions + typing + read receipts
   + ADMIN API + power modes
   Node.js + Express + WebSockets. Deploy as a Render Web Service.

   Accounts: POST /api/signup {user, pass} and POST /api/login
   {user, pass} -> { ok, token, user }. Passwords hashed with
   scrypt. The client sends the token in hello { token, ... }.
   A valid token locks the username (no spoofing); guests can
   still join but can't claim a registered name.
   NOTE: accounts live in memory — they reset if the server
   restarts (free-tier sleep). The client re-logs-in gracefully.

   Protocol additions (v6):
   - send_audio { audio } -> message { kind:'audio' } (data:audio/, 1MB cap)
   - hello { token, avatar } -> avatar shown in messages + online list
   - auth_expired {} (bad/unknown token — client must log in again)
   - messages carry { avatar, verified }
   - online users carry { avatar, verified }

   Admin API: all previous endpoints, plus
   - GET  /admin/accounts
   - POST /admin/delete-account { user }
   ============================================================ */

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { promisify } = require('util');
const { WebSocketServer } = require('ws');

const scrypt = promisify(crypto.scrypt);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json({ limit: '2mb' }));

// CORS: the web OS site + admin panel live on another origin
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-admin-token');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const MAX_MESSAGES = 200;
const MAX_IMAGE_BYTES = 700 * 1024;
const MAX_AUDIO_BYTES = 1024 * 1024; // 1MB voice messages
const MAX_AVATAR_BYTES = 30 * 1024; // 30KB profile pics
const REACT_EMOJIS = ['❤️', '👍', '😂', '😮', '😢', '🔥', '👏'];
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const rooms = new Map();
let nextId = 1;

// server power mode: 'online' | 'maintenance' | 'shutdown'
let serverMode = 'online';
let modeMessage = '';
const MODE_LABEL = { online: 'online', maintenance: 'maintenance mode', shutdown: 'shut down' };

// accounts (in-memory — resets on server restart)
const accounts = new Map(); // username -> { salt, hash, created }
const tokens = new Map();   // token -> username

/* ---------------- helpers ---------------- */

function cleanName(raw) {
  return String(raw || 'guest')
    .replace(/[^A-Za-z0-9 _-]/g, '')
    .trim()
    .slice(0, 24) || 'guest';
}

function cleanPhone(raw) {
  return String(raw || '')
    .replace(/[^0-9+()\-.\s]/g, '')
    .trim()
    .slice(0, 20);
}

function cleanRoom(raw) {
  return String(raw || 'lobby')
    .replace(/[^A-Za-z0-9_-]/g, '')
    .trim()
    .slice(0, 32) || 'lobby';
}

function cleanAvatar(raw) {
  const s = String(raw || '');
  if (!s.startsWith('data:image/') || s.length > MAX_AVATAR_BYTES) return '';
  return s;
}

function cleanPass(raw) {
  const p = String(raw || '');
  return (p.length >= 4 && p.length <= 64) ? p : null;
}

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { messages: [], clients: new Map(), lastActive: Date.now() });
  return rooms.get(code);
}

function onlineUsers(room) {
  const seen = new Map();
  for (const c of room.clients.values()) {
    if (!seen.has(c.user)) seen.set(c.user, { user: c.user, phone: c.phone, avatar: c.avatar || '', verified: !!c.verified });
  }
  return [...seen.values()];
}

function broadcastToRoom(room, obj, except) {
  const data = JSON.stringify(obj);
  for (const [ws] of room.clients) {
    if (ws !== except && ws.readyState === 1) ws.send(data);
  }
}

function throttled(ws, c) {
  const now = Date.now();
  if (now - c.lastMsg < 2000) {
    ws.send(JSON.stringify({ type: 'error', error: 'Slow down a little.' }));
    return true;
  }
  c.lastMsg = now;
  return false;
}

function pushMessage(room, m) {
  room.messages.push(m);
  room.lastActive = Date.now();
  if (room.messages.length > MAX_MESSAGES) room.messages.shift();
  broadcastToRoom(room, { type: 'message', message: m });
}

async function hashPass(pass) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = (await scrypt(pass, salt, 64)).toString('hex');
  return { salt, hash };
}

async function verifyPass(pass, salt, hash) {
  try {
    const h = (await scrypt(pass, salt, 64)).toString('hex');
    return h.length === hash.length && crypto.timingSafeEqual(Buffer.from(h, 'hex'), Buffer.from(hash, 'hex'));
  } catch (e) { return false; }
}

// brute-force guard: 10 auth attempts per IP per minute
const authAttempts = new Map();
function authRateLimited(req, res) {
  const ip = req.ip || (req.connection && req.connection.remoteAddress) || 'x';
  const now = Date.now();
  let a = authAttempts.get(ip);
  if (!a || now > a.reset) { a = { count: 0, reset: now + 60000 }; authAttempts.set(ip, a); }
  a.count++;
  if (a.count > 10) {
    res.status(429).json({ ok: false, error: 'Too many attempts. Try again in a minute.' });
    return true;
  }
  return false;
}

/* ---------------- public account API ---------------- */

app.post('/api/signup', async (req, res) => {
  if (authRateLimited(req, res)) return;
  const user = cleanName(req.body && req.body.user);
  const pass = cleanPass(req.body && req.body.pass);
  if (!user || user === 'guest') return res.json({ ok: false, error: 'Pick a valid username.' });
  if (!pass) return res.json({ ok: false, error: 'Password must be 4-64 characters.' });
  if (accounts.has(user)) return res.json({ ok: false, error: 'That name is taken.' });
  const { salt, hash } = await hashPass(pass);
  accounts.set(user, { salt, hash, created: Date.now() });
  const token = crypto.randomBytes(32).toString('hex');
  tokens.set(token, user);
  res.json({ ok: true, token, user });
});

app.post('/api/login', async (req, res) => {
  if (authRateLimited(req, res)) return;
  const user = cleanName(req.body && req.body.user);
  const pass = String((req.body && req.body.pass) || '');
  const acc = accounts.get(user);
  if (!acc || !(await verifyPass(pass, acc.salt, acc.hash))) {
    return res.json({ ok: false, error: 'Wrong username or password.' });
  }
  const token = crypto.randomBytes(32).toString('hex');
  tokens.set(token, user);
  res.json({ ok: true, token, user });
});

/* ---------------- admin auth ---------------- */

function adminAuth(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({ ok: false, error: 'Admin panel is not configured. Set ADMIN_TOKEN in the server environment.' });
  }
  const t = String(req.header('x-admin-token') || req.query.token || '');
  let good = false;
  try {
    good = t.length === ADMIN_TOKEN.length &&
      crypto.timingSafeEqual(Buffer.from(t), Buffer.from(ADMIN_TOKEN));
  } catch (e) { good = false; }
  if (!good) return res.status(403).json({ ok: false, error: 'Bad admin token.' });
  next();
}

/* ---------------- admin API ---------------- */

app.get('/admin/stats', adminAuth, (req, res) => {
  let users = 0, messages = 0;
  for (const r of rooms.values()) { users += r.clients.size; messages += r.messages.length; }
  res.json({ ok: true, version: 6, uptime: Math.floor(process.uptime()), rooms: rooms.size, users, messages, mode: serverMode, accounts: accounts.size });
});

app.get('/admin/rooms', adminAuth, (req, res) => {
  const list = [...rooms.entries()].map(([code, r]) => ({
    code, users: r.clients.size, messages: r.messages.length, lastActive: r.lastActive || 0
  }));
  list.sort((a, b) => b.lastActive - a.lastActive);
  res.json({ ok: true, rooms: list });
});

app.get('/admin/room/:code', adminAuth, (req, res) => {
  const code = cleanRoom(req.params.code);
  const room = rooms.get(code);
  if (!room) return res.json({ ok: false, error: 'No such room.' });
  res.json({ ok: true, room: code, users: onlineUsers(room), messages: room.messages.slice(-100) });
});

app.get('/admin/accounts', adminAuth, (req, res) => {
  const list = [...accounts.entries()].map(([user, a]) => ({ user, created: a.created }));
  list.sort((x, y) => x.user.localeCompare(y.user));
  res.json({ ok: true, accounts: list });
});

app.post('/admin/delete-account', adminAuth, (req, res) => {
  const user = cleanName(req.body && req.body.user);
  if (!accounts.has(user)) return res.json({ ok: false, error: 'No such account.' });
  accounts.delete(user);
  for (const [t, u] of [...tokens]) if (u === user) tokens.delete(t);
  // bounce their live sessions
  for (const room of rooms.values()) {
    for (const [ws, c] of [...room.clients]) {
      if (c.user === user && c.verified) {
        try { ws.send(JSON.stringify({ type: 'auth_expired' })); } catch (e) {}
        try { ws.close(4002, 'auth_expired'); } catch (e) {}
      }
    }
  }
  res.json({ ok: true });
});

app.post('/admin/delete-message', adminAuth, (req, res) => {
  const code = cleanRoom(req.body && req.body.room);
  const id = Number(req.body && req.body.id);
  const room = rooms.get(code);
  if (!room) return res.json({ ok: false, error: 'No such room.' });
  const i = room.messages.findIndex(m => m.id === id);
  if (i < 0) return res.json({ ok: false, error: 'Message not found.' });
  room.messages.splice(i, 1);
  broadcastToRoom(room, { type: 'message_deleted', id });
  res.json({ ok: true });
});

app.post('/admin/clear-room', adminAuth, (req, res) => {
  const code = cleanRoom(req.body && req.body.room);
  const room = rooms.get(code);
  if (!room) return res.json({ ok: false, error: 'No such room.' });
  room.messages.length = 0;
  broadcastToRoom(room, { type: 'room_cleared' });
  res.json({ ok: true });
});

app.post('/admin/kick', adminAuth, (req, res) => {
  const code = cleanRoom(req.body && req.body.room);
  const user = cleanName(req.body && req.body.user);
  const room = rooms.get(code);
  if (!room) return res.json({ ok: false, error: 'No such room.' });
  let n = 0;
  for (const [ws, c] of [...room.clients]) {
    if (c.user === user) {
      try { ws.send(JSON.stringify({ type: 'kicked', reason: 'Removed by an admin.' })); } catch (e) {}
      try { ws.close(4000, 'kicked'); } catch (e) {}
      n++;
    }
  }
  broadcastToRoom(room, { type: 'user_kicked', user });
  res.json({ ok: true, kicked: n });
});

app.post('/admin/announce', adminAuth, (req, res) => {
  const text = String((req.body && req.body.text) || '').trim().slice(0, 500);
  if (!text) return res.json({ ok: false, error: 'Empty announcement.' });
  const m = { id: nextId++, user: 'ZSATC Admin', phone: '', kind: 'text', text, ts: Date.now(), reactions: {}, system: true };
  let n = 0;
  for (const room of rooms.values()) { pushMessage(room, m); n++; }
  res.json({ ok: true, rooms: n });
});

app.post('/admin/power', adminAuth, (req, res) => {
  const mode = String((req.body && req.body.mode) || '').trim();
  if (!['online', 'maintenance', 'shutdown'].includes(mode)) {
    return res.json({ ok: false, error: "Mode must be 'online', 'maintenance' or 'shutdown'." });
  }
  const message = String((req.body && req.body.message) || '').trim().slice(0, 200);
  serverMode = mode;
  modeMessage = message;
  const icon = mode === 'online' ? '🟢' : mode === 'maintenance' ? '🔧' : '🛑';
  const sysText = mode === 'online'
    ? '🟢 Server is back online.'
    : `${icon} Server ${MODE_LABEL[mode]}${message ? ': ' + message : ''}`;
  for (const room of rooms.values()) {
    pushMessage(room, { id: nextId++, user: 'ZSATC Admin', phone: '', kind: 'text', text: sysText, ts: Date.now(), reactions: {}, system: true });
    if (mode !== 'online') {
      for (const [ws] of [...room.clients]) {
        try { ws.send(JSON.stringify({ type: mode, message: modeMessage })); } catch (e) {}
        try { ws.close(4001, mode); } catch (e) {}
      }
    }
  }
  res.json({ ok: true, mode: serverMode });
});

/* ---------------- public routes ---------------- */

app.get('/health', (req, res) => {
  let users = 0, messages = 0;
  for (const r of rooms.values()) { users += r.clients.size; messages += r.messages.length; }
  res.json({ ok: true, service: 'zsatc-chat-server', version: 6, rooms: rooms.size, users, messages, admin: !!ADMIN_TOKEN, mode: serverMode });
});

app.get('/', (req, res) => {
  res.send('ZSATC Chat Server is running. Connect with the web OS Messenger app.');
});

wss.on('connection', (ws) => {
  let myRoom = null;
  let myCode = null;

  ws.on('message', (raw) => {
    let d;
    try { d = JSON.parse(raw.toString()); } catch { return; }

    if (d.type === 'hello') {
      if (serverMode !== 'online') {
        try { ws.send(JSON.stringify({ type: serverMode, message: modeMessage })); } catch (e) {}
        try { ws.close(4001, serverMode); } catch (e) {}
        return;
      }
      myCode = cleanRoom(d.room);
      myRoom = getRoom(myCode);
      // account auth: a valid token locks the username; guests can't spoof registered names
      let user = cleanName(d.user), verified = false;
      const tok = String(d.token || '');
      if (tok) {
        const tUser = tokens.get(tok);
        if (!tUser || !accounts.has(tUser)) {
          try { ws.send(JSON.stringify({ type: 'auth_expired' })); } catch (e) {}
          try { ws.close(4002, 'auth_expired'); } catch (e) {}
          return;
        }
        user = tUser;
        verified = true;
      } else if (accounts.has(user)) {
        try { ws.send(JSON.stringify({ type: 'error', error: 'That name has an account. Log in to use it.' })); } catch (e) {}
        try { ws.close(4003, 'name_taken'); } catch (e) {}
        return;
      }
      myRoom.clients.set(ws, {
        user, phone: cleanPhone(d.phone), avatar: cleanAvatar(d.avatar),
        verified, lastMsg: 0, lastTyping: 0
      });
      ws.send(JSON.stringify({ type: 'history', messages: myRoom.messages }));
      ws.send(JSON.stringify({ type: 'room', room: myCode }));
      broadcastToRoom(myRoom, { type: 'online', users: onlineUsers(myRoom) });
      return;
    }

    if (!myRoom || !myRoom.clients.has(ws)) return;
    const c = myRoom.clients.get(ws);

    if (d.type === 'send') {
      if (serverMode !== 'online') {
        ws.send(JSON.stringify({ type: 'error', error: `Server is ${MODE_LABEL[serverMode]}.` }));
        return;
      }
      const text = String(d.text || '').trim().slice(0, 500);
      if (!text || throttled(ws, c)) return;
      pushMessage(myRoom, { id: nextId++, user: c.user, phone: c.phone, avatar: c.avatar, verified: c.verified, kind: 'text', text, ts: Date.now(), reactions: {} });
      return;
    }

    if (d.type === 'send_image') {
      if (serverMode !== 'online') {
        ws.send(JSON.stringify({ type: 'error', error: `Server is ${MODE_LABEL[serverMode]}.` }));
        return;
      }
      const img = String(d.image || '');
      if (!img.startsWith('data:image/') || img.length > MAX_IMAGE_BYTES) {
        ws.send(JSON.stringify({ type: 'error', error: img.length > MAX_IMAGE_BYTES ? 'Image too large.' : 'Invalid image.' }));
        return;
      }
      if (throttled(ws, c)) return;
      pushMessage(myRoom, { id: nextId++, user: c.user, phone: c.phone, avatar: c.avatar, verified: c.verified, kind: 'image', image: img, ts: Date.now(), reactions: {} });
      return;
    }

    if (d.type === 'send_audio') {
      if (serverMode !== 'online') {
        ws.send(JSON.stringify({ type: 'error', error: `Server is ${MODE_LABEL[serverMode]}.` }));
        return;
      }
      const au = String(d.audio || '');
      if (!au.startsWith('data:audio/') || au.length > MAX_AUDIO_BYTES) {
        ws.send(JSON.stringify({ type: 'error', error: au.length > MAX_AUDIO_BYTES ? 'Voice message too long.' : 'Invalid audio.' }));
        return;
      }
      if (throttled(ws, c)) return;
      pushMessage(myRoom, { id: nextId++, user: c.user, phone: c.phone, avatar: c.avatar, verified: c.verified, kind: 'audio', audio: au, ts: Date.now(), reactions: {} });
      return;
    }

    if (d.type === 'react') {
      const id = Number(d.id);
      const emoji = String(d.emoji || '');
      if (!REACT_EMOJIS.includes(emoji)) return;
      const m = myRoom.messages.find(m => m.id === id);
      if (!m) return;
      if (!m.reactions) m.reactions = {};
      const list = m.reactions[emoji] || [];
      if (list.includes(c.user)) {
        m.reactions[emoji] = list.filter(u => u !== c.user);
        if (!m.reactions[emoji].length) delete m.reactions[emoji];
      } else {
        m.reactions[emoji] = [...list, c.user];
      }
      broadcastToRoom(myRoom, { type: 'reaction', id, reactions: m.reactions });
      return;
    }

    if (d.type === 'typing') {
      const now = Date.now();
      if (now - c.lastTyping < 3000) return; // relay at most every 3s per user
      c.lastTyping = now;
      broadcastToRoom(myRoom, { type: 'typing', user: c.user }, ws);
      return;
    }

    if (d.type === 'read') {
      const id = Number(d.id);
      if (!id) return;
      broadcastToRoom(myRoom, { type: 'read', user: c.user, id }, ws);
      return;
    }
  });

  ws.on('close', () => {
    if (myRoom && myRoom.clients.delete(ws)) {
      broadcastToRoom(myRoom, { type: 'online', users: onlineUsers(myRoom) });
      if (myRoom.clients.size === 0) rooms.delete(myCode);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`ZSATC chat server v6 listening on ${PORT}`));
