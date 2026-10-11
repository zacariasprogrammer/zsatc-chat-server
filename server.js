/* ============================================================
   ZSATC Chat Server v2 — private rooms + profiles
   Node.js + Express + WebSockets. Deploy as a Render Web Service.

   What's new in v2:
   - Invite-only rooms: clients join with a room code. There is
     no public lobby — nobody can stumble into your chat.
   - Profiles: each client sends a username + phone number.
   - Per-room history (last 200), per-room presence.

   RENDER SETUP:
   1) Put this file + package.json in a GitHub repo.
   2) Render dashboard → New → Web Service (or Blueprint) → select the repo.
      Build command: npm install
      Start command:  node server.js
      Plan: Free.
   ============================================================ */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const MAX_MESSAGES = 200;
const rooms = new Map(); // roomCode -> { messages: [], clients: Map(ws -> {user, phone, lastMsg}) }
let nextId = 1;

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

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { messages: [], clients: new Map() });
  return rooms.get(code);
}

function onlineUsers(room) {
  const seen = new Map();
  for (const c of room.clients.values()) {
    if (!seen.has(c.user)) seen.set(c.user, { user: c.user, phone: c.phone });
  }
  return [...seen.values()];
}

function broadcastToRoom(room, obj, except) {
  const data = JSON.stringify(obj);
  for (const [ws] of room.clients) {
    if (ws !== except && ws.readyState === 1) ws.send(data);
  }
}

app.get('/health', (req, res) => {
  let users = 0, messages = 0;
  for (const r of rooms.values()) { users += r.clients.size; messages += r.messages.length; }
  res.json({ ok: true, service: 'zsatc-chat-server', version: 2, rooms: rooms.size, users, messages });
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
      myCode = cleanRoom(d.room);
      myRoom = getRoom(myCode);
      myRoom.clients.set(ws, {
        user: cleanName(d.user),
        phone: cleanPhone(d.phone),
        lastMsg: 0,
      });
      ws.send(JSON.stringify({ type: 'history', messages: myRoom.messages }));
      ws.send(JSON.stringify({ type: 'room', room: myCode }));
      broadcastToRoom(myRoom, { type: 'online', users: onlineUsers(myRoom) });
      return;
    }

    if (!myRoom || !myRoom.clients.has(ws)) return;
    const c = myRoom.clients.get(ws);

    if (d.type === 'send') {
      const text = String(d.text || '').trim().slice(0, 500);
      if (!text) return;
      const now = Date.now();
      if (now - c.lastMsg < 2000) {
        ws.send(JSON.stringify({ type: 'error', error: 'Slow down a little.' }));
        return;
      }
      c.lastMsg = now;
      const m = { id: nextId++, user: c.user, phone: c.phone, text, ts: now };
      myRoom.messages.push(m);
      if (myRoom.messages.length > MAX_MESSAGES) myRoom.messages.shift();
      broadcastToRoom(myRoom, { type: 'message', message: m });
    }
  });

  ws.on('close', () => {
    if (myRoom && myRoom.clients.delete(ws)) {
      broadcastToRoom(myRoom, { type: 'online', users: onlineUsers(myRoom) });
      // drop empty rooms so /health stays honest
      if (myRoom.clients.size === 0) rooms.delete(myCode);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`ZSATC chat server v2 listening on ${PORT}`));
