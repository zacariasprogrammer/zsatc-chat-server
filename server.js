/* ============================================================
   ZSATC Chat Server v4 — private rooms + profiles + images
   + reactions + typing indicators + read receipts
   Node.js + Express + WebSockets. Deploy as a Render Web Service.

   Protocol:
   - hello { user, phone, room } -> history, room, online
   - send { text } -> message { kind:'text' }
   - send_image { image } -> message { kind:'image' }
   - react { id, emoji } -> reaction { id, reactions }
   - typing {} -> typing { user } (relayed, throttled)
   - read { id } -> read { user, id } (relayed)
   ============================================================ */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const MAX_MESSAGES = 200;
const MAX_IMAGE_BYTES = 700 * 1024; // 700KB cap per image (client downscales first)
const REACT_EMOJIS = ['❤️', '👍', '😂', '😮', '😢', '🔥', '👏'];
const rooms = new Map(); // roomCode -> { messages: [], clients: Map(ws -> {user, phone, lastMsg, lastTyping}) }
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
  if (room.messages.length > MAX_MESSAGES) room.messages.shift();
  broadcastToRoom(room, { type: 'message', message: m });
}

app.get('/health', (req, res) => {
  let users = 0, messages = 0;
  for (const r of rooms.values()) { users += r.clients.size; messages += r.messages.length; }
  res.json({ ok: true, service: 'zsatc-chat-server', version: 4, rooms: rooms.size, users, messages });
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
      myRoom.clients.set(ws, { user: cleanName(d.user), phone: cleanPhone(d.phone), lastMsg: 0, lastTyping: 0 });
      ws.send(JSON.stringify({ type: 'history', messages: myRoom.messages }));
      ws.send(JSON.stringify({ type: 'room', room: myCode }));
      broadcastToRoom(myRoom, { type: 'online', users: onlineUsers(myRoom) });
      return;
    }

    if (!myRoom || !myRoom.clients.has(ws)) return;
    const c = myRoom.clients.get(ws);

    if (d.type === 'send') {
      const text = String(d.text || '').trim().slice(0, 500);
      if (!text || throttled(ws, c)) return;
      pushMessage(myRoom, { id: nextId++, user: c.user, phone: c.phone, kind: 'text', text, ts: Date.now(), reactions: {} });
      return;
    }

    if (d.type === 'send_image') {
      const img = String(d.image || '');
      if (!img.startsWith('data:image/') || img.length > MAX_IMAGE_BYTES) {
        ws.send(JSON.stringify({ type: 'error', error: img.length > MAX_IMAGE_BYTES ? 'Image too large.' : 'Invalid image.' }));
        return;
      }
      if (throttled(ws, c)) return;
      pushMessage(myRoom, { id: nextId++, user: c.user, phone: c.phone, kind: 'image', image: img, ts: Date.now(), reactions: {} });
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
server.listen(PORT, () => console.log(`ZSATC chat server v4 listening on ${PORT}`));
