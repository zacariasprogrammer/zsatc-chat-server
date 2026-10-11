/* ============================================================
   ZSATC Chat Server — real-time multi-user chat for the web OS
   Node.js + Express + WebSockets. Deploy as a Render Web Service.

   RENDER SETUP:
   1) Put this file + package.json in a GitHub repo.
   2) Render dashboard → New → Web Service → select the repo.
      Build command: npm install
      Start command:  node server.js
      Plan: Free.
   3) Copy your service URL (https://YOUR-NAME.onrender.com),
      then in the web OS index.html set CHAT_URL to
      wss://YOUR-NAME.onrender.com  (https → wss).

   Notes: messages live in memory (last 200). On Render's free
   plan the service sleeps when idle and cold-starts (~30-60s) —
   the web OS client auto-reconnects, so this is handled.
   ============================================================ */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const MAX_MESSAGES = 200;
const messages = []; // { id, user, text, ts }
let nextId = 1;
const clients = new Map(); // ws -> { user, lastMsg }

function cleanName(raw) {
  return String(raw || 'guest')
    .replace(/[^A-Za-z0-9 _-]/g, '')
    .trim()
    .slice(0, 24) || 'guest';
}

function onlineUsers() {
  return [...new Set([...clients.values()].map(c => c.user))];
}

function broadcast(obj, except) {
  const data = JSON.stringify(obj);
  for (const [ws] of clients) {
    if (ws !== except && ws.readyState === 1) ws.send(data);
  }
}

app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'zsatc-chat-server', users: clients.size, messages: messages.length });
});

app.get('/', (req, res) => {
  res.send('ZSATC Chat Server is running. Connect with the web OS Messenger app.');
});

wss.on('connection', (ws) => {
  clients.set(ws, { user: 'guest', lastMsg: 0 });

  // catch the newcomer up
  ws.send(JSON.stringify({ type: 'history', messages }));
  ws.send(JSON.stringify({ type: 'online', users: onlineUsers() }));

  ws.on('message', (raw) => {
    let d;
    try { d = JSON.parse(raw.toString()); } catch { return; }
    const c = clients.get(ws);
    if (!c) return;

    if (d.type === 'hello') {
      c.user = cleanName(d.user);
      broadcast({ type: 'online', users: onlineUsers() });
      return;
    }

    if (d.type === 'send') {
      const text = String(d.text || '').trim().slice(0, 500);
      if (!text) return;
      const now = Date.now();
      if (now - c.lastMsg < 2000) {
        ws.send(JSON.stringify({ type: 'error', error: 'Slow down a little.' }));
        return;
      }
      c.lastMsg = now;
      const m = { id: nextId++, user: c.user, text, ts: now };
      messages.push(m);
      if (messages.length > MAX_MESSAGES) messages.shift();
      broadcast({ type: 'message', message: m });
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    broadcast({ type: 'online', users: onlineUsers() });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`ZSATC chat server listening on ${PORT}`));
