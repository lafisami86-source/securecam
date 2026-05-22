const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// Try both approaches for maximum Railway compatibility
let wss;
try {
  wss = new WebSocket.Server({ server });
  console.log('[WS] Using attached server mode');
} catch(e) {
  wss = new WebSocket.Server({ noServer: true });
  server.on('upgrade', (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  });
  console.log('[WS] Using noServer mode');
}

app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// ── State ──────────────────────────────────────────────────
const rooms = new Map();
const clientMeta = new WeakMap();
// Polling fallback: queue messages per client id
const pollQueues = new Map(); // clientId → []
const pollClients = new Map(); // clientId → { role, roomCode, res, timer }

function generateCode() { return crypto.randomBytes(3).toString('hex').toUpperCase(); }

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function sendToClient(clientId, obj) {
  // Try WebSocket first
  const q = pollQueues.get(clientId);
  if (q) {
    q.push(obj);
    // If client is waiting (long poll), flush immediately
    const pc = pollClients.get(clientId);
    if (pc && pc.res) {
      clearTimeout(pc.timer);
      pc.res.json({ messages: pollQueues.get(clientId) || [] });
      pollQueues.set(clientId, []);
      pc.res = null;
    }
  }
}

function getOrCreateRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, { camera: null, viewers: new Set(), wsCamera: null, events: [], created: new Date() });
  }
  return rooms.get(code);
}

function broadcastToViewers(room, obj) {
  // WS viewers
  room.viewers.forEach(id => sendToClient(id, obj));
  // Legacy WS
  if (room.wsViewers) room.wsViewers.forEach(ws => send(ws, obj));
}

function handleMessage(msg, clientId, wsClient) {
  const room = clientId ? rooms.get(pollClients.get(clientId)?.roomCode) : null;

  if (msg.type === 'join-camera') {
    const code = msg.code || generateCode();
    const r = getOrCreateRoom(code);
    if (clientId) {
      r.camera = clientId;
      pollClients.set(clientId, { role: 'camera', roomCode: code, res: null, timer: null });
      sendToClient(clientId, { type: 'joined', role: 'camera', code });
      // Notify viewers
      r.viewers.forEach(vid => sendToClient(vid, { type: 'camera-ready' }));
    }
    if (wsClient) {
      r.wsCamera = wsClient;
      clientMeta.set(wsClient, { role: 'camera', roomCode: code });
      send(wsClient, { type: 'joined', role: 'camera', code });
    }
    console.log(`[${code}] Camera joined`);
    return code;
  }

  if (msg.type === 'join-viewer') {
    const code = msg.code?.toUpperCase();
    if (!code || !rooms.has(code)) {
      if (clientId) sendToClient(clientId, { type: 'error', message: 'Invalid pairing code' });
      if (wsClient) send(wsClient, { type: 'error', message: 'Invalid pairing code' });
      return;
    }
    const r = rooms.get(code);
    if (clientId) {
      r.viewers.add(clientId);
      pollClients.set(clientId, { role: 'viewer', roomCode: code, res: null, timer: null });
      sendToClient(clientId, { type: 'joined', role: 'viewer', code, cameraOnline: !!(r.camera || r.wsCamera) });
      sendToClient(clientId, { type: 'events-history', events: r.events.slice(-20) });
      if (r.camera) sendToClient(r.camera, { type: 'viewer-joined', id: clientId });
      if (r.wsCamera) send(r.wsCamera, { type: 'viewer-joined', id: clientId });
    }
    if (wsClient) {
      if (!r.wsViewers) r.wsViewers = new Set();
      r.wsViewers.add(wsClient);
      clientMeta.set(wsClient, { role: 'viewer', roomCode: code, id: crypto.randomBytes(2).toString('hex') });
      send(wsClient, { type: 'joined', role: 'viewer', code, cameraOnline: !!(r.camera || r.wsCamera) });
    }
    console.log(`[${code}] Viewer joined`);
    return;
  }

  if (msg.type === 'offer') {
    const r = clientId ? rooms.get(pollClients.get(clientId)?.roomCode) :
                         wsClient ? rooms.get(clientMeta.get(wsClient)?.roomCode) : null;
    if (!r) return;
    if (msg.targetId) sendToClient(msg.targetId, { type: 'offer', sdp: msg.sdp, fromId: 'camera' });
    if (r.wsViewers) r.wsViewers.forEach(ws => send(ws, { type: 'offer', sdp: msg.sdp, fromId: 'camera' }));
  }

  if (msg.type === 'answer') {
    const r = clientId ? rooms.get(pollClients.get(clientId)?.roomCode) :
                         wsClient ? rooms.get(clientMeta.get(wsClient)?.roomCode) : null;
    if (!r) return;
    const fromId = clientId || (clientMeta.get(wsClient)?.id);
    if (r.camera) sendToClient(r.camera, { type: 'answer', sdp: msg.sdp, fromId });
    if (r.wsCamera) send(r.wsCamera, { type: 'answer', sdp: msg.sdp, fromId });
  }

  if (msg.type === 'ice-candidate') {
    const r = clientId ? rooms.get(pollClients.get(clientId)?.roomCode) :
                         wsClient ? rooms.get(clientMeta.get(wsClient)?.roomCode) : null;
    if (!r) return;
    const role = clientId ? pollClients.get(clientId)?.role : clientMeta.get(wsClient)?.role;
    if (role === 'camera') {
      if (msg.targetId) sendToClient(msg.targetId, { type: 'ice-candidate', candidate: msg.candidate, fromId: 'camera' });
      if (r.wsViewers) r.wsViewers.forEach(ws => send(ws, { type: 'ice-candidate', candidate: msg.candidate, fromId: 'camera' }));
    } else {
      const fromId = clientId || clientMeta.get(wsClient)?.id;
      if (r.camera) sendToClient(r.camera, { type: 'ice-candidate', candidate: msg.candidate, fromId });
      if (r.wsCamera) send(r.wsCamera, { type: 'ice-candidate', candidate: msg.candidate, fromId });
    }
  }

  if (msg.type === 'motion-event') {
    const r = clientId ? rooms.get(pollClients.get(clientId)?.roomCode) :
                         wsClient ? rooms.get(clientMeta.get(wsClient)?.roomCode) : null;
    if (!r) return;
    const event = { id: crypto.randomBytes(4).toString('hex'), timestamp: new Date().toISOString(), snapshot: msg.snapshot || null, level: msg.level || 'medium' };
    r.events.push(event);
    if (r.events.length > 100) r.events.shift();
    r.viewers.forEach(vid => sendToClient(vid, { type: 'motion-alert', event }));
    if (r.wsViewers) r.wsViewers.forEach(ws => send(ws, { type: 'motion-alert', event }));
  }

  if (msg.type === 'ping') {
    if (clientId) sendToClient(clientId, { type: 'pong' });
    if (wsClient) send(wsClient, { type: 'pong' });
  }
}

// ── WebSocket (if supported) ───────────────────────────────
wss.on('connection', (ws, req) => {
  console.log('[WS] Client connected');
  send(ws, { type: 'connected' });
  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    handleMessage(msg, null, ws);
  });
  ws.on('close', () => {
    const meta = clientMeta.get(ws);
    if (!meta) return;
    const r = rooms.get(meta.roomCode);
    if (!r) return;
    if (meta.role === 'camera') { r.wsCamera = null; if (r.wsViewers) r.wsViewers.forEach(v => send(v, { type: 'camera-disconnected' })); broadcastToViewers(r, { type: 'camera-disconnected' }); }
    else if (r.wsViewers) r.wsViewers.delete(ws);
  });
});

setInterval(() => { wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.ping(); }); }, 20000);

// ── HTTP Polling Fallback ──────────────────────────────────
app.post('/api/poll/register', (req, res) => {
  const clientId = crypto.randomBytes(8).toString('hex');
  pollQueues.set(clientId, []);
  res.json({ clientId });
});

app.post('/api/poll/send', (req, res) => {
  const { clientId, message } = req.body;
  if (!clientId || !pollQueues.has(clientId)) return res.status(400).json({ error: 'Unknown client' });
  handleMessage(message, clientId, null);
  res.json({ ok: true });
});

app.get('/api/poll/recv', (req, res) => {
  const { clientId } = req.query;
  if (!clientId || !pollQueues.has(clientId)) return res.status(400).json({ error: 'Unknown client' });

  const msgs = pollQueues.get(clientId);
  if (msgs.length > 0) {
    pollQueues.set(clientId, []);
    return res.json({ messages: msgs });
  }

  // Long poll — wait up to 25s
  res.setTimeout(26000);
  const pc = pollClients.get(clientId) || {};
  pc.res = res;
  pc.timer = setTimeout(() => {
    pc.res = null;
    if (!res.headersSent) res.json({ messages: [] });
  }, 25000);
  pollClients.set(clientId, pc);
});

// ── REST ──────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size, wsClients: wss.clients.size }));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, '../public/index.html')));
app.get('/camera', (req, res) => res.sendFile(path.join(__dirname, '../public/camera.html')));
app.get('/viewer', (req, res) => res.sendFile(path.join(__dirname, '../public/viewer.html')));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`SecureCam on port ${PORT}`));
