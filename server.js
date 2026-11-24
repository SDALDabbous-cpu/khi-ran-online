'use strict';

const path = require('node:path');
const crypto = require('node:crypto');
const http = require('node:http');
const express = require('express');
const { Server } = require('socket.io');

const PORT = Number(process.env.PORT) || 10000;
const HOST = '0.0.0.0';
const MAX_CHAT_LENGTH = 180;
const CHAT_COOLDOWN_MS = 1100;
const STATE_COOLDOWN_MS = 55;
const MAX_STATE_COORD = 5000;
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const ALLOWED_FORMS = new Set(['human', 'yuta', 'flash', 'mettaton', 'bakugo', 'jogo', 'asgore', 'omega-flowery', 'flowery']);
const ALLOWED_BOATS = new Set(['speedboat', 'yacht', 'fishing', 'racer']);

const ROOM_CONFIG = [
  { id: 'main', name: 'KHIRAN MAIN', description: 'Free roam and explore', capacity: 16 },
  { id: 'racing', name: 'RACE SERVER', description: 'Meet up for boat races', capacity: 16 },
  { id: 'chill', name: 'CHILL SERVER', description: 'Relax on the coast', capacity: 16 },
];
const roomsById = new Map(ROOM_CONFIG.map(room => [room.id, room]));

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', 1);
app.use(express.json({ limit: '12kb' }));

const publicDir = path.join(__dirname, 'public');
app.use(express.static(publicDir, { extensions: ['html'], maxAge: process.env.NODE_ENV === 'production' ? '1h' : 0 }));

const server = http.createServer(app);
const io = new Server(server, {
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 64 * 1024,
});

const sessions = new Map();
const chatHistory = new Map(ROOM_CONFIG.map(room => [room.id, []]));
const joinAttempts = new Map();

function getBearerToken(req) {
  const value = req.headers.authorization || '';
  return value.startsWith('Bearer ') ? value.slice(7) : '';
}

function getSessionUser(token) {
  const session = sessions.get(token);
  if (!session) return null;
  if (Date.now() - session.createdAt > SESSION_TTL_MS) {
    sessions.delete(token);
    return null;
  }
  return session.user;
}

function safeRoomId(value) {
  return roomsById.has(value) ? value : 'main';
}

function cleanPlayerName(value) {
  if (typeof value !== 'string') return '';
  const name = value.normalize('NFKC').replace(/[\u0000-\u001f\u007f]/g, '').trim().replace(/\s+/g, ' ');
  if (name.length < 2 || name.length > 22) return '';
  if (!/^[\p{L}\p{N}_ -]+$/u.test(name)) return '';
  return name;
}

function playerSockets(roomId) {
  return Array.from(io.sockets.sockets.values()).filter(socket => socket.data.roomId === roomId && socket.connected);
}

function directorySnapshot() {
  return ROOM_CONFIG.map(room => ({
    id: room.id,
    name: room.name,
    description: room.description,
    players: playerSockets(room.id).length,
    capacity: room.capacity,
    status: playerSockets(room.id).length >= room.capacity ? 'full' : 'online',
  }));
}

function broadcastDirectory() {
  io.emit('servers:list', { servers: directorySnapshot() });
}

function publicPlayer(socket) {
  const state = socket.data.state || {};
  return {
    id: socket.id,
    username: socket.data.user.username,
    displayName: socket.data.user.displayName,
    x: state.x || 0,
    y: state.y || 0,
    z: state.z || 0,
    yaw: state.yaw || 0,
    mode: state.mode === 'boat' ? 'boat' : 'foot',
    form: state.form || 'human',
    boat: state.boat || 'speedboat',
  };
}

function limitJoin(req) {
  const key = req.ip || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  const recent = (joinAttempts.get(key) || []).filter(time => now - time < 60000);
  if (recent.length >= 12) return false;
  recent.push(now);
  joinAttempts.set(key, recent);
  if (joinAttempts.size > 1000) {
    for (const [ip, times] of joinAttempts) {
      if (!times.some(time => now - time < 60000)) joinAttempts.delete(ip);
    }
  }
  return true;
}

app.get('/health', (_req, res) => {
  res.json({ ok: true, name: 'KHIRAN Online', rooms: directorySnapshot() });
});

app.get('/api/servers', (_req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json({ servers: directorySnapshot() });
});

app.post('/api/join', (req, res) => {
  if (!limitJoin(req)) return res.status(429).json({ error: 'Too many join attempts. Try again in a minute.' });
  const username = cleanPlayerName(req.body && req.body.username);
  if (!username) return res.status(400).json({ error: 'Use 2–22 letters, numbers, spaces, underscores or hyphens for your player name.' });
  const roomId = safeRoomId(req.body && req.body.room);
  const room = roomsById.get(roomId);
  if (playerSockets(roomId).length >= room.capacity) return res.status(409).json({ error: 'That server is full. Choose another server.' });

  const token = crypto.randomBytes(32).toString('hex');
  const user = { id: crypto.randomUUID(), username, displayName: username, role: 'player' };
  sessions.set(token, { user, createdAt: Date.now() });
  res.set('Cache-Control', 'no-store');
  res.json({ token, user, room: roomId });
});

app.get('/api/me', (req, res) => {
  const user = getSessionUser(getBearerToken(req));
  if (!user) return res.status(401).json({ error: 'Your session expired. Join again.' });
  res.set('Cache-Control', 'no-store');
  res.json({ user });
});

io.use((socket, next) => {
  const token = socket.handshake.auth && socket.handshake.auth.token;
  const user = getSessionUser(typeof token === 'string' ? token : '');
  if (!user) return next(new Error('Your session expired. Join KHIRAN again.'));
  const roomId = safeRoomId(socket.handshake.auth && socket.handshake.auth.room);
  const room = roomsById.get(roomId);
  if (playerSockets(roomId).length >= room.capacity) return next(new Error('SERVER_FULL'));
  socket.data.user = user;
  socket.data.roomId = roomId;
  socket.data.lastStateAt = 0;
  socket.data.lastChatAt = 0;
  next();
});

io.on('connection', socket => {
  const roomId = socket.data.roomId;
  const room = roomsById.get(roomId);
  const currentPlayers = playerSockets(roomId).filter(player => player.id !== socket.id);

  socket.join(roomId);
  socket.emit('players:init', currentPlayers.map(publicPlayer));
  socket.emit('room:info', { id: room.id, name: room.name, count: currentPlayers.length + 1, capacity: room.capacity });
  socket.emit('chat:history', chatHistory.get(roomId) || []);
  socket.to(roomId).emit('player:joined', {
    id: socket.id,
    username: socket.data.user.username,
    displayName: socket.data.user.displayName,
  });
  socket.to(roomId).emit('system:message', { text: socket.data.user.displayName + ' joined ' + room.name + '.' });
  io.to(roomId).emit('presence:update', { count: currentPlayers.length + 1 });
  broadcastDirectory();

  socket.on('player:state', raw => {
    const now = Date.now();
    if (now - socket.data.lastStateAt < STATE_COOLDOWN_MS) return;
    socket.data.lastStateAt = now;
    if (!raw || typeof raw !== 'object') return;
    const number = (value, min, max) => {
      const n = Number(value);
      return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : 0;
    };
    const state = {
      x: number(raw.x, -MAX_STATE_COORD, MAX_STATE_COORD),
      y: number(raw.y, -20, 150),
      z: number(raw.z, -MAX_STATE_COORD, MAX_STATE_COORD),
      yaw: number(raw.yaw, -1000, 1000),
      mode: raw.mode === 'boat' ? 'boat' : 'foot',
      form: ALLOWED_FORMS.has(raw.form) ? raw.form : 'human',
      boat: ALLOWED_BOATS.has(raw.boat) ? raw.boat : 'speedboat',
    };
    socket.data.state = state;
    socket.to(roomId).emit('player:update', publicPlayer(socket));
  });

  socket.on('chat:send', raw => {
    const now = Date.now();
    if (now - socket.data.lastChatAt < CHAT_COOLDOWN_MS) return;
    socket.data.lastChatAt = now;
    const text = raw && typeof raw.text === 'string'
      ? raw.text.replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, MAX_CHAT_LENGTH)
      : '';
    if (!text) return;
    if (text.startsWith('/')) {
      socket.emit('system:message', { text: 'Server commands are disabled in this public starter server. Use regular chat instead.' });
      return;
    }
    const message = {
      id: socket.id,
      username: socket.data.user.username,
      displayName: socket.data.user.displayName,
      text,
      at: Date.now(),
    };
    const history = chatHistory.get(roomId);
    history.push(message);
    while (history.length > 40) history.shift();
    io.to(roomId).emit('chat:message', message);
  });

  socket.on('disconnect', () => {
    io.to(roomId).except(socket.id).emit('player:left', { id: socket.id, username: socket.data.user.username });
    io.to(roomId).emit('presence:update', { count: playerSockets(roomId).filter(player => player.id !== socket.id).length });
    broadcastDirectory();
  });
});

server.listen(PORT, HOST, () => {
  console.log('KHIRAN Online listening on http://' + HOST + ':' + PORT);
});

function shutdown(signal) {
  console.log(signal + ': closing KHIRAN Online');
  io.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
