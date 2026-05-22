'use strict';

// ── Transport Layer (HTTP polling primary, WebSocket optional upgrade) ──
// Railway and many cloud hosts don't support WebSocket over their proxy.
// Polling is reliable everywhere, so we use it as the PRIMARY transport,
// and optionally upgrade to WebSocket in the background if available.
class SignalingClient extends EventTarget {
  constructor() {
    super();
    this.connected = false;
    this._intentionalClose = false;
    this._clientId = null;
    this._ws = null;
    this._polling = false;
    this._pollActive = false;
    this._reconnectDelay = 1000;
    this._maxReconnectDelay = 8000;
    this._reconnectAttempts = 0;
    this._queue = [];
    this._heartbeatInterval = null;
    this._wsUpgradeDone = false;
  }

  connect() {
    this._intentionalClose = false;
    this._reconnectAttempts = 0;
    // Start with polling immediately (works on Railway, Render, etc.)
    this._startPolling();
    // Try WebSocket upgrade in background (faster if supported)
    this._tryWebSocketUpgrade();
  }

  // ── WebSocket Upgrade (background, non-blocking) ───────
  _tryWebSocketUpgrade() {
    if (this._wsUpgradeDone || this._intentionalClose) return;

    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    // Try /ws path first, then root
    const paths = ['/ws', '/'];
    let pathIndex = 0;

    const tryNext = () => {
      if (pathIndex >= paths.length) {
        console.log('[Transport] WebSocket not available, staying on polling');
        this._wsUpgradeDone = true; // Don't try again
        return;
      }
      const url = `${proto}://${location.host}${paths[pathIndex]}`;
      console.log('[Transport] Trying WebSocket upgrade:', url);

      let ws;
      try {
        ws = new WebSocket(url);
      } catch(e) {
        pathIndex++;
        tryNext();
        return;
      }

      let opened = false;

      // Short timeout — if WS doesn't connect in 3s, move on
      const timeout = setTimeout(() => {
        if (!opened) {
          ws.close();
          pathIndex++;
          tryNext();
        }
      }, 3000);

      ws.onopen = () => {
        opened = true;
        clearTimeout(timeout);
        console.log('[Transport] WebSocket upgrade successful on', paths[pathIndex]);
        this._ws = ws;
        this._polling = false; // Stop polling, use WS
        this._stopPollLoop();
        this._startHeartbeat();
        this._wsUpgradeDone = true;

        // Don't re-dispatch 'open' — we're already connected via polling
        // Flush any queued messages via WS
        for (const msg of this._queue) this._wsSend(msg);
        this._queue = [];
      };

      ws.onmessage = (e) => {
        let msg; try { msg = JSON.parse(e.data); } catch { return; }
        if (msg.type === 'connected') return;
        if (msg.type === 'pong') return;
        this.dispatchEvent(new CustomEvent('message', { detail: msg }));
      };

      ws.onclose = () => {
        clearTimeout(timeout);
        this._stopHeartbeat();
        if (!opened) return; // handled by timeout
        // WS was connected but dropped — fall back to polling
        console.log('[Transport] WebSocket disconnected, falling back to polling');
        this._ws = null;
        if (!this._intentionalClose && !this._polling) {
          this._startPolling();
        }
      };

      ws.onerror = () => {
        clearTimeout(timeout);
      };
    };

    tryNext();
  }

  // ── Heartbeat ──────────────────────────────────────────
  _startHeartbeat() {
    this._stopHeartbeat();
    this._heartbeatInterval = setInterval(() => {
      if (this._ws && this._ws.readyState === WebSocket.OPEN) {
        this._wsSend({ type: 'ping' });
      }
    }, 15000);
  }

  _stopHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }
  }

  // ── Polling (PRIMARY transport) ────────────────────────
  _stopPollLoop() {
    this._pollActive = false;
  }

  async _startPolling() {
    if (this._polling) return;
    this._polling = true;
    this._ws = null;

    try {
      const r = await fetch('/api/poll/register', { method: 'POST' });
      if (!r.ok) throw new Error('Register failed: ' + r.status);
      const { clientId } = await r.json();
      this._clientId = clientId;
      this.connected = true;
      this._pollActive = true;
      this._reconnectAttempts = 0;
      this.dispatchEvent(new Event('open'));
      this._pollLoop();

      // Flush queued messages
      for (const msg of this._queue) await this._pollSend(msg);
      this._queue = [];
    } catch(e) {
      console.error('[Transport] Polling setup failed:', e);
      this._polling = false;
      this._reconnectAttempts++;
      const delay = Math.min(1000 * this._reconnectAttempts, 5000);
      setTimeout(() => this._startPolling(), delay);
    }
  }

  async _pollLoop() {
    while (this._pollActive && !this._intentionalClose) {
      try {
        const r = await fetch(`/api/poll/recv?clientId=${this._clientId}`);
        if (!r.ok) throw new Error('Poll failed: ' + r.status);
        const { messages } = await r.json();
        for (const msg of messages) {
          if (msg.type === 'pong') continue;
          this.dispatchEvent(new CustomEvent('message', { detail: msg }));
        }
      } catch(e) {
        if (!this._intentionalClose) await new Promise(r => setTimeout(r, 1500));
      }
    }
  }

  async _pollSend(obj) {
    try {
      const r = await fetch('/api/poll/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientId: this._clientId, message: obj })
      });
      if (!r.ok) throw new Error('Send failed: ' + r.status);
    } catch(e) {
      console.error('[Poll] Send failed:', e);
    }
  }

  _wsSend(obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  }

  send(obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._wsSend(obj);
    } else if (this._polling && this._clientId && this._pollActive) {
      this._pollSend(obj);
    } else {
      this._queue.push(obj);
    }
  }

  close() {
    this._intentionalClose = true;
    this._stopHeartbeat();
    this._stopPollLoop();
    if (this._ws) this._ws.close();
  }
}

// ── WebRTC ─────────────────────────────────────────────────
const ICE_SERVERS = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:global.stun.twilio.com:3478' },
    // Free TURN servers for better connectivity through NAT/firewalls
    {
      urls: 'turn:openrelay.metered.ca:80',
      username: 'openrelayproject',
      credential: 'openrelayproject'
    },
    {
      urls: 'turn:openrelay.metered.ca:443',
      username: 'openrelayproject',
      credential: 'openrelayproject'
    }
  ]
};

class PeerCamera extends EventTarget {
  constructor(signaling, viewerId) {
    super();
    this.signaling = signaling;
    this.viewerId = viewerId;
    this.pc = new RTCPeerConnection(ICE_SERVERS);
    this.pc.onicecandidate = (e) => {
      if (e.candidate) this.signaling.send({ type: 'ice-candidate', candidate: e.candidate, targetId: this.viewerId });
    };
    this.pc.onconnectionstatechange = () => {
      this.dispatchEvent(new CustomEvent('statechange', { detail: this.pc.connectionState }));
    };
    this.pc.oniceconnectionstatechange = () => {
      if (this.pc.iceConnectionState === 'failed') this.pc.restartIce();
    };
  }
  async addStream(stream) { stream.getTracks().forEach(t => this.pc.addTrack(t, stream)); }
  async createOffer() {
    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.signaling.send({ type: 'offer', sdp: offer, targetId: this.viewerId });
  }
  async handleAnswer(sdp) { await this.pc.setRemoteDescription(new RTCSessionDescription(sdp)); }
  async handleIce(c) { try { await this.pc.addIceCandidate(new RTCIceCandidate(c)); } catch {} }
  close() { this.pc.close(); }
}

class PeerViewer extends EventTarget {
  constructor(signaling) {
    super();
    this.signaling = signaling;
    this.pc = new RTCPeerConnection(ICE_SERVERS);
    this.pc.onicecandidate = (e) => {
      if (e.candidate) this.signaling.send({ type: 'ice-candidate', candidate: e.candidate });
    };
    this.pc.ontrack = (e) => { this.dispatchEvent(new CustomEvent('stream', { detail: e.streams[0] })); };
    this.pc.onconnectionstatechange = () => {
      this.dispatchEvent(new CustomEvent('statechange', { detail: this.pc.connectionState }));
    };
    this.pc.oniceconnectionstatechange = () => {
      if (this.pc.iceConnectionState === 'failed') this.pc.restartIce();
    };
  }
  async handleOffer(sdp) {
    await this.pc.setRemoteDescription(new RTCSessionDescription(sdp));
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);
    this.signaling.send({ type: 'answer', sdp: answer });
  }
  async handleIce(c) { try { await this.pc.addIceCandidate(new RTCIceCandidate(c)); } catch {} }
  close() { this.pc.close(); }
}

// ── Motion Detector ────────────────────────────────────────
class MotionDetector extends EventTarget {
  constructor(options = {}) {
    super();
    this.threshold = options.threshold || 25;
    this.sensitivity = options.sensitivity || 0.015;
    this.cooldown = options.cooldown || 3000;
    this._canvas = document.createElement('canvas');
    this._ctx = this._canvas.getContext('2d', { willReadFrequently: true });
    this._prevData = null;
    this._lastEvent = 0;
    this._active = false;
  }
  start(videoEl) { this._video = videoEl; this._active = true; this._loop(); }
  stop() { this._active = false; }
  setSensitivity(v) { this.sensitivity = v; }
  _loop() {
    if (!this._active) return;
    this._analyze();
    setTimeout(() => { if (this._active) requestAnimationFrame(() => this._loop()); }, 100);
  }
  _analyze() {
    const v = this._video;
    if (!v || v.readyState < 2) return;
    const w = v.videoWidth || 320, h = v.videoHeight || 240;
    if (this._canvas.width !== w) this._canvas.width = w;
    if (this._canvas.height !== h) this._canvas.height = h;
    this._ctx.drawImage(v, 0, 0, w, h);
    const data = this._ctx.getImageData(0, 0, w, h).data;
    if (!this._prevData) { this._prevData = new Uint8ClampedArray(data); return; }
    let diff = 0;
    for (let i = 0; i < data.length; i += 4) {
      if ((Math.abs(data[i]-this._prevData[i]) + Math.abs(data[i+1]-this._prevData[i+1]) + Math.abs(data[i+2]-this._prevData[i+2])) / 3 > this.threshold) diff++;
    }
    this._prevData = new Uint8ClampedArray(data);
    const fraction = diff / (w * h);
    const now = Date.now();
    if (fraction > this.sensitivity && now - this._lastEvent > this.cooldown) {
      this._lastEvent = now;
      const snapshot = this._canvas.toDataURL('image/jpeg', 0.6);
      this.dispatchEvent(new CustomEvent('motion', { detail: { fraction: Math.round(fraction * 1000) / 10, snapshot } }));
    }
  }
}

class NotificationManager {
  constructor() { this.permission = 'Notification' in window ? Notification.permission : 'denied'; }
  async requestPermission() {
    const r = await Notification.requestPermission();
    this.permission = r; return r === 'granted';
  }
  notify(title, body) {
    if (this.permission !== 'granted') return;
    const n = new Notification(title, { body, tag: 'motion', renotify: true });
    setTimeout(() => n.close(), 6000);
  }
}

function generateQRSVG(text) {
  return `https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=${encodeURIComponent(text)}&bgcolor=0d1117&color=00e5ff&margin=10`;
}

window.SecureCam = { SignalingClient, PeerCamera, PeerViewer, MotionDetector, NotificationManager, generateQRSVG };
