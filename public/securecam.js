'use strict';

// ── Transport Layer (WebSocket with HTTP polling fallback) ──
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
  }

  connect() {
    this._intentionalClose = false;
    this._reconnectAttempts = 0;
    this._tryWebSocket();
  }

  _tryWebSocket() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    // Try /ws path first (Railway/proxy friendly), fall back to root
    const url = `${proto}://${location.host}/ws`;
    console.log('[Transport] Trying WebSocket:', url);

    let ws;
    try {
      ws = new WebSocket(url);
    } catch(e) {
      console.warn('[Transport] WebSocket constructor failed, trying root path:', e);
      this._tryWebSocketRoot();
      return;
    }

    let opened = false;

    // 10s timeout — Railway cold starts need more time
    const timeout = setTimeout(() => {
      if (!opened) {
        console.log('[Transport] WebSocket timeout on /ws, trying root path');
        ws.close();
        this._tryWebSocketRoot();
      }
    }, 10000);

    ws.onopen = () => {
      opened = true;
      clearTimeout(timeout);
      console.log('[Transport] WebSocket connected on /ws');
      this._ws = ws;
      this._polling = false;
      this._reconnectAttempts = 0;
      this._reconnectDelay = 1000;
      this.connected = true;
      this._stopPolling();
      this._startHeartbeat();
      this.dispatchEvent(new Event('open'));
      // Flush queued messages
      for (const msg of this._queue) this._wsSend(msg);
      this._queue = [];
    };

    ws.onmessage = (e) => {
      let msg; try { msg = JSON.parse(e.data); } catch { return; }
      if (msg.type === 'connected') return; // handshake ack
      if (msg.type === 'pong') return; // heartbeat ack
      this.dispatchEvent(new CustomEvent('message', { detail: msg }));
    };

    ws.onclose = () => {
      clearTimeout(timeout);
      this._stopHeartbeat();
      if (!opened) return; // already handled by timeout
      this._ws = null;
      this.connected = false;
      this.dispatchEvent(new Event('close'));
      if (!this._intentionalClose) {
        // Try WebSocket first, then fallback to polling if repeated failures
        this._reconnectAttempts++;
        if (this._reconnectAttempts >= 3) {
          console.log('[Transport] WebSocket failed 3+ times, falling back to polling');
          this._startPolling();
        } else {
          const delay = Math.min(this._reconnectDelay * Math.pow(1.5, this._reconnectAttempts - 1), this._maxReconnectDelay);
          console.log(`[Transport] Reconnecting WebSocket in ${Math.round(delay)}ms (attempt ${this._reconnectAttempts})`);
          setTimeout(() => this._tryWebSocket(), delay);
        }
      }
    };

    ws.onerror = (e) => {
      console.warn('[Transport] WebSocket error:', e);
      clearTimeout(timeout);
    };

    ws.onunexpected = () => {};
  }

  // Fallback: try connecting to root path (for older deployments)
  _tryWebSocketRoot() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const url = `${proto}://${location.host}`;
    console.log('[Transport] Trying WebSocket on root:', url);
    const ws = new WebSocket(url);
    let opened = false;

    const timeout = setTimeout(() => {
      if (!opened) {
        console.log('[Transport] WebSocket timeout on root, falling back to polling');
        ws.close();
        this._startPolling();
      }
    }, 10000);

    ws.onopen = () => {
      opened = true;
      clearTimeout(timeout);
      console.log('[Transport] WebSocket connected on root');
      this._ws = ws;
      this._polling = false;
      this._reconnectAttempts = 0;
      this._reconnectDelay = 1000;
      this.connected = true;
      this._stopPolling();
      this._startHeartbeat();
      this.dispatchEvent(new Event('open'));
      for (const msg of this._queue) this._wsSend(msg);
      this._queue = [];
    };

    ws.onmessage = (e) => {
      let msg; try { msg = JSON.parse(e.data); } catch { return; }
      if (msg.type === 'connected' || msg.type === 'pong') return;
      this.dispatchEvent(new CustomEvent('message', { detail: msg }));
    };

    ws.onclose = () => {
      clearTimeout(timeout);
      this._stopHeartbeat();
      if (!opened) return;
      this._ws = null;
      this.connected = false;
      this.dispatchEvent(new Event('close'));
      if (!this._intentionalClose) {
        this._reconnectAttempts++;
        if (this._reconnectAttempts >= 3) {
          this._startPolling();
        } else {
          const delay = Math.min(this._reconnectDelay * Math.pow(1.5, this._reconnectAttempts - 1), this._maxReconnectDelay);
          setTimeout(() => this._tryWebSocket(), delay);
        }
      }
    };

    ws.onerror = () => { clearTimeout(timeout); };
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

  // ── Polling ────────────────────────────────────────────
  _stopPolling() {
    this._pollActive = false;
  }

  async _startPolling() {
    if (this._polling) return;
    console.log('[Transport] Falling back to HTTP polling');
    this._polling = true;
    this._ws = null;

    try {
      const r = await fetch('/api/poll/register', { method: 'POST' });
      if (!r.ok) throw new Error('Register failed: ' + r.status);
      const { clientId } = await r.json();
      this._clientId = clientId;
      this.connected = true;
      this._pollActive = true;
      this.dispatchEvent(new Event('open'));
      this._pollLoop();

      // Flush queued messages
      for (const msg of this._queue) await this._pollSend(msg);
      this._queue = [];
    } catch(e) {
      console.error('[Transport] Polling setup failed:', e);
      this._polling = false;
      const delay = Math.min(2000 * (this._reconnectAttempts + 1), 10000);
      setTimeout(() => { this._reconnectAttempts++; this._startPolling(); }, delay);
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
    // If polling stopped but not intentional, try to reconnect via WebSocket
    if (!this._intentionalClose && !this._pollActive) {
      setTimeout(() => this._tryWebSocket(), 2000);
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
    this._stopPolling();
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
