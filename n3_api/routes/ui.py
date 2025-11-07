from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["UI"])

INDEX_HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>Noema — local console</title>
<style>
body { font-family: system-ui, sans-serif; margin: 24px; background:#fafafa; }
#layout { display:flex; gap:24px; align-items:flex-start; }
#chat { flex:1; }
#log { border:1px solid #ddd; background:#fff; padding:12px; height:50vh; overflow:auto; box-shadow:0 1px 2px rgba(0,0,0,0.08); }
.msg { margin:6px 0; }
.msg.user { color:#333; }
.msg.assistant { color:#0a6; }
.msg.system { color:#777; font-style: italic; }
#dash { width:340px; border:1px solid #ddd; background:#fff; padding:12px; box-shadow:0 1px 2px rgba(0,0,0,0.08); }
#dash h3 { margin-top:0; }
.stat { margin:6px 0; font-size:13px; }
canvas { width:100%; height:110px; border:1px solid #e5e5e5; margin-top:8px; background:#fcfcfc; }
input, button { font-size: 14px; padding: 4px 8px; }
.small { font-size:12px; color:#666; }
.row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
textarea { font-family: system-ui, sans-serif; font-size:13px; }
</style>
</head>
<body>
<h2>Noema — local console</h2>
<div class="row">
  <label>Thread ID:</label>
  <input id="tid" value="t-demo" />
  <button id="connect">Connect</button>
  <span class="small" id="train-res"></span>
</div>
<div id="layout">
  <div id="chat">
    <div id="log"></div>
    <div style="margin-top:12px;">
      <input id="text" style="width:70%;" placeholder="Type message..." />
      <button id="send">Send</button>
    </div>
  </div>
  <div id="dash">
    <h3>Runtime dashboard</h3>

    <!-- 🟢 NEW: Ingest block -->
    <div style="margin:8px 0; padding-bottom:8px; border-bottom:1px dashed #ddd;">
      <div style="font-weight:600; margin-bottom:4px;">Ingest</div>
      <div class="row" style="margin-bottom:4px;">
        <input id="ing-id" placeholder="doc id" value="kb:intro" />
      </div>
      <div>
        <textarea id="ing-text" style="width:100%; height:80px;"
          placeholder="Paste text to index..."></textarea>
      </div>
      <div class="row" style="margin-top:6px;">
        <button id="btn-ingest">Ingest</button>
        <span class="small" id="ing-res"></span>
      </div>
    </div>

    <!-- Existing controls -->
    <div style="margin:8px 0;">
      <div class="row" style="margin-bottom:6px;">
        <button id="btn-train">Train</button>
        <button id="btn-apply">Apply Policy</button>
      </div>
      <div class="row">
        <label class="small">Reward:</label>
        <input type="range" id="rw" min="0" max="1" step="0.1" value="0.8" />
        <button id="btn-reward">Give</button>
        <span class="small" id="rw-res"></span>
      </div>
    </div>

    <div class="stat">Uncertainty: <span id="stat-unc">-</span></div>
    <div class="stat">Policy confidence: <span id="stat-conf">-</span></div>
    <div class="stat">Avg reward: <span id="stat-reward">-</span></div>
    <div class="stat">Concept version: <span id="stat-concept">-</span></div>
    <canvas id="chart-unc" width="320" height="110"></canvas>
    <canvas id="chart-reward" width="320" height="110"></canvas>
    <canvas id="chart-updates" width="320" height="110"></canvas>
  </div>
</div>

<script>
let wsPush = null, wsChat = null, tid = 't-demo', statsTimer = null, lastPush = '';
const history = { unc: [], reward: [], updates: [] };
const maxPoints = 60;
const log = (role, text) => {
  const d = document.createElement('div');
  d.className = 'msg ' + role;
  d.textContent = role + ': ' + text;
  const logDiv = document.getElementById('log');
  logDiv.appendChild(d);
  logDiv.scrollTop = logDiv.scrollHeight;
};

const renderSpark = (canvasId, data, color) => {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!data.length) return;
  const w = canvas.width, h = canvas.height;
  const maxVal = Math.max(...data, 1);
  const minVal = Math.min(...data, 0);
  const range = Math.max(0.0001, maxVal - minVal);
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  data.forEach((v, i) => {
    const x = (i / Math.max(1, data.length - 1)) * w;
    const y = h - ((v - minVal) / range) * h;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();
};

const pushHistory = (key, value) => {
  if (!history[key]) history[key] = [];
  history[key].push(value);
  if (history[key].length > maxPoints) history[key].shift();
};

const fetchStats = () => {
  fetch(`/introspect/${tid}`).then(r => r.json()).then(data => {
    const adaptation = data.adaptation || {};
    const concept = data.concept || {};
    const telemetry = data.telemetry || {};
    const summary = (telemetry.summary) || {};
    const to3 = v => (typeof v === 'number' && v.toFixed) ? v.toFixed(3) : v;

    document.getElementById('stat-unc').textContent = to3(summary.uncertainty ?? '-');
    document.getElementById('stat-conf').textContent = to3(adaptation.confidence ?? '-');
    document.getElementById('stat-reward').textContent = to3(adaptation.avg_reward ?? '-');
    document.getElementById('stat-concept').textContent = (concept.version || {}).id || '-';

    const unc = typeof summary.uncertainty === 'number' ? summary.uncertainty : 0;
    const rew = typeof adaptation.avg_reward === 'number' ? adaptation.avg_reward : 0;
    const upd = typeof adaptation.updates === 'number' ? adaptation.updates : 0;
    pushHistory('unc', unc);
    pushHistory('reward', rew);
    pushHistory('updates', upd);
    renderSpark('chart-unc', history.unc, '#5b8');
    renderSpark('chart-reward', history.reward, '#58a');
    renderSpark('chart-updates', history.updates, '#a85');
  }).catch(() => {});
};

const connectSockets = () => {
  if (wsPush) wsPush.close();
  if (wsChat) wsChat.close();
  lastPush = '';
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  wsPush = new WebSocket(`${proto}://${location.host}/ws/` + tid);
  wsPush.onmessage = (ev) => {
    if (ev.data === lastPush) return;
    lastPush = ev.data;
    try { const m = JSON.parse(ev.data); log('assistant', m.text || JSON.stringify(m)); }
    catch { log('assistant', ev.data); }
  };
  wsPush.onopen = () => log('system', 'push connected');
  wsPush.onclose = () => log('system', 'push closed');

  wsChat = new WebSocket(`${proto}://${location.host}/ws/chat/` + tid);
  wsChat.onopen  = () => log('system', 'chat connected');
  wsChat.onclose = () => log('system', 'chat closed');
  wsChat.onmessage = (ev) => {
    try { const m = JSON.parse(ev.data); if (m.text) log('assistant', m.text); }
    catch {}
  };

  if (statsTimer) clearInterval(statsTimer);
  fetchStats();
  statsTimer = setInterval(fetchStats, 3000);
};

document.getElementById('connect').onclick = () => {
  tid = document.getElementById('tid').value || 't-demo';
  connectSockets();
};

document.getElementById('send').onclick = () => {
  const t = document.getElementById('text').value;
  if (!t || !wsChat || wsChat.readyState !== 1) return;
  log('user', t);
  wsChat.send(t);
  document.getElementById('text').value = '';
  document.getElementById('text').focus(); // 🟢 focus again
};

// 🟢 NEW: send on Enter
document.getElementById('text').addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    document.getElementById('send').click();
  }
});

// 🟢 NEW: Ingest handler
document.getElementById('btn-ingest').onclick = async () => {
  const id = document.getElementById('ing-id').value || `kb:${Date.now()}`;
  const text = document.getElementById('ing-text').value || '';
  const r = await fetch('/skills/', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      thread_id: tid,
      calls: [{req_id:'r-ing', skill_id:'skill.dev.ingest', params:{id, text}}]
    })
  });
  const j = await r.json();
  document.getElementById('ing-res').textContent = r.ok ? 'ok' : ('err: ' + (j.detail || j));
};

// Existing Train / Apply / Reward
document.getElementById('btn-train').onclick = async () => {
  const r = await fetch('/policy/train', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({thread_id: tid})
  });
  const j = await r.json();
  document.getElementById('train-res').textContent = 'trained: ' + (j.policy_updates?.version || '');
  fetchStats();
};

document.getElementById('btn-apply').onclick = async () => {
  const payload = {
    thread_id: tid,
    changes: [
      {"path":"policy.learning.enabled","op":"set","value":true},
      {"path":"features.cheap_models","op":"set","value":false}
    ]
  };
  const r = await fetch('/policy/apply', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const j = await r.json();
  document.getElementById('train-res').textContent = 'applied: ' + (j.activated_version?.id || '');
  fetchStats();
};

document.getElementById('btn-reward').onclick = async () => {
  const score = parseFloat(document.getElementById('rw').value);
  const payload = {
    thread_id: tid,
    calls: [{req_id:'r-rew', skill_id:'skill.dev.reward', params:{score, reason:'manual'}}]
  };
  const r = await fetch('/skills/', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  if (r.ok) document.getElementById('rw-res').textContent = 'ok';
  fetchStats();
};
</script>
</body>
</html>
"""

@router.get("/", response_class=HTMLResponse)
def index():
    """Serve the chat UI."""
    return HTMLResponse(INDEX_HTML)
