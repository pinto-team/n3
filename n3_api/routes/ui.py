# File: noema/n3_api/routes/ui.py


from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["UI"])

INDEX_HTML = """
<!doctype html>
<html>
<head>
<meta charset=\"utf-8\" />
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
#dash { width:320px; border:1px solid #ddd; background:#fff; padding:12px; box-shadow:0 1px 2px rgba(0,0,0,0.08); }
#dash h3 { margin-top:0; }
.stat { margin:6px 0; font-size:13px; }
canvas { width:100%; height:110px; border:1px solid #e5e5e5; margin-top:8px; background:#fcfcfc; }
input, button { font-size: 14px; padding: 4px 8px; }
</style>
</head>
<body>
<h2>Noema — local console</h2>
<div>
  <label>Thread ID:</label>
  <input id=\"tid\" value=\"t-demo\" />
  <button id=\"connect\">Connect</button>
</div>
<div id=\"layout\">
  <div id=\"chat\">
    <div id=\"log\"></div>
    <div style=\"margin-top:12px;\">
      <input id=\"text\" style=\"width:70%;\" placeholder=\"Type message...\" />
      <button id=\"send\">Send</button>
    </div>
  </div>
  <div id=\"dash\">
    <h3>Runtime dashboard</h3>
    <div class=\"stat\">Uncertainty: <span id=\"stat-unc\">-</span></div>
    <div class=\"stat\">Policy confidence: <span id=\"stat-conf\">-</span></div>
    <div class=\"stat\">Avg reward: <span id=\"stat-reward\">-</span></div>
    <div class=\"stat\">Concept version: <span id=\"stat-concept\">-</span></div>
    <canvas id=\"chart-unc\" width=\"320\" height=\"110\"></canvas>
    <canvas id=\"chart-reward\" width=\"320\" height=\"110\"></canvas>
    <canvas id=\"chart-updates\" width=\"320\" height=\"110\"></canvas>
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
    document.getElementById('stat-unc').textContent = (summary.uncertainty ?? '-').toFixed ? summary.uncertainty.toFixed(3) : summary.uncertainty;
    document.getElementById('stat-conf').textContent = (adaptation.confidence ?? '-').toFixed ? adaptation.confidence.toFixed(3) : adaptation.confidence;
    document.getElementById('stat-reward').textContent = (adaptation.avg_reward ?? '-').toFixed ? adaptation.avg_reward.toFixed(3) : adaptation.avg_reward;
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
};
</script>
</body>
</html>
"""

@router.get("/", response_class=HTMLResponse)
def index():
    """Serve the chat UI."""
    return HTMLResponse(INDEX_HTML)
