# File: noema/n3_api/routes/ui.py


from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["UI"])

INDEX_HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>Noema — local chat</title>
<style>
body { font-family: system-ui, sans-serif; margin: 24px; }
#log { border:1px solid #ddd; padding:12px; height:50vh; overflow:auto; }
.msg { margin:6px 0; }
.msg.user { color:#333; }
.msg.assistant { color:#0a6; }
.msg.system { color:#999; font-style: italic; }
input, button { font-size: 14px; padding: 4px 8px; }
</style>
</head>
<body>
<h2>Noema — local chat (WebSocket)</h2>
<div>
  <label>Thread ID:</label>
  <input id="tid" value="t-demo" />
  <button id="connect">Connect</button>
</div>
<div id="log"></div>
<div style="margin-top:12px;">
  <input id="text" style="width:70%;" placeholder="Type message..." />
  <button id="send">Send</button>
</div>
<script>
let wsPush = null, wsChat = null, tid = "t-demo";
const log = (role, text) => {
  const d = document.createElement('div');
  d.className = 'msg ' + role;
  d.textContent = role + ': ' + text;
  document.getElementById('log').appendChild(d);
  document.getElementById('log').scrollTop = 1e9;
};
document.getElementById('connect').onclick = () => {
  tid = document.getElementById('tid').value || 't-demo';
  if (wsPush) wsPush.close();
  if (wsChat) wsChat.close();
  wsPush = new WebSocket(`ws://${location.host}/ws/` + tid);
  wsPush.onmessage = (ev) => {
    try { const m = JSON.parse(ev.data); log('assistant', m.text || JSON.stringify(m)); }
    catch { log('assistant', ev.data); }
  };
  wsPush.onopen = () => log('system', 'push connected');
  wsPush.onclose = () => log('system', 'push closed');

  wsChat = new WebSocket(`ws://${location.host}/ws/chat/` + tid);
  wsChat.onopen  = () => log('system', 'chat connected');
  wsChat.onclose = () => log('system', 'chat closed');
  wsChat.onmessage = (ev) => {
    try { const m = JSON.parse(ev.data); if (m.text) log('assistant', m.text); }
    catch {}
  };
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
