# Folder: noema/ui
# File:   streamlit_chat.py

import time
import threading
from typing import Any, Dict, List
import streamlit as st

from n3_runtime.loop.io_tick import run_tick_io
from examples.minimal_chat.drivers_dev import build_drivers
from n3_drivers.transport import http_dev

# ---------- bootstrap ----------

def seed_state(thread_id: str) -> Dict[str, Any]:
    return {
        "session": {"thread_id": thread_id},
        "policy": {
            "apply_stage": {
                "version": {"id": "ver-dev", "parent_id": None, "created_at": "2025-11-07T09:00:00Z"},
                "doc": {"config": {
                    "guardrails": {
                        "must_confirm": {"u_threshold": 0.4},
                        "block_execute_when": {"slo_below": 0.0},
                        "latency_soft_limit_ms": 1500,
                        "index_queue_soft_max": 1000
                    },
                    "executor": {"timeout_ms": 15000, "parallelism": {"max_inflight": 2}},
                    "features": {"cheap_models": True}
                }},
                "rollback_point": {"id": "ver-dev", "parent_id": None, "keys": []}
            }
        },
        "observability": {"slo": {"score": 0.95}},
        "world_model": {"uncertainty": {"score": 0.2}}
    }

if "drivers" not in st.session_state:
    st.session_state.drivers = build_drivers()
if "noema_state" not in st.session_state:
    st.session_state.noema_state = seed_state("t-local")
if "daemon_running" not in st.session_state:
    st.session_state.daemon_running = False
if "outbox_cursor" not in st.session_state:
    st.session_state.outbox_cursor = 0
if "history" not in st.session_state:
    st.session_state.history: List[Dict[str, str]] = []

# ---------- background daemon ----------

def _daemon_loop():
    while True:
        try:
            st.session_state.noema_state = run_tick_io(st.session_state.noema_state, st.session_state.drivers)
        except Exception:
            pass
        time.sleep(0.25)

if not st.session_state.daemon_running:
    t = threading.Thread(target=_daemon_loop, daemon=True)
    t.start()
    st.session_state.daemon_running = True

# ---------- UI ----------

st.set_page_config(page_title="Noema (local)", page_icon="🧠")
st.title("Noema — local chat (dev)")

with st.sidebar:
    st.markdown("### Ingest")
    with st.form("ingest_form"):
        doc_id = st.text_input("doc_id", "kb:noema:intro")
        doc_text = st.text_area("text", "Noema is a modular reasoning kernel with pure-core blocks and pluggable drivers.")
        submitted = st.form_submit_button("Ingest")
        if submitted:
            # Push into index queue and run one IO tick
            s = st.session_state.noema_state
            s.setdefault("index", {}).setdefault("queue", []).append({"type": "doc", "id": doc_id, "text": doc_text})
            st.session_state.noema_state = run_tick_io(s, st.session_state.drivers)
            st.success("Ingested into FTS index.")

# render history
for msg in st.session_state.history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["text"])

# consume new emitted messages from transport outbox
ob = http_dev.outbox()
cur = st.session_state.outbox_cursor
new = ob[cur:]
if new:
    for m in new:
        txt = m.get("text") or ""
        st.session_state.history.append({"role": "assistant", "text": str(txt)})
        with st.chat_message("assistant"):
            st.markdown(str(txt))
    st.session_state.outbox_cursor = len(ob)

# input
user_text = st.chat_input("Type your message…")
if user_text:
    # show user message
    st.session_state.history.append({"role": "user", "text": user_text})
    with st.chat_message("user"):
        st.markdown(user_text)

    # Phase 1: execute a search; fallback to echo if no hits
    s = st.session_state.noema_state
    s.setdefault("executor", {}).setdefault("requests", []).append(
        {"req_id": "r-chat", "skill_id": "skill.dev.search", "params": {"q": user_text, "k": 5}}
    )
    s = run_tick_io(s, st.session_state.drivers)

    items = (((s.get("executor") or {}).get("results") or {}).get("items") or [])
    hits = []
    if items and isinstance(items[0].get("data"), dict):
        hits = items[0]["data"].get("hits", [])
    if hits:
        answer = hits[0].get("snippet") or "Found."
    else:
        answer = f'{{"echo": "{user_text}"}}'

    # Phase 2: set dialog.final and let the driver emit
    s.setdefault("executor", {})["requests"] = []
    s.setdefault("dialog", {})["final"] = {"move": "answer", "text": answer}
    s = run_tick_io(s, st.session_state.drivers)
    st.session_state.noema_state = s

    # force show the just-emitted item from outbox
    ob2 = http_dev.outbox()
    for m in ob2[len(ob):]:
        txt = m.get("text") or ""
        st.session_state.history.append({"role": "assistant", "text": str(txt)})
        with st.chat_message("assistant"):
            st.markdown(str(txt))
    st.session_state.outbox_cursor = len(ob2)
