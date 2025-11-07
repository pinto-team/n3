# ============================
# File: noema/n3_api/routes/chat.py
# ============================

import json
from typing import Any, Dict, List

from fastapi import APIRouter
from n3_api.schemas import ChatRequest
from n3_api.utils.state import ensure_state, update_state
from n3_runtime.loop.io_tick import run_tick_io
from examples.minimal_chat.drivers_dev import build_drivers

router = APIRouter(prefix="/chat", tags=["Chat"])
_DRIVERS = build_drivers()

@router.post("/", response_model=dict)
def chat(req: ChatRequest):
    """Simple echo/search chat endpoint."""
    state = ensure_state(req.thread_id)

    # Phase 1: run search skill first (mirrors the WS endpoint behaviour)
    reqs: List[Dict[str, Any]] = state.setdefault("executor", {}).setdefault("requests", [])
    reqs.append({
        "req_id": "r-chat-search",
        "skill_id": "skill.dev.search",
        "params": {"q": req.text, "k": 5},
    })
    state = run_tick_io(state, _DRIVERS)

    # Inspect search hits and pick the best snippet; otherwise fall back to echo
    items = (((state.get("executor") or {}).get("results") or {}).get("items") or [])
    snippet = ""
    for item in items:
        if item.get("req_id") != "r-chat-search":
            continue
        data = item.get("data") if isinstance(item.get("data"), dict) else {}
        hits: List[Dict[str, Any]] = data.get("hits") if isinstance(data.get("hits"), list) else []
        if not hits:
            continue
        top = hits[0] if isinstance(hits[0], dict) else {}
        snippet = top.get("snippet") or top.get("text") or ""
        if not snippet:
            snippet = json.dumps(top, ensure_ascii=False)
        break

    if not snippet:
        snippet = json.dumps({"echo": req.text}, ensure_ascii=False)

    # Phase 2: emit answer via transport
    state.setdefault("executor", {})["requests"] = []
    state["dialog"] = {"final": {"move": "answer", "text": snippet}}
    before = len(_DRIVERS["transport"]["outbox"]())
    state = run_tick_io(state, _DRIVERS)
    after = len(_DRIVERS["transport"]["outbox"]())
    new_items = _DRIVERS["transport"]["outbox"]()[before:after]

    update_state(req.thread_id, state)
    return {"ok": True, "thread_id": req.thread_id, "emitted": new_items}
