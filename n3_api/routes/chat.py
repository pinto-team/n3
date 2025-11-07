# ============================
# File: noema/n3_api/routes/chat.py
# ============================

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

    # Phase 1: run echo skill
    state.setdefault("executor", {}).setdefault("requests", [])
    state["executor"]["requests"].append({
        "req_id": "r-chat",
        "skill_id": "skill.dev.echo",
        "params": {"msg": req.text},
    })
    state = run_tick_io(state, _DRIVERS)

    best = (((state.get("executor") or {}).get("results") or {}).get("best") or {})
    text = best.get("text") or best.get("data") or {"echo": {"msg": req.text}}

    import json
    if isinstance(text, dict):
        text = json.dumps(text, ensure_ascii=False)

    # Phase 2: emit
    state.setdefault("executor", {})["requests"] = []
    state["dialog"] = {"final": {"move": "answer", "text": text}}
    before = len(_DRIVERS["transport"]["outbox"]())
    state = run_tick_io(state, _DRIVERS)
    after = len(_DRIVERS["transport"]["outbox"]())
    new_items = _DRIVERS["transport"]["outbox"]()[before:after]

    update_state(req.thread_id, state)
    return {"ok": True, "thread_id": req.thread_id, "emitted": new_items}
