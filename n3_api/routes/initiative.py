# ============================
# File: noema/n3_api/routes/initiative.py
# ============================

from fastapi import APIRouter
from n3_api.schemas import InitiativeAddRequest
from n3_api.utils.state import ensure_state, update_state, now_ms

router = APIRouter(prefix="/initiative", tags=["Initiative"])

@router.post("/add", response_model=dict)
def initiative_add(req: InitiativeAddRequest):
    """Add new scheduled initiatives."""
    state = ensure_state(req.thread_id)
    q = state.setdefault("initiative", {}).setdefault("queue", [])
    base_now = now_ms()
    for it in req.items:
        d = it.model_dump()
        when_ms = d.get("when_ms") or (base_now + int(d.get("in_ms") or 0))
        q.append({
            "id": str(d["id"]),
            "type": d["type"],
            "when_ms": int(when_ms),
            "once": bool(d.get("once", True)),
            "cooldown_ms": int(d.get("cooldown_ms") or 0),
            "payload": d.get("payload") or {},
        })
    update_state(req.thread_id, state)
    return {"ok": True, "thread_id": req.thread_id, "queue_len": len(q)}
