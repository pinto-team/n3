# ============================
# File: noema/n3_api/routes/knowledge.py
# ============================

from fastapi import APIRouter
from n3_api.schemas import IngestRequest
from n3_api.utils.state import ensure_state, update_state
from n3_runtime.loop.io_tick import run_tick_io
from examples.minimal_chat.drivers_dev import build_drivers

router = APIRouter(prefix="/knowledge", tags=["Knowledge"])
_DRIVERS = build_drivers()

@router.post("/ingest", response_model=dict)
def knowledge_ingest(req: IngestRequest):
    """Ingest a document into the index queue."""
    state = ensure_state(req.thread_id)
    q = state.setdefault("index", {}).setdefault("queue", [])
    q.append({"type": "doc", "id": req.doc_id, "text": req.text})
    new_state = run_tick_io(state, _DRIVERS)
    update_state(req.thread_id, new_state)
    apply_ok = ((new_state.get("storage") or {}).get("apply_result") or {}).get("ok", True)
    idx = ((new_state.get("storage") or {}).get("index_result") or {})
    return {"ok": True, "thread_id": req.thread_id, "apply_ok": apply_ok, "indexed_items": idx.get("items", 0)}
