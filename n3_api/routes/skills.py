# noema/n3_api/routes/skills.py
from fastapi import APIRouter
from n3_api.schemas import SkillsRequest
from n3_api.utils.state import ensure_state, update_state
from n3_runtime.loop.io_tick import run_tick_io
from n3_api.utils.drivers import build_drivers_safe  # ⬅️

router = APIRouter(prefix="/skills", tags=["Skills"])
_DRIVERS = build_drivers_safe()  # ⬅️

@router.post("/", response_model=dict)
def run_skills(req: SkillsRequest):
    """
    Run one or more skill calls through the executor system.
    """
    state = ensure_state(req.thread_id)
    state.setdefault("executor", {}).setdefault("requests", [])
    state["executor"]["requests"].extend([c.model_dump() for c in req.calls])

    new_state = run_tick_io(state, _DRIVERS)
    update_state(req.thread_id, new_state)

    agg = (((new_state.get("executor") or {}).get("results") or {}).get("aggregate") or {})
    return {"ok": True, "thread_id": req.thread_id, "aggregate": agg}
