# ============================
# File: noema/n3_api/routes/policy.py
# ============================

from fastapi import APIRouter
from n3_api.schemas import PolicyApplyRequest
from n3_api.utils.state import ensure_state, update_state, now_iso
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_kernel_step
from n3_runtime.adapters.registry import build_registry

router = APIRouter(prefix="/policy", tags=["Policy"])

@router.post("/apply", response_model=dict)
def policy_apply(req: PolicyApplyRequest):
    """Apply policy deltas and activate runtime configuration."""
    state = ensure_state(req.thread_id)
    delta = {
        "changes": [c.model_dump() for c in req.changes],
        "guards": {"max_changes": 100, "ttl": {"seconds": 3600}},
        "meta": {"created_at": now_iso()},
    }
    state.setdefault("policy", {})["delta"] = delta
    reg = build_registry()
    out = b0f1_kernel_step(
        state, reg,
        order=["b10f2_plan_policy_apply", "b10f3_stage_policy_apply", "b11f1_activate_config"]
    )
    new_state = out.get("state", state)
    update_state(req.thread_id, new_state)
    runtime = (new_state.get("runtime") or {})
    version = (runtime.get("version") or {})
    return {
        "ok": True,
        "thread_id": req.thread_id,
        "activated_version": version,
        "diff_keys": ((runtime.get("diff") or {}).get("changed", {}) or {}).keys(),
    }
