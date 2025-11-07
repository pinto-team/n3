# ============================
# File: noema/n3_api/routes/policy.py
# ============================

from fastapi import APIRouter
from n3_api.schemas import PolicyApplyRequest, TickRequest
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


@router.post("/train", response_model=dict)
def policy_train(req: TickRequest):
    """Trigger a learning/adaptation tick using reward and concept traces."""
    state = ensure_state(req.thread_id)
    reg = build_registry()
    order = [
        "b4f1_mine_patterns",
        "b4f2_manage_nodes",
        "b4f3_score_edges",
        "b4f4_extract_rules",
        "b10f1_plan_policy_delta",
        "b9f1_aggregate_telemetry",
    ]
    out = b0f1_kernel_step(state, reg, order=order)
    new_state = out.get("state", state)
    update_state(req.thread_id, new_state)

    adaptation = (new_state.get("adaptation") or {}).get("policy", {}) or {}
    concept = (new_state.get("concept_graph") or {})
    concept_version = ((concept.get("version") or {}).get("id"))
    concept_updates = concept.get("updates") if isinstance(concept.get("updates"), dict) else {}

    return {
        "ok": True,
        "thread_id": req.thread_id,
        "policy_updates": {
            "updates": adaptation.get("updates", 0),
            "avg_reward": adaptation.get("avg_reward", 0.0),
            "confidence": adaptation.get("confidence", 0.0),
            "version": adaptation.get("learning_version") or ((new_state.get("policy", {}) or {}).get("learning", {}) or {}).get("version", {}).get("id"),
        },
        "concept": {
            "version": concept_version,
            "updates": concept_updates,
        },
        "kernel": out.get("noema", {}).get("kernel", {}),
    }
