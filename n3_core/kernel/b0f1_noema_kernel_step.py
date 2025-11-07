# Folder: noema/n3_core/kernel
# File:   b0f1_noema_kernel_step.py

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Callable, Tuple

__all__ = ["b0f1_kernel_step"]

RULES_VERSION = "1.0"

StepFn = Callable[[Dict[str, Any]], Dict[str, Any]]


# ------------------------- utils -------------------------

def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merges 'src' into 'dst' (dict-only). Scalars/lists overwrite.
    """
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = copy.deepcopy(v)
    return dst


def _call(reg: Dict[str, StepFn], key: str, state: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    fn = reg.get(key)
    if not callable(fn):
        return "SKIP", {}
    try:
        out = fn(copy.deepcopy(state)) or {}
        return str(out.get("status", "OK")).upper(), out
    except Exception as e:
        return "FAIL", {"error": f"{key}: {e.__class__.__name__}: {e}"}


# ------------------------- pipeline -------------------------

DEFAULT_ORDER: List[str] = [
    # Persistence
    "b8f2_plan_apply", "b8f3_optimize_apply",
    # Observability
    "b9f1_aggregate_telemetry", "b9f2_build_trace", "b9f3_evaluate_slo",
    # Adaptation (policy)
    "b10f1_plan_policy_delta", "b10f2_plan_policy_apply", "b10f3_stage_policy_apply",
    # Runtime
    "b11f1_activate_config", "b11f2_gatekeeper", "b11f3_schedule_runtime",
    # Orchestration
    "b12f1_orchestrate", "b12f2_envelope_actions", "b12f3_build_jobs",
    # Drivers
    "b13f1_build_protocol", "b13f2_normalize_driver_replies", "b13f3_plan_retry",
]


# ------------------------- main -------------------------

def b0f1_kernel_step(input_json: Dict[str, Any], registry: Dict[str, StepFn], order: List[str] = None) -> Dict[
    str, Any]:
    """
    B0F1 — Noema.KernelStep
    Pure composition of available blocks. Each present step is invoked with the current state, and its
    result is deep-merged back. Steps not present in registry are skipped safely.

    Args:
      input_json: initial state/artifacts for this tick
      registry:   dict of { step_name: callable(state)->result_dict }
      order:      optional explicit step order (defaults to DEFAULT_ORDER)

    Returns:
      {
        "status": "OK|FAIL",
        "noema": { "kernel": { "ran":[...], "skipped":[...], "errors":[...], "rules_version":"1.0" } },
        "state": { ... merged state after all steps ... }
      }
    """
    steps = list(order or DEFAULT_ORDER)
    state: Dict[str, Any] = copy.deepcopy(input_json or {})
    ran: List[str] = []
    skipped: List[str] = []
    errors: List[Dict[str, Any]] = []

    for key in steps:
        status, out = _call(registry, key, state)
        if status == "FAIL":
            errors.append({"step": key, "error": out.get("error", "unknown")})
            # continue; kernel stays resilient
            continue
        if status == "SKIP":
            skipped.append(key)
            continue
        ran.append(key)
        _deep_merge(state, out)

    return {
        "status": "OK" if not errors else "FAIL",
        "noema": {
            "kernel": {
                "ran": ran,
                "skipped": skipped,
                "errors": errors,
                "rules_version": RULES_VERSION
            }
        },
        "state": state
    }


if __name__ == "__main__":
    # Minimal demo with stubs (replace with real functions in registry)
    def stub(name):
        def _fn(s):
            return {"status": "OK", name.split("_", 1)[0]: {name: {"ok": True}}}

        return _fn


    REG = {
        "b8f2_plan_apply": stub("b8f2_plan_apply"),
        "b8f3_optimize_apply": stub("b8f3_optimize_apply"),
        "b9f1_aggregate_telemetry": stub("b9f1_aggregate_telemetry"),
        "b12f1_orchestrate": stub("b12f1_orchestrate"),
    }

    initial = {"session": {"thread_id": "t-1"}}
    out = b0f1_kernel_step(initial, REG)
    print(json.dumps(out["noema"]["kernel"], ensure_ascii=False, indent=2))
    print("keys in state:", list(out["state"].keys()))

# Back-compat alias (some tests may import this name)
b0f1_noema_kernel_step = b0f1_kernel_step
