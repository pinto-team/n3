# Folder: n3_runtime/loop
# File:   io_tick.py

from typing import Any, Dict, List, Tuple
import time
from n3_runtime.adapters.registry import build_registry
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_kernel_step

ORDER = [
    "b8f2_plan_apply","b8f3_optimize_apply",
    "b9f1_aggregate_telemetry","b9f2_build_trace","b9f3_evaluate_slo",
    "b10f1_plan_policy_delta","b10f2_plan_policy_apply","b10f3_stage_policy_apply",
    "b11f1_activate_config","b11f2_gatekeeper","b11f3_schedule_runtime",
    "b11f4_initiative_scheduler",                 # <-- new
    "b12f1_orchestrate","b12f2_envelope_actions","b12f3_build_jobs",
    "b13f1_build_protocol"
]

def _inject_clock(s: Dict[str, Any]) -> Dict[str, Any]:
    s.setdefault("clock", {})["now_ms"] = int(time.time() * 1000)
    return s

def _dispatch_frames(frames: List[Dict[str, Any]], drivers: Dict[str, Any]) -> List[Dict[str, Any]]:
    replies: List[Dict[str, Any]] = []
    for fr in frames:
        typ = str(fr.get("type") or "").lower()
        if typ == "transport":
            replies.append(drivers["transport"]["emit"](fr))
        elif typ == "skills":
            replies.append(drivers["skills"]["execute"](fr))
        elif typ == "storage":
            replies.append(drivers["storage"]["apply_index"](fr))
        elif typ == "timer":
            replies.append(drivers["timer"]["sleep"](fr))
    return replies

def run_tick_io(state: Dict[str, Any], drivers: Dict[str, Any]) -> Dict[str, Any]:
    registry = build_registry()
    state = _inject_clock(state)
    out1 = b0f1_kernel_step(state, registry, order=ORDER)
    s1 = out1.get("state", state)
    frames = (((s1.get("driver") or {}).get("protocol") or {}).get("frames") or [])
    if frames:
        replies = _dispatch_frames(frames, drivers)
        s1.setdefault("driver", {})["replies"] = replies
        out2 = b0f1_kernel_step(s1, registry, order=["b13f2_normalize_driver_replies","b9f1_aggregate_telemetry","b9f3_evaluate_slo","b13f3_plan_retry"])
        return out2.get("state", s1)
    return s1
