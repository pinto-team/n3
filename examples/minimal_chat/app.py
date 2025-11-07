# Folder: noema/examples/minimal_chat
# File:   app.py

from typing import Any, Dict
import json
from examples.minimal_chat.drivers_dev import build_drivers
from n3_runtime.loop.io_tick import run_tick_io

def _dev_config() -> Dict[str, Any]:
    return {
        "guardrails": {
            "must_confirm": {"u_threshold": 0.4},
            "block_execute_when": {"slo_below": 0.0},
            "latency_soft_limit_ms": 1500,
            "index_queue_soft_max": 1000
        },
        "executor": {"timeout_ms": 15000, "parallelism": {"max_inflight": 2}},
        "features": {"cheap_models": True}
    }

def _seed_state() -> Dict[str, Any]:
    return {
        "session": {"thread_id": "t-demo"},
        # B11F1 will activate this staged config
        "policy": {
            "apply_stage": {
                "version": {"id": "ver-dev", "parent_id": None, "created_at": "2025-11-07T09:00:00Z"},
                "doc": {"config": _dev_config()},
                "rollback_point": {"id": "ver-dev", "parent_id": None, "keys": []}
            }
        },
        # Provide one execute request (picked up by B11F3 → B12F1..B13F1)
        "executor": {"requests": [
            {"req_id": "r-1", "skill_id": "skill.dev.echo", "params": {"msg": "hello from noema"}}
        ]},
        # Signals for B11F2
        "observability": {"slo": {"score": 0.95}},
        "world_model": {"uncertainty": {"score": 0.2}}
    }

def _best_text(state: Dict[str, Any]) -> str:
    best = (((state.get("executor") or {}).get("results") or {}).get("best") or {})
    # Prefer text; else compact JSON of data
    if isinstance(best.get("text"), str) and best["text"]:
        return best["text"]
    data = best.get("data")
    return json.dumps(data, ensure_ascii=False) if data is not None else "Done."

if __name__ == "__main__":
    drivers = build_drivers()
    state = _seed_state()

    # Tick 1: execute skill.dev.echo and collect normalized results
    state = run_tick_io(state, drivers)

    # Prepare an answer from results (simple presenter for demo)
    if state.get("executor", {}).get("results"):
        text = _best_text(state)
        # Clear execute requests to avoid re-execution on next tick
        state.setdefault("executor", {})["requests"] = []
        state["dialog"] = {"final": {"move": "answer", "text": text}}

    # Tick 2: emit the answer via transport and update telemetry/retry planners
    state = run_tick_io(state, drivers)

    # Show transport ack summary (also available in state["transport"]["outbound"])
    ob = drivers["transport"]["outbox"]()
    print("Outbox messages:", ob)
