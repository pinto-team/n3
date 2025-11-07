# Folder: noema/tests/unit_core
# File:   test_kernel_smoke.py

from n3_runtime.adapters.registry import build_registry
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_kernel_step
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_noema_kernel_step

def _seed_state():
    return {
        "session": {"thread_id": "t-kernel"},
        "policy": {
            "apply_stage": {
                "version": {"id": "ver-dev", "parent_id": None, "created_at": "2025-11-07T09:00:00Z"},
                "doc": {"config": {
                    "guardrails": {
                        "must_confirm": {"u_threshold": 0.4},
                        "block_execute_when": {"slo_below": 0.0},
                        "latency_soft_limit_ms": 1500,
                        "index_queue_soft_max": 1000
                    },
                    "executor": {"timeout_ms": 15000, "parallelism": {"max_inflight": 2}},
                    "features": {"cheap_models": True}
                }},
                "rollback_point": {"id": "ver-dev", "parent_id": None, "keys": []}
            }
        },
        "observability": {"slo": {"score": 0.9}},
        "world_model": {"uncertainty": {"score": 0.2}},
        "dialog": {"final": {"move": "answer", "text": "ok"}}
    }

def test_registry_has_core_steps():
    reg = build_registry()
    assert isinstance(reg, dict)
    # Expect a healthy subset present
    expected = {"b11f1_activate_config","b11f2_gatekeeper","b11f3_schedule_runtime"}
    assert expected.issubset(set(reg.keys()))

def test_kernel_step_runs_minimal_runtime_path():
    reg = build_registry()
    state = _seed_state()
    order = ["b11f1_activate_config","b11f2_gatekeeper","b11f3_schedule_runtime"]
    out = b0f1_noema_kernel_step(state, reg, order=order)  # type: ignore[name-defined]
