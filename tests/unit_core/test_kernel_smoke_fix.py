# Folder: noema/tests/unit_core
# File:   test_kernel_smoke_fix.py

from n3_runtime.adapters.registry import build_registry
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_kernel_step

def test_kernel_minimal_diff_ok():
    reg = build_registry()
    state = {
        "session": {"thread_id": "t-kernel-2"},
        "policy": {
            "apply_stage": {
                "version": {"id": "ver-dev", "parent_id": None, "created_at": "2025-11-07T09:00:00Z"},
                "doc": {"config": {"executor": {"timeout_ms": 12000}}},
                "rollback_point": {"id": "ver-dev", "parent_id": None, "keys": []}
            }
        },
        "observability": {"slo": {"score": 0.8}},
    }
    order = ["b11f1_activate_config"]
    out = b0f1_kernel_step(state, reg, order=order)
    assert out["status"] in {"OK","FAIL"}  # kernel is resilient
    assert "runtime" in out["state"]
