# ================================
# File: noema/n3_api/utils/dev_config.py
# ================================

def dev_config():
    return {
        "guardrails": {
            "must_confirm": {"u_threshold": 0.8, "rec_requires_confirm": False},
            "block_execute_when": {"slo_below": 0.0},
            "latency_soft_limit_ms": 1500,
            "index_queue_soft_max": 1000
        },
        "executor": {"timeout_ms": 15000, "parallelism": {"max_inflight": 2}},
        "features": {"cheap_models": True}
    }
