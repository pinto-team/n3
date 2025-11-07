from n3_core.block_10_adaptation.b10f1_policy_delta_planner import b10f1_plan_policy_delta


def _base_learning_state():
    labels = [
        "direct_answer",
        "execute_action",
        "ask_clarification",
        "acknowledge_only",
        "small_talk",
        "closing",
        "refuse_or_safecheck",
        "other",
    ]
    weights = {label: 0.5 for label in labels}
    return {
        "weights": weights,
        "version": {"id": "v0", "parent_id": None, "updated_at": "2025-01-01T00:00:00Z"},
        "rollback": {"version": None, "weights": weights},
        "summary": {"avg_reward": 0.0, "updates": 0, "confidence": 0.5},
    }


def test_policy_learning_updates_with_rewards():
    trace = [
        {"reward": 0.9, "target": "direct_answer", "actual": "ask_clarification", "top_pred": "ask_clarification"},
        {"reward": 0.2, "target": "execute_action", "actual": "execute_action", "top_pred": "direct_answer"},
        {"reward": 0.75, "target": "execute_action", "actual": "execute_action", "top_pred": "execute_action"},
    ]
    state = {
        "policy": {"learning": _base_learning_state()},
        "world_model": {"trace": {"error_history": trace}, "uncertainty": {"score": 0.35}},
        "observability": {"telemetry": {"metrics": []}},
    }

    result = b10f1_plan_policy_delta(state)
    learning = result["policy"]["learning"]
    summary = result["adaptation"]["policy"]

    assert learning["version"]["id"] != "v0"
    assert summary["updates"] == len(trace)
    assert learning["delta"]["direct_answer"] > 0
    assert learning["delta"]["execute_action"] > 0
    assert summary["avg_reward"] > 0
    assert 0 <= summary["confidence"] <= 1
