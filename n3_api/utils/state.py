# ================================
# File: noema/n3_api/utils/state.py
# ================================

from typing import Dict, Any
from datetime import datetime, timezone
from n3_api.utils.dev_config import dev_config

LABELS = [
    "direct_answer",
    "execute_action",
    "ask_clarification",
    "acknowledge_only",
    "small_talk",
    "closing",
    "refuse_or_safecheck",
    "other",
]


def _default_learning_state() -> Dict[str, Any]:
    base_weight = 0.5
    now = now_iso()
    weights = {label: base_weight for label in LABELS}
    return {
        "version": {
            "id": "policy-learning-v0",
            "parent_id": None,
            "updated_at": now,
        },
        "weights": weights,
        "rollback": {
            "version": None,
            "weights": weights,
        },
        "summary": {
            "avg_reward": 0.0,
            "updates": 0,
            "confidence": 0.5,
        },
    }

_SESSIONS: Dict[str, Dict[str, Any]] = {}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def now_ms() -> int:
    import time
    return int(time.time() * 1000)

def ensure_state(thread_id: str) -> Dict[str, Any]:
    """Initialize or fetch an in-memory session state."""
    if thread_id not in _SESSIONS:
        learning = _default_learning_state()
        _SESSIONS[thread_id] = {
            "session": {"thread_id": thread_id},
            "policy": {
                "apply_stage": {
                    "version": {"id": "ver-dev", "parent_id": None, "created_at": now_iso()},
                    "doc": {"config": dev_config()},
                    "rollback_point": {"id": "ver-dev", "parent_id": None, "keys": []}
                },
                "learning": learning,
            },
            "observability": {
                "slo": {"score": 0.95},
                "telemetry": {
                    "summary": {
                        "policy_updates": 0,
                        "concept_new_rules": 0,
                        "avg_reward": 0.0,
                        "uncertainty": 0.2,
                    }
                },
            },
            "world_model": {"uncertainty": {"score": 0.2}},
            "concept_graph": {
                "version": {"id": "concept-v0", "parent_id": None, "updated_at": now_iso()},
            },
            "adaptation": {"policy": learning["summary"]},
        }
    return _SESSIONS[thread_id]

def update_state(thread_id: str, new_state: Dict[str, Any]):
    _SESSIONS[thread_id] = new_state

def get_sessions():
    return _SESSIONS
