# ================================
# File: noema/n3_api/utils/state.py
# ================================

from typing import Dict, Any
from datetime import datetime, timezone
from n3_api.utils.dev_config import dev_config

_SESSIONS: Dict[str, Dict[str, Any]] = {}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def now_ms() -> int:
    import time
    return int(time.time() * 1000)

def ensure_state(thread_id: str) -> Dict[str, Any]:
    """Initialize or fetch an in-memory session state."""
    if thread_id not in _SESSIONS:
        _SESSIONS[thread_id] = {
            "session": {"thread_id": thread_id},
            "policy": {
                "apply_stage": {
                    "version": {"id": "ver-dev", "parent_id": None, "created_at": now_iso()},
                    "doc": {"config": dev_config()},
                    "rollback_point": {"id": "ver-dev", "parent_id": None, "keys": []}
                }
            },
            "observability": {"slo": {"score": 0.95}},
            "world_model": {"uncertainty": {"score": 0.2}},
        }
    return _SESSIONS[thread_id]

def update_state(thread_id: str, new_state: Dict[str, Any]):
    _SESSIONS[thread_id] = new_state

def get_sessions():
    return _SESSIONS
