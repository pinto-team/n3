# Folder: noema/n3_core/block_11_runtime
# File:   b11f1_config_activator.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import unicodedata

__all__ = ["b11f1_activate_config"]

RULES_VERSION = "1.0"


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _deepcopy(obj: Any) -> Any:
    return json.loads(json.dumps(obj, ensure_ascii=False))


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


# ------------------------- diff -------------------------

def _diff(old: Dict[str, Any], new: Dict[str, Any], path: str = "") -> Dict[str, Any]:
    """
    Returns a structural diff with three buckets: added/changed/removed.
    Only shallow stringification for values to keep it safe for logs.
    """
    added: Dict[str, Any] = {}
    changed: Dict[str, Dict[str, Any]] = {}
    removed: List[str] = []

    old = old or {}
    new = new or {}
    okeys = set(old.keys())
    nkeys = set(new.keys())

    for k in sorted(nkeys - okeys):
        added[k] = new[k]

    for k in sorted(okeys - nkeys):
        removed.append(k)

    for k in sorted(okeys & nkeys):
        ov = old[k]
        nv = new[k]
        if isinstance(ov, dict) and isinstance(nv, dict):
            sub = _diff(ov, nv, path + "." + k if path else k)
            # bubble up only if meaningful
            if sub["added"] or sub["changed"] or sub["removed"]:
                changed[k] = {"nested": sub}
        elif ov != nv:
            changed[k] = {"old": ov, "new": nv}

    return {"added": added, "changed": changed, "removed": removed}


# ------------------------- main -------------------------

def b11f1_activate_config(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B11F1 — Runtime.ConfigActivator (Noema)

    Input:
      {
        "policy": {
          "apply_stage": {
            "version": { "id": str, "parent_id": str?, "created_at": str, ... },
            "doc": { "config": {...} },
            "rollback_point": { "id": str, "parent_id": str?, "keys": [ ... ] }
          },
          "current_runtime": { ... }?      # previous runtime config snapshot (optional)
        },
        "session": { "thread_id": str? }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "runtime": {
          "config": { ... },               # activated runtime config
          "version": { "id": str, "parent_id": str?, "activated_at": str, "source_stage": str },
          "diff": { "added":{}, "changed":{}, "removed":[] },
          "rollback_token": { "version_id": str, "parent_id": str?, "sig": str },
          "meta": { "source": "B11F1", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|no_stage" }
      }
    """
    stage = _get(input_json, ["policy", "apply_stage"], {})
    if not isinstance(stage, dict) or not stage:
        return {"status": "SKIP", "runtime": {"config": {}}, "diag": {"reason": "no_stage"}}

    staged_cfg = _get(stage, ["doc", "config"], {}) or {}
    version = _get(stage, ["version"], {}) or {}
    rollback = _get(stage, ["rollback_point"], {}) or {}
    prev = _get(input_json, ["policy", "current_runtime"], {}) or {}

    # Compute diff against previous runtime snapshot
    diff = _diff(prev if isinstance(prev, dict) else {}, staged_cfg if isinstance(staged_cfg, dict) else {})

    # Build runtime snapshot
    activated_at = _now_z()
    runtime_cfg = _deepcopy(staged_cfg)

    runtime = {
        "config": runtime_cfg,
        "version": {
            "id": version.get("id"),
            "parent_id": version.get("parent_id"),
            "activated_at": activated_at,
            "source_stage": "B10F3",
        },
        "diff": diff,
        "rollback_token": {
            "version_id": version.get("id"),
            "parent_id": version.get("parent_id"),
            "sig": _hash({"rid": version.get("id"), "parent": version.get("parent_id")}),
        },
        "meta": {"source": "B11F1", "rules_version": RULES_VERSION},
    }

    return {"status": "OK", "runtime": runtime, "diag": {"reason": "ok"}}


if __name__ == "__main__":
    sample = {
        "policy": {
            "current_runtime": {
                "dialog": {"surface": {"max_len": 800}},
                "executor": {"timeout_ms": 30000, "retries": {"max": 2}}
            },
            "apply_stage": {
                "version": {"id": "ver-abc", "parent_id": "ver-previous", "created_at": "2025-11-07T09:10:00Z"},
                "doc": {"config": {
                    "dialog": {"surface": {"max_len": 720}},
                    "executor": {"timeout_ms": 27000, "retries": {"max": 3}},
                    "guardrails": {"must_confirm": {"u_threshold": 0.35}}
                }},
                "rollback_point": {"id": "ver-abc", "parent_id": "ver-previous",
                                   "keys": ["pointers/current", "versions/ver-abc", "configs/ver-abc"]}
            }
        }
    }
    out = b11f1_activate_config(sample)
    print(out["diag"], out["runtime"]["version"])
    print(out["runtime"]["diff"]["changed"].keys(), out["runtime"]["diff"]["added"].keys())
