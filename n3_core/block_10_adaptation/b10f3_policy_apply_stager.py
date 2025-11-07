# Folder: noema/n3_core/block_10_adaptation
# File:   b10f3_policy_apply_stager.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import unicodedata

__all__ = ["b10f3_stage_policy_apply"]

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


def _sha1(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _ns(inp: Dict[str, Any]) -> str:
    tid = _get(inp, ["session", "thread_id"], "") or "default"
    return f"config/noema/{tid}"


def _kv(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if isinstance(p, str) and p)


def _apply_ops(base: Dict[str, Any], ops: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Apply "set" ops to a shadow copy of base (pure transformation).
    state = json.loads(json.dumps(base, ensure_ascii=False))
    for op in ops:
        if not isinstance(op, dict) or op.get("op") != "set":
            continue
        path = op.get("path")
        if not isinstance(path, str) or not path:
            continue
        parts = [p for p in path.split(".") if p]
        cur = state
        for i, p in enumerate(parts):
            if i == len(parts) - 1:
                cur[p] = op.get("value")
            else:
                if p not in cur or not isinstance(cur[p], dict):
                    cur[p] = {}
                cur = cur[p]
    return state


# ------------------------- main -------------------------

def b10f3_stage_policy_apply(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B10F3 — Adaptation.PolicyApplyStager (Noema)
    Turns B10F2 apply_plan into a versioned config + storage.apply WAL with rollback metadata (pure; no I/O).

    Input:
      {
        "policy": {
          "current": { ... },
          "apply_plan": {
            "ops": [ {"op":"set","path":str,"value":Any}, ... ],
            "accepted": [...], "rejected": [...],
            "preview": { "config": {...}, "diff": {"set": {path:{old,new}}, "changed_keys":[...]} }?,
            "meta": {...}
          },
          "version": { "current_id": str? }?
        },
        "session": { "thread_id": str? }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "policy": {
          "apply_stage": {
            "version": { "id": str, "parent_id": str?, "created_at": str, "author": "noema", "changes": int, "comment": str },
            "doc": { "config": {...} },
            "storage_apply": {
              "namespace": str,
              "ops": [
                { "op":"put", "key": "versions/<ver_id>", "value": {...}, "seq": int? },
                { "op":"put", "key": "configs/<ver_id>", "value": {...}, "seq": int? },
                { "op":"put", "key": "pointers/current", "value": {"version_id": "<ver_id>"}, "seq": int? }
              ],
              "meta": { "source": "B10F3", "rules_version": "1.0" }
            },
            "rollback_point": { "id": str, "parent_id": str?, "keys": [ "pointers/current", "versions/<ver_id>", "configs/<ver_id>" ] },
            "meta": { "source": "B10F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_ops", "counts": { "set_ops": int } }
      }
    """
    pol = input_json.get("policy", {}) if isinstance(input_json.get("policy"), dict) else {}
    current = pol.get("current", {}) if isinstance(pol.get("current"), dict) else {}
    plan = pol.get("apply_plan", {}) if isinstance(pol.get("apply_plan"), dict) else {}
    ops = plan.get("ops") if isinstance(plan.get("ops"), list) else []

    if not ops:
        return {
            "status": "SKIP",
            "policy": {"apply_stage": {}},
            "diag": {"reason": "no_ops", "counts": {"set_ops": 0}},
        }

    # Build proposed config (prefer preview.config if present, else apply ops)
    preview_cfg = _get(plan, ["preview", "config"], None)
    proposed_cfg = preview_cfg if isinstance(preview_cfg, dict) else _apply_ops(current, ops)

    # Versioning
    parent_id = _get(pol, ["version", "current_id"], None)
    change_sig = {"parent": parent_id, "ops": ops, "proposed_cfg": proposed_cfg}
    ver_id = _sha1(change_sig)
    created_at = _now_z()

    version_doc = {
        "id": ver_id,
        "parent_id": parent_id,
        "created_at": created_at,
        "author": "noema",
        "rules_version": RULES_VERSION,
        "changes": len(ops),
        "meta": {"from": "B10F2.apply_plan"},
    }

    ns = _ns(input_json)
    apply_ops = [
        {"op": "put", "key": _kv(ns, "versions", ver_id), "value": version_doc},
        {"op": "put", "key": _kv(ns, "configs", ver_id), "value": proposed_cfg},
        {"op": "put", "key": _kv(ns, "pointers", "current"), "value": {"version_id": ver_id, "updated_at": created_at}},
    ]

    stage = {
        "version": {"id": ver_id, "parent_id": parent_id, "created_at": created_at, "author": "noema",
                    "changes": len(ops), "comment": "staged by B10F3"},
        "doc": {"config": proposed_cfg},
        "storage_apply": {"namespace": ns, "ops": apply_ops,
                          "meta": {"source": "B10F3", "rules_version": RULES_VERSION}},
        "rollback_point": {"id": ver_id, "parent_id": parent_id,
                           "keys": [_kv(ns, "pointers", "current"), _kv(ns, "versions", ver_id),
                                    _kv(ns, "configs", ver_id)]},
        "meta": {"source": "B10F3", "rules_version": RULES_VERSION},
    }

    return {
        "status": "OK",
        "policy": {"apply_stage": stage},
        "diag": {"reason": "ok", "counts": {"set_ops": len(ops)}},
    }


if __name__ == "__main__":
    # Demo: stage a versioned apply from an apply_plan
    sample = {
        "session": {"thread_id": "t-007"},
        "policy": {
            "current": {
                "dialog": {"surface": {"max_len": 800}},
                "safety_filter": {"max_out_len": 1200},
                "executor": {"timeout_ms": 30000, "retries": {"max": 2}},
                "guardrails": {"must_confirm": {"u_threshold": 0.4}}
            },
            "apply_plan": {
                "ops": [
                    {"op": "set", "path": "dialog.surface.max_len", "value": 720},
                    {"op": "set", "path": "executor.retries.max", "value": 3},
                    {"op": "set", "path": "guardrails.must_confirm.u_threshold", "value": 0.35}
                ],
                "meta": {"source": "B10F2"}
            },
            "version": {"current_id": "ver-previous"}
        }
    }
    out = b10f3_stage_policy_apply(sample)
    print(out["diag"])
    print(out["policy"]["apply_stage"]["version"])
    for op in out["policy"]["apply_stage"]["storage_apply"]["ops"]:
        print(op["op"], op["key"])
