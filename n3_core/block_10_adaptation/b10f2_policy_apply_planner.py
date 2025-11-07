# Folder: noema/n3_core/block_10_adaptation
# File:   b10f2_policy_apply_planner.py

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import unicodedata

__all__ = ["b10f2_plan_policy_apply"]

RULES_VERSION = "1.0"
MAX_PREVIEW_ENTRIES = 200


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur: Any = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _set_path(obj: Dict[str, Any], dotted: str, value: Any) -> Dict[str, Any]:
    parts = [p for p in dotted.split(".") if p]
    cur = obj
    for i, p in enumerate(parts):
        if i == len(parts) - 1:
            cur[p] = value
        else:
            if p not in cur or not isinstance(cur[p], dict):
                cur[p] = {}
            cur = cur[p]
    return obj


def _get_path(obj: Dict[str, Any], dotted: str, default=None) -> Any:
    parts = [p for p in dotted.split(".") if p]
    cur: Any = obj
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def _iso_to_dt(s: str) -> Optional[datetime]:
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _now_z() -> datetime:
    return datetime.now(timezone.utc)


def _clip_preview(d: Dict[str, Any], limit: int = MAX_PREVIEW_ENTRIES) -> Dict[str, Any]:
    # Shallow key clipping to keep preview lightweight
    out: Dict[str, Any] = {}
    for i, (k, v) in enumerate(d.items()):
        if i >= limit:
            out["..."] = f"+{len(d) - limit} more"
            break
        out[k] = v
    return out


def _safe_json(v: Any) -> Any:
    try:
        json.dumps(v, ensure_ascii=False)
        return v
    except Exception:
        return str(v)


# ------------------------- validators -------------------------

def _validate_change(change: Dict[str, Any]) -> Tuple[bool, str]:
    path = change.get("path")
    if not isinstance(path, str) or not path.strip():
        return False, "invalid_path"
    ctype = change.get("change_type")
    if ctype not in {"tighten", "relax", "retune", "set"}:
        return False, "invalid_change_type"
    if "new_value" not in change:
        return False, "missing_value"
    # Bounds check if provided
    b = change.get("bounds")
    if isinstance(b, dict) and ("min" in b or "max" in b):
        mn = b.get("min", None)
        mx = b.get("max", None)
        nv = change["new_value"]
        try:
            if (mn is not None and nv < mn) or (mx is not None and nv > mx):
                return False, "out_of_bounds"
        except Exception:
            return False, "bounds_type_error"
    return True, "ok"


def _validate_guards(delta: Dict[str, Any]) -> Tuple[bool, str]:
    guards = delta.get("guards", {}) if isinstance(delta.get("guards"), dict) else {}
    ttl = _get(guards, ["ttl", "seconds"], None)
    created = _get(delta, ["meta", "created_at"], None)
    if isinstance(ttl, (int, float)) and isinstance(created, str):
        created_dt = _iso_to_dt(created)
        if created_dt and (_now_z() - created_dt).total_seconds() > float(ttl):
            return False, "ttl_expired"
    return True, "ok"


# ------------------------- diff/plan -------------------------

def _plan_apply(current: Dict[str, Any], delta_changes: List[Dict[str, Any]], max_changes: Optional[int]) -> Tuple[
    List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    ops: List[Dict[str, Any]] = []
    diff_set: Dict[str, Dict[str, Any]] = {}

    budget = int(max_changes) if isinstance(max_changes, (int, float)) else None
    used = 0

    for ch in delta_changes:
        ok, reason = _validate_change(ch)
        if not ok:
            rejected.append({**ch, "reason": reason})
            continue
        if budget is not None and used >= budget:
            rejected.append({**ch, "reason": "over_max_changes"})
            continue

        path = ch["path"]
        new_val = _safe_json(ch["new_value"])
        old_val = _get_path(current, path, default=None)

        if old_val == new_val:
            # No-op; skip but mark accepted-nop
            accepted.append({**ch, "note": "noop"})
            continue

        # Prepare op
        op = {"op": "set", "path": path, "value": new_val}
        ops.append(op)
        accepted.append(ch)
        diff_set[path] = {"old": old_val, "new": new_val}
        # Mutate a shadow copy
        _set_path(current, path, new_val)
        used += 1

    preview_config = _clip_preview(current)
    preview_diff = {"set": diff_set, "changed_keys": list(diff_set.keys())}

    plan = {
        "accepted": accepted,
        "rejected": rejected,
        "ops": ops,
        "preview": {"config": preview_config, "diff": preview_diff},
    }
    return accepted, rejected, plan


# ------------------------- main -------------------------

def b10f2_plan_policy_apply(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B10F2 — Adaptation.PolicyApplyPlanner (Noema)

    Input:
      {
        "policy": {
          "current": { ... },                     # current policy/config (dict)
          "delta": {
            "changes": [ {path, new_value, change_type, rationale, confidence, bounds?}, ... ],
            "guards": { "max_changes": int?, "ttl": {"seconds": int}? },
            "meta": { "created_at": iso8601? }
          }
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "policy": {
          "apply_plan": {
            "accepted": [change, ...],
            "rejected": [change+reason, ...],
            "ops": [ {"op":"set","path":str,"value":Any}, ... ],
            "preview": { "config": {...}, "diff": {"set": {path:{old,new}}, "changed_keys":[...]} },
            "meta": { "source":"B10F2", "rules_version":"1.0" }
          }
        },
        "diag": { "reason": "ok|no_delta|ttl_expired|empty", "counts": { "accepted": int, "rejected": int, "ops": int } }
      }
    """
    pol = input_json.get("policy", {}) if isinstance(input_json.get("policy"), dict) else {}
    cur = pol.get("current", {}) if isinstance(pol.get("current"), dict) else {}
    delta = pol.get("delta", {}) if isinstance(pol.get("delta"), dict) else {}

    changes = delta.get("changes", [])
    if not isinstance(changes, list) or not changes:
        return {
            "status": "SKIP",
            "policy": {"apply_plan": {"accepted": [], "rejected": [], "ops": [],
                                      "preview": {"config": {}, "diff": {"set": {}, "changed_keys": []}},
                                      "meta": {"source": "B10F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_delta", "counts": {"accepted": 0, "rejected": 0, "ops": 0}},
        }

    g_ok, g_reason = _validate_guards(delta)
    if not g_ok:
        return {
            "status": "SKIP",
            "policy": {"apply_plan": {"accepted": [], "rejected": [{"reason": g_reason}], "ops": [],
                                      "preview": {"config": {}, "diff": {"set": {}, "changed_keys": []}},
                                      "meta": {"source": "B10F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": g_reason, "counts": {"accepted": 0, "rejected": len(changes), "ops": 0}},
        }

    # Work on a shadow copy of current policy
    current_copy = json.loads(json.dumps(cur, ensure_ascii=False))

    accepted, rejected, plan = _plan_apply(current_copy, changes, delta.get("guards", {}).get("max_changes"))

    out = {
        "status": "OK",
        "policy": {
            "apply_plan": {
                **plan,
                "meta": {"source": "B10F2", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok" if plan["ops"] else "empty",
                 "counts": {"accepted": len(accepted), "rejected": len(rejected), "ops": len(plan["ops"])}},
    }
    return out


if __name__ == "__main__":
    sample = {
        "policy": {
            "current": {
                "dialog": {"surface": {"max_len": 800}},
                "safety_filter": {"max_out_len": 1200},
                "executor": {"timeout_ms": 30000, "retries": {"max": 2}},
                "guardrails": {"must_confirm": {"u_threshold": 0.4}}
            },
            "delta": {
                "changes": [
                    {"path": "dialog.surface.max_len", "new_value": 720, "change_type": "tighten",
                     "rationale": "reduce length", "confidence": 0.7, "bounds": {"min": 400, "max": 2000}},
                    {"path": "executor.retries.max", "new_value": 3, "change_type": "relax", "rationale": "error rate",
                     "confidence": 0.6, "bounds": {"min": 0, "max": 6}},
                    {"path": "guardrails.must_confirm.u_threshold", "new_value": 0.35, "change_type": "tighten",
                     "rationale": "more conservative", "confidence": 0.64, "bounds": {"min": 0.25, "max": 0.7}}
                ],
                "guards": {"max_changes": 5, "ttl": {"seconds": 3600}},
                "meta": {"created_at": "2025-11-07T09:00:00Z"}
            }
        }
    }
    res = b10f2_plan_policy_apply(sample)
    print(res["diag"])
    print(res["policy"]["apply_plan"]["ops"])
    print(res["policy"]["apply_plan"]["preview"]["diff"]["changed_keys"])
