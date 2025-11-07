# Folder: noema/n3_core/block_13_drivers
# File:   b13f2_driver_reply_normalizer.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b13f2_normalize_driver_replies"]

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


def _num(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _trim(s: Optional[str], n: int = 1200) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else (s[: n - 1] + "…")


def _bool(x: Any) -> bool:
    return bool(x) if isinstance(x, bool) else False


# ------------------------- normalizers -------------------------

def _norm_transport(rep: Dict[str, Any]) -> Dict[str, Any]:
    msgs = _get(rep, ["messages"], []) or _get(rep, ["payload", "messages"], []) or []
    delivered = [m for m in msgs if isinstance(m, dict)]
    return {
        "transport": {
            "outbound": {
                "delivered": len(delivered),
                "ids": [m.get("id") for m in delivered if isinstance(m.get("id"), str)],
                "channel": _get(rep, ["channel"], "default"),
                "ok": _bool(rep.get("ok", True)),
            }
        }
    }


def _norm_skills(rep: Dict[str, Any]) -> Dict[str, Any]:
    calls = _get(rep, ["calls"], []) or _get(rep, ["results"], []) or []
    items: List[Dict[str, Any]] = []
    total_cost = 0.0
    lat_sum = 0.0
    ok_n = err_n = 0

    for c in calls:
        if not isinstance(c, dict):
            continue
        ok = _bool(c.get("ok", True))
        kind = c.get("kind") or ("json" if isinstance(c.get("data"), (dict, list)) else "text")
        text = c.get("text") if isinstance(c.get("text"), str) else ""
        data = c.get("data") if isinstance(c.get("data"), (dict, list)) else None
        usage = c.get("usage") if isinstance(c.get("usage"), dict) else {}
        dur = _num(c.get("latency_ms") or c.get("duration_ms"), 0.0)
        cost = _num(usage.get("cost"), 0.0)

        item = {
            "ok": ok,
            "kind": kind,
            "text": _trim(text) if text else "",
            "data": data,
            "attachments": c.get("attachments", []) if isinstance(c.get("attachments"), list) else [],
            "usage": usage,
            "duration_ms": int(dur),
            "score": _num(c.get("score"), 0.0),
            "req_id": c.get("req_id"),
        }
        items.append(item)

        total_cost += cost
        lat_sum += dur
        if ok:
            ok_n += 1
        else:
            err_n += 1

    count = ok_n + err_n
    agg = {
        "count": count,
        "ok": ok_n,
        "errors": err_n,
        "avg_latency_ms": (lat_sum / count) if count else 0.0,
        "total_cost": total_cost,
    }

    return {"executor": {"results": {"items": items, "aggregate": agg, "best": (items[0] if items else {})}}}


def _norm_storage(rep: Dict[str, Any]) -> Dict[str, Any]:
    apply_ops = _get(rep, ["apply", "ops"], []) or _get(rep, ["apply_ops"], []) or []
    idx_items = _get(rep, ["index", "queue"], []) or _get(rep, ["index_queue"], []) or []
    return {
        "storage": {
            "apply_result": {"ok": _bool(rep.get("ok", True)),
                             "ops": len([x for x in apply_ops if isinstance(x, dict)])},
            "index_result": {"ok": True, "items": len([x for x in idx_items if isinstance(x, dict)])},
        }
    }


def _norm_timer(rep: Dict[str, Any]) -> Dict[str, Any]:
    ms = int(_num(rep.get("sleep_ms") or _get(rep, ["payload", "ms"]), 0))
    return {"timers": {"sleep": {"ms": ms, "ok": _bool(rep.get("ok", True))}}}


# ------------------------- main -------------------------

def b13f2_normalize_driver_replies(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B13F2 — Drivers.ReplyNormalizer (Noema)
    Normalizes driver replies into core artifacts: executor.results, transport acks, storage/index results, timers.
    Pure; no I/O.

    Input:
      {
        "driver": {
          "replies": [
            { "type":"transport", "ok": bool, "messages":[...] }?,
            { "type":"skills", "ok": bool, "calls":[ {ok, req_id, kind?, text?, data?, usage?, latency_ms?, score?, attachments?}, ... ] }?,
            { "type":"storage", "ok": bool, "apply": {"ops":[...]}, "index": {"queue":[...]} }?,
            { "type":"timer", "ok": bool, "sleep_ms": int }?
          ]
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "executor": {"results": {"items":[...], "aggregate": {...}, "best": {...}} }?,
        "transport": {"outbound": {"delivered": int, "ids":[...], "channel": str, "ok": bool}}?,
        "storage": {"apply_result": {...}, "index_result": {...}}?,
        "timers": {"sleep": {"ms": int, "ok": bool}}?,
        "diag": { "reason": "ok|no_replies" }
      }
    """
    reps = _get(input_json, ["driver", "replies"], [])
    if not isinstance(reps, list) or not reps:
        return {"status": "SKIP", "diag": {"reason": "no_replies"}}

    out: Dict[str, Any] = {"status": "OK", "diag": {"reason": "ok"}}

    # Aggregate across replies; last-wins per subsystem.
    for rep in reps:
        if not isinstance(rep, dict):
            continue
        typ = (_cf(rep.get("type")) if isinstance(rep.get("type"), str) else "")
        if typ == "transport":
            out.update(_norm_transport(rep))
        elif typ == "skills":
            out.update(_norm_skills(rep))
        elif typ == "storage":
            out.update(_norm_storage(rep))
        elif typ == "timer":
            out.update(_norm_timer(rep))
        else:
            # ignore unknown types safely
            continue

    return out


if __name__ == "__main__":
    # Demo
    sample = {
        "driver": {
            "replies": [
                {"type": "transport", "ok": True, "channel": "default",
                 "messages": [{"id": "m1", "role": "assistant", "text": "Done."}]},
                {"type": "skills", "ok": True, "calls": [
                    {"ok": True, "req_id": "r1", "kind": "json", "data": [{"title": "A", "value": 1}],
                     "usage": {"cost": 0.0003}, "latency_ms": 520},
                    {"ok": False, "req_id": "r2", "text": "failed", "usage": {"cost": 0.0}, "latency_ms": 200}
                ]},
                {"type": "storage", "ok": True, "apply": {"ops": [{"op": "put"}]},
                 "index": {"queue": [{"type": "packz"}]}},
                {"type": "timer", "ok": True, "sleep_ms": 180}
            ]
        }
    }
    out = b13f2_normalize_driver_replies(sample)
    print(out["executor"]["results"]["aggregate"])
    print(out["transport"]["outbound"])
    print(out["storage"]["apply_result"], out["storage"]["index_result"])
    print(out["timers"]["sleep"])
