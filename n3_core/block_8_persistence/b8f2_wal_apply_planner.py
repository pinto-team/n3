# Folder: noema/n3_core/block_8_persistence
# File:   b8f2_wal_apply_planner.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b8f2_plan_apply"]

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


def _iso_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha1(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _ns(inp: Dict[str, Any]) -> str:
    tid = _get(inp, ["session", "thread_id"], "") or "default"
    return f"store/noema/{tid}"


def _kv_key(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if isinstance(p, str) and p)


def _next_seq(start_seq: Optional[int]) -> int:
    return int(start_seq) + 1 if isinstance(start_seq, int) else 0


def _clip_text(s: Optional[str], n: int = 4000) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else (s[: n - 1] + "…")


# ------------------------- collectors -------------------------

def _collect_wal(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    wal = _get(inp, ["memory", "wal", "ops"], [])
    return [op for op in wal if isinstance(op, dict)]


def _last_seq(inp: Dict[str, Any]) -> Optional[int]:
    st = _get(inp, ["storage", "last_seq"], None)
    return int(st) if isinstance(st, int) else None


# ------------------------- planners -------------------------

def _plan_put_turn(turn: Dict[str, Any], ns: str, seq: Optional[int]) -> Tuple[
    List[Dict[str, Any]], List[Dict[str, Any]]]:
    ops: List[Dict[str, Any]] = []
    idx: List[Dict[str, Any]] = []

    tid = str(turn.get("id", "")) or _sha1(turn)
    role = str(turn.get("role", ""))
    key = _kv_key(ns, "turns", tid)

    # Put turn document
    doc = {
        "id": tid,
        "role": role,
        "text": _clip_text(turn.get("text", "")),
        "lang": turn.get("lang"),
        "move": turn.get("move"),
        "time": turn.get("time") or _iso_now_z(),
        "plan": turn.get("plan", None),
    }
    ops.append({"op": "put", "key": key, "value": doc, "seq": seq})

    # Optional: index PackZ for search
    packz = turn.get("packz")
    if isinstance(packz, dict):
        idx.append({
            "type": "packz",
            "id": tid,
            "text": packz.get("text", ""),
            "signals": packz.get("signals", {}),
            "meta": packz.get("meta", {}),
            "ns": ns
        })

    return ops, idx


def _plan_put_result(res: Dict[str, Any], link_turn_id: Optional[str], ns: str, seq: Optional[int]) -> List[
    Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    rid = str(res.get("req_id") or _sha1(res))
    key = _kv_key(ns, "results", rid)
    doc = {
        "req_id": rid,
        "ok": bool(res.get("ok", True)),
        "kind": res.get("kind"),
        "text": _clip_text(res.get("text", "")),
        "attachments": res.get("attachments", []),
        "usage": res.get("usage", {}),
        "duration_ms": res.get("duration_ms", 0),
        "score": res.get("score", 0.0),
        "time": _iso_now_z(),
    }
    ops.append({"op": "put", "key": key, "value": doc, "seq": seq})

    if link_turn_id:
        ops.append({
            "op": "link",
            "key": _kv_key(ns, "links", "assistant_turn_to_result"),
            "value": {"assistant_turn_id": link_turn_id, "result_req_id": rid},
            "seq": _next_seq(seq) if isinstance(seq, int) else None
        })
    return ops


def _plan_inc_counters(counters: Dict[str, Any], ns: str, seq: Optional[int]) -> List[Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    for k, v in counters.items():
        if not isinstance(v, (int, float)):
            continue
        ops.append({"op": "inc", "key": _kv_key(ns, "counters", k), "delta": int(v), "seq": seq})
        if isinstance(seq, int):
            seq += 1
    return ops


def _plan_concept_version(op: Dict[str, Any], ns: str, seq: Optional[int]) -> List[Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    version = op.get("doc") if isinstance(op.get("doc"), dict) else {}
    ver_id = version.get("id") if isinstance(version.get("id"), str) else None
    if not ver_id:
        return ops
    updates = op.get("updates") if isinstance(op.get("updates"), dict) else {}
    ops.append({"op": "put", "key": _kv_key(ns, "concept", "versions", ver_id), "value": version, "seq": seq})
    if isinstance(seq, int):
        seq += 1
    ops.append({"op": "put", "key": _kv_key(ns, "concept", "updates", ver_id), "value": updates, "seq": seq})
    if isinstance(seq, int):
        seq += 1
    ops.append({"op": "put", "key": _kv_key(ns, "concept", "current"),
                "value": {"version_id": ver_id, "updated_at": version.get("updated_at")}, "seq": seq})
    return ops


# ------------------------- main -------------------------

def b8f2_plan_apply(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B8F2 — Persistence.WALApplyPlanner (Noema)
    Transforms memory.wal.ops into storage.apply ops and index.queue items (pure; no I/O).

    Input:
      {
        "memory": { "wal": { "ops": [
            { "op":"append_turn", "turn": {...} } |
            { "op":"append_result", "result": {...}, "link": {"assistant_turn_id": str}? } |
            { "op":"bump_counters", "keys": {"turns": +1, ...} }
        ] } },
        "storage": { "last_seq": int? }?,
        "session": { "thread_id": str? }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "storage": {
          "apply": {
            "namespace": str,
            "ops": [ { "op":"put|inc|link", "key": str, "value|delta": ..., "seq": int? }, ... ],
            "meta": { "source": "B8F2", "rules_version": "1.0", "seq_start": int? }
          }
        },
        "index": {
          "queue": {
            "items": [ { "type":"packz", "id": str, "text": str, "signals": {...}, "meta": {...}, "ns": str }, ... ],
            "meta": { "source": "B8F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_wal", "counts": { "ops": int, "puts": int, "incs": int, "links": int, "index_items": int } }
      }
    """
    wal = _collect_wal(input_json)
    if not wal:
        return {
            "status": "SKIP",
            "storage": {"apply": {"namespace": _ns(input_json), "ops": [],
                                  "meta": {"source": "B8F2", "rules_version": RULES_VERSION}}},
            "index": {"queue": {"items": [], "meta": {"source": "B8F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_wal", "counts": {"ops": 0, "puts": 0, "incs": 0, "links": 0, "index_items": 0}},
        }

    ns = _ns(input_json)
    seq = _last_seq(input_json)
    seq_start = _next_seq(seq) if isinstance(seq, int) else None
    if isinstance(seq, int):
        seq = seq_start

    apply_ops: List[Dict[str, Any]] = []
    index_items: List[Dict[str, Any]] = []

    puts = incs = links = 0

    for op in wal:
        kind = op.get("op")
        if kind == "append_turn" and isinstance(op.get("turn"), dict):
            t_ops, idx = _plan_put_turn(op["turn"], ns, seq)
            apply_ops.extend(t_ops)
            index_items.extend(idx)
            puts += sum(1 for x in t_ops if x["op"] == "put")
            links += sum(1 for x in t_ops if x["op"] == "link")
            if isinstance(seq, int):
                seq += len(t_ops)

        elif kind == "append_result" and isinstance(op.get("result"), dict):
            link_turn_id = _get(op, ["link", "assistant_turn_id"], None)
            r_ops = _plan_put_result(op["result"], link_turn_id, ns, seq)
            apply_ops.extend(r_ops)
            puts += sum(1 for x in r_ops if x["op"] == "put")
            links += sum(1 for x in r_ops if x["op"] == "link")
            if isinstance(seq, int):
                seq += len(r_ops)

        elif kind == "bump_counters" and isinstance(op.get("keys"), dict):
            c_ops = _plan_inc_counters(op["keys"], ns, seq)
            apply_ops.extend(c_ops)
            incs += len(c_ops)
            if isinstance(seq, int):
                seq += len(c_ops)

        elif kind == "record_concept_version":
            cv_ops = _plan_concept_version(op, ns, seq)
            apply_ops.extend(cv_ops)
            puts += sum(1 for x in cv_ops if x.get("op") == "put")
            if isinstance(seq, int):
                seq += len(cv_ops)

        else:
            # Unknown WAL op: ignore safely
            continue

    out = {
        "status": "OK",
        "storage": {
            "apply": {
                "namespace": ns,
                "ops": apply_ops,
                "meta": {"source": "B8F2", "rules_version": RULES_VERSION, "seq_start": seq_start},
            }
        },
        "index": {
            "queue": {
                "items": index_items,
                "meta": {"source": "B8F2", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {
                "ops": len(apply_ops),
                "puts": puts,
                "incs": incs,
                "links": links,
                "index_items": len(index_items),
            }
        },
    }
    return out


if __name__ == "__main__":
    # Demo
    sample = {
        "session": {"thread_id": "t-42"},
        "storage": {"last_seq": 99},
        "memory": {
            "wal": {
                "ops": [
                    {"op": "append_turn", "turn": {
                        "id": "u1", "role": "user", "text": "Hi Noema", "lang": "en",
                        "move": "user_input", "time": "2025-11-07T09:00:00Z",
                        "packz": {"id": "u1", "text": "Hi Noema", "signals": {"direction": "ltr"},
                                  "meta": {"commit_time": "2025-11-07T09:00:00Z"}}
                    }},
                    {"op": "append_turn", "turn": {
                        "id": "a1", "role": "assistant", "text": "Done.", "lang": "en",
                        "move": "answer", "time": "2025-11-07T09:00:02Z",
                        "plan": {"plan_id": "p1", "skill_id": "skill.answer"},
                        "packz": {"id": "a1", "text": "Done.", "signals": {"direction": "ltr"},
                                  "meta": {"commit_time": "2025-11-07T09:00:02Z"}}
                    }},
                    {"op": "append_result", "result": {
                        "req_id": "r1", "ok": True, "kind": "json", "text": "{\"ok\":true}", "duration_ms": 420,
                        "score": 0.7
                    }, "link": {"assistant_turn_id": "a1"}},
                    {"op": "bump_counters", "keys": {"turns": 2, "assistant_answers": 1, "executions": 1}}
                ]
            }
        }
    }
    out = b8f2_plan_apply(sample)
    print(out["storage"]["apply"]["meta"])
    print(out["diag"])
    print(len(out["storage"]["apply"]["ops"]), "ops,", len(out["index"]["queue"]["items"]), "index items")
