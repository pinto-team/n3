# Folder: noema/n3_core/block_3_memory
# File:   b3f4_context_cache.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

__all__ = ["b3f4_context_cache"]

RULES_VERSION = "1.0"
MAX_RECENT = 6
SNIPPET_MAX = 160


# ------------------------- helpers -------------------------

def _iso_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_commit_time(ts: Optional[str]) -> Optional[datetime]:
    if not isinstance(ts, str) or not ts:
        return None
    t = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None


def _best_snippet(text: str, max_len: int = SNIPPET_MAX) -> str:
    t = (text or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def _packz_like_to_entry(x: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Accept both raw packz or {packz: {...}}
    if "packz" in x and isinstance(x["packz"], dict):
        x = x["packz"]
    if not isinstance(x, dict):
        return None
    pid = x.get("id") if isinstance(x.get("id"), str) else None
    text = x.get("text") if isinstance(x.get("text"), str) else None
    if not pid or not text:
        return None
    sig = x.get("signals", {}) if isinstance(x.get("signals"), dict) else {}
    meta = x.get("meta", {}) if isinstance(x.get("meta"), dict) else {}
    return {
        "id": pid,
        "text": _best_snippet(text),
        "signals": {"direction": sig.get("direction"), "speech_act": sig.get("speech_act")},
        "meta": {"commit_time": meta.get("commit_time")},
    }


def _collect_current_packz(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    per = inp.get("perception", {})
    if isinstance(per, dict) and isinstance(per.get("packz"), dict):
        return _packz_like_to_entry(per["packz"])
    if isinstance(inp.get("packz"), dict):
        return _packz_like_to_entry(inp["packz"])
    return None


def _collect_recent_existing(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    # From context.recent_packz or memory.retrieved_packz (best-effort)
    out: List[Dict[str, Any]] = []
    ctx = inp.get("context", {})
    if isinstance(ctx, dict) and isinstance(ctx.get("recent_packz"), list):
        for it in ctx["recent_packz"]:
            if isinstance(it, dict):
                ent = _packz_like_to_entry(it)
                if ent:
                    out.append(ent)
    mem = inp.get("memory", {})
    if isinstance(mem, dict) and isinstance(mem.get("retrieved_packz"), list):
        for it in mem["retrieved_packz"]:
            if isinstance(it, dict):
                ent = _packz_like_to_entry(it)
                if ent:
                    out.append(ent)
    return out


def _dedupe_sort_clip(entries: List[Dict[str, Any]], keep: int = MAX_RECENT) -> Tuple[List[Dict[str, Any]], List[str]]:
    # Deduplicate by id, keep most recent by commit_time, sort ascending then take last K
    by_id: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        pid = e.get("id")
        if not isinstance(pid, str):
            continue
        prev = by_id.get(pid)
        if not prev:
            by_id[pid] = e
            continue
        # keep the one with newer commit_time
        t_new = _parse_commit_time(e.get("meta", {}).get("commit_time"))
        t_old = _parse_commit_time(prev.get("meta", {}).get("commit_time"))
        if (t_new or datetime.min) >= (t_old or datetime.min):
            by_id[pid] = e

    arr = list(by_id.values())
    arr.sort(key=lambda x: _parse_commit_time(x.get("meta", {}).get("commit_time")) or datetime.min)
    evicted: List[str] = []
    if len(arr) > keep:
        evicted = [x["id"] for x in arr[:-keep]]
        arr = arr[-keep:]
    return arr, evicted


def _retrieval_summary(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mem = inp.get("memory", {})
    if not isinstance(mem, dict):
        return None
    ret = mem.get("retrieval", {})
    if not isinstance(ret, dict):
        return None
    res = ret.get("results")
    if not isinstance(res, list) or not res:
        return None
    topk = int(ret.get("top_k", len(res))) if isinstance(ret.get("top_k"), int) else len(res)
    # Keep only ids and scores to remain compact
    out = [{"id": r.get("id"), "score": float(r.get("score", 0.0))} for r in res if
           isinstance(r, dict) and isinstance(r.get("id"), str)]
    if not out:
        return None
    return {"top_k": topk, "items": out[:topk]}


def _namespace(inp: Dict[str, Any]) -> str:
    sess = inp.get("session", {})
    if isinstance(sess, dict) and isinstance(sess.get("thread_id"), str) and sess.get("thread_id"):
        return f"cache/noema/{sess['thread_id']}"
    return "cache/noema/default"


# ------------------------- main -------------------------

def b3f4_context_cache(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B3F4 — Memory.ContextCache (Noema)
    Produces cache operations (pure, no I/O) to maintain a small LRU of recent PackZ frames
    and the last retrieval summary. The actual persistence layer applies these ops.

    Input (best-effort):
      {
        "perception": { "packz": {...} }?,
        "context": { "recent_packz": [packz_like, ...] }?,
        "memory": { "retrieved_packz": [packz_like, ...]?, "retrieval": { "results": [...], "top_k": int? }? },
        "session": { "thread_id": "optional" }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "memory": {
          "context_cache": {
            "namespace": "cache/noema/<thread_id|default>",
            "ops": [
              { "op": "put",   "key": "recent_packz",  "value": [ {id, text, signals{direction,speech_act}, meta{commit_time}}, ... ] },
              { "op": "put",   "key": "last_retrieval","value": { "top_k": int, "items": [ {id, score}, ... ] } }?,
              { "op": "touch", "key": "last_seen_at",  "value": "ISO8601Z" },
              { "op": "evict", "key": "evicted_ids",   "value": [id, ...] }?
            ],
            "meta": { "source": "B3F4", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_input", "counts": { "recent": int, "evicted": int, "retrieval_items": int } }
      }
    """
    ns = _namespace(input_json)

    current = _collect_current_packz(input_json)
    existing = _collect_recent_existing(input_json)

    entries: List[Dict[str, Any]] = []
    if existing:
        entries.extend(existing)
    if current:
        entries.append(current)

    # If nothing to cache and no retrieval summary → SKIP
    ret_sum = _retrieval_summary(input_json)
    if not entries and not ret_sum:
        return {
            "status": "SKIP",
            "memory": {"context_cache": {"namespace": ns, "ops": [],
                                         "meta": {"source": "B3F4", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_input", "counts": {"recent": 0, "evicted": 0, "retrieval_items": 0}},
        }

    recent, evicted = _dedupe_sort_clip(entries, keep=MAX_RECENT)

    ops: List[Dict[str, Any]] = [
        {"op": "put", "key": "recent_packz", "value": recent},
        {"op": "touch", "key": "last_seen_at", "value": _iso_now_z()},
    ]
    if ret_sum:
        ops.append({"op": "put", "key": "last_retrieval", "value": ret_sum})
    if evicted:
        ops.append({"op": "evict", "key": "evicted_ids", "value": evicted})

    return {
        "status": "OK",
        "memory": {
            "context_cache": {
                "namespace": ns,
                "ops": ops,
                "meta": {"source": "B3F4", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {"recent": len(recent), "evicted": len(evicted),
                       "retrieval_items": len(ret_sum["items"]) if ret_sum else 0},
        },
    }


if __name__ == "__main__":
    sample = {
        "session": {"thread_id": "t-42"},
        "perception": {
            "packz": {
                "id": "cur123",
                "text": "سلام نوما، لطفاً این پیام را هم به حافظهٔ اخیر اضافه کن.",
                "signals": {"direction": "rtl", "speech_act": "request"},
                "meta": {"commit_time": "2025-11-07T10:15:00Z"}
            }
        },
        "context": {
            "recent_packz": [
                {
                    "packz": {
                        "id": "old1",
                        "text": "جلسهٔ قبل دربارهٔ ایندکس حرف زدیم.",
                        "signals": {"direction": "rtl", "speech_act": "statement"},
                        "meta": {"commit_time": "2025-11-06T18:00:00Z"}
                    }
                }
            ]
        },
        "memory": {
            "retrieval": {
                "top_k": 2,
                "results": [
                    {"id": "d2", "score": 0.81, "snippet": "خلاصه معماری Noema...", "facets": {"dir": "rtl"}},
                    {"id": "d1", "score": 0.64}
                ]
            }
        }
    }
    out = b3f4_context_cache(sample)
    print(out["memory"]["context_cache"]["namespace"])
    for op in out["memory"]["context_cache"]["ops"]:
        print(op["op"], op["key"])
