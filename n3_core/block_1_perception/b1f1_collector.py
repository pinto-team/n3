# Folder: noema/n3_core/block_1_perception
# File:   b1f1_collector.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

__all__ = ["b1f1_collect"]


def _validate_events(evs: Any) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(evs, list):
        return None
    ok = []
    for e in evs:
        if isinstance(e, dict) and isinstance(e.get("type"), str):
            ok.append(e)
    return ok if ok else None


def _last_commit(evs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # انتخاب آخرین message_commit
    for e in reversed(evs):
        if e.get("type") == "message_commit":
            return e
    return None


def _typing_trace(evs: List[Dict[str, Any]]) -> List[str]:
    kinds = {"typing_start": "typing_start", "typing_stop": "typing_stop"}
    trace: List[str] = []
    for e in evs:
        k = kinds.get(e.get("type"))
        if k:
            trace.append(k)
    return trace


def b1f1_collect(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F1 — Perception.Text.Collector (Noema)
    ورودی:
      { "events": [ { "type": "typing_start"|"typing_stop"|"message_commit", "text"?, "t"? }, ... ] }
    خروجی (دقیقاً مطابق قرارداد Noema):
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "raw_text": "...",                # فقط در حالت OK
          "events": ["typing_start", ... , "commit"]?,
          "meta": { "source":"B1F1", "commit_time": "ISO8601"?, "truncated": False }
        },
        "diag": { "reason": "ok|no_events|no_commit|invalid_text_type", "len_raw_text"?: int }
      }
    نکته: این طبقه متن را تغییر نمی‌دهد (هیچ نرمال‌سازی/برش) — مسئولیت آن با B1F2 است.
    """
    evs = input_json.get("events", None)
    evs = _validate_events(evs)
    if evs is None:
        return {
            "status": "SKIP",
            "perception": {"events": [], "meta": {"source": "B1F1", "truncated": False}},
            "diag": {"reason": "no_events"},
        }

    last = _last_commit(evs)
    if not last:
        return {
            "status": "SKIP",
            "perception": {"events": _typing_trace(evs), "meta": {"source": "B1F1", "truncated": False}},
            "diag": {"reason": "no_commit"},
        }

    txt = last.get("text", None)
    if not isinstance(txt, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    out_events = _typing_trace(evs) + ["commit"]

    return {
        "status": "OK",
        "perception": {
            "raw_text": txt,
            "events": out_events,
            "meta": {"source": "B1F1", "commit_time": last.get("t"), "truncated": False},
        },
        "diag": {"reason": "ok", "len_raw_text": len(txt)},
    }


if __name__ == "__main__":
    sample = {
        "events": [
            {"type": "typing_start", "t": "."},
            {"type": "message_commit", "text": "سلام نوما", "t": "2025-11-05T12:00:00Z"},
        ]
    }
    print(b1f1_collect(sample))
