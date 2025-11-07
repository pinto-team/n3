# Folder: noema/n3_core/block_7_execution
# File:   b7f3_result_presenter.py

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b7f3_present_results"]

RULES_VERSION = "1.0"
MAX_ROWS = 6
MAX_COLS = 8
MAX_TEXT = 1200


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


def _trim(s: str, n: int = MAX_TEXT) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _is_table_like(obj: Any) -> bool:
    if isinstance(obj, list) and obj and all(isinstance(r, dict) for r in obj):
        keys0 = set(obj[0].keys())
        if keys0 and all(set(r.keys()) == keys0 for r in obj[: min(10, len(obj))]):
            return True
    return False


def _detect_lang(hint_text: str, dir_hint: Optional[str]) -> str:
    if isinstance(dir_hint, str) and dir_hint.lower() == "rtl":
        return "fa"
    # Basic script hint
    for ch in hint_text or "":
        cp = ord(ch)
        if (0x0600 <= cp <= 0x06FF) or (0x0750 <= cp <= 0x08FF) or (0xFB50 <= cp <= 0xFEFF):
            return "fa"
    for ch in hint_text or "":
        if ("A" <= ch <= "Z") or ("a" <= ch <= "z"):
            return "en"
    return "und"


def _mk_table_md(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    cols = list(rows[0].keys())[:MAX_COLS]
    head = "| " + " | ".join(str(c)[:40] for c in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body_lines = []
    for r in rows[:MAX_ROWS]:
        body_lines.append("| " + " | ".join(_trim(str(r.get(c, "")), 80) for c in cols) + " |")
    return "\n".join([head, sep] + body_lines)


def _mk_attachments_list(atts: List[Dict[str, Any]], lang: str) -> str:
    if not atts:
        return ""
    lines = []
    label = "ضمیمه‌ها" if lang == "fa" else "Attachments"
    for i, a in enumerate(atts[:12]):
        t = a.get("type", "file")
        ref = a.get("ref", a.get("url", ""))
        lines.append(f"- {t}: {ref}")
    return f"\n\n{label}:\n" + "\n".join(lines)


def _compose_from_item(item: Dict[str, Any], lang: str) -> Tuple[str, str]:
    """
    Returns (move, content)
    """
    ok = bool(item.get("ok", True))
    kind = item.get("kind", "text")
    text = item.get("text") if isinstance(item.get("text"), str) else ""
    data = item.get("data")
    atts = item.get("attachments") if isinstance(item.get("attachments"), list) else []

    if not ok:
        msg = text or ("Operation failed." if lang != "fa" else "اجرا با خطا مواجه شد.")
        return ("answer", _trim(msg))

    if kind in {"text", "markdown", "url"}:
        body = text or ("Done." if lang != "fa" else "انجام شد.")
        body += _mk_attachments_list(atts, lang)
        return ("answer", _trim(body))

    if kind in {"json", "table"}:
        if _is_table_like(data):
            md = _mk_table_md(data)
            lead = "Top results:" if lang != "fa" else "خلاصه نتایج:"
            body = f"{lead}\n\n{md}"
        else:
            # compact JSON-as-text already provided in item["text"]
            body = text if text else ("Result is ready." if lang != "fa" else "نتیجه آماده است.")
        body += _mk_attachments_list(atts, lang)
        return ("answer", _trim(body))

    if kind in {"image", "audio", "video", "binary"}:
        hint = text or ("Result is available." if lang != "fa" else "نتیجه آماده است.")
        body = hint + _mk_attachments_list(atts, lang)
        return ("answer", _trim(body))

    # Fallback
    body = text or ("Result ready." if lang != "fa" else "نتیجه آماده است.")
    body += _mk_attachments_list(atts, lang)
    return ("answer", _trim(body))


# ------------------------- main -------------------------

def b7f3_present_results(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B7F3 — Execution.ResultPresenter (Noema)

    Input:
      {
        "executor": {
          "results": {
            "items": [ {normalized item}, ... ],
            "best": {normalized item}?,
            "aggregate": {...}
          }
        },
        "planner": { "plan": {"skill_name": str}? }?,
        "world_model": { "context": { "features": { "dir": "rtl|ltr" } } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "dialog": {
          "turn": {
            "move": "answer|ack|refuse",
            "content": str,
            "attachments": [ {type, ref, ...}, ... ]?,
            "meta": { "source": "B7F3", "rules_version": "1.0", "skill_name": str? }
          }
        },
        "diag": { "reason": "ok|no_results|empty", "counts": { "items": int } }
      }
    """
    res = _get(input_json, ["executor", "results"], {})
    if not isinstance(res, dict):
        return {"status": "SKIP", "dialog": {"turn": {}}, "diag": {"reason": "no_results", "counts": {"items": 0}}}

    items = res.get("items") if isinstance(res.get("items"), list) else []
    best = res.get("best") if isinstance(res.get("best"), dict) else (items[0] if items else None)
    if not best:
        return {"status": "SKIP", "dialog": {"turn": {}}, "diag": {"reason": "no_results", "counts": {"items": 0}}}

    skill_name = _get(input_json, ["planner", "plan", "skill_name"], None)
    dir_hint = _get(input_json, ["world_model", "context", "features", "dir"], None)
    lang = _detect_lang(best.get("text", ""), dir_hint)

    move, content = _compose_from_item(best, lang)
    atts = best.get("attachments") if isinstance(best.get("attachments"), list) else []

    # If content is empty after composition, acknowledge softly
    if not content:
        move, content = ("ack", "باشه." if lang == "fa" else "Okay.")

    turn = {
        "move": move,
        "content": content,
        "attachments": atts[:12] if atts else [],
        "meta": {"source": "B7F3", "rules_version": RULES_VERSION, "skill_name": skill_name},
    }

    return {
        "status": "OK",
        "dialog": {"turn": turn},
        "diag": {"reason": "ok", "counts": {"items": len(items)}},
    }


if __name__ == "__main__":
    # Demo A: table-like data
    sample = {
        "executor": {
            "results": {
                "items": [
                    {"ok": True, "kind": "json",
                     "data": [{"title": "A", "value": 1}, {"title": "B", "value": 2}, {"title": "C", "value": 3}],
                     "attachments": []}
                ],
                "best": {"ok": True, "kind": "json",
                         "data": [{"title": "A", "value": 1}, {"title": "B", "value": 2}, {"title": "C", "value": 3}]}
            }
        },
        "world_model": {"context": {"features": {"dir": "rtl"}}},
        "planner": {"plan": {"skill_name": "Web Summarizer"}}
    }
    out = b7f3_present_results(sample)
    print(out["dialog"]["turn"]["move"])
    print(out["dialog"]["turn"]["content"])
