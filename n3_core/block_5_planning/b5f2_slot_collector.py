# Folder: noema/n3_core/block_5_planning
# File:   b5f2_slot_collector.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Optional

import unicodedata

__all__ = ["b5f2_collect_slots"]

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


def _safe_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _merge_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a or {})
    for k, v in (b or {}).items():
        if k not in out and v is not None:
            out[k] = v
    return out


# ------------------------- collectors -------------------------

def _collect_intent(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pl = inp.get("planner", {})
    if isinstance(pl, dict) and isinstance(pl.get("intent"), dict) and pl["intent"]:
        return pl["intent"]
    return None


def _collect_entities(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    ent = _get(inp, ["perception", "entities"], []) or _get(inp, ["entities"], [])
    return [e for e in ent if isinstance(e, dict)]


def _collect_text(inp: Dict[str, Any]) -> str:
    return (
            _get(inp, ["perception", "packz", "text"], "")
            or _get(inp, ["perception", "normalized_text"], "")
            or _get(inp, ["text"], "")
            or ""
    )


def _collect_defaults(inp: Dict[str, Any], skill_id: str) -> Dict[str, Any]:
    plan = inp.get("planning", {})
    if not isinstance(plan, dict):
        return {}
    defs = plan.get("skill_defaults", {})
    if isinstance(defs, dict) and isinstance(defs.get(skill_id), dict):
        return defs[skill_id]
    return {}


def _uncertainty(inp: Dict[str, Any]) -> Tuple[float, str]:
    u = _get(inp, ["world_model", "uncertainty", "score"], 0.0)
    rec = _get(inp, ["world_model", "uncertainty", "recommendation"], "") or ""
    return _safe_float(u, 0.0), str(rec)


def _reply_and_sa(inp: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    top = _get(inp, ["world_model", "prediction", "top"], "") or ""
    sa = _get(inp, ["world_model", "context", "features", "speech_act"], None)
    return str(top), sa if isinstance(sa, str) else None


# ------------------------- heuristics -------------------------

_RE_URL = re.compile(r"(https?://\S+)")
_RE_EMAIL = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
_RE_NUMBER = re.compile(r"\b\d+(\.\d+)?\b")
_RE_FILEPATH = re.compile(r"([A-Za-z]:\\[^\s]+|/[^ \n\t]+)")


def _infer_from_text(slot: str, text: str) -> Optional[str]:
    name = _cf(slot)
    if name in {"url", "link"}:
        m = _RE_URL.search(text)
        return m.group(1) if m else None
    if name in {"email"}:
        m = _RE_EMAIL.search(text)
        return m.group(0) if m else None
    if name in {"path", "filepath", "file"}:
        m = _RE_FILEPATH.search(text)
        return m.group(1) if m else None
    if name in {"count", "k", "n", "limit"}:
        m = _RE_NUMBER.search(text)
        return m.group(0) if m else None
    if name in {"language", "lang"}:
        # simple language hint
        for ch in text:
            cp = ord(ch)
            if 0x0600 <= cp <= 0x06FF:
                return "fa"
        return "en" if any("A" <= ch <= "Z" or "a" <= ch <= "z" for ch in text) else None
    return None


def _infer_from_entities(slot: str, entities: List[Dict[str, Any]]) -> Optional[str]:
    s_cf = _cf(slot)
    for e in entities:
        etype = _cf(e.get("type", "")) or _cf(e.get("label", ""))
        ename = _cf(e.get("name", ""))
        if s_cf in {etype, ename}:
            val = e.get("value", e.get("text", e.get("name")))
            if isinstance(val, (str, int, float)):
                return str(val)
    return None


def _question_for_slot(slot: str) -> str:
    # Persian natural questions (strings are fine to localize; comments remain English)
    template = {
        "url": "لینک دقیق را ارسال می‌کنی؟",
        "action": "چه عملی باید انجام شود؟",
        "object": "روی چه چیزی این عمل انجام شود؟",
        "email": "ایمیل مورد نظر چیست؟",
        "path": "مسیر فایل/پوشه چیست؟",
        "language": "زبان خروجی را مشخص می‌کنی؟",
        "format": "خروجی با چه فرمتی باشد؟",
    }
    return template.get(slot, f"مقدار «{slot}» را مشخص می‌کنی؟")


# ------------------------- main -------------------------

def b5f2_collect_slots(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B5F2 — Planning.SlotCollector (Noema)
    Fills planner.intent slots from entities/text/defaults, proposes questions for missing slots,
    and decides readiness & confirmation flags.

    Input:
      {
        "planner": { "intent": { "skill_id": str, "skill_name": str, "score": float,
                                  "slots": { "schema": [{name,required}], "filled": {...}, "missing": [...] } } },
        "perception": { "packz": {"text": str}, "entities": [...] }?,
        "planning": { "skill_defaults": { skill_id: {slot: default, ...} } }?,
        "world_model": { "uncertainty": { "score": float, "recommendation": str }, "prediction": {"top": str}, "context": {"features":{"speech_act": str}} }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "planner": {
          "slot_collect": {
            "skill_id": str,
            "filled": {slot: value, ...},
            "missing": [slot, ...],
            "candidates": { slot: [ {value, source, score}, ... ] },
            "questions": [ {slot, text} ],
            "assumptions": [ {slot, value, confidence, reason} ],
            "ready": bool,
            "must_confirm": bool,
            "meta": { "source": "B5F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_intent" }
      }
    """
    intent = _collect_intent(input_json)
    if not intent:
        return {"status": "SKIP", "planner": {"slot_collect": {}}, "diag": {"reason": "no_intent"}}

    skill_id = intent.get("skill_id", "")
    schema = intent.get("slots", {}).get("schema", []) if isinstance(intent.get("slots"), dict) else []
    already = intent.get("slots", {}).get("filled", {}) if isinstance(intent.get("slots"), dict) else {}
    missing_init = intent.get("slots", {}).get("missing", []) if isinstance(intent.get("slots"), dict) else []

    entities = _collect_entities(input_json)
    text = _collect_text(input_json)
    defaults = _collect_defaults(input_json, skill_id)
    u_score, u_rec = _uncertainty(input_json)
    reply_top, sa = _reply_and_sa(input_json)

    # Candidates per slot from multiple sources
    candidates: Dict[str, List[Dict[str, Any]]] = {}
    filled = dict(already)

    for s in schema:
        name = s.get("name")
        if not isinstance(name, str) or not name:
            continue
        sname = name

        # Skip if already filled
        if sname in filled and filled[sname] not in [None, ""]:
            continue

        cands: List[Dict[str, Any]] = []

        v_ent = _infer_from_entities(sname, entities)
        if v_ent:
            cands.append({"value": v_ent, "source": "entity", "score": 0.9})

        v_text = _infer_from_text(sname, text)
        if v_text and (not v_ent or v_text != v_ent):
            cands.append({"value": v_text, "source": "text", "score": 0.7})

        v_def = defaults.get(sname)
        if isinstance(v_def, (str, int, float)) and str(v_def):
            cands.append({"value": str(v_def), "source": "default", "score": 0.55})

        # Choose best candidate if any
        if cands:
            cands.sort(key=lambda x: x["score"], reverse=True)
            filled[sname] = cands[0]["value"]

        candidates[sname] = cands

    # Compute remaining missing
    required = [s["name"] for s in schema if s.get("required", False)]
    missing = [n for n in required if n not in filled or filled[n] in [None, ""]]

    # Build questions for missing
    questions = [{"slot": m, "text": _question_for_slot(m)} for m in missing]

    # Assumptions: when not required or defaults used with medium uncertainty
    assumptions: List[Dict[str, Any]] = []
    for s in schema:
        n = s["name"]
        if n in filled and n not in required:
            assumptions.append({
                "slot": n,
                "value": filled[n],
                "confidence": 0.6 if any(c["source"] == "default" for c in candidates.get(n, [])) else 0.75,
                "reason": "auto-filled optional slot",
            })

    # Readiness and confirmation logic
    ready = len(missing) == 0
    must_confirm = False
    if ready:
        # If predicted to execute action or recommendation says probe, require confirmation under uncertainty
        if reply_top == "execute_action" or (sa in {"request", "command"}):
            must_confirm = u_score >= 0.4 or u_rec in {"probe_first", "answer_or_probe"}
        else:
            must_confirm = u_score >= 0.7

    return {
        "status": "OK",
        "planner": {
            "slot_collect": {
                "skill_id": skill_id,
                "filled": filled,
                "missing": missing,
                "candidates": candidates,
                "questions": questions,
                "assumptions": assumptions,
                "ready": bool(ready),
                "must_confirm": bool(must_confirm),
                "meta": {"source": "B5F2", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    sample = {
        "planner": {
            "intent": {
                "skill_id": "skill.web_summarize",
                "skill_name": "Web Document Summarizer",
                "score": 0.74,
                "slots": {
                    "schema": [{"name": "action", "required": True}, {"name": "url", "required": True},
                               {"name": "language", "required": False}],
                    "filled": {"action": "summarize"},
                    "missing": ["url"]
                }
            }
        },
        "perception": {
            "packz": {"text": "نوما، لطفاً این لینک رو خلاصه کن: https://example.com/report.pdf"},
            "entities": [{"type": "action", "value": "summarize"}]
        },
        "planning": {
            "skill_defaults": {
                "skill.web_summarize": {"language": "fa"}
            }
        },
        "world_model": {
            "prediction": {"top": "execute_action"},
            "uncertainty": {"score": 0.42, "recommendation": "answer_or_probe"},
            "context": {"features": {"speech_act": "request"}}
        }
    }
    out = b5f2_collect_slots(sample)
    print(out["planner"]["slot_collect"]["filled"], out["planner"]["slot_collect"]["missing"],
          out["planner"]["slot_collect"]["must_confirm"])
