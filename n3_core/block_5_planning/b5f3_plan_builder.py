# Folder: noema/n3_core/block_5_planning
# File:   b5f3_plan_builder.py

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple
import unicodedata

__all__ = ["b5f3_build_plan"]

RULES_VERSION = "1.0"


# ------------------------- helpers -------------------------

def _mc_config(inp: Dict[str, Any]):
    cfg = _get(inp, ["runtime", "config", "guardrails", "must_confirm"], {}) or {}
    try:
        u_th = float(cfg.get("u_threshold", 0.8))
    except Exception:
        u_th = 0.8
    rec_req = bool(cfg.get("rec_requires_confirm", False))
    return u_th, rec_req


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


def _make_id(stuff: Dict[str, Any]) -> str:
    payload = json.dumps(stuff, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _uncertainty(inp: Dict[str, Any]) -> Tuple[float, str]:
    u = _get(inp, ["world_model", "uncertainty", "score"], 0.0)
    rec = _get(inp, ["world_model", "uncertainty", "recommendation"], "") or ""
    return _safe_float(u, 0.0), str(rec)


def _prediction_top(inp: Dict[str, Any]) -> str:
    return str(_get(inp, ["world_model", "prediction", "top"], "") or "")


def _collect_intent(inp: Dict[str, Any]) -> Dict[str, Any]:
    return _get(inp, ["planner", "intent"], {}) or {}


def _collect_slots(inp: Dict[str, Any]) -> Dict[str, Any]:
    return _get(inp, ["planner", "slot_collect"], {}) or {}


def _packz_text(inp: Dict[str, Any]) -> str:
    return (
        _get(inp, ["perception", "packz", "text"], "")
        or _get(inp, ["perception", "normalized_text"], "")
        or _get(inp, ["text"], "")
        or ""
    )


# ------------------------- plan logic -------------------------

def _confirmation_summary(skill_name: str, filled: Dict[str, Any]) -> str:
    if filled:
        kv = ", ".join(f"{k}={filled[k]}" for k in sorted(filled.keys()))
        return f"Confirm to run '{skill_name}' with {kv}"
    return f"Confirm to run '{skill_name}'"


def _question_steps(missing: List[str], questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    qmap = {q.get("slot"): q.get("text") for q in questions if isinstance(q, dict)}
    steps = []
    for s in missing:
        steps.append({
            "op": "ask_slot",
            "slot": s,
            "text": qmap.get(s) or f"Please provide '{s}'.",
        })
    return steps


def _execute_step(skill_id: str, skill_name: str, filled: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "op": "execute_skill",
        "skill_id": skill_id,
        "skill_name": skill_name,
        "params": dict(filled),
        "expects": {"type": "result_or_error"}
    }


def _answer_step(text_hint: str = "") -> Dict[str, Any]:
    return {
        "op": "generate_answer",
        "hint": text_hint or "Direct answer based on current context",
        "expects": {"type": "text"}
    }


def _ack_step() -> Dict[str, Any]:
    return {"op": "acknowledge", "expects": {"type": "text"}}


# ------------------------- main -------------------------

def b5f3_build_plan(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B5F3 — Planning.PlanBuilder (Noema)
    """
    intent = _collect_intent(input_json)
    if not intent:
        return {"status": "SKIP", "planner": {"plan": {}}, "diag": {"reason": "no_intent"}}

    slots = _collect_slots(input_json)
    filled = dict(slots.get("filled") or intent.get("slots", {}).get("filled") or {})
    missing = list(slots.get("missing") or intent.get("slots", {}).get("missing") or [])
    questions = list(slots.get("questions") or [])
    ready = bool(slots.get("ready", len(missing) == 0))
    must_confirm_flag = bool(slots.get("must_confirm", False))

    u_score, u_rec = _uncertainty(input_json)
    reply_top = _prediction_top(input_json)
    u_th, rec_req = _mc_config(input_json)

    skill_id = str(intent.get("skill_id") or "")
    skill_name = str(intent.get("skill_name") or "Answer Generation")

    steps: List[Dict[str, Any]] = []
    next_move = "ask_user"
    must_confirm = False  # always defined

    # Force-disable confirm if runtime says so (e.g., /mc u=0.95 rec=off)
    force_disable_confirm = (u_th >= 0.9) and (rec_req is False)

    if missing:
        steps.extend(_question_steps(missing, questions))
        next_move = "ask_user"
    else:
        confirm_summary = _confirmation_summary(skill_name, filled)

        base_confirm = (
            must_confirm_flag
            or (reply_top == "execute_action" and u_score >= u_th)
            or (rec_req and (u_rec in {"probe_first", "answer_or_probe"}))
        )
        must_confirm = False if force_disable_confirm else base_confirm

        if skill_id.startswith("skill.answer") or reply_top in {"direct_answer", "acknowledge_only", "small_talk"} or not skill_id:
            if must_confirm:
                steps.append({"op": "confirm", "text": confirm_summary, "expects": {"type": "yes_no"}})
                steps.append(_answer_step(text_hint=_packz_text(input_json)))
                next_move = "confirm"
            else:
                steps.append(_answer_step(text_hint=_packz_text(input_json)))
                next_move = "answer"
        else:
            if must_confirm:
                steps.append({"op": "confirm", "text": confirm_summary, "expects": {"type": "yes_no"}})
                steps.append(_execute_step(skill_id, skill_name, filled))
                next_move = "confirm"
            else:
                steps.append(_execute_step(skill_id, skill_name, filled))
                next_move = "execute"

    plan_id = _make_id({"skill_id": skill_id, "filled": filled, "steps": steps})

    out = {
        "status": "OK",
        "planner": {
            "plan": {
                "id": plan_id,
                "next_move": next_move,
                "steps": steps,
                "guardrails": {
                    "must_confirm": bool(must_confirm),
                    "uncertainty": round(u_score, 3),
                    "recommendation": u_rec,
                },
                "dry_run_summary": (
                    _confirmation_summary(skill_name, filled)
                    if not missing else f"Need slots: {', '.join(missing)}"
                ),
                "meta": {"source": "B5F3", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    sample = {
        "planner": {
            "intent": {
                "skill_id": "skill.web_summarize",
                "skill_name": "Web Document Summarizer",
                "score": 0.81,
                "slots": {
                    "schema": [{"name": "action", "required": True}, {"name": "url", "required": True}],
                    "filled": {"action": "summarize"},
                    "missing": ["url"]
                },
                "rationale": ["reply_top=execute_action", "sa=request"]
            },
            "slot_collect": {
                "filled": {"action": "summarize", "url": "https://example.com/a.pdf"},
                "missing": [],
                "questions": [],
                "ready": True,
                "must_confirm": True
            }
        },
        "world_model": {
            "uncertainty": {"score": 0.46, "recommendation": "answer_or_probe"},
            "prediction": {"top": "execute_action"}
        },
        "perception": {"packz": {"text": "لطفاً این لینک را خلاصه کن: https://example.com/a.pdf"}}
    }
    res = b5f3_build_plan(sample)
    print(res["planner"]["plan"]["next_move"], len(res["planner"]["plan"]["steps"]),
          res["planner"]["plan"]["dry_run_summary"])
