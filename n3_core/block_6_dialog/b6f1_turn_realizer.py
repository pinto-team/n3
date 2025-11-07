# Folder: noema/n3_core/block_6_dialog
# File:   b6f1_turn_realizer.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b6f1_realize_turn"]

RULES_VERSION = "1.0"


# ------------------------- helpers -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _ensure_str(x: Any) -> str:
    return x if isinstance(x, str) else ""


def _join_questions(questions: List[Dict[str, Any]]) -> str:
    qs = [q.get("text") for q in questions if isinstance(q, dict) and isinstance(q.get("text"), str)]
    if not qs:
        return ""
    if len(qs) == 1:
        return qs[0]
    # Persian strings are allowed; only comments must be English.
    lines = [f"{i + 1}. {qs[i]}" for i in range(len(qs))]
    return "\n".join(lines)


def _confirm_text(plan: Dict[str, Any], skill_name: str, filled: Dict[str, Any]) -> str:
    kv = ", ".join(f"{k}={filled[k]}" for k in sorted(filled.keys()))
    base = _ensure_str(plan.get("dry_run_summary"))
    return base or f"تأیید می‌کنی «{skill_name}» با {kv} اجرا شود؟"


def _pick_step(plan_steps: List[Dict[str, Any]], kind: str) -> Optional[Dict[str, Any]]:
    for st in plan_steps:
        if isinstance(st, dict) and st.get("op") == kind:
            return st
    return None


def _safe_bool(x: Any) -> bool:
    return bool(x) if isinstance(x, bool) else False


# ------------------------- main -------------------------

def b6f1_realize_turn(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B6F1 — Dialog.TurnRealizer (Noema)

    Input:
      {
        "planner": {
          "plan": {
            "id": str,
            "next_move": "ask_user|confirm|execute|answer|ack",
            "steps": [ {"op": "ask_slot"|"confirm"|"execute_skill"|"generate_answer"|"acknowledge", ...}, ... ],
            "guardrails": { "must_confirm": bool, "uncertainty": float, "recommendation": str },
            "dry_run_summary": str,
            "meta": {...}
          }
        },
        "planner": {
          "slot_collect": { "questions": [ {slot, text}, ... ], "filled": {...}, "missing": [...] }
        },
        "world_model": { "prediction": { "hints": {"safecheck_needed": bool} } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "dialog": {
          "turn": {
            "move": "ask|confirm|execute|answer|ack|refuse",
            "content": str,                 # for ask/confirm/answer/ack/refuse
            "ops": [ { "op": "execute_skill", "skill_id": str, "params": {...} } ]?,  # for execute
            "safety": { "required": bool, "reason": str? },
            "meta": { "source": "B6F1", "rules_version": "1.0", "plan_id": str }
          }
        },
        "diag": { "reason": "ok|no_plan" }
      }
    """
    plan = _get(input_json, ["planner", "plan"], {})
    if not isinstance(plan, dict) or not plan:
        return {"status": "SKIP", "dialog": {"turn": {}}, "diag": {"reason": "no_plan"}}

    steps = plan.get("steps", []) if isinstance(plan.get("steps"), list) else []
    next_move = _ensure_str(plan.get("next_move"))
    sc = _get(input_json, ["planner", "slot_collect"], {}) or {}
    questions = sc.get("questions", []) if isinstance(sc.get("questions"), list) else []
    filled = sc.get("filled", {}) if isinstance(sc.get("filled"), dict) else (
        plan.get("filled", {}) if isinstance(plan.get("filled"), dict) else {})
    must_confirm = _safe_bool(_get(plan, ["guardrails", "must_confirm"]))
    safecheck_needed = bool(_get(input_json, ["world_model", "prediction", "hints", "safecheck_needed"], False))

    # Safety policy: if safecheck is needed, prefer confirm before execute/answer.
    safety_required = safecheck_needed or must_confirm
    safety_reason = "safecheck_needed" if safecheck_needed else ("must_confirm" if must_confirm else "")

    turn: Dict[str, Any] = {
        "move": "",
        "content": "",
        "ops": [],
        "safety": {"required": bool(safety_required), "reason": safety_reason or None},
        "meta": {"source": "B6F1", "rules_version": RULES_VERSION, "plan_id": _ensure_str(plan.get("id"))},
    }

    if next_move == "ask_user":
        text = _join_questions(questions)
        if not text:
            # Fallback: synthesize from missing slot names
            missing = sc.get("missing", []) if isinstance(sc.get("missing"), list) else []
            if missing:
                bullets = [f"{i + 1}. مقدار «{missing[i]}» را مشخص می‌کنی؟" for i in range(len(missing))]
                text = "\n".join(bullets)
            else:
                text = "برای ادامه، لطفاً جزئیات لازم را مشخص کن."
        turn["move"] = "ask"
        turn["content"] = text
    elif next_move == "confirm":
        skill_name = _ensure_str(
            plan.get("skill_name") or _get(input_json, ["planner", "intent", "skill_name"], "Skill"))
        text = _confirm_text(plan, skill_name, filled or {})
        turn["move"] = "confirm"
        turn["content"] = text
    elif next_move == "execute":
        st = _pick_step(steps, "execute_skill")
        if not st:
            # If execute step is missing, downgrade to confirm or ack.
            turn["move"] = "ack"
            turn["content"] = "طرح اجرایی کامل نیست؛ اقدام شما تأیید شد."
        else:
            op = {
                "op": "execute_skill",
                "skill_id": _ensure_str(st.get("skill_id")),
                "params": st.get("params", {}) if isinstance(st.get("params"), dict) else {},
            }
            turn["move"] = "execute"
            turn["ops"] = [op]
            # Optional lightweight surface text
            kv = ", ".join(f"{k}={op['params'][k]}" for k in sorted(op["params"].keys()))
            turn["content"] = f"اجرا: «{st.get('skill_name', 'Skill')}» با {kv}"
            # If safety is required, suggest human confirmation first
            if safety_required:
                turn["move"] = "confirm"
                turn["content"] = _confirm_text(plan, _ensure_str(st.get("skill_name", "Skill")), op["params"])
                turn["ops"] = [op]  # kept for the runner after confirmation
    elif next_move == "answer":
        st = _pick_step(steps, "generate_answer")
        hint = _ensure_str(st.get("hint")) if st else ""
        # Produce a minimal draft; a downstream surface-NLG can refine it.
        body = "پاسخ مستقیم بر پایهٔ زمینهٔ فعلی."
        if hint:
            body = f"{body}\n\nراهنما: {hint}"
        if safety_required:
            turn["move"] = "confirm"
            turn["content"] = "قبل از ارسال پاسخ، تأیید می‌کنی نسخهٔ پیشنهادی ارسال شود؟"
        else:
            turn["move"] = "answer"
            turn["content"] = body
    elif next_move == "ack":
        turn["move"] = "ack"
        turn["content"] = "باشه."
    else:
        # Unknown next_move → refuse softly
        turn["move"] = "refuse"
        turn["content"] = "امکان ساخت حرکت بعدی فراهم نشد."

    return {
        "status": "OK",
        "dialog": {"turn": turn},
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    # Demo 1: ask
    sample_ask = {
        "planner": {
            "plan": {"id": "p1", "next_move": "ask_user", "steps": [], "guardrails": {"must_confirm": False}},
            "slot_collect": {
                "questions": [
                    {"slot": "url", "text": "لینک دقیق را ارسال می‌کنی؟"},
                    {"slot": "language", "text": "زبان خروجی را مشخص می‌کنی؟"}
                ],
                "missing": ["url", "language"]
            }
        }
    }
    out1 = b6f1_realize_turn(sample_ask)
    print(out1["dialog"]["turn"]["move"])
    print(out1["dialog"]["turn"]["content"])

    # Demo 2: execute with confirm due to safety
    sample_exec = {
        "planner": {
            "plan": {
                "id": "p2",
                "next_move": "execute",
                "steps": [
                    {"op": "execute_skill", "skill_id": "skill.web_summarize", "skill_name": "Web Summarizer",
                     "params": {"action": "summarize", "url": "https://example.com/a.pdf"}}
                ],
                "guardrails": {"must_confirm": True},
                "dry_run_summary": "تأیید می‌کنی «Web Summarizer» با action=summarize, url=https://example.com/a.pdf اجرا شود؟"
            },
            "slot_collect": {"filled": {"action": "summarize", "url": "https://example.com/a.pdf"}}
        },
        "world_model": {"prediction": {"hints": {"safecheck_needed": False}}}
    }
    out2 = b6f1_realize_turn(sample_exec)
    print(out2["dialog"]["turn"]["move"])
    print(out2["dialog"]["turn"]["content"])
    print(out2["dialog"]["turn"]["ops"][0]["op"] if out2["dialog"]["turn"]["ops"] else None)
