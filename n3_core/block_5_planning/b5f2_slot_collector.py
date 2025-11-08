# Folder: noema/n3_core/block_5_planning
# File:   b5f2_slot_collector.py

from __future__ import annotations

from typing import Any, Dict, List

__all__ = ["b5f2_collect_slots"]

RULES_VERSION = "1.0"

# --- helpers -------------------------------------------------

def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _packz_text(inp: Dict[str, Any]) -> str:
    return (
        _get(inp, ["perception", "packz", "text"], "")
        or _get(inp, ["perception", "normalized_text"], "")
        or _get(inp, ["text"], "")
        or ""
    )

# --- main ----------------------------------------------------

def b5f2_collect_slots(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B5F2 — Slot Collector (Answer-first)
    - اگر مهارت «answer» باشد: هیچ اسلات اجباری، آماده=True، تأیید=False.
    - مسیر عمومی (برای مهارت‌های اکشن) فعلاً ساده و بدون الزام پیاده‌سازی شده تا
      در فاز بعد توسعه یابد.
    """
    intent = _get(input_json, ["planner", "intent"], {}) or {}
    skill_id = str(intent.get("skill_id") or "")
    slots = intent.get("slots") if isinstance(intent.get("slots"), dict) else {}
    schema = list(slots.get("schema") or [])
    filled = dict(slots.get("filled") or {})

    text = _packz_text(input_json)
    if skill_id.startswith("skill.answer"):
        # هیچ اسلات اجباری برای پاسخ مستقیم
        if text and "text" not in filled:
            filled["text"] = text
        out = {
            "skill_id": skill_id,
            "filled": filled,
            "missing": [],
            "candidates": {},
            "questions": [],
            "assumptions": [],
            "ready": True,
            "must_confirm": False,
            "meta": {"source": "B5F2", "rules_version": RULES_VERSION},
        }
        return {"status": "OK", "planner": {"slot_collect": out}, "diag": {"reason": "ok-answer"}}

    # ----- مسیر عمومی (در صورت داشتن مهارت‌های اقدام‌محور) -----
    required_names = [s.get("name") for s in schema if isinstance(s, dict) and s.get("required")]
    missing = [n for n in required_names if n not in filled or filled[n] in (None, "")]
    questions = [{"slot": m, "text": f"مقدار «{m}» را مشخص می‌کنی؟"} for m in missing]

    out = {
        "skill_id": skill_id,
        "filled": filled,
        "missing": missing,
        "candidates": {},
        "questions": questions,
        "assumptions": [],
        "ready": len(missing) == 0,
        "must_confirm": False,  # اجازه بده B5F3 بر اساس /mc تصمیم بگیرد
        "meta": {"source": "B5F2", "rules_version": RULES_VERSION},
    }
    return {"status": "OK", "planner": {"slot_collect": out}, "diag": {"reason": "ok-generic"}}

if __name__ == "__main__":
    sample = {
        "planner": {
            "intent": {
                "skill_id": "skill.answer.direct",
                "slots": {"schema": [], "filled": {}}
            }
        },
        "perception": {"packz": {"text": "نوما چیه؟"}}
    }
    print(b5f2_collect_slots(sample)["planner"]["slot_collect"])
