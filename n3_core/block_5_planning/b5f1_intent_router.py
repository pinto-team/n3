# Folder: noema/n3_core/block_5_planning
# File:   b5f1_intent_router.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

__all__ = ["b5f1_route_intent"]

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

def _speech_act(inp: Dict[str, Any]) -> str:
    return (
        _get(inp, ["perception", "packz", "signals", "speech_act"], "")
        or _get(inp, ["world_model", "context", "features", "speech_act"], "")
        or ""
    )

def _prediction(inp: Dict[str, Any]) -> Tuple[str, Dict[str, float]]:
    pred = _get(inp, ["world_model", "prediction"], {}) or {}
    top = pred.get("top") if isinstance(pred.get("top"), str) else ""
    dist = pred.get("expected_reply") if isinstance(pred.get("expected_reply"), dict) else {}
    # normalize
    dist = {str(k): float(v) for k, v in dist.items()} if dist else {}
    return (top or ""), dist

# تشخیص تعریف: "X یعنی Y" یا "X means Y"
_DEF_PATTERNS = [
    re.compile(r"^\s*(?P<k>.+?)\s+(?:یعنی|برابر|معنی(?:\s*اش)?|معنیش)\s+(?P<v>.+?)\s*$"),
    re.compile(r"^\s*(?P<k>.+?)\s+(?:is|means|=)\s+(?P<v>.+?)\s*$", re.IGNORECASE),
]

def _is_definition(t: str) -> bool:
    if not t:
        return False
    for p in _DEF_PATTERNS:
        if p.match(t):
            return True
    return False

# --- main ----------------------------------------------------

def b5f1_route_intent(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B5F1 — Intent Router (Safe default)
    - برای سوال/تعریف/گفتگوی عادی همیشه skill.answer.direct را انتخاب می‌کند.
    - هیچ اسلات اجباری تعریف نمی‌شود تا مسیر 'missing_info' شکل نگیرد.
    """
    text = _packz_text(input_json)
    sa = (_speech_act(input_json) or "").lower()
    top, dist = _prediction(input_json)

    # هر نوع ورودی محاوره‌ای و تعریف → پاسخ مستقیم
    # (در صورت نیاز بعداً مسیرهای action را اضافه می‌کنیم)
    skill_id = "skill.answer.direct"
    skill_name = "Answer Generation"

    intent = {
        "skill_id": skill_id,
        "skill_name": skill_name,
        "score": 0.95,  # bias to answer
        "rationale": [
            f"sa={sa}" if sa else "sa=unknown",
            f"pred_top={top or 'n/a'}",
            "rule=prefer_direct_answer",
            "no_required_slots",
            "def_pattern" if _is_definition(text) else "generic_qa",
        ],
        "slots": {
            "schema": [],                 # هیچ اسلات اجباری
            "filled": {"text": text} if text else {},
            "missing": []
        },
        "meta": {"source": "B5F1", "rules_version": RULES_VERSION},
    }

    return {
        "status": "OK",
        "planner": {"intent": intent},
        "diag": {"reason": "ok", "rules_version": RULES_VERSION}
    }

if __name__ == "__main__":
    sample = {"perception": {"packz": {"text": "نوما چیه؟", "signals": {"speech_act": "question"}}}}
    print(b5f1_route_intent(sample)["planner"]["intent"])
