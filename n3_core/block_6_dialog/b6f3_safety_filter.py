# Folder: noema/n3_core/block_6_dialog
# File:   b6f3_safety_filter.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

import unicodedata

__all__ = ["b6f3_safety_filter"]

RULES_VERSION = "1.0"
MAX_OUT_LEN = 1200


# -------- utils --------

def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _trim(s: str, n: int = MAX_OUT_LEN) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _ensure_str(x: Any) -> str:
    return x if isinstance(x, str) else ""


def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


# -------- redaction patterns --------

RE_EMAIL = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
RE_URL_TOKEN = re.compile(r"([?&](?:token|key|api[_\-]?key|access[_\-]?token)=[^&\s]+)", re.IGNORECASE)
RE_POSSIBLE_CC = re.compile(r"\b(?:\d[ \-]?){13,19}\b")
RE_PHONE = re.compile(r"\b(?:\+?\d{1,3}[ \-]?)?(?:\(?\d{2,4}\)?[ \-]?)?\d{3,4}[ \-]?\d{3,4}\b")
RE_API_KEYS = [
    re.compile(r"\bsk\-[A-Za-z0-9]{16,}\b"),  # OpenAI-like
    re.compile(r"\bghp_[A-Za-z0-9]{36}\b"),  # GitHub PAT
    re.compile(r"\bAIza[0-9A-Za-z\-\_]{35}\b"),  # Google API key
    re.compile(r"\bxox[abpr]\-[A-Za-z0-9\-]{10,}\b"),  # Slack tokens
]


def _luhn_check(num: str) -> bool:
    digits = [int(c) for c in num if c.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    s = 0
    alt = False
    for d in reversed(digits):
        s += (d * 2 - 9) if alt and d * 2 > 9 else (d * 2 if alt else d)
        alt = not alt
    return s % 10 == 0


# -------- redaction engine --------

def _redact(text: str) -> Tuple[str, List[Dict[str, Any]], bool]:
    """
    Returns (redacted_text, redactions_list, blocked_flag).
    blocked_flag becomes True if highly sensitive secrets were found (API keys or CC number).
    """
    if not isinstance(text, str) or not text:
        return "", [], False

    redactions: List[Dict[str, Any]] = []
    blocked = False
    out = text

    # 1) Emails
    emails = RE_EMAIL.findall(out)
    if emails:
        out = RE_EMAIL.sub("[REDACTED_EMAIL]", out)
        redactions.append({"type": "email", "count": len(emails)})

    # 2) Tokens in URLs
    tokens = RE_URL_TOKEN.findall(out)
    if tokens:
        out = RE_URL_TOKEN.sub(lambda m: m.group(0).split("=")[0] + "=[REDACTED]", out)
        redactions.append({"type": "url_token", "count": len(tokens)})

    # 3) API-like secrets
    api_hits = 0
    for rx in RE_API_KEYS:
        hits = rx.findall(out)
        if hits:
            api_hits += len(hits)
            out = rx.sub("[REDACTED_SECRET]", out)
    if api_hits:
        redactions.append({"type": "api_key", "count": api_hits})
        blocked = True  # strong signal

    # 4) Credit cards (with Luhn)
    cc_hits = []

    def _cc_repl(m):
        s = m.group(0)
        norm = "".join(ch for ch in s if ch.isdigit())
        if _luhn_check(norm):
            cc_hits.append(s)
            return "[REDACTED_CARD]"
        return s

    out = RE_POSSIBLE_CC.sub(_cc_repl, out)
    if cc_hits:
        redactions.append({"type": "credit_card", "count": len(cc_hits)})
        blocked = True

    # 5) Phone numbers (best-effort; do not block)
    phones = RE_PHONE.findall(out)
    if phones:
        out = RE_PHONE.sub("[REDACTED_PHONE]", out)
        redactions.append({"type": "phone", "count": len(phones)})

    return out, redactions, blocked


# -------- main --------

def b6f3_safety_filter(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B6F3 — Dialog.SafetyFilter (Noema)

    Input:
      {
        "dialog": { "surface": { "text": str, "move": str, "language": "fa|en|und" } },
        "planner": { "plan": { "guardrails": {"must_confirm": bool}, "dry_run_summary": str?, "skill_name": str? } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "dialog": {
          "final": {
            "move": "ask|confirm|answer|ack|refuse",
            "text": str,
            "redactions": [ {type, count}, ... ],
            "blocked": bool,
            "reason": str?,
            "meta": { "source": "B6F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_surface" }
      }
    """
    surf = _get(input_json, ["dialog", "surface"], {})
    if not isinstance(surf, dict) or not surf:
        return {"status": "SKIP", "dialog": {"final": {}}, "diag": {"reason": "no_surface"}}

    move = _ensure_str(surf.get("move"))
    text = _ensure_str(surf.get("text"))
    lang = _ensure_str(surf.get("language"))

    must_confirm = bool(_get(input_json, ["planner", "plan", "guardrails", "must_confirm"], False))
    dry_summary = _ensure_str(_get(input_json, ["planner", "plan", "dry_run_summary"], ""))

    red_text, redacts, blocked = _redact(text)
    red_text = _trim(red_text, MAX_OUT_LEN)

    # If blocked or guard says confirm, convert to confirm move with preview
    reason = ""
    final_move = move
    final_text = red_text

    if blocked:
        reason = "secret_detected"
        final_move = "confirm"
        preview = red_text if red_text else (dry_summary or "")
        final_text = (f"ارسال شامل دادهٔ حساس است و رداکت شد.\n"
                      f"تأیید می‌کنی نسخهٔ امن ارسال شود؟\n\nپیش‌نمایش:\n{_trim(preview)}") if lang == "fa" \
            else (f"The message contained sensitive data and was redacted.\n"
                  f"Confirm sending the sanitized version?\n\nPreview:\n{_trim(preview)}")
    elif must_confirm and move in {"answer", "ack", "execute"}:
        reason = "must_confirm"
        final_move = "confirm"
        final_text = dry_summary or (("تأیید می‌کنی ارسال شود؟" if lang == "fa" else "Confirm to send?"))

    out = {
        "status": "OK",
        "dialog": {
            "final": {
                "move": final_move,
                "text": final_text,
                "redactions": redacts,
                "blocked": bool(blocked),
                "reason": reason or None,
                "meta": {"source": "B6F3", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    # Demo 1: normal answer, no secrets
    sample1 = {"dialog": {"surface": {"move": "answer", "text": "باشه، انجام شد.", "language": "fa"}}}
    print(b6f3_safety_filter(sample1)["dialog"]["final"])

    # Demo 2: contains API key and email → redacts and forces confirm
    sample2 = {"dialog": {
        "surface": {"move": "answer", "text": "Contact me at a@b.com. Key=sk-1234567890ABCDEF", "language": "en"}}}
    res2 = b6f3_safety_filter(sample2)
    print(res2["dialog"]["final"]["move"], res2["dialog"]["final"]["redactions"], res2["dialog"]["final"]["text"][:80])

    # Demo 3: must_confirm from guardrails
    sample3 = {
        "dialog": {"surface": {"move": "answer", "text": "Result ready.", "language": "en"}},
        "planner": {"plan": {"guardrails": {"must_confirm": True}, "dry_run_summary": "Confirm to send: Result ready."}}
    }
    res3 = b6f3_safety_filter(sample3)
    print(res3["dialog"]["final"]["move"], res3["dialog"]["final"]["text"])
