# Folder: noema/n3_core/block_6_dialog
# File:   b6f2_surface_nlg.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b6f2_surface_nlg"]

RULES_VERSION = "1.0"
MAX_LEN = 800  # hard cap for outgoing text


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


def _detect_lang(text: str, dir_hint: Optional[str]) -> str:
    if isinstance(dir_hint, str) and dir_hint.lower() == "rtl":
        return "fa"
    # Basic script hint
    for ch in text or "":
        cp = ord(ch)
        if (0x0600 <= cp <= 0x06FF) or (0x0750 <= cp <= 0x08FF) or (0xFB50 <= cp <= 0xFEFF):
            return "fa"
    for ch in text or "":
        if ("A" <= ch <= "Z") or ("a" <= ch <= "z"):
            return "en"
    return "und"


def _trim(s: str) -> str:
    s = (s or "").strip()
    return s[:MAX_LEN]


def _bullets(s: str, lang: str) -> str:
    if not s:
        return s
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    if len(lines) <= 1:
        return s
    prefix = "• " if lang != "fa" else "• "
    return "\n".join(prefix + ln for ln in lines)


def _clean_spaces(s: str) -> str:
    # Collapse runs of whitespace except newlines
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r" *\n *", "\n", s)
    return s.strip()


def _ensure_period(s: str, lang: str) -> str:
    if not s:
        return s
    if s.endswith(("؟", "؟ ", ".")) or s.endswith("...") or s.endswith("…") or s.endswith("!"):
        return s
    return s + ("." if lang != "fa" else " .").replace(" .", " .")  # keep simple; Persian templates already punctuate


# ------------------------- style & templates -------------------------

def _style_config(lang: str) -> Dict[str, Any]:
    # Minimal persona: concise, warm, no emojis by default
    return {
        "lang": lang,
        "tone": "concise-friendly",
        "emoji": False,
        "prefixes": {
            "fa": {"ask": "", "confirm": "", "answer": "", "ack": "", "refuse": ""},
            "en": {"ask": "", "confirm": "", "answer": "", "ack": "", "refuse": ""},
        }
    }


def _render_move(move: str, content: str, filled: Dict[str, Any], lang: str) -> str:
    move = (move or "").lower()
    if move == "ask":
        base = content or "لطفاً اطلاعات لازم را ارسال کن."
        return _bullets(base, lang)
    if move == "confirm":
        if content:
            return _bullets(content, lang)
        # Fallback summary if content is missing
        kv = ", ".join(f"{k}={filled[k]}" for k in sorted(filled.keys()))
        return (f"تأیید می‌کنی با {kv} انجام شود؟" if lang == "fa" else f"Confirm to proceed with {kv}?")
    if move == "answer":
        return content or ("باشه." if lang == "fa" else "Got it.")
    if move == "ack":
        return content or ("باشه." if lang == "fa" else "Okay.")
    if move == "refuse":
        return content or ("نمی‌توانم در این مورد کمک کنم." if lang == "fa" else "I can’t help with that.")
    if move == "execute":
        # Usually not surfaced to user; provide a brief status line if needed
        return content or ("در حال اجرا..." if lang == "fa" else "Running the action...")
    # Unknown
    return content or ("باشه." if lang == "fa" else "Okay.")


# ------------------------- main -------------------------

def b6f2_surface_nlg(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B6F2 — Dialog.SurfaceNLG (Noema)

    Input:
      {
        "dialog": { "turn": { "move": "ask|confirm|answer|ack|refuse|execute", "content": str, "ops": [...], "meta": {...} } },
        "planner": { "slot_collect": { "filled": {...} } }?,
        "world_model": { "context": { "features": { "dir": "rtl|ltr" } } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "dialog": {
          "surface": {
            "text": str,
            "language": "fa|en|und",
            "move": str,
            "meta": { "source": "B6F2", "rules_version": "1.0", "style": {...} }
          }
        },
        "diag": { "reason": "ok|no_turn" }
      }
    """
    turn = _get(input_json, ["dialog", "turn"], {})
    if not isinstance(turn, dict) or not turn:
        return {"status": "SKIP", "dialog": {"surface": {}}, "diag": {"reason": "no_turn"}}

    move = str(turn.get("move") or "")
    content = str(turn.get("content") or "")
    filled = _get(input_json, ["planner", "slot_collect", "filled"], {}) or {}

    # Language hint
    dir_hint = _get(input_json, ["world_model", "context", "features", "dir"], None)
    lang = _detect_lang(content, dir_hint)

    styled = _style_config(lang)
    text = _render_move(move, content, filled, lang)
    text = _clean_spaces(text)
    text = _trim(text)

    return {
        "status": "OK",
        "dialog": {
            "surface": {
                "text": text,
                "language": lang,
                "move": move,
                "meta": {"source": "B6F2", "rules_version": RULES_VERSION, "style": styled}
            }
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    # Demo 1: ask (multi-question bulleting)
    sample1 = {
        "dialog": {"turn": {"move": "ask", "content": "لینک دقیق را ارسال می‌کنی؟\nزبان خروجی را مشخص می‌کنی؟"}},
        "world_model": {"context": {"features": {"dir": "rtl"}}}
    }
    out1 = b6f2_surface_nlg(sample1)
    print(out1["dialog"]["surface"]["language"], out1["dialog"]["surface"]["text"])

    # Demo 2: confirm with filled slots
    sample2 = {
        "dialog": {"turn": {"move": "confirm", "content": ""}},
        "planner": {"slot_collect": {"filled": {"action": "summarize", "url": "https://example.com/a.pdf"}}},
        "world_model": {"context": {"features": {"dir": "ltr"}}}
    }
    out2 = b6f2_surface_nlg(sample2)
    print(out2["dialog"]["surface"]["language"], out2["dialog"]["surface"]["text"])

    # Demo 3: answer
    sample3 = {
        "dialog": {"turn": {"move": "answer", "content": "پاسخ مستقیم بر پایهٔ زمینهٔ فعلی.\n\nراهنما: ..."}},
        "world_model": {"context": {"features": {"dir": "rtl"}}}
    }
    out3 = b6f2_surface_nlg(sample3)
    print(out3["dialog"]["surface"]["move"], out3["dialog"]["surface"]["text"])
