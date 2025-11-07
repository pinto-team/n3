# Folder: noema/n3_core/block_1_perception
# File:   b1f5_script_tagger.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b1f5_script_tagger"]

RULES_VERSION = "1.0"

# Basic emoji (single code point) coverage
RE_EMOJI = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]")

# Unicode ranges for major scripts
R_LATIN = [
    (0x0041, 0x005A), (0x0061, 0x007A),
    (0x00C0, 0x024F), (0x1E00, 0x1EFF)
]
R_ARABIC = [
    (0x0600, 0x06FF), (0x0750, 0x077F), (0x08A0, 0x08FF),
    (0xFB50, 0xFDFF), (0xFE70, 0xFEFF)
]
R_CYRIL = [(0x0400, 0x04FF), (0x0500, 0x052F)]
R_GREEK = [(0x0370, 0x03FF)]
R_HEBREW = [(0x0590, 0x05FF)]
R_DEVAN = [(0x0900, 0x097F)]
R_HAN = [(0x4E00, 0x9FFF), (0x3400, 0x4DBF)]
R_HIRA = [(0x3040, 0x309F)]
R_KATA = [(0x30A0, 0x30FF)]
R_HANGUL = [(0xAC00, 0xD7AF)]

PERSIAN_HINTS = set("پچژگکی")  # simple indicator of Persian vs Arabic


def _in_ranges(cp: int, ranges: List[Tuple[int, int]]) -> bool:
    for a, b in ranges:
        if a <= cp <= b:
            return True
    return False


def _char_script(ch: str) -> str:
    if RE_EMOJI.match(ch):
        return "Emoji"
    if ch.isdigit():
        return "Number"
    cat = unicodedata.category(ch)
    if cat.startswith(("P", "S")):
        return "Common"
    cp = ord(ch)
    if _in_ranges(cp, R_ARABIC):
        return "Arabic"
    if _in_ranges(cp, R_LATIN):
        return "Latin"
    if _in_ranges(cp, R_CYRIL):
        return "Cyrillic"
    if _in_ranges(cp, R_GREEK):
        return "Greek"
    if _in_ranges(cp, R_HEBREW):
        return "Hebrew"
    if _in_ranges(cp, R_DEVAN):
        return "Devanagari"
    if _in_ranges(cp, R_HAN):
        return "Han"
    if _in_ranges(cp, R_HIRA):
        return "Hiragana"
    if _in_ranges(cp, R_KATA):
        return "Katakana"
    if _in_ranges(cp, R_HANGUL):
        return "Hangul"
    if ch.isspace():
        return "Common"
    return "Other"


def _token_main_script(text: str) -> Tuple[str, float, List[str]]:
    counts: Dict[str, int] = {}
    letters = 0
    seen_scripts: List[str] = []
    for ch in text:
        sc = _char_script(ch)
        if sc in ("Common", "Number", "Emoji", "Other"):
            continue
        counts[sc] = counts.get(sc, 0) + 1
        letters += 1
        if not seen_scripts or seen_scripts[-1] != sc:
            seen_scripts.append(sc)

    if letters == 0:
        return ("Common", 1.0, seen_scripts)

    if len(counts) == 1:
        sc = next(iter(counts))
        return (sc, counts[sc] / letters, seen_scripts)

    # mixed: choose majority but mark as Mixed if minority exists
    sc_major = max(counts.items(), key=lambda kv: kv[1])[0]
    conf = counts[sc_major] / letters
    if conf < 1.0:
        return ("Mixed", conf, seen_scripts)
    return (sc_major, conf, seen_scripts)


def _lang_hint(script: str, text: str) -> str:
    if script == "Arabic":
        # crude fa vs ar split using Persian-specific letters
        if any(ch in PERSIAN_HINTS for ch in text):
            return "fa"
        return "ar"
    if script == "Latin":
        # weak default
        return "en"
    return "und"


def _direction(script: str) -> str:
    return "rtl" if script in {"Arabic", "Hebrew"} else "ltr"


def _get_tokens(inp: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    p = inp.get("perception", {})
    toks = p.get("tokens")
    if isinstance(toks, list):
        return toks
    return None


def b1f5_script_tagger(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F5 — Perception.Text.ScriptTagger (Noema)
    Input:
      { "perception": { "tokens": [ { "text": str, "span": { "start": int, "end": int }, "type": str }, ... ] } }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "script_tags": [
            { "span": {"start": int, "end": int}, "script": str, "dir": "ltr|rtl",
              "lang_hint": "fa|ar|en|und", "confidence": float }
          ],
          "meta": { "source": "B1F5", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|no_tokens|invalid_tokens", "distribution": {script: count} }
      }
    """
    tokens = _get_tokens(input_json)
    if tokens is None or len(tokens) == 0:
        return {
            "status": "SKIP",
            "perception": {"script_tags": [], "meta": {"source": "B1F5", "rules_version": RULES_VERSION}},
            "diag": {"reason": "no_tokens", "distribution": {}},
        }
    if not isinstance(tokens, list):
        return {"status": "FAIL", "diag": {"reason": "invalid_tokens"}}

    tags: List[Dict[str, Any]] = []
    dist: Dict[str, int] = {}

    for t in tokens:
        if not isinstance(t, dict):
            continue
        text = t.get("text")
        span = t.get("span")
        if not isinstance(text, str) or not isinstance(span, dict) or "start" not in span or "end" not in span:
            continue

        script, conf, _ = _token_main_script(text)
        lang = _lang_hint(script if script != "Mixed" else "und", text)
        dirn = _direction(script if script != "Mixed" else "ltr")

        tags.append({
            "span": {"start": int(span["start"]), "end": int(span["end"])},
            "script": script,
            "dir": dirn,
            "lang_hint": lang,
            "confidence": round(conf, 4),
        })
        dist[script] = dist.get(script, 0) + 1

    return {
        "status": "OK",
        "perception": {"script_tags": tags, "meta": {"source": "B1F5", "rules_version": RULES_VERSION}},
        "diag": {"reason": "ok", "distribution": dist},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "tokens": [
                {"text": "Hello", "span": {"start": 0, "end": 4}, "type": "word"},
                {"text": "،", "span": {"start": 5, "end": 5}, "type": "punct"},
                {"text": "دنیا", "span": {"start": 7, "end": 10}, "type": "word"},
                {"text": "3.14", "span": {"start": 12, "end": 15}, "type": "number"},
                {"text": "😊", "span": {"start": 17, "end": 17}, "type": "emoji"},
                {"text": "کامپیوتر", "span": {"start": 19, "end": 25}, "type": "word"},
            ]
        }
    }
    out = b1f5_script_tagger(sample)
    for s in out["perception"]["script_tags"]:
        print(s)
