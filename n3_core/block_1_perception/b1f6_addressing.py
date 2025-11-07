# Folder: noema/n3_core/block_1_perception
# File:   b1f6_addressing.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b1f6_addressing"]

RULES_VERSION = "1.0"
NOEMA_ALIASES = {
    "noema",  # Latin
    "نوما",  # Persian/Arabic script
    "noëma",  # diacritic variant
}

MENTION_NORMALIZER = re.compile(r"^@+")

# Simple vocative surface forms near sentence start (case-insensitive)
VOCATIVE_TRIGGERS = {
    "hey", "hi", "hello", "dear",
    "سلام", "درود",  # Persian greetings (strings are fine; comments remain English)
}


def _get_text_and_tokens(inp: Dict[str, Any]) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    p = inp.get("perception", {})
    text = None
    if isinstance(p, dict) and isinstance(p.get("normalized_text"), str):
        text = p["normalized_text"]
    elif isinstance(inp.get("text"), str):
        text = inp["text"]
    elif isinstance(inp.get("raw_text"), str):
        text = inp["raw_text"]

    toks = []
    if isinstance(p, dict) and isinstance(p.get("tokens"), list):
        toks = p["tokens"]
    return text, toks


def _norm_str(s: str) -> str:
    # Unicode NFC + casefold to compare across scripts/diacritics
    return unicodedata.normalize("NFC", s).casefold()


def _token_text(token: Dict[str, Any]) -> str:
    t = token.get("text")
    return t if isinstance(t, str) else ""


def _collect_mentions(tokens: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for t in tokens:
        if t.get("type") == "mention":
            mt = _token_text(t)
            mt = MENTION_NORMALIZER.sub("", mt)
            if mt:
                out.append(mt)
    return out


def _is_noema_name(s: str) -> bool:
    ns = _norm_str(s)
    return ns in {_norm_str(a) for a in NOEMA_ALIASES}


def _mentions_include_noema(mentions: List[str]) -> bool:
    for m in mentions:
        if _is_noema_name(m):
            return True
    return False


def _name_hits_in_tokens(tokens: List[Dict[str, Any]]) -> List[Tuple[int, str]]:
    hits: List[Tuple[int, str]] = []
    for idx, t in enumerate(tokens):
        txt = _token_text(t)
        if not txt:
            continue
        if _is_noema_name(txt):
            hits.append((idx, txt))
    return hits


def _detect_vocatives(tokens: List[Dict[str, Any]], window: int = 5) -> List[str]:
    # Look at the first 'window' non-space-like tokens and collect patterns like "Hello", "Hey", "سلام", and then a name.
    vocs: List[str] = []
    seen: List[str] = []
    for t in tokens[:window]:
        txt = _token_text(t)
        if not txt.strip():
            continue
        seen.append(txt)

    # If greeting present at start, and followed by a name/mention, return both as a vocative phrase
    if seen:
        first = _norm_str(seen[0])
        if first in {_norm_str(x) for x in VOCATIVE_TRIGGERS} and len(seen) >= 2:
            phrase = " ".join(seen[:2])
            vocs.append(phrase)
        # Also capture direct name as vocative if name is first token
        if _is_noema_name(seen[0]):
            vocs.append(seen[0])
    return vocs


def _unique_strs(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for it in items:
        key = _norm_str(it)
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def b1f6_addressing(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F6 — Perception.Text.Addressing (Noema)
    Input:
      {
        "perception": {
          "tokens": [ { "text": str, "span": {"start": int, "end": int}, "type": str }, ... ],
          "normalized_text": "..."?
        }
      }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "addressing": {
            "is_to_noema": bool,
            "addressees": [ {"name": str, "method": "mention|name"} ],
            "mentions": [str],
            "vocatives": [str],
            "meta": { "source": "B1F6", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_content|invalid_tokens", "signals": { "mention": bool, "name": bool, "voc": bool } }
      }
    """
    text, tokens = _get_text_and_tokens(input_json)

    if (not text or not text.strip()) and not tokens:
        return {
            "status": "SKIP",
            "perception": {"addressing": {"is_to_noema": False, "addressees": [], "mentions": [], "vocatives": [],
                                          "meta": {"source": "B1F6", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_content", "signals": {}},
        }

    if tokens is not None and not isinstance(tokens, list):
        return {"status": "FAIL", "diag": {"reason": "invalid_tokens"}}

    mentions = _collect_mentions(tokens or [])
    name_hits = _name_hits_in_tokens(tokens or [])
    vocatives = _detect_vocatives(tokens or [])

    addressees: List[Dict[str, Any]] = []
    for m in mentions:
        addressees.append({"name": m, "method": "mention"})
    for _, name in name_hits:
        # Avoid duplication with mentions
        if not any(_is_noema_name(a["name"]) and a["method"] == "mention" for a in addressees):
            addressees.append({"name": name, "method": "name"})

    # Is-to-Noema logic: explicit @mention or name presence near start or anywhere
    is_to_noema = _mentions_include_noema(mentions) or any(_is_noema_name(nm) for _, nm in name_hits)
    # As a soft fallback, scan raw text for "noema" strings if tokens are missing or corrupted
    if not is_to_noema and isinstance(text, str):
        lowered = _norm_str(text)
        if any(_norm_str(a) in lowered for a in NOEMA_ALIASES):
            is_to_noema = True

    # Deduplicate list strings (keep original surface form)
    mentions = _unique_strs(mentions)
    vocatives = _unique_strs(vocatives)

    out = {
        "status": "OK",
        "perception": {
            "addressing": {
                "is_to_noema": bool(is_to_noema),
                "addressees": addressees,
                "mentions": mentions,
                "vocatives": vocatives,
                "meta": {"source": "B1F6", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "signals": {
                "mention": _mentions_include_noema(mentions),
                "name": any(_is_noema_name(nm) for _, nm in name_hits),
                "voc": any(_is_noema_name(v) for v in vocatives),
            },
        },
    }
    return out


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "سلام نوما، لطفاً اینو بررسی کن @noema",
            "tokens": [
                {"text": "سلام", "span": {"start": 0, "end": 3}, "type": "word"},
                {"text": "نوما", "span": {"start": 5, "end": 8}, "type": "word"},
                {"text": "،", "span": {"start": 9, "end": 9}, "type": "punct"},
                {"text": "لطفاً", "span": {"start": 11, "end": 15}, "type": "word"},
                {"text": "@noema", "span": {"start": 28, "end": 33}, "type": "mention"},
            ]
        }
    }
    print(b1f6_addressing(sample))
