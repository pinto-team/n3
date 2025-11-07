# Folder: noema/n3_core/block_1_perception
# File:   b1f2_normalizer.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b1f2_normalize"]

MAX_CHARS = 8000  # conservative safety cap

# Zero-width characters to remove (keep ZWNJ \u200C for Persian orthography)
_ZW_REMOVE = {
    "\u200B",  # ZERO WIDTH SPACE
    "\uFEFF",  # ZERO WIDTH NO-BREAK SPACE / BOM
    "\u2060",  # WORD JOINER
}

_CTRL_KEEP = {"\n", "\t"}  # keep new line and tab; drop other C0 controls


def _get_input_text(inp: Dict[str, Any]) -> Optional[str]:
    # Prefer pipeline contract: perception.raw_text (from B1F1)
    p = inp.get("perception", {})
    if isinstance(p, dict) and isinstance(p.get("raw_text"), str):
        return p["raw_text"]
    # Fallbacks for robustness
    if isinstance(inp.get("text"), str):
        return inp["text"]
    if isinstance(inp.get("raw_text"), str):
        return inp["raw_text"]
    return None


def _normalize_unicode_nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def _strip_bom_zw(s: str) -> str:
    # Remove BOM and selected zero-width chars but KEEP ZWNJ (\u200C)
    if not s:
        return s
    s = "".join(ch for ch in s if ch not in _ZW_REMOVE)
    return s


def _normalize_newlines(s: str) -> str:
    # CRLF/CR -> LF
    return s.replace("\r\n", "\n").replace("\r", "\n")


def _strip_disallowed_controls(s: str) -> str:
    # Replace any C0 control (U+0000..U+001F) except \n and \t with a space
    if not s:
        return s
    out_chars: List[str] = []
    for ch in s:
        oc = ord(ch)
        if 0 <= oc < 32 and ch not in _CTRL_KEEP:
            out_chars.append(" ")
        else:
            out_chars.append(ch)
    return "".join(out_chars)


def _trim_edges(s: str) -> str:
    # Do not collapse internal spaces to preserve semantics/formatting
    return s.strip()


def _truncate_if_needed(s: str) -> (str, bool):
    if len(s) <= MAX_CHARS:
        return s, False
    return s[:MAX_CHARS], True


def b1f2_normalize(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F2 — Perception.Text.Normalizer (Noema)
    Input:
      {
        "perception": { "raw_text": "..." } | { ... },
        ...
      }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "normalized_text": "...",          # only on OK
          "meta": {
            "source": "B1F2",
            "truncated": bool,
            "ops": ["unicode_nfc","strip_bom_zw","normalize_newlines","strip_controls","trim","cap"]  # applied ops
          }
        },
        "diag": {
          "reason": "ok|no_text|invalid_text_type",
          "len_in": int?,
          "len_out": int?
        }
      }
    """
    raw = _get_input_text(input_json)
    if raw is None:
        return {
            "status": "SKIP",
            "perception": {"meta": {"source": "B1F2", "truncated": False, "ops": []}},
            "diag": {"reason": "no_text"},
        }
    if not isinstance(raw, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    ops_applied: List[str] = []
    txt = raw

    txt2 = _normalize_unicode_nfc(txt)
    if txt2 != txt:
        ops_applied.append("unicode_nfc")
        txt = txt2

    txt2 = _strip_bom_zw(txt)
    if txt2 != txt:
        ops_applied.append("strip_bom_zw")
        txt = txt2

    txt2 = _normalize_newlines(txt)
    if txt2 != txt:
        ops_applied.append("normalize_newlines")
        txt = txt2

    txt2 = _strip_disallowed_controls(txt)
    if txt2 != txt:
        ops_applied.append("strip_controls")
        txt = txt2

    txt2 = _trim_edges(txt)
    if txt2 != txt:
        ops_applied.append("trim")
        txt = txt2

    txt2, truncated = _truncate_if_needed(txt)
    if truncated:
        ops_applied.append("cap")
    txt = txt2

    return {
        "status": "OK",
        "perception": {
            "normalized_text": txt,
            "meta": {"source": "B1F2", "truncated": truncated, "ops": ops_applied},
        },
        "diag": {"reason": "ok", "len_in": len(raw), "len_out": len(txt)},
    }


if __name__ == "__main__":
    sample = {
        "perception": {"raw_text": "\ufeffHello\u200B  \r\nWorld\t!\n"},
    }
    print(b1f2_normalize(sample))
