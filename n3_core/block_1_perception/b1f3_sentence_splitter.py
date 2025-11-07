# Folder: noema/n3_core/block_1_perception
# File:   b1f3_sentence_splitter.py

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

__all__ = ["b1f3_split_sentences"]

# Basic Unicode-aware sentence enders
SENT_END_CHARS = {".", "!", "?", "؟", "…", "。", "！", "？"}
CLOSE_QUOTES = {"\"", "'", "”", "“", "’", "«", "»", ")", "]", "}"}

# Common abbreviations to avoid splitting on (lowercased, without trailing dot)
ABBREVIATIONS = {
    # English
    "mr", "mrs", "ms", "dr", "prof", "sr", "jr", "st", "vs", "etc", "i.e", "e.g",
    "no", "fig", "al", "dept", "est", "approx",
    # Short multilingual set (extend as needed)
    "u.s", "u.k", "ph.d", "m.sc", "b.sc",
}


def _get_text(inp: Dict[str, Any]) -> Optional[str]:
    p = inp.get("perception", {})
    if isinstance(p, dict) and isinstance(p.get("normalized_text"), str):
        return p["normalized_text"]
    if isinstance(inp.get("text"), str):
        return inp["text"]
    if isinstance(inp.get("raw_text"), str):
        return inp["raw_text"]
    return None


def _is_number_period(txt: str, i: int) -> bool:
    # Detect decimals like 3.14 or 1.000.000
    if i <= 0 or i >= len(txt) - 1:
        return False
    return txt[i - 1].isdigit() and txt[i + 1].isdigit()


def _is_ellipsis(txt: str, i: int) -> Tuple[bool, int]:
    # Detect "..." or "……" or single "…" (U+2026). Return (is_ellipsis, end_index)
    ch = txt[i]
    if ch == "…":
        return True, i
    if ch == ".":
        # Check for ... sequence
        j = i
        dots = 0
        while j < len(txt) and txt[j] == ".":
            dots += 1
            j += 1
        if dots >= 3:
            return True, j - 1
    return False, i


def _is_abbreviation(txt: str, i: int) -> bool:
    # i points to '.' character. Look back to previous word before '.'
    if txt[i] != ".":
        return False
    j = i - 1
    # Skip quotes or closing parens before the dot
    while j >= 0 and txt[j] in CLOSE_QUOTES:
        j -= 1
    # Collect token before dot
    end = j + 1
    while j >= 0 and (txt[j].isalpha() or txt[j] in {"/"}):
        j -= 1
    token = txt[j + 1:end].strip().lower()
    if not token:
        return False
    # Single-letter initials like "A." or "T."
    if len(token) == 1 and token.isalpha():
        return True
    # Known abbreviations (store without trailing dot)
    if token in ABBREVIATIONS:
        return True
    # Simple pattern "p.m." / "a.m."
    if token in {"a", "p"}:
        # Check next char(s) to be 'm' then dot (e.g., "a.m.")
        k = i + 1
        if k + 1 < len(txt) and txt[k] == "m" and txt[k + 1] == ".":
            return True
    return False


def _consume_closing_quotes(txt: str, idx: int) -> int:
    # Include trailing closing quotes/brackets in the sentence span
    j = idx + 1
    while j < len(txt) and txt[j].isspace():
        j += 1
    while j < len(txt) and txt[j] in CLOSE_QUOTES:
        idx = j
        j += 1
        # absorb any immediate closing punctuation like more quotes/brackets
        while j < len(txt) and txt[j] in CLOSE_QUOTES:
            idx = j
            j += 1
    return idx


def _trim_span(txt: str, start: int, end: int) -> Tuple[int, int]:
    # Trim leading/trailing whitespace, but keep original offsets precise
    while start <= end and txt[start].isspace():
        start += 1
    while end >= start and txt[end].isspace():
        end -= 1
    return start, end


def _make_sentence_item(txt: str, start: int, end: int) -> Optional[Dict[str, Any]]:
    start, end = _trim_span(txt, start, end)
    if start > end:
        return None
    return {
        "text": txt[start:end + 1],
        "span": {"start": start, "end": end},
    }


def b1f3_split_sentences(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F3 — Perception.Text.SentenceSplitter (Noema)
    Input:
      { "perception": { "normalized_text": "..." }, ... }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "sentences": [ { "text": str, "span": {"start": int, "end": int} }, ... ],
          "meta": { "source": "B1F3", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|no_text|invalid_text_type", "count": int }
      }
    """
    txt = _get_text(input_json)
    if txt is None or txt.strip() == "":
        return {
            "status": "SKIP",
            "perception": {"sentences": [], "meta": {"source": "B1F3", "rules_version": "1.0"}},
            "diag": {"reason": "no_text", "count": 0},
        }
    if not isinstance(txt, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    n = len(txt)
    start = 0
    i = 0
    sentences: List[Dict[str, Any]] = []

    while i < n:
        ch = txt[i]

        # Handle sentence enders, with guards
        if ch in SENT_END_CHARS:
            # Ellipsis handling
            is_ell, end_ell = _is_ellipsis(txt, i)
            end_idx = end_ell if is_ell else i

            # Abbreviation and numeric decimal guards
            if ch == "." and (_is_abbreviation(txt, i) or _is_number_period(txt, i)):
                i += 1
                continue

            # Include closing quotes/brackets after the ender
            end_idx = _consume_closing_quotes(txt, end_idx)

            # Commit sentence if it contains non-space
            item = _make_sentence_item(txt, start, end_idx)
            if item:
                sentences.append(item)

            # Move start to the next non-space after end_idx
            i = end_idx + 1
            while i < n and txt[i].isspace():
                i += 1
            start = i
            continue

        i += 1

    # Tail (no terminal punctuation)
    if start < n:
        item = _make_sentence_item(txt, start, n - 1)
        if item:
            sentences.append(item)

    return {
        "status": "OK",
        "perception": {"sentences": sentences, "meta": {"source": "B1F3", "rules_version": "1.0"}},
        "diag": {"reason": "ok", "count": len(sentences)},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "Hi Dr. Smith. Version 3.14 is live... آیا خوب است؟ بله!"
        }
    }
    out = b1f3_split_sentences(sample)
    for s in out["perception"]["sentences"]:
        print(s["span"], repr(s["text"]))
