# Folder: noema/n3_core/block_1_perception
# File:   b1f4_tokenizer.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b1f4_tokenize"]

# Priority patterns (left-to-right)
RE_WS = re.compile(r"\s+", re.UNICODE)
RE_URL = re.compile(r"(?:https?://|www\.)\S+", re.UNICODE | re.IGNORECASE)
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.UNICODE)
RE_HASHTAG = re.compile(r"#\w+", re.UNICODE)
RE_MENTION = re.compile(r"@\w+", re.UNICODE)
RE_NUMBER = re.compile(r"\d+(?:[.,]\d+)*(?:%|[A-Za-z])?", re.UNICODE)
RE_WORD = re.compile(r"[^\W\d_]+(?:[’'\-][^\W\d_]+)*", re.UNICODE)  # letters w/ internal ' or -

# Basic emoji ranges (single codepoint)
RE_EMOJI = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]", re.UNICODE)

SENTENCE_META_VERSION = "1.0"
TOKENIZER_RULES_VERSION = "1.0"


def _get_text_and_sentences(inp: Dict[str, Any]) -> Tuple[Optional[str], Optional[List[Dict[str, Any]]]]:
    p = inp.get("perception", {})
    text = None
    if isinstance(p, dict) and isinstance(p.get("normalized_text"), str):
        text = p["normalized_text"]
    elif isinstance(inp.get("text"), str):
        text = inp["text"]
    elif isinstance(inp.get("raw_text"), str):
        text = inp["raw_text"]

    sents = None
    if isinstance(p, dict) and isinstance(p.get("sentences"), list):
        sents = p["sentences"]
    return text, sents


def _is_punct_char(ch: str) -> bool:
    return unicodedata.category(ch).startswith("P")


def _scan_tokens(txt: str, base: int) -> List[Dict[str, Any]]:
    tokens: List[Dict[str, Any]] = []
    i, n = 0, len(txt)

    while i < n:
        seg = txt[i:]

        # 1) Skip whitespace
        m = RE_WS.match(seg)
        if m:
            i += m.end()
            continue

        # 2) Ordered recognizers
        for label, rx in (
                ("url", RE_URL),
                ("email", RE_EMAIL),
                ("hashtag", RE_HASHTAG),
                ("mention", RE_MENTION),
                ("number", RE_NUMBER),
                ("emoji", RE_EMOJI),
                ("word", RE_WORD),
        ):
            m = rx.match(seg)
            if m:
                start = base + i
                end = base + i + (m.end() - 1)
                tokens.append({"text": m.group(0), "span": {"start": start, "end": end}, "type": label})
                i += m.end()
                break
        else:
            ch = txt[i]
            start = base + i
            end = start
            if _is_punct_char(ch):
                tokens.append({"text": ch, "span": {"start": start, "end": end}, "type": "punct"})
            else:
                # Symbol or miscellaneous (math signs, currency, etc.)
                tokens.append({"text": ch, "span": {"start": start, "end": end}, "type": "symbol"})
            i += 1

    return tokens


def _tokenize_with_global_spans(full_text: str, sentences: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not sentences:
        return _scan_tokens(full_text, 0)

    out: List[Dict[str, Any]] = []
    for s in sentences:
        if not isinstance(s, dict) or "span" not in s or "text" not in s:
            continue
        span = s["span"]
        if not isinstance(span, dict) or "start" not in span or "end" not in span:
            continue
        start, end = int(span["start"]), int(span["end"])
        chunk = s.get("text")
        # Fallback to slicing to ensure correctness if text is missing or altered
        if not isinstance(chunk, str) or full_text[start:end + 1] != chunk:
            chunk = full_text[start:end + 1]
        out.extend(_scan_tokens(chunk, start))
    return out


def b1f4_tokenize(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F4 — Perception.Text.Tokenizer (Noema)
    Input:
      { "perception": { "normalized_text": "...", "sentences": [ {"text": "...", "span": {"start": int, "end": int}} ]? } }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "tokens": [ { "text": str, "span": {"start": int, "end": int}, "type": str }, ... ],
          "meta": { "source": "B1F4", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|no_text|invalid_text_type", "count": int }
      }
    """
    text, sentences = _get_text_and_sentences(input_json)

    if text is None or text.strip() == "":
        return {
            "status": "SKIP",
            "perception": {"tokens": [], "meta": {"source": "B1F4", "rules_version": TOKENIZER_RULES_VERSION}},
            "diag": {"reason": "no_text", "count": 0},
        }
    if not isinstance(text, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    tokens = _tokenize_with_global_spans(text, sentences)

    return {
        "status": "OK",
        "perception": {"tokens": tokens, "meta": {"source": "B1F4", "rules_version": TOKENIZER_RULES_VERSION}},
        "diag": {"reason": "ok", "count": len(tokens)},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "Visit https://example.com or email me@test.io 😊. نسخه 3.14 منتشر شد!",
            "sentences": [
                {"text": "Visit https://example.com or email me@test.io 😊.", "span": {"start": 0, "end": 53}},
                {"text": " نسخه 3.14 منتشر شد!", "span": {"start": 54, "end": 75}},
            ],
        }
    }
    out = b1f4_tokenize(sample)
    for t in out["perception"]["tokens"]:
        print(t["type"], t["span"], repr(t["text"]))
