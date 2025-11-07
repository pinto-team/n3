# Folder: noema/n3_core/block_3_memory
# File:   b3f2_indexer.py

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Iterable

import unicodedata

__all__ = ["b3f2_index"]

RULES_VERSION = "1.0"

MAX_TERMS = 5000
MAX_POS_PER_TERM = 64
MAX_GRAMS = 12000
GRAM_N = 3
SKETCH_K = 64

ALLOWED_TOKEN_TYPES = {
    "word", "number", "emoji", "url", "email", "hashtag", "mention"
}
EXCLUDED_TOKEN_TYPES = {"punct", "symbol"}

RE_WS = re.compile(r"\s+", re.UNICODE)


# ------------------------- extractors -------------------------

def _get_packz_like(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Prefer memory.wal.record if present (downstream of B3F1), else perception.packz
    mem = inp.get("memory")
    if isinstance(mem, dict):
        wal = mem.get("wal")
        if isinstance(wal, dict) and isinstance(wal.get("record"), dict):
            return wal["record"]
    per = inp.get("perception")
    if isinstance(per, dict) and isinstance(per.get("packz"), dict):
        return per["packz"]
    if isinstance(inp.get("packz"), dict):
        return inp["packz"]
    return None


def _nfc_casefold(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _collapse_ws(s: str) -> str:
    return RE_WS.sub(" ", s).strip()


def _safe_commit_date(meta: Dict[str, Any]) -> Optional[str]:
    ts = meta.get("commit_time") if isinstance(meta, dict) else None
    if not isinstance(ts, str) or not ts:
        return None
    # Support ...Z or offset forms
    t = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
    try:
        dt = datetime.fromisoformat(t)
        return dt.date().isoformat()
    except Exception:
        return None


def _collect_tokens(pk: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Prefer spans.tokens if available; otherwise, split text as fallback
    spans = pk.get("spans") if isinstance(pk.get("spans"), dict) else {}
    toks = spans.get("tokens") if isinstance(spans.get("tokens"), list) else None
    if isinstance(toks, list) and toks:
        return toks
    # Fallback: whitespace tokenization without spans
    text = pk.get("text") if isinstance(pk.get("text"), str) else ""
    if not text.strip():
        return []
    offs = 0
    out: List[Dict[str, Any]] = []
    for m in RE_WS.split(text):
        if not m:
            continue
        pos = text.find(m, offs)
        if pos < 0:
            pos = offs
        out.append({"text": m, "span": {"start": pos, "end": pos + len(m) - 1}, "type": "word"})
        offs = pos + len(m)
    return out


# ------------------------- index builders -------------------------

def _build_lexical(tokens: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    postings: Dict[str, List[int]] = {}
    counts: Dict[str, int] = {}

    for t in tokens:
        if not isinstance(t, dict):
            continue
        typ = t.get("type")
        if typ in EXCLUDED_TOKEN_TYPES:
            continue
        if typ not in ALLOWED_TOKEN_TYPES:
            # treat unknown token types as words
            typ = "word"

        text = t.get("text")
        span = t.get("span", {})
        if not isinstance(text, str):
            continue
        if not isinstance(span, dict) or "start" not in span:
            continue

        norm = _nfc_casefold(text)
        if not norm:
            continue

        pos = int(span["start"])
        lst = postings.setdefault(norm, [])
        if len(lst) < MAX_POS_PER_TERM:
            lst.append(pos)
        counts[norm] = counts.get(norm, 0) + 1

    # Convert to compact list with caps
    items = []
    for term, pos_list in postings.items():
        items.append({"t": term, "tf": counts.get(term, len(pos_list)), "pos": pos_list})
    # Cap total terms
    if len(items) > MAX_TERMS:
        items = items[:MAX_TERMS]
    return items, counts


def _char_ngrams(s: str, n: int = GRAM_N) -> Iterable[str]:
    s = _collapse_ws(_nfc_casefold(s))
    L = len(s)
    if L == 0:
        return []
    if L < n:
        return [s]
    return (s[i:i + n] for i in range(L - n + 1))


def _build_chargrams(text: str) -> Tuple[List[Dict[str, Any]], List[int]]:
    tf: Dict[str, int] = {}
    for g in _char_ngrams(text, GRAM_N):
        tf[g] = tf.get(g, 0) + 1
        if len(tf) >= MAX_GRAMS:
            break
    grams = [{"g": k, "tf": v} for k, v in tf.items()]
    # Sketch: 64-bit hashes; pick smallest SKETCH_K for a stable, compact signature
    hashes: List[int] = []
    for g in tf.keys():
        h = int(hashlib.sha1(g.encode("utf-8")).hexdigest()[:16], 16)  # 64-bit via first 16 hex
        hashes.append(h)
    hashes.sort()
    sketch = hashes[:SKETCH_K]
    return grams, sketch


def _build_facets(pk: Dict[str, Any]) -> Dict[str, Any]:
    sig = pk.get("signals") if isinstance(pk.get("signals"), dict) else {}
    meta = pk.get("meta") if isinstance(pk.get("meta"), dict) else {}
    return {
        "dir": sig.get("direction", "ltr"),
        "sa": sig.get("speech_act"),
        "to_noema": bool(sig.get("addressed_to_noema", False)),
        "date": _safe_commit_date(meta),
    }


# ------------------------- main -------------------------

def b3f2_index(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B3F2 — Memory.Indexer (Noema)
    Builds index operations for lexical terms, character 3-grams, and facets from a PackZ-like record.

    Input (best-effort):
      { "perception": { "packz": {...} } }  OR  { "memory": { "wal": { "record": {...} } } }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "memory": {
          "index_ops": {
            "doc_id": str,
            "ops": [
              { "index": "lexical", "action": "upsert", "terms": [ {"t": str, "tf": int, "pos": [int,...]}, ... ] },
              { "index": "ngram3", "action": "upsert", "grams": [ {"g": str, "tf": int}, ... ], "sketch": [int,...] },
              { "index": "facet",  "action": "upsert", "facets": {"dir": str, "sa": str|None, "to_noema": bool, "date": "YYYY-MM-DD"|None} }
            ],
            "meta": { "source": "B3F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_packz|invalid", "counts": {"terms": int, "unique_terms": int, "grams": int} }
      }
    """
    pk = _get_packz_like(input_json)
    if pk is None:
        return {
            "status": "SKIP",
            "memory": {
                "index_ops": {"doc_id": "", "ops": [], "meta": {"source": "B3F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_packz"},
        }
    if not isinstance(pk, dict):
        return {"status": "FAIL", "diag": {"reason": "invalid"}}

    doc_id = pk.get("id") if isinstance(pk.get("id"), str) else None
    text = pk.get("text") if isinstance(pk.get("text"), str) else ""
    if not doc_id or not text.strip():
        return {
            "status": "SKIP",
            "memory": {
                "index_ops": {"doc_id": "", "ops": [], "meta": {"source": "B3F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_packz"},
        }

    tokens = _collect_tokens(pk)
    terms, term_counts = _build_lexical(tokens)
    grams, sketch = _build_chargrams(text)
    facets = _build_facets(pk)

    ops = [
        {"index": "lexical", "action": "upsert", "terms": terms},
        {"index": "ngram3", "action": "upsert", "grams": grams, "sketch": sketch},
        {"index": "facet", "action": "upsert", "facets": facets},
    ]

    return {
        "status": "OK",
        "memory": {
            "index_ops": {
                "doc_id": doc_id,
                "ops": ops,
                "meta": {"source": "B3F2", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {
                "terms": sum(tc for tc in term_counts.values()),
                "unique_terms": len(term_counts),
                "grams": len(grams),
            }
        },
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "packz": {
                "id": "abc123",
                "text": "سلام نوما، لطفاً این متن را ایندکس کن. سلام!",
                "counts": {"chars": 44, "words": 8, "tokens": 0, "sentences": 2},
                "signals": {"direction": "rtl", "addressed_to_noema": True, "speech_act": "request", "confidence": 0.9,
                            "novelty": 0.6},
                "meta": {"commit_time": "2025-11-07T09:40:00Z", "truncated_spans": False},
                "spans": {
                    "tokens": [
                        {"text": "سلام", "span": {"start": 0, "end": 3}, "type": "word"},
                        {"text": "نوما", "span": {"start": 5, "end": 8}, "type": "word"},
                        {"text": "لطفاً", "span": {"start": 11, "end": 15}, "type": "word"},
                        {"text": "ایندکس", "span": {"start": 26, "end": 31}, "type": "word"},
                        {"text": "کن", "span": {"start": 33, "end": 34}, "type": "word"},
                        {"text": "سلام", "span": {"start": 37, "end": 40}, "type": "word"},
                        {"text": "!", "span": {"start": 41, "end": 41}, "type": "punct"},
                    ]
                }
            }
        }
    }
    out = b3f2_index(sample)
    print(out["memory"]["index_ops"]["doc_id"], out["diag"])
