# Folder: noema/n3_core/block_1_perception
# File:   b1f10_packz.py

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b1f10_packz"]

RULES_VERSION = "1.0"

PACK_LIMITS = {
    "tokens_max": 5000,
    "sentences_max": 1000,
    "script_tags_max": 5000,
}


def _get_perception(inp: Dict[str, Any]) -> Dict[str, Any]:
    p = inp.get("perception")
    return p if isinstance(p, dict) else {}


def _get_text(p: Dict[str, Any], inp: Dict[str, Any]) -> Optional[str]:
    if isinstance(p.get("normalized_text"), str):
        return p["normalized_text"]
    if isinstance(inp.get("text"), str):
        return inp["text"]
    if isinstance(inp.get("raw_text"), str):
        return inp["raw_text"]
    return None


def _first_meta_commit_time(p: Dict[str, Any]) -> Optional[str]:
    # Try common locations for commit_time (left by B1F1 or upstream)
    meta = p.get("meta", {})
    if isinstance(meta, dict) and isinstance(meta.get("commit_time"), str):
        return meta["commit_time"]
    # Scan nested metas
    for v in p.values():
        if isinstance(v, dict):
            m = v.get("meta", {})
            if isinstance(m, dict) and isinstance(m.get("commit_time"), str):
                return m["commit_time"]
    return None


def _hash_id(text: str, commit_time: Optional[str]) -> str:
    h = hashlib.sha1()
    h.update(unicodedata.normalize("NFC", text).encode("utf-8"))
    if commit_time:
        h.update(commit_time.encode("utf-8"))
    return h.hexdigest()


def _majority_direction(script_tags: List[Dict[str, Any]]) -> str:
    if not script_tags:
        return "ltr"
    counts = {"rtl": 0, "ltr": 0}
    for t in script_tags:
        d = t.get("dir")
        if d in counts:
            counts[d] += 1
    return "rtl" if counts["rtl"] > counts["ltr"] else "ltr"


def _safe_take(items: List[Any], n: int) -> Tuple[List[Any], bool]:
    if len(items) <= n:
        return items, False
    return items[:n], True


def _pack_sentences(p: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
    sents = p.get("sentences")
    if not isinstance(sents, list):
        return [], False
    out = []
    for s in sents:
        if isinstance(s, dict) and isinstance(s.get("text"), str) and isinstance(s.get("span"), dict):
            sp = s["span"]
            if "start" in sp and "end" in sp:
                out.append({"text": s["text"], "span": {"start": int(sp["start"]), "end": int(sp["end"])}})
    return _safe_take(out, PACK_LIMITS["sentences_max"])


def _pack_tokens(p: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
    toks = p.get("tokens")
    if not isinstance(toks, list):
        return [], False
    out = []
    for t in toks:
        if isinstance(t, dict) and isinstance(t.get("text"), str) and isinstance(t.get("span"), dict):
            sp = t["span"]
            if "start" in sp and "end" in sp:
                out.append({"text": t["text"], "span": {"start": int(sp["start"]), "end": int(sp["end"])},
                            "type": t.get("type")})
    return _safe_take(out, PACK_LIMITS["tokens_max"])


def _pack_script_tags(p: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
    tags = p.get("script_tags")
    if not isinstance(tags, list):
        return [], False
    out = []
    for t in tags:
        sp = t.get("span", {})
        if not isinstance(sp, dict) or "start" not in sp or "end" not in sp:
            continue
        out.append({
            "span": {"start": int(sp["start"]), "end": int(sp["end"])},
            "script": t.get("script"),
            "dir": t.get("dir"),
            "confidence": float(t.get("confidence", 1.0)) if isinstance(t.get("confidence"), (int, float)) else 1.0,
        })
    return _safe_take(out, PACK_LIMITS["script_tags_max"])


def _get_signals(p: Dict[str, Any], direction: str) -> Dict[str, Any]:
    addressing = p.get("addressing") if isinstance(p.get("addressing"), dict) else {}
    speech_act = p.get("speech_act") if isinstance(p.get("speech_act"), dict) else {}
    confidence = p.get("confidence") if isinstance(p.get("confidence"), dict) else {}
    novelty = p.get("novelty") if isinstance(p.get("novelty"), dict) else {}

    return {
        "direction": direction,
        "addressed_to_noema": bool(addressing.get("is_to_noema")) if isinstance(addressing, dict) else False,
        "speech_act": speech_act.get("top"),
        "confidence": float(confidence.get("score", 0.0)) if isinstance(confidence.get("score"), (int, float)) else 0.0,
        "novelty": float(novelty.get("score", 0.0)) if isinstance(novelty.get("score"), (int, float)) else 0.0,
    }


def b1f10_packz(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F10 — Perception.Text.PackZ (Noema)
    Consolidates perception outputs into a compact, deterministic package for downstream blocks.
    Input:  { "perception": { ... } }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "packz": {
            "id": str,                       # stable SHA1 over text + commit_time
            "text": str,                     # final normalized text
            "counts": { "chars": int, "words": int, "tokens": int, "sentences": int },
            "signals": { "direction": "ltr|rtl", "addressed_to_noema": bool, "speech_act": str?, "confidence": float, "novelty": float },
            "spans": { "sentences": [...], "tokens": [...], "script_tags": [...] },
            "meta": { "source": "B1F10", "rules_version": "1.0", "commit_time": str?, "truncated_spans": bool }
          }
        },
        "diag": { "reason": "ok|no_text" }
      }
    """
    p = _get_perception(input_json)
    text = _get_text(p, input_json)

    if text is None or (isinstance(text, str) and text.strip() == ""):
        return {
            "status": "SKIP",
            "perception": {
                "packz": {"id": "", "text": "", "counts": {"chars": 0, "words": 0, "tokens": 0, "sentences": 0},
                          "signals": {}, "spans": {"sentences": [], "tokens": [], "script_tags": []},
                          "meta": {"source": "B1F10", "rules_version": RULES_VERSION, "truncated_spans": False}}},
            "diag": {"reason": "no_text"},
        }

    commit_time = _first_meta_commit_time(p)
    pid = _hash_id(text, commit_time)

    # Spans (with caps)
    sent_items, sent_trunc = _pack_sentences(p)
    tok_items, tok_trunc = _pack_tokens(p)
    script_items, script_trunc = _pack_script_tags(p)
    truncated_spans = sent_trunc or tok_trunc or script_trunc

    # Direction
    direction = _majority_direction(script_items)

    # Signals
    signals = _get_signals(p, direction)

    counts = {
        "chars": len(text),
        "words": len(text.split()),
        "tokens": len(tok_items),
        "sentences": len(sent_items),
    }

    out = {
        "status": "OK",
        "perception": {
            "packz": {
                "id": pid,
                "text": text,
                "counts": counts,
                "signals": signals,
                "spans": {
                    "sentences": sent_items,
                    "tokens": tok_items,
                    "script_tags": script_items,
                },
                "meta": {
                    "source": "B1F10",
                    "rules_version": RULES_VERSION,
                    "commit_time": commit_time,
                    "truncated_spans": truncated_spans,
                },
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "سلام نوما، لطفاً این متن را بسته‌بندی کن!",
            "meta": {"commit_time": "2025-11-05T12:00:00Z"},
            "sentences": [
                {"text": "سلام نوما، لطفاً این متن را بسته‌بندی کن!", "span": {"start": 0, "end": 39}}
            ],
            "tokens": [
                {"text": "سلام", "span": {"start": 0, "end": 3}, "type": "word"},
                {"text": "!", "span": {"start": 39, "end": 39}, "type": "punct"},
            ],
            "script_tags": [
                {"span": {"start": 0, "end": 3}, "script": "Arabic", "dir": "rtl", "confidence": 0.99},
                {"span": {"start": 39, "end": 39}, "script": "Common", "dir": "ltr", "confidence": 1.0},
            ],
            "addressing": {"is_to_noema": True},
            "speech_act": {"top": "request"},
            "confidence": {"score": 0.88},
            "novelty": {"score": 0.72},
        }
    }
    res = b1f10_packz(sample)
    print(res["perception"]["packz"]["id"], res["perception"]["packz"]["signals"])
