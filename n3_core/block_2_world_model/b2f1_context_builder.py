# Folder: noema/n3_core/block_2_world_model
# File:   b2f1_context_builder.py

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

__all__ = ["b2f1_build_context"]

RULES_VERSION = "1.0"
MAX_RECENT_FRAMES = 6
NGRAM_N = 3  # char-level n-gram for lightweight similarity


# ------------------------- helpers -------------------------

def _get_packz(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    p = inp.get("perception")
    if isinstance(p, dict):
        pk = p.get("packz")
        if isinstance(pk, dict):
            return pk
    # Accept raw shapes for robustness
    if isinstance(inp.get("packz"), dict):
        return inp["packz"]
    return None


def _norm_iso(ts: Optional[str]) -> Optional[str]:
    if not isinstance(ts, str) or not ts:
        return None
    # Normalize Z to +00:00 so datetime.fromisoformat can parse
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return ts


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    tsn = _norm_iso(ts)
    if not tsn:
        return None
    try:
        return datetime.fromisoformat(tsn)
    except Exception:
        return None


def _char_ngrams(s: str, n: int) -> List[str]:
    s = s.strip()
    if len(s) < n:
        return [s] if s else []
    return [s[i:i + n] for i in range(len(s) - n + 1)]


def _jaccard(a: List[str], b: List[str]) -> float:
    if not a and not b:
        return 1.0
    A, B = set(a), set(b)
    inter = len(A & B)
    union = len(A | B) or 1
    return inter / union


def _frame_from_packz(pk: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    text = pk.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    counts = pk.get("counts") if isinstance(pk.get("counts"), dict) else {}
    signals = pk.get("signals") if isinstance(pk.get("signals"), dict) else {}
    meta = pk.get("meta") if isinstance(pk.get("meta"), dict) else {}
    return {
        "id": pk.get("id") or "",
        "text": text,
        "signals": {
            "direction": signals.get("direction"),
            "addressed_to_noema": bool(signals.get("addressed_to_noema", False)),
            "speech_act": signals.get("speech_act"),
            "confidence": float(signals.get("confidence", 0.0)) if isinstance(signals.get("confidence"),
                                                                              (int, float)) else 0.0,
            "novelty": float(signals.get("novelty", 0.0)) if isinstance(signals.get("novelty"), (int, float)) else 0.0,
        },
        "counts": {
            "chars": int(counts.get("chars", len(text))) if isinstance(counts.get("chars"), (int, float)) else len(
                text),
            "words": int(counts.get("words", len(text.split()))) if isinstance(counts.get("words"),
                                                                               (int, float)) else len(text.split()),
            "tokens": int(counts.get("tokens", 0)) if isinstance(counts.get("tokens"), (int, float)) else 0,
            "sentences": int(counts.get("sentences", 0)) if isinstance(counts.get("sentences"), (int, float)) else 0,
        },
        "meta": {
            "commit_time": meta.get("commit_time"),
            "truncated_spans": bool(meta.get("truncated_spans", False)),
        }
    }


def _collect_recent_frames(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Accept multiple shapes to keep the stage decoupled
    ctx = inp.get("context", {}) if isinstance(inp.get("context"), dict) else {}
    recent_packz = []
    if isinstance(ctx.get("recent_packz"), list):
        recent_packz = ctx["recent_packz"]
    elif isinstance(ctx.get("recent"), list):  # fallback alias
        recent_packz = ctx["recent"]
    # memory.retrieved_packz (optional)
    mem = inp.get("memory", {}) if isinstance(inp.get("memory"), dict) else {}
    if not recent_packz and isinstance(mem.get("retrieved_packz"), list):
        recent_packz = mem["retrieved_packz"]

    frames: List[Dict[str, Any]] = []
    for item in recent_packz:
        if isinstance(item, dict):
            if "packz" in item and isinstance(item["packz"], dict):  # {packz: {...}}
                f = _frame_from_packz(item["packz"])
            else:
                f = _frame_from_packz(item)
            if f:
                frames.append(f)

    # Sort by commit_time if present; otherwise keep input order
    def _sort_key(fr: Dict[str, Any]):
        t = _parse_iso(fr["meta"].get("commit_time"))
        return t or datetime.min

    frames.sort(key=_sort_key)
    # Keep last MAX_RECENT_FRAMES
    return frames[-MAX_RECENT_FRAMES:]


def _similarity_to_last(current_text: str, recents: List[Dict[str, Any]]) -> Tuple[float, float]:
    if not recents:
        return (0.0, 0.0)
    g0 = _char_ngrams(current_text, NGRAM_N)
    sims = []
    for fr in recents:
        t = fr.get("text")
        if isinstance(t, str) and t.strip():
            sims.append(_jaccard(g0, _char_ngrams(t, NGRAM_N)))
    if not sims:
        return (0.0, 0.0)
    return (sims[-1], sum(sims) / len(sims))


# ------------------------- main -------------------------

def b2f1_build_context(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B2F1 — WorldModel.ContextBuilder (Noema)

    Input:
      {
        "perception": { "packz": { ... } },
        "context": {
          "recent_packz": [ {packz}, ... ] | [packz, ... ]   # optional
        },
        "memory": {
          "retrieved_packz": [ packz, ... ]                   # optional fallback
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "world_model": {
          "context": {
            "current": { "id": str, "text": str, "signals": {...}, "counts": {...}, "meta": {...} },
            "recent":  [ same as current ... ],
            "features": {
              "dir": "ltr|rtl",
              "is_to_noema": bool,
              "speech_act": str | None,
              "confidence": float,
              "novelty": float,
              "len_chars": int,
              "len_tokens": int,
              "len_sentences": int,
              "sim_to_last": float,
              "sim_to_avg": float,
              "history_size": int
            },
            "meta": { "source": "B2F1", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_packz" }
      }
    """
    pk = _get_packz(input_json)
    if not pk:
        return {
            "status": "SKIP",
            "world_model": {"context": {"current": {}, "recent": [], "features": {},
                                        "meta": {"source": "B2F1", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_packz"},
        }

    cur = _frame_from_packz(pk)
    if not cur:
        return {
            "status": "SKIP",
            "world_model": {"context": {"current": {}, "recent": [], "features": {},
                                        "meta": {"source": "B2F1", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_packz"},
        }

    recent_frames = _collect_recent_frames(input_json)
    sim_last, sim_avg = _similarity_to_last(cur["text"], recent_frames)

    feats = {
        "dir": cur["signals"]["direction"] or "ltr",
        "is_to_noema": bool(cur["signals"]["addressed_to_noema"]),
        "speech_act": cur["signals"]["speech_act"],
        "confidence": float(cur["signals"]["confidence"]),
        "novelty": float(cur["signals"]["novelty"]),
        "len_chars": int(cur["counts"]["chars"]),
        "len_tokens": int(cur["counts"]["tokens"]),
        "len_sentences": int(cur["counts"]["sentences"]),
        "sim_to_last": round(sim_last, 3),
        "sim_to_avg": round(sim_avg, 3),
        "history_size": len(recent_frames),
    }

    out = {
        "status": "OK",
        "world_model": {
            "context": {
                "current": cur,
                "recent": recent_frames,
                "features": feats,
                "meta": {"source": "B2F1", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    sample = {
        "perception": {
            "packz": {
                "id": "abc123",
                "text": "سلام نوما، لطفاً این درخواست را بررسی کن.",
                "counts": {"chars": 33, "words": 6, "tokens": 8, "sentences": 1},
                "signals": {"direction": "rtl", "addressed_to_noema": True, "speech_act": "request", "confidence": 0.86,
                            "novelty": 0.71},
                "meta": {"commit_time": "2025-11-07T09:10:00Z", "truncated_spans": False},
            }
        },
        "context": {
            "recent_packz": [
                {
                    "packz": {
                        "id": "old1",
                        "text": "دیروز درباره ساختار پوشه‌ها صحبت کردیم.",
                        "counts": {"chars": 36, "words": 5, "tokens": 7, "sentences": 1},
                        "signals": {"direction": "rtl", "addressed_to_noema": True, "speech_act": "statement",
                                    "confidence": 0.9, "novelty": 0.5},
                        "meta": {"commit_time": "2025-11-06T18:00:00Z", "truncated_spans": False},
                    }
                }
            ]
        }
    }
    res = b2f1_build_context(sample)
    print(res["world_model"]["context"]["features"])
