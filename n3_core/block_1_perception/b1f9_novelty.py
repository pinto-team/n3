# Folder: noema/n3_core/block_1_perception
# File:   b1f9_novelty.py

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

__all__ = ["b1f9_novelty"]

RULES_VERSION = "1.0"


def _get_text(inp: Dict[str, Any]) -> Optional[str]:
    p = inp.get("perception", {})
    if isinstance(p, dict) and isinstance(p.get("normalized_text"), str):
        return p["normalized_text"]
    if isinstance(inp.get("text"), str):
        return inp["text"]
    if isinstance(inp.get("raw_text"), str):
        return inp["raw_text"]
    return None


def _get_tokens(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    p = inp.get("perception", {})
    toks = p.get("tokens")
    return toks if isinstance(toks, list) else []


def _get_history_texts(inp: Dict[str, Any]) -> List[str]:
    # Optional recent messages to compare against
    # Accept multiple possible shapes to keep the stage decoupled
    ctx = inp.get("context", {}) if isinstance(inp.get("context"), dict) else {}
    if isinstance(ctx.get("recent_texts"), list):
        return [t for t in ctx["recent_texts"] if isinstance(t, str)]
    p = inp.get("perception", {})
    if isinstance(p, dict):
        h = p.get("history", {})
        if isinstance(h, dict) and isinstance(h.get("texts"), list):
            return [t for t in h["texts"] if isinstance(t, str)]
    return []


def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def _char_ngrams(s: str, n: int = 3) -> List[str]:
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


def _unique_token_ratio(tokens: List[Dict[str, Any]]) -> float:
    if not tokens:
        return 0.0
    vals = [t.get("text") for t in tokens if isinstance(t, dict) and isinstance(t.get("text"), str)]
    if not vals:
        return 0.0
    uniq = len(set(vals))
    return uniq / max(1, len(vals))


def _noise_ratio(tokens: List[Dict[str, Any]]) -> float:
    if not tokens:
        return 0.0
    noisy = {"punct", "symbol", "emoji"}
    n_noisy = sum(1 for t in tokens if t.get("type") in noisy)
    return n_noisy / max(1, len(tokens))


def _self_redundancy(text: str, n: int = 3) -> float:
    grams = _char_ngrams(text, n)
    if not grams:
        return 0.0
    uniq = len(set(grams))
    # redundancy = portion of repeated n-grams
    return 1.0 - (uniq / len(grams))


def _history_similarity_stats(text: str, history: List[str], n: int = 3) -> Tuple[float, float, float, int]:
    if not history:
        return (0.0, 0.0, 0.0, 0)
    g0 = _char_ngrams(text, n)
    sims = []
    for h in history:
        if not isinstance(h, str) or not h.strip():
            continue
        sims.append(_jaccard(g0, _char_ngrams(h, n)))
    if not sims:
        return (0.0, 0.0, 0.0, 0)
    return (min(sims), max(sims), sum(sims) / len(sims), len(sims))


def b1f9_novelty(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F9 — Perception.Text.Novelty (Noema)
    Computes a 0..1 novelty score for the current message.
    Inputs (best effort):
      - perception.normalized_text (required)
      - perception.tokens (optional)
      - context.recent_texts OR perception.history.texts (optional)
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "novelty": {
            "score": float,
            "breakdown": [ { "name": str, "value": float, "weight": float, "contrib": float }, ... ],
            "similarity": { "history_min": float, "history_max": float, "history_avg": float, "compared": int },
            "signals": { "unique_token_ratio": float, "self_redundancy": float, "noise_ratio": float },
            "meta": { "source": "B1F9", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_text|invalid_text_type" }
      }
    """
    text = _get_text(input_json)
    if text is None or (isinstance(text, str) and text.strip() == ""):
        return {
            "status": "SKIP",
            "perception": {"novelty": {"score": 0.0, "breakdown": [],
                                       "similarity": {"history_min": 0.0, "history_max": 0.0, "history_avg": 0.0,
                                                      "compared": 0}, "signals": {},
                                       "meta": {"source": "B1F9", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_text"},
        }
    if not isinstance(text, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    tokens = _get_tokens(input_json)
    history = _get_history_texts(input_json)

    uniq_tok_ratio = _unique_token_ratio(tokens)
    noise = _noise_ratio(tokens)
    redund = _self_redundancy(text, n=3)
    h_min, h_max, h_avg, h_k = _history_similarity_stats(text, history, n=3)

    # Convert similarity to novelty: higher similarity -> lower novelty
    history_novelty = 1.0 - h_max if h_k > 0 else 0.8  # optimistic prior when no history

    # Weighting strategy — interpretable and bounded
    W_UNIQ = 0.45  # favors diverse vocabulary
    W_RED = 0.30  # penalizes self-repetition
    W_HIST = 0.25  # novelty against recent history

    # Effective signals (flip redundancy)
    newness_signal = uniq_tok_ratio
    anti_redundancy = 1.0 - redund
    hist_signal = history_novelty

    base = W_UNIQ * newness_signal + W_RED * anti_redundancy + W_HIST * hist_signal

    # Soft penalty for excessive noise tokens
    if noise > 0.6:
        base -= 0.1
    score = _clamp01(round(base, 3))

    breakdown = [
        {"name": "unique_token_ratio", "value": round(newness_signal, 3), "weight": W_UNIQ,
         "contrib": round(W_UNIQ * newness_signal, 3)},
        {"name": "anti_redundancy", "value": round(anti_redundancy, 3), "weight": W_RED,
         "contrib": round(W_RED * anti_redundancy, 3)},
        {"name": "history_novelty", "value": round(hist_signal, 3), "weight": W_HIST,
         "contrib": round(W_HIST * hist_signal, 3)},
        {"name": "noise_penalty", "value": round(noise, 3), "weight": -0.1 if noise > 0.6 else 0.0,
         "contrib": round(-0.1 if noise > 0.6 else 0.0, 3)},
    ]

    return {
        "status": "OK",
        "perception": {
            "novelty": {
                "score": score,
                "breakdown": breakdown,
                "similarity": {"history_min": round(h_min, 3), "history_max": round(h_max, 3),
                               "history_avg": round(h_avg, 3), "compared": h_k},
                "signals": {"unique_token_ratio": round(uniq_tok_ratio, 3), "self_redundancy": round(redund, 3),
                            "noise_ratio": round(noise, 3)},
                "meta": {"source": "B1F9", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "سلام نوما، یک سوال جدید درباره معماری دارم. معماری قبلی را تغییر دادیم؟",
            "tokens": [
                {"text": "سلام", "span": {"start": 0, "end": 3}, "type": "word"},
                {"text": "نوما", "span": {"start": 5, "end": 8}, "type": "word"},
                {"text": "،", "span": {"start": 9, "end": 9}, "type": "punct"},
                {"text": "یک", "span": {"start": 11, "end": 12}, "type": "word"},
                {"text": "سوال", "span": {"start": 14, "end": 17}, "type": "word"},
                {"text": "جدید", "span": {"start": 19, "end": 22}, "type": "word"},
            ]
        },
        "context": {
            "recent_texts": [
                "سلام نوما، یک سوال درباره معماری دارم.",
                "لطفاً اسکریپت‌ها را به پوشه‌های جدا منتقل کن.",
            ]
        }
    }
    print(b1f9_novelty(sample))
