# Folder: noema/n3_core/block_1_perception
# File:   b1f8_confidence.py

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

__all__ = ["b1f8_confidence"]

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


def _get_flag_truncated(inp: Dict[str, Any]) -> bool:
    # Try perception.meta.truncated from previous stages (e.g., B1F2)
    p = inp.get("perception", {})
    if isinstance(p, dict):
        meta = p.get("meta", {})
        if isinstance(meta, dict) and isinstance(meta.get("truncated"), bool):
            return meta["truncated"]
        # Also check nested metas commonly used by stages
        for k, v in p.items():
            if isinstance(v, dict):
                m = v.get("meta", {})
                if isinstance(m, dict) and isinstance(m.get("truncated"), bool):
                    return m["truncated"]
    return False


def _get_tokens(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    p = inp.get("perception", {})
    toks = p.get("tokens")
    return toks if isinstance(toks, list) else []


def _get_script_tags(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    p = inp.get("perception", {})
    st = p.get("script_tags")
    return st if isinstance(st, list) else []


def _get_addressing_to_noema(inp: Dict[str, Any]) -> bool:
    p = inp.get("perception", {})
    addr = p.get("addressing", {})
    if isinstance(addr, dict) and isinstance(addr.get("is_to_noema"), bool):
        return addr["is_to_noema"]
    return False


def _get_speech_act_top(inp: Dict[str, Any]) -> Optional[str]:
    p = inp.get("perception", {})
    sa = p.get("speech_act", {})
    if isinstance(sa, dict) and isinstance(sa.get("top"), str):
        return sa["top"]
    return None


def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def _length_score(n_chars: int) -> float:
    # Piecewise: short texts are low; optimal band ~ 10..400 chars; very long mildly decays
    if n_chars <= 1:
        return 0.05
    if n_chars < 5:
        return 0.15
    if n_chars < 10:
        return 0.3
    if n_chars <= 400:
        # grow from 0.6 at 10 to 1.0 at 200, then keep ~0.95-1 in band
        if n_chars <= 200:
            return 0.6 + 0.4 * (n_chars - 10) / 190.0
        return 0.95
    if n_chars <= 2000:
        # slight decay
        return max(0.7, 0.95 - 0.25 * (n_chars - 400) / 1600.0)
    # extremely long input
    return 0.6


def _noise_score(tokens: List[Dict[str, Any]]) -> Tuple[float, Dict[str, float]]:
    if not tokens:
        return 0.5, {"noise_ratio": 0.5}
    n = len(tokens)
    noisy_types = {"punct", "symbol", "emoji"}
    noise = sum(1 for t in tokens if t.get("type") in noisy_types)
    ratio = noise / max(1, n)
    # 0 noise -> 1.0; >=0.6 noise -> 0.2
    val = 1.0 - min(0.8, ratio * 1.333)  # cap at 0.8 drop when ratio ~0.6
    return _clamp01(val), {"noise_ratio": round(ratio, 3)}


def _script_consistency_score(tags: List[Dict[str, Any]]) -> Tuple[float, Dict[str, int]]:
    if not tags:
        return 0.7, {}
    counts: Dict[str, int] = {}
    for t in tags:
        sc = t.get("script")
        if isinstance(sc, str):
            counts[sc] = counts.get(sc, 0) + 1
    total = sum(counts.values()) or 1
    maj_count = max(counts.values()) if counts else 0
    maj_ratio = maj_count / total
    # Mixed heavily -> lower; single dominant -> higher
    base = 0.4 + 0.6 * maj_ratio
    # Penalize explicit "Mixed"
    if "Mixed" in counts:
        base -= 0.15
    return _clamp01(base), counts


def _truncation_penalty(truncated: bool) -> float:
    return -0.25 if truncated else 0.0


def _addressing_bonus(to_noema: bool) -> float:
    # Slight boost if the message is clearly addressed to the agent
    return 0.05 if to_noema else 0.0


def _speech_act_prior(label: Optional[str]) -> float:
    if not label:
        return 0.0
    priors = {
        "request": 0.15,
        "command": 0.12,
        "question": 0.1,
        "statement": 0.05,
        "thanks": 0.03,
        "apology": 0.02,
        "greeting": 0.02,
        "affirmation": 0.03,
        "negation": 0.03,
        "exclamation": 0.0,
    }
    return priors.get(label, 0.0)


def b1f8_confidence(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F8 — Perception.Text.Confidence (Noema)
    Computes a 0..1 confidence score that the perceived message is suitable for downstream processing.
    Inputs (best effort):
      - perception.normalized_text
      - perception.tokens (optional)
      - perception.script_tags (optional)
      - perception.addressing.is_to_noema (optional)
      - perception.speech_act.top (optional)
      - any meta.truncated flag from upstream (optional)
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "confidence": {
            "score": float,
            "breakdown": [ { "name": str, "value": float, "weight": float, "contrib": float }, ... ],
            "flags": [str],
            "meta": { "source": "B1F8", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_text|invalid_text_type" }
      }
    """
    text = _get_text(input_json)
    if text is None or (isinstance(text, str) and text.strip() == ""):
        return {
            "status": "SKIP",
            "perception": {"confidence": {"score": 0.0, "breakdown": [], "flags": [],
                                          "meta": {"source": "B1F8", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_text"},
        }
    if not isinstance(text, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    tokens = _get_tokens(input_json)
    script_tags = _get_script_tags(input_json)
    truncated = _get_flag_truncated(input_json)
    to_noema = _get_addressing_to_noema(input_json)
    sa_top = _get_speech_act_top(input_json)

    # Signals
    len_signal = _length_score(len(text))
    noise_signal, noise_meta = _noise_score(tokens)
    script_signal, script_dist = _script_consistency_score(script_tags)
    trunc_pen = _truncation_penalty(truncated)
    addr_bonus = _addressing_bonus(to_noema)
    sa_prior = _speech_act_prior(sa_top)

    # Weights (sum ~ 1.0 for interpretable contribution)
    W_LEN = 0.35
    W_NOISE = 0.25
    W_SCRIPT = 0.15
    W_PRIOR = 0.10
    W_ADDR = 0.05
    # Truncation is a direct penalty, applied after sum (not normalized)
    W_TRUNC = 1.0

    # Weighted sum
    base = (
            W_LEN * len_signal +
            W_NOISE * noise_signal +
            W_SCRIPT * script_signal +
            W_PRIOR * (0.5 + sa_prior) +  # center around 0.5, add prior
            W_ADDR * (0.5 + addr_bonus)  # tiny shift if addressed
    )

    score = base + W_TRUNC * trunc_pen
    score = _clamp01(round(score, 3))

    # Flags for transparency
    flags: List[str] = []
    if truncated:
        flags.append("truncated_input")
    if noise_meta["noise_ratio"] > 0.5:
        flags.append("high_token_noise")
    if script_dist and (script_dist.get("Mixed", 0) > 0 or len(script_dist) > 2):
        flags.append("script_mixed")

    breakdown = [
        {"name": "length", "value": round(len_signal, 3), "weight": W_LEN, "contrib": round(W_LEN * len_signal, 3)},
        {"name": "noise", "value": round(noise_signal, 3), "weight": W_NOISE,
         "contrib": round(W_NOISE * noise_signal, 3)},
        {"name": "script_consistency", "value": round(script_signal, 3), "weight": W_SCRIPT,
         "contrib": round(W_SCRIPT * script_signal, 3)},
        {"name": "speech_act_prior", "value": round(0.5 + sa_prior, 3), "weight": W_PRIOR,
         "contrib": round(W_PRIOR * (0.5 + sa_prior), 3)},
        {"name": "addressing", "value": round(0.5 + addr_bonus, 3), "weight": W_ADDR,
         "contrib": round(W_ADDR * (0.5 + addr_bonus), 3)},
        {"name": "truncation_penalty", "value": float(truncated), "weight": -0.25, "contrib": round(trunc_pen, 3)},
    ]

    return {
        "status": "OK",
        "perception": {
            "confidence": {
                "score": score,
                "breakdown": breakdown,
                "flags": flags,
                "meta": {"source": "B1F8", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "سلام نوما، میشه این فایل رو بررسی کنی؟ ممنون!",
            "tokens": [
                {"text": "سلام", "span": {"start": 0, "end": 3}, "type": "word"},
                {"text": "،", "span": {"start": 4, "end": 4}, "type": "punct"},
                {"text": "نوما", "span": {"start": 6, "end": 9}, "type": "word"},
                {"text": "؟", "span": {"start": 32, "end": 32}, "type": "punct"},
                {"text": "!", "span": {"start": 40, "end": 40}, "type": "punct"},
            ],
            "script_tags": [
                {"span": {"start": 0, "end": 3}, "script": "Arabic"},
                {"span": {"start": 4, "end": 4}, "script": "Common"},
                {"span": {"start": 6, "end": 9}, "script": "Arabic"},
            ],
            "addressing": {"is_to_noema": True},
            "speech_act": {"top": "request"},
            "meta": {"truncated": False},
        }
    }
    res = b1f8_confidence(sample)
    print(res["perception"]["confidence"]["score"], res["perception"]["confidence"]["flags"])
