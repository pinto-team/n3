# Folder: noema/n3_core/block_2_world_model
# File:   b2f4_uncertainty.py

from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

__all__ = ["b2f4_uncertainty"]

RULES_VERSION = "1.0"

LABELS = [
    "direct_answer",
    "execute_action",
    "ask_clarification",
    "acknowledge_only",
    "small_talk",
    "closing",
    "refuse_or_safecheck",
    "other",
]


def _get_ctx_pred_error(inp: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, float], Dict[str, Any]]:
    wm = inp.get("world_model", {}) if isinstance(inp.get("world_model"), dict) else {}
    ctx = wm.get("context", {}) if isinstance(wm.get("context"), dict) else {}
    pred = wm.get("prediction", {}) if isinstance(wm.get("prediction"), dict) else {}
    err = wm.get("error", {}) if isinstance(wm.get("error"), dict) else {}

    exp = pred.get("expected_reply")
    if not isinstance(exp, dict):
        exp = {}

    # normalize distribution
    dist = {k: float(exp.get(k, 0.0)) for k in LABELS}
    s = sum(dist.values())
    if s > 0:
        dist = {k: v / s for k, v in dist.items()}
    else:
        dist = {}

    return ctx, dist, err


def _safe_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _entropy(dist: Dict[str, float]) -> float:
    if not dist:
        return 1.0
    n = len(LABELS)
    h = 0.0
    for p in dist.values():
        if p > 0:
            h -= p * math.log(p)
    return min(1.0, h / math.log(n))


def _top_gap_uncertainty(dist: Dict[str, float]) -> float:
    if not dist:
        return 1.0
    vals = sorted(dist.values(), reverse=True)
    if len(vals) == 1:
        return 1.0 - vals[0]
    gap = vals[0] - vals[1]
    return 1.0 - gap  # smaller gap -> higher uncertainty


def _kl_to_01(kl: float) -> float:
    # Monotone squash: 0 -> 0, large -> approach 1
    return 1.0 - math.exp(-max(0.0, kl))


def _band(score: float) -> str:
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def _recommendation(score: float, conf: float, act_prob: float) -> str:
    if score >= 0.7:
        return "probe_first"
    if score >= 0.4:
        return "answer_or_probe"
    # low uncertainty
    if conf >= 0.8 and act_prob >= 0.45:
        return "act_first"
    if conf >= 0.8:
        return "answer_first"
    return "balanced"


def b2f4_uncertainty(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B2F4 — WorldModel.Uncertainty (Noema)
    Computes a 0..1 uncertainty score using:
      - prediction.expected_reply distribution (entropy and top-gap),
      - world_model.error metrics (L1, KL),
      - world_model.context.features (confidence, novelty).
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "world_model": {
          "uncertainty": {
            "score": float,                          # 0..1 (higher = more uncertain)
            "breakdown": [ {name, value, weight, contrib}, ... ],
            "flags": [str],
            "band": "low|medium|high",
            "recommendation": "probe_first|answer_or_probe|answer_first|act_first|balanced",
            "meta": { "source": "B2F4", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_prediction" }
      }
    """
    ctx, dist, err = _get_ctx_pred_error(input_json)
    if not dist:
        return {
            "status": "SKIP",
            "world_model": {"uncertainty": {"score": 1.0, "breakdown": [], "flags": ["no_prediction"], "band": "high",
                                            "recommendation": "probe_first",
                                            "meta": {"source": "B2F4", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_prediction"},
        }

    feats = ctx.get("features", {}) if isinstance(ctx.get("features"), dict) else {}

    # Components
    u_entropy = _entropy(dist)  # 0..1
    u_topgap = _top_gap_uncertainty(dist)  # 0..1
    u_l1 = _safe_float(err.get("l1"), 0.0)  # 0..1
    u_kl = _kl_to_01(_safe_float(err.get("kl"), 0.0))
    u_conf = 1.0 - _safe_float(feats.get("confidence"), 0.0)
    u_nov = _safe_float(feats.get("novelty"), 0.5) * 0.5  # mild

    # Weights (sum ~ 1.0; error treated jointly)
    W_ENT = 0.35
    W_GAP = 0.20
    W_ERR = 0.25  # will combine L1 & KL
    W_CONF = 0.15
    W_NOV = 0.05

    u_err = 0.5 * u_l1 + 0.5 * u_kl

    score = (
            W_ENT * u_entropy +
            W_GAP * u_topgap +
            W_ERR * u_err +
            W_CONF * u_conf +
            W_NOV * u_nov
    )
    score = max(0.0, min(1.0, round(score, 3)))

    flags: List[str] = []
    if u_entropy >= 0.75: flags.append("high_entropy")
    if u_topgap >= 0.75: flags.append("ambiguous_top2")
    if u_err >= 0.5:  flags.append("high_model_error")
    if u_conf >= 0.5:  flags.append("low_confidence")
    if u_nov >= 0.35: flags.append("high_novelty")

    act_prob = dist.get("execute_action", 0.0)
    band = _band(score)
    rec = _recommendation(score, _safe_float(feats.get("confidence"), 0.0), act_prob)

    breakdown = [
        {"name": "entropy", "value": round(u_entropy, 3), "weight": W_ENT, "contrib": round(W_ENT * u_entropy, 3)},
        {"name": "top_gap_uncertainty", "value": round(u_topgap, 3), "weight": W_GAP,
         "contrib": round(W_GAP * u_topgap, 3)},
        {"name": "model_error", "value": round(u_err, 3), "weight": W_ERR, "contrib": round(W_ERR * u_err, 3)},
        {"name": "inv_confidence", "value": round(u_conf, 3), "weight": W_CONF, "contrib": round(W_CONF * u_conf, 3)},
        {"name": "novelty_mild", "value": round(u_nov, 3), "weight": W_NOV, "contrib": round(W_NOV * u_nov, 3)},
    ]

    return {
        "status": "OK",
        "world_model": {
            "uncertainty": {
                "score": score,
                "breakdown": breakdown,
                "flags": flags,
                "band": band,
                "recommendation": rec,
                "meta": {"source": "B2F4", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    sample = {
        "world_model": {
            "context": {
                "features": {"confidence": 0.78, "novelty": 0.62}
            },
            "prediction": {
                "expected_reply": {
                    "direct_answer": 0.28,
                    "execute_action": 0.34,
                    "ask_clarification": 0.25,
                    "acknowledge_only": 0.03,
                    "small_talk": 0.03,
                    "closing": 0.02,
                    "refuse_or_safecheck": 0.04,
                    "other": 0.01,
                }
            },
            "error": {"l1": 0.18, "kl": 0.07}
        }
    }
    res = b2f4_uncertainty(sample)
    print(res["world_model"]["uncertainty"]["score"], res["world_model"]["uncertainty"]["flags"],
          res["world_model"]["uncertainty"]["recommendation"])
