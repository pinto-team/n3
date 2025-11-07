# Folder: noema/n3_core/block_2_world_model
# File:   b2f3_error_computer.py

from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

__all__ = ["b2f3_compute_error"]

RULES_VERSION = "1.0"

TRACE_LIMIT = 12

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


def _get_context_and_pred(inp: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, float]]:
    wm = inp.get("world_model", {}) if isinstance(inp.get("world_model"), dict) else {}
    ctx = wm.get("context", {}) if isinstance(wm.get("context"), dict) else {}
    pred = wm.get("prediction", {}) if isinstance(wm.get("prediction"), dict) else {}

    # prefer latest prediction at wm.prediction; fallback to ctx.prediction if present
    expected = pred.get("expected_reply")
    if not isinstance(expected, dict):
        ctx_pred = ctx.get("prediction", {})
        expected = ctx_pred.get("expected_reply") if isinstance(ctx_pred, dict) else None
    if not isinstance(expected, dict):
        expected = {}

    # normalize to ensure valid distribution over LABELS
    dist = {k: float(expected.get(k, 0.0)) for k in LABELS}
    s = sum(dist.values())
    if s > 0:
        dist = {k: v / s for k, v in dist.items()}
    else:
        # uniform fallback (will be treated as low-information)
        dist = {k: 1.0 / len(LABELS) for k in LABELS}

    return ctx, dist


def _safe_cf(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _target_from_speech_act(sa: str) -> Dict[str, float]:
    # Canonical target mapping derived from conversational priors
    t = {k: 0.0 for k in LABELS}
    sac = (sa or "").lower()
    if sac == "question":
        t["direct_answer"] = 0.75
        t["ask_clarification"] = 0.15
        t["other"] = 0.10
    elif sac == "request":
        t["execute_action"] = 0.70
        t["ask_clarification"] = 0.20
        t["other"] = 0.10
    elif sac == "command":
        t["execute_action"] = 0.75
        t["refuse_or_safecheck"] = 0.10
        t["ask_clarification"] = 0.10
        t["other"] = 0.05
    elif sac == "thanks":
        t["acknowledge_only"] = 0.70
        t["closing"] = 0.20
        t["other"] = 0.10
    elif sac == "greeting":
        t["small_talk"] = 0.70
        t["acknowledge_only"] = 0.20
        t["other"] = 0.10
    elif sac == "apology":
        t["acknowledge_only"] = 0.60
        t["other"] = 0.40
    elif sac == "affirmation":
        t["direct_answer"] = 0.45
        t["execute_action"] = 0.25
        t["acknowledge_only"] = 0.20
        t["other"] = 0.10
    elif sac == "negation":
        t["ask_clarification"] = 0.60
        t["other"] = 0.40
    elif sac == "exclamation":
        t["small_talk"] = 0.40
        t["other"] = 0.60
    else:
        # Unknown → soft uniform bias
        return {k: 1.0 / len(LABELS) for k in LABELS}
    return t


def _move_to_label(move: str) -> str:
    mv = (move or "").lower()
    if mv in {"answer", "final_answer", "nlg"}:
        return "direct_answer"
    if mv in {"execute", "action", "dispatch"}:
        return "execute_action"
    if mv in {"ask", "clarify", "confirm"}:
        return "ask_clarification"
    if mv in {"ack", "acknowledge"}:
        return "acknowledge_only"
    if mv in {"smalltalk", "small_talk"}:
        return "small_talk"
    if mv in {"closing", "goodbye"}:
        return "closing"
    if mv in {"refuse", "safecheck"}:
        return "refuse_or_safecheck"
    return "other"


def _actual_outcome(inp: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    dialog = inp.get("dialog", {}) if isinstance(inp.get("dialog"), dict) else {}
    final = dialog.get("final", {}) if isinstance(dialog.get("final"), dict) else {}
    turn = dialog.get("turn", {}) if isinstance(dialog.get("turn"), dict) else {}

    move = final.get("move") or turn.get("move")
    label = _move_to_label(move if isinstance(move, str) else "")

    exec_info = inp.get("executor", {}) if isinstance(inp.get("executor"), dict) else {}
    best = exec_info.get("results", {}).get("best") if isinstance(exec_info.get("results"), dict) else None
    exec_meta: Dict[str, Any] = {}
    if isinstance(best, dict) and best:
        exec_meta = {
            "req_id": best.get("req_id"),
            "ok": bool(best.get("ok", True)),
            "kind": best.get("kind"),
        }
        if best.get("ok"):
            label = "execute_action"

    return label, exec_meta


def _l1_distance(p: Dict[str, float], q: Dict[str, float]) -> float:
    return 0.5 * sum(abs(p[k] - q[k]) for k in LABELS)


def _kl_divergence(p: Dict[str, float], q: Dict[str, float], eps: float = 1e-12) -> float:
    s = 0.0
    for k in LABELS:
        pv = max(eps, p[k])
        qv = max(eps, q[k])
        s += pv * math.log(pv / qv)
    return s


def _top_key(d: Dict[str, float]) -> Tuple[str, float]:
    k = max(d.items(), key=lambda kv: kv[1])[0]
    return k, d[k]


def _trace_history(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    wm = inp.get("world_model", {}) if isinstance(inp.get("world_model"), dict) else {}
    trace = wm.get("trace", {}) if isinstance(wm.get("trace"), dict) else {}
    hist = trace.get("error_history") if isinstance(trace.get("error_history"), list) else []
    return [h for h in hist if isinstance(h, dict)]


def b2f3_compute_error(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B2F3 — WorldModel.ErrorComputer (Noema)
    Compares the predicted expected-reply distribution (from B2F2) with a target distribution
    implied by the current speech_act. Outputs scalar errors (L1 and KL) and diagnostic fields.

    Input:
      {
        "world_model": {
          "context": { "features": { "speech_act": str, "confidence": float } },
          "prediction": { "expected_reply": {label: prob, ...} }   # from B2F2
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "world_model": {
          "error": {
            "l1": float,                 # 0..1
            "kl": float,                 # >= 0
            "target": {label: prob, ...},
            "predicted": {label: prob, ...},
            "canonical_top": {"label": str, "prob": float},
            "predicted_on_canonical": float,
            "components": {
              "speech_act": str | None,
              "confidence": float
            },
            "meta": { "source": "B2F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_context|no_prediction" }
      }
    """
    ctx, pred = _get_context_and_pred(input_json)
    feats = ctx.get("features", {}) if isinstance(ctx.get("features"), dict) else {}
    sa = feats.get("speech_act")
    if not isinstance(ctx, dict) or not feats:
        return {
            "status": "SKIP",
            "world_model": {"error": {"l1": 0.0, "kl": 0.0, "target": {}, "predicted": {}, "canonical_top": {},
                                      "predicted_on_canonical": 0.0, "components": {},
                                      "meta": {"source": "B2F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_context"},
        }

    # If prediction object was missing and we fell back to uniform, mark it
    pred_sum = sum(pred.values())
    if pred_sum <= 0:
        return {
            "status": "SKIP",
            "world_model": {"error": {"l1": 0.0, "kl": 0.0, "target": {}, "predicted": {}, "canonical_top": {},
                                      "predicted_on_canonical": 0.0, "components": {},
                                      "meta": {"source": "B2F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_prediction"},
        }

    target = _target_from_speech_act(sa or "")
    # Normalize target to be safe
    t_sum = sum(target.values())
    if t_sum <= 0:
        target = {k: 1.0 / len(LABELS) for k in LABELS}

    actual_label, exec_meta = _actual_outcome(input_json)
    actual = {k: 0.0 for k in LABELS}
    if actual_label in actual:
        actual[actual_label] = 1.0
    else:
        actual = {k: target.get(k, 0.0) for k in LABELS}
        actual = {k: v / (sum(actual.values()) or 1.0) for k, v in actual.items()}

    l1 = round(_l1_distance(pred, actual), 6)
    kl = round(_kl_divergence(pred, actual), 6)

    canon_label, canon_prob = _top_key(actual)
    pred_on_actual = pred.get(canon_label, 0.0)
    top_pred_label, top_pred_prob = _top_key(pred)
    reward = 1.0 if top_pred_label == canon_label else max(0.0, pred_on_actual - 0.2)

    trace_entry = {
        "actual": actual_label,
        "target": canon_label,
        "top_pred": top_pred_label,
        "reward": round(reward, 4),
        "l1": l1,
        "kl": kl,
        "speech_act": sa,
        "exec": exec_meta,
    }

    history = (_trace_history(input_json) + [trace_entry])[-TRACE_LIMIT:]

    out = {
        "status": "OK",
        "world_model": {
            "error": {
                "l1": l1,
                "kl": kl,
                "target": {k: round(v, 6) for k, v in actual.items()},
                "predicted": {k: round(v, 6) for k, v in pred.items()},
                "canonical_top": {"label": canon_label, "prob": round(actual[canon_label], 6)},
                "predicted_on_canonical": round(pred_on_actual, 6),
                "components": {
                    "speech_act": sa if isinstance(sa, str) else None,
                    "confidence": _safe_cf(feats.get("confidence"), 0.0),
                    "actual_move": actual_label,
                    "reward": round(reward, 4),
                },
                "meta": {"source": "B2F3", "rules_version": RULES_VERSION},
            },
            "trace": {
                "error_history": history
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    sample = {
        "world_model": {
            "context": {
                "features": {"speech_act": "request", "confidence": 0.82}
            },
            "prediction": {
                "expected_reply": {
                    "direct_answer": 0.12,
                    "execute_action": 0.58,
                    "ask_clarification": 0.18,
                    "acknowledge_only": 0.03,
                    "small_talk": 0.02,
                    "closing": 0.01,
                    "refuse_or_safecheck": 0.05,
                    "other": 0.01,
                }
            }
        }
    }
    res = b2f3_compute_error(sample)
    print(res["world_model"]["error"]["l1"], res["world_model"]["error"]["kl"],
          res["world_model"]["error"]["canonical_top"])
