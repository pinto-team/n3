# Folder: noema/n3_core/block_2_world_model
# File:   b2f2_predictor.py

from __future__ import annotations

import hashlib
import math
from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple

__all__ = ["b2f2_predict"]

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

BASE_PRIORS = {
    "direct_answer": 0.25,
    "execute_action": 0.20,
    "ask_clarification": 0.15,
    "acknowledge_only": 0.10,
    "small_talk": 0.10,
    "closing": 0.05,
    "refuse_or_safecheck": 0.05,
    "other": 0.10,
}

EMB_N = 3
EMB_DIM = 64
TRACE_LIMIT = 12


def _get_context(inp: Dict[str, Any]) -> Dict[str, Any]:
    wm = inp.get("world_model", {})
    if not isinstance(wm, dict):
        return {}
    ctx = wm.get("context", {})
    return ctx if isinstance(ctx, dict) else {}


def _safe_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _clip_nonneg(d: Dict[str, float]) -> Dict[str, float]:
    return {k: (v if v > 0 else 0.0) for k, v in d.items()}


def _normalize(d: Dict[str, float]) -> Dict[str, float]:
    s = sum(d.values())
    if s <= 0:
        # fallback to uniform distribution
        n = len(LABELS)
        return {k: 1.0 / n for k in LABELS}
    return {k: v / s for k, v in d.items()}


def _add(d: Dict[str, float], key: str, delta: float, notes: List[str], note: str):
    if key in d:
        d[key] += delta
    else:
        d[key] = delta
    if note:
        notes.append(note)


def _speech_act_adjust(sa: str, probs: Dict[str, float], notes: List[str]) -> None:
    sac = (sa or "").lower()
    if sac == "question":
        _add(probs, "direct_answer", 0.50, notes, "speech_act:question→direct_answer")
        _add(probs, "ask_clarification", 0.10, notes, "speech_act:question→ask_clarification")
    elif sac == "request":
        _add(probs, "execute_action", 0.45, notes, "speech_act:request→execute_action")
        _add(probs, "ask_clarification", 0.10, notes, "speech_act:request→ask_clarification")
    elif sac == "command":
        _add(probs, "execute_action", 0.50, notes, "speech_act:command→execute_action")
        _add(probs, "refuse_or_safecheck", 0.05, notes, "speech_act:command→safecheck")
    elif sac == "greeting":
        _add(probs, "small_talk", 0.50, notes, "speech_act:greeting→small_talk")
        _add(probs, "acknowledge_only", 0.10, notes, "speech_act:greeting→ack")
    elif sac == "thanks":
        _add(probs, "acknowledge_only", 0.50, notes, "speech_act:thanks→ack")
        _add(probs, "closing", 0.20, notes, "speech_act:thanks→closing")
    elif sac == "apology":
        _add(probs, "acknowledge_only", 0.30, notes, "speech_act:apology→ack")
    elif sac == "affirmation":
        _add(probs, "direct_answer", 0.20, notes, "speech_act:affirmation→continue_answer")
        _add(probs, "execute_action", 0.10, notes, "speech_act:affirmation→continue_action")
    elif sac == "negation":
        _add(probs, "ask_clarification", 0.30, notes, "speech_act:negation→clarify")
    elif sac == "exclamation":
        _add(probs, "small_talk", 0.10, notes, "speech_act:exclamation→small_talk")


def _confidence_adjust(conf: float, sa: str, probs: Dict[str, float], notes: List[str]) -> None:
    if conf < 0.4:
        _add(probs, "ask_clarification", 0.30, notes, "low_conf→clarify")
        _add(probs, "execute_action", -0.20, notes, "low_conf→reduce_action")
        _add(probs, "direct_answer", -0.10, notes, "low_conf→reduce_answer")
    elif conf < 0.7:
        _add(probs, "ask_clarification", 0.10, notes, "mid_conf→slight_clarify")
    elif conf > 0.85:
        if (sa or "").lower() in {"request", "command"}:
            _add(probs, "execute_action", 0.10, notes, "high_conf+act→execute_action")
        elif (sa or "").lower() == "question":
            _add(probs, "direct_answer", 0.10, notes, "high_conf+question→direct_answer")
        _add(probs, "ask_clarification", -0.10, notes, "high_conf→less_clarify")


def _addressing_adjust(to_noema: bool, probs: Dict[str, float], notes: List[str]) -> None:
    if not to_noema:
        _add(probs, "acknowledge_only", 0.10, notes, "not_addressed→ack_only")
        _add(probs, "direct_answer", -0.10, notes, "not_addressed→reduce_answer")
        _add(probs, "execute_action", -0.10, notes, "not_addressed→reduce_action")


def _novelty_adjust(nov: float, probs: Dict[str, float], notes: List[str]) -> None:
    if nov > 0.7:
        _add(probs, "ask_clarification", 0.10, notes, "high_novelty→clarify")
    elif nov < 0.3:
        _add(probs, "direct_answer", 0.05, notes, "low_novelty→direct_answer_bias")


def _gate_hint(probs: Dict[str, float]) -> str:
    # Compact hint for downstream turn-gating
    if probs.get("ask_clarification", 0) >= 0.35:
        return "probe_first"
    if probs.get("execute_action", 0) >= 0.40:
        return "act_first"
    if probs.get("direct_answer", 0) >= 0.45:
        return "answer_first"
    return "balanced"


def _iter_char_ngrams(text: str, n: int = EMB_N) -> Iterable[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= n:
        return [text]
    return (text[i:i + n] for i in range(len(text) - n + 1))


def _text_embedding(text: str, dim: int = EMB_DIM) -> Tuple[List[float], List[Tuple[str, int]]]:
    counts = Counter(_iter_char_ngrams(text))
    if not counts:
        return [0.0] * dim, []
    vec = [0.0] * dim
    for gram, cnt in counts.items():
        h = int(hashlib.sha1(gram.encode("utf-8")).hexdigest()[:8], 16) % dim
        vec[h] += float(cnt)
    # L2 normalize
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:5]
    return vec, top


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    return sum(x * y for x, y in zip(a, b))


def _blend(a: float, b: float, alpha: float) -> float:
    return (1.0 - alpha) * a + alpha * b


def _transitions_for_sa(inp: Dict[str, Any], sa: str) -> Dict[str, float]:
    wm = inp.get("world_model", {})
    model = wm.get("model", {}) if isinstance(wm.get("model"), dict) else {}
    trans = model.get("transitions", {}) if isinstance(model.get("transitions"), dict) else {}
    row = trans.get((sa or "").lower(), {})
    if isinstance(row, dict):
        return {k: float(v) for k, v in row.items() if k in LABELS}
    return {}


def _prototype_vectors(inp: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    wm = inp.get("world_model", {})
    model = wm.get("model", {}) if isinstance(wm.get("model"), dict) else {}
    prot = model.get("prototypes", {}) if isinstance(model.get("prototypes"), dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    for label, info in prot.items():
        if label not in LABELS or not isinstance(info, dict):
            continue
        vec = info.get("vector")
        if isinstance(vec, list) and vec:
            out[label] = {
                "vector": [float(x) for x in vec],
                "count": int(info.get("count", 1))
            }
    return out


def _scores_from_transitions(sa: str, probs: Dict[str, float], inp: Dict[str, Any], notes: List[str]) -> None:
    row = _transitions_for_sa(inp, sa)
    total = sum(row.values())
    if total <= 0:
        return
    for label, cnt in row.items():
        weight = cnt / total
        _add(probs, label, 0.6 * weight, notes, f"transition:{sa}->{label}:{weight:.2f}")


def _scores_from_prototypes(vec: List[float], probs: Dict[str, float], inp: Dict[str, Any], notes: List[str]) -> Dict[str, float]:
    prot = _prototype_vectors(inp)
    sims: Dict[str, float] = {}
    if not prot:
        return sims
    for label, info in prot.items():
        sim = max(0.0, _cosine(vec, info.get("vector", [])))
        if sim <= 0:
            continue
        sims[label] = sim
        weight = min(0.5, sim * math.log1p(max(1, info.get("count", 1))))
        _add(probs, label, weight, notes, f"prototype:{label}:{sim:.2f}")
    return sims


def _collect_history(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    wm = inp.get("world_model", {})
    hist = wm.get("trace", {}) if isinstance(wm.get("trace"), dict) else {}
    records = hist.get("prediction_history") if isinstance(hist.get("prediction_history"), list) else []
    return [r for r in records if isinstance(r, dict)]


def b2f2_predict(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B2F2 — WorldModel.Predictor (Noema)
    Uses lightweight heuristics over world_model.context.features to predict the most suitable next system move.

    Input:
      {
        "world_model": {
          "context": {
            "current": {...},                 # from B2F1
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
            }
          }
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "world_model": {
          "prediction": {
            "top": str,
            "expected_reply": { label: prob, ... },  # normalized
            "hints": {
              "turn_gate": "probe_first|act_first|answer_first|balanced",
              "should_collect_slots": bool,
              "safecheck_needed": bool
            },
            "rationale": [str],
            "meta": { "source": "B2F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_context" }
      }
    """
    ctx = _get_context(input_json)
    cur = ctx.get("current", {})
    feats = ctx.get("features", {})

    if not isinstance(cur, dict) or not isinstance(feats, dict) or not cur:
        return {
            "status": "SKIP",
            "world_model": {"prediction": {"top": "", "expected_reply": {}, "hints": {}, "rationale": [],
                                           "meta": {"source": "B2F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_context"},
        }

    # Seed with priors
    probs: Dict[str, float] = dict(BASE_PRIORS)
    notes: List[str] = []

    sa = feats.get("speech_act") or cur.get("signals", {}).get("speech_act")
    conf = _safe_float(feats.get("confidence"), 0.0)
    nov = _safe_float(feats.get("novelty"), 0.5)
    to_noema = bool(feats.get("is_to_noema"))

    _speech_act_adjust(sa or "", probs, notes)
    _confidence_adjust(conf, sa or "", probs, notes)
    _addressing_adjust(to_noema, probs, notes)
    _novelty_adjust(nov, probs, notes)

    text = cur.get("text") if isinstance(cur.get("text"), str) else ""
    vec, top_grams = _text_embedding(text)

    sim_notes = _scores_from_prototypes(vec, probs, input_json, notes)
    _scores_from_transitions(sa or "", probs, input_json, notes)

    history = _collect_history(input_json)
    if history:
        last = history[-1]
        last_label = last.get("top") if isinstance(last.get("top"), str) else ""
        if last_label in LABELS:
            _add(probs, last_label, 0.05, notes, f"history_bias:{last_label}")

    probs = _clip_nonneg(probs)
    probs = _normalize(probs)

    # Hints for downstream planners
    gate = _gate_hint(probs)
    should_collect = probs.get("ask_clarification", 0.0) >= 0.35
    safecheck = ((sa or "").lower() == "command" and conf < 0.7) or (probs.get("execute_action", 0.0) > 0.5)

    top_label, top_prob = max(probs.items(), key=lambda kv: kv[1])

    trace_entry = {
        "turn_id": cur.get("id"),
        "text_hash": hashlib.sha1(text.encode("utf-8")).hexdigest() if text else "",
        "top": top_label,
        "top_prob": round(top_prob, 4),
        "speech_act": sa,
        "confidence": conf,
        "novelty": nov,
        "sim_notes": {k: round(v, 4) for k, v in sim_notes.items()},
        "grams": top_grams,
        "notes": notes[:TRACE_LIMIT],
    }

    history_out = (history + [trace_entry])[-TRACE_LIMIT:]

    out = {
        "status": "OK",
        "world_model": {
            "prediction": {
                "top": top_label,
                "expected_reply": {k: round(v, 4) for k, v in probs.items()},
                "hints": {
                    "turn_gate": gate,
                    "should_collect_slots": bool(should_collect),
                    "safecheck_needed": bool(safecheck),
                },
                "rationale": notes[:12],  # keep it short
                "meta": {"source": "B2F2", "rules_version": RULES_VERSION},
            },
            "trace": {
                "prediction_history": history_out
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    sample = {
        "world_model": {
            "context": {
                "current": {
                    "id": "abc123",
                    "text": "سلام نوما، میشه این گزارش را خلاصه کنی؟",
                    "signals": {"speech_act": "request"}
                },
                "features": {
                    "dir": "rtl",
                    "is_to_noema": True,
                    "speech_act": "request",
                    "confidence": 0.82,
                    "novelty": 0.55,
                    "len_chars": 40,
                    "len_tokens": 10,
                    "len_sentences": 1,
                    "sim_to_last": 0.12,
                    "sim_to_avg": 0.18,
                    "history_size": 3
                }
            }
        }
    }
    res = b2f2_predict(sample)
    print(res["world_model"]["prediction"]["top"], res["world_model"]["prediction"]["expected_reply"])
