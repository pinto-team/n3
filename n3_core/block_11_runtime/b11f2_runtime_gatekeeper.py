# Folder: noema/n3_core/block_11_runtime
# File:   b11f2_runtime_gatekeeper.py

from __future__ import annotations

import hashlib
from typing import Any, Dict, List

import unicodedata

__all__ = ["b11f2_gatekeeper"]

RULES_VERSION = "1.0"


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _hash_to_int(s: str) -> int:
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def _metric_value(metrics: List[Dict[str, Any]], name: str, default: float = 0.0) -> float:
    if not isinstance(metrics, list):
        return default
    # pick the latest occurrence
    for i in range(len(metrics) - 1, -1, -1):
        m = metrics[i]
        if isinstance(m, dict) and _cf(m.get("name", "")) == _cf(name):
            v = m.get("value")
            return float(v) if isinstance(v, (int, float)) else default
    return default


def _bool(v: Any, default: bool = False) -> bool:
    return bool(v) if isinstance(v, bool) else default


def _num(v: Any, default: float = 0.0) -> float:
    return float(v) if isinstance(v, (int, float)) else default


# ------------------------- flag evaluation -------------------------

def _eval_flag(flag_cfg: Any, context: Dict[str, Any]) -> bool:
    """
    Supports:
      - bool
      - {"rollout": 0..100, "salt": str?, "when": {"slo_score_min": float?, "uncertainty_max": float?}}
    """
    if isinstance(flag_cfg, bool):
        return flag_cfg
    if not isinstance(flag_cfg, dict):
        return False

    rollout = int(flag_cfg.get("rollout", 0)) if isinstance(flag_cfg.get("rollout"), (int, float)) else 0
    salt = str(flag_cfg.get("salt", "noema"))
    slo = float(context.get("slo_score", 1.0) or 0.0)
    u = float(context.get("uncertainty", 0.0) or 0.0)

    when = flag_cfg.get("when", {}) if isinstance(flag_cfg.get("when"), dict) else {}
    slo_min = when.get("slo_score_min", None)
    u_max = when.get("uncertainty_max", None)

    if slo_min is not None and slo < float(slo_min):
        return False
    if u_max is not None and u > float(u_max):
        return False

    subject = str(context.get("thread_id") or "default") + "|" + salt
    bucket = _hash_to_int(subject) % 100
    return bucket < max(0, min(100, rollout))


# ------------------------- main -------------------------

def b11f2_gatekeeper(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B11F2 — Runtime.Gatekeeper (Noema)

    Input:
      {
        "runtime": { "config": {
            "guardrails": {
              "must_confirm": {"u_threshold": float?},
              "block_execute_when": {"slo_below": float?},
              "latency_soft_limit_ms": int?,          # default 1500
              "index_queue_soft_max": int?            # default 1000
            },
            "executor": {
              "timeout_ms": int?, "parallelism": {"max_inflight": int?}
            },
            "features": { "<flag>": bool|{rollout:%, salt?:str, when?:{slo_score_min?:float, uncertainty_max?:float}}, ... }
        } },
        "observability": {
          "slo": {"score": float?},
          "telemetry": {"metrics": [ {name, value, labels{}, ts}, ... ]}?
        },
        "world_model": {"uncertainty": {"score": float?}}?,
        "session": {"thread_id": str?}
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "runtime": {
          "gates": {
            "allow_execute": bool,
            "allow_answer": bool,
            "require_confirm": bool,
            "throttle_ms": int,
            "limits": { "timeout_ms": int?, "max_inflight": int? },
            "features": { "<flag>": bool }
          },
          "reasons": [str],
          "meta": { "source": "B11F2", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|no_config" }
      }
    """
    cfg = _get(input_json, ["runtime", "config"], {})
    if not isinstance(cfg, dict) or not cfg:
        return {"status": "SKIP", "runtime": {"gates": {}}, "diag": {"reason": "no_config"}}

    # Context
    slo_score = _num(_get(input_json, ["observability", "slo", "score"], None), 1.0)
    metrics = _get(input_json, ["observability", "telemetry", "metrics"], []) or []
    u_score = _num(_get(input_json, ["world_model", "uncertainty", "score"], None), 0.0)
    thread_id = str(_get(input_json, ["session", "thread_id"], "") or "default")

    latency = _metric_value(metrics, "exec_avg_latency_ms", 0.0)
    idx_q = _metric_value(metrics, "index_queue_items", 0.0)

    # Config knobs (with defaults)
    g = _get(cfg, ["guardrails"], {}) or {}
    must_confirm_u = _num(_get(g, ["must_confirm", "u_threshold"], None), 0.4)
    slo_block = _num(_get(g, ["block_execute_when", "slo_below"], None), 0.0)  # 0 disables
    lat_soft = int(_num(_get(g, ["latency_soft_limit_ms"], None), 1500))
    idx_soft = int(_num(_get(g, ["index_queue_soft_max"], None), 1000))

    timeout_ms = int(_num(_get(cfg, ["executor", "timeout_ms"], None), 30000))
    max_inflight = int(_num(_get(cfg, ["executor", "parallelism", "max_inflight"], None), 4))

    # Base decisions
    allow_answer = True
    allow_execute = True
    require_confirm = u_score >= must_confirm_u
    reasons: List[str] = []

    # SLO-based execute block
    if slo_block > 0 and slo_score < slo_block:
        allow_execute = False
        reasons.append(f"block_execute: slo_score({slo_score}) < {slo_block}")

    # Throttling logic (best-effort, soft)
    throttle_ms = 0
    if latency > lat_soft:
        # Increase throttle proportional to overage, up to 1200ms
        over = latency - lat_soft
        throttle_ms = min(1200, int(over * 0.5))
        reasons.append(f"throttle: high_latency={latency}ms > {lat_soft}ms")

    if idx_q > idx_soft:
        # Add extra backpressure (cumulative)
        extra = min(600, int((idx_q - idx_soft) * 0.1))
        throttle_ms = min(1500, throttle_ms + extra)
        reasons.append(f"throttle: index_queue={int(idx_q)} > {idx_soft}")

    # Evaluate feature flags
    flags_cfg = _get(cfg, ["features"], {}) or {}
    features: Dict[str, bool] = {}
    ctx = {"slo_score": slo_score, "uncertainty": u_score, "thread_id": thread_id}
    for name, val in (flags_cfg.items() if isinstance(flags_cfg, dict) else []):
        features[str(name)] = _eval_flag(val, ctx)

    gates = {
        "allow_execute": bool(allow_execute),
        "allow_answer": bool(allow_answer),
        "require_confirm": bool(require_confirm),
        "throttle_ms": int(max(0, throttle_ms)),
        "limits": {"timeout_ms": timeout_ms, "max_inflight": max_inflight},
        "features": features
    }

    return {
        "status": "OK",
        "runtime": {
            "gates": gates,
            "reasons": reasons,
            "meta": {"source": "B11F2", "rules_version": RULES_VERSION}
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    # Demo
    sample = {
        "session": {"thread_id": "t-42"},
        "runtime": {
            "config": {
                "guardrails": {
                    "must_confirm": {"u_threshold": 0.4},
                    "block_execute_when": {"slo_below": 0.35},
                    "latency_soft_limit_ms": 1500,
                    "index_queue_soft_max": 1000
                },
                "executor": {"timeout_ms": 28000, "parallelism": {"max_inflight": 3}},
                "features": {
                    "fast_nlg": {"rollout": 50, "salt": "n3", "when": {"slo_score_min": 0.5}},
                    "cheap_models": True
                }
            }
        },
        "observability": {
            "slo": {"score": 0.48},
            "telemetry": {"metrics": [
                {"name": "exec_avg_latency_ms", "value": 1900},
                {"name": "index_queue_items", "value": 1200}
            ]}
        },
        "world_model": {"uncertainty": {"score": 0.46}}
    }
    out = b11f2_gatekeeper(sample)
    print(out["runtime"]["gates"])
    print(out["runtime"]["reasons"])
