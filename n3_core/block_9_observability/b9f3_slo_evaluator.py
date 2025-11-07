# Folder: noema/n3_core/block_9_observability
# File:   b9f3_slo_evaluator.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b9f3_evaluate_slo"]

RULES_VERSION = "1.0"


# ---------- helpers ----------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _as_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _as_int(x: Any, default: int = 0) -> int:
    return int(x) if isinstance(x, (int, float)) else default


def _find_metric(metrics: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    # pick the latest occurrence if multiple
    for i in range(len(metrics) - 1, -1, -1):
        m = metrics[i]
        if isinstance(m, dict) and _cf(m.get("name")) == _cf(name):
            return m
    return None


def _metric_value(metrics: List[Dict[str, Any]], name: str, default: float = 0.0) -> float:
    m = _find_metric(metrics, name)
    if not m:
        return default
    return _as_float(m.get("value"), default)


def _labels(metrics: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    m = _find_metric(metrics, name)
    if not m:
        return {}
    labs = m.get("labels")
    return labs if isinstance(labs, dict) else {}


def _ratio_good(x: float, thresh: float, hi_good: bool) -> float:
    """
    Returns a soft score 0..1 describing how well x meets the threshold.
    If hi_good=True: larger is better, threshold is minimum. Else: smaller is better, threshold is maximum.
    """
    if hi_good:
        if x >= thresh:
            return 1.0
        # soft ramp 0.5 at 0.8*thresh, 0 at 0.5*thresh
        if x <= 0.5 * thresh:
            return 0.0
        return max(0.0, min(1.0, (x - 0.5 * thresh) / (0.5 * thresh)))
    else:
        if x <= thresh:
            return 1.0
        # soft ramp 0.5 at 1.25*thresh, 0 at 2*thresh
        if x >= 2.0 * thresh:
            return 0.0
        return max(0.0, min(1.0, (2.0 * thresh - x) / (0.75 * thresh)))


def _mk_check(name: str, value: float, threshold: float, weight: float, ok: bool, details: Dict[str, Any]) -> Dict[
    str, Any]:
    return {
        "name": name,
        "ok": bool(ok),
        "value": value,
        "threshold": threshold,
        "weight": weight,
        "details": details,
    }


def _alert(severity: str, title: str, summary: str, suggest: str, tags: Dict[str, Any]) -> Dict[str, Any]:
    return {"severity": severity, "title": title, "summary": summary, "suggest": suggest, "tags": tags}


# ---------- main ----------

def b9f3_evaluate_slo(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B9F3 — Observability.SLOEvaluator (Noema)

    Input (best-effort):
      {
        "observability": { "telemetry": { "metrics": [ {name, value, labels{}, ts}, ... ] } }?,
        "planner": { "plan": { "guardrails": {"must_confirm": bool}, "next_move": str } }?,
        "dialog":  { "final": { "move": str, "reason": str? }, "turn": { "move": str } }?,
        "executor":{ "results": { "aggregate": {count, ok, errors, total_cost, avg_latency_ms} } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "observability": {
          "slo": {
            "score": float,                     # 0..1
            "checks": [ {name, ok, value, threshold, weight, details{}}, ... ],
            "alerts": [ {severity, title, summary, suggest, tags{}}, ... ],
            "meta": { "source": "B9F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_metrics" }
      }
    """
    metrics = _get(input_json, ["observability", "telemetry", "metrics"], [])
    if not isinstance(metrics, list) or not metrics:
        # Build minimal metrics from aggregates if possible
        agg = _get(input_json, ["executor", "results", "aggregate"], {}) or {}
        if not agg:
            return {"status": "SKIP", "observability": {"slo": {}}, "diag": {"reason": "no_metrics"}}
        # create pseudo metrics
        metrics = [
            {"name": "exec_total_cost", "value": _as_float(agg.get("total_cost", 0.0)), "labels": {}},
            {"name": "exec_avg_latency_ms", "value": _as_float(agg.get("avg_latency_ms", 0.0)), "labels": {}},
            {"name": "exec_items", "value": _as_int(agg.get("count", 0)),
             "labels": {"ok": _as_int(agg.get("ok", 0)), "errors": _as_int(agg.get("errors", 0))}},
        ]

    # Thresholds / budgets (tunable)
    TH = {
        "dialog_out_length_max": 900.0,
        "exec_latency_ms_max": 1500.0,
        "exec_error_rate_max": 0.2,
        "exec_total_cost_max": 0.01,  # per turn $ budget
        "wal_ops_max": 80.0,
        "index_queue_items_max": 1000.0,
    }

    # Values (from metrics or fallbacks)
    out_len = _metric_value(metrics, "dialog_out_length", 0.0)
    latency = _metric_value(metrics, "exec_avg_latency_ms",
                            _as_float(_get(input_json, ["executor", "results", "aggregate", "avg_latency_ms"], 0.0)))
    total_cost = _metric_value(metrics, "exec_total_cost",
                               _as_float(_get(input_json, ["executor", "results", "aggregate", "total_cost"], 0.0)))
    wal_ops = _metric_value(metrics, "wal_ops", 0.0)
    idx_items = _metric_value(metrics, "index_queue_items", 0.0)

    # Error rate from exec_items metric labels
    mi = _find_metric(metrics, "exec_items")
    ok_n = _as_int(_get(mi or {}, ["labels", "ok"], _get(input_json, ["executor", "results", "aggregate", "ok"], 0)))
    err_n = _as_int(
        _get(mi or {}, ["labels", "errors"], _get(input_json, ["executor", "results", "aggregate", "errors"], 0)))
    cnt = max(1, ok_n + err_n)
    err_rate = err_n / cnt

    # Must-confirm adherence
    must_confirm = bool(_get(input_json, ["planner", "plan", "guardrails", "must_confirm"], False))
    final_move = (_get(input_json, ["dialog", "final", "move"], "") or _get(input_json, ["dialog", "turn", "move"],
                                                                            "") or "").lower()
    final_reason = (_get(input_json, ["dialog", "final", "reason"], "") or "").lower()
    confirm_ok = (not must_confirm) or (final_move == "confirm") or (
                final_reason in {"must_confirm", "secret_detected"})

    # Build checks with weights
    checks: List[Dict[str, Any]] = []
    total_weight = 0.0
    score_sum = 0.0

    def add_check(name: str, value: float, thresh: float, hi_good: bool, weight: float, details: Dict[str, Any]):
        nonlocal score_sum, total_weight, checks
        s = _ratio_good(value, thresh, hi_good)
        checks.append(_mk_check(name, value, thresh, weight, bool(s >= 1.0), {**details, "score": round(s, 4)}))
        score_sum += s * weight
        total_weight += weight

    add_check("answer.length", out_len, TH["dialog_out_length_max"], hi_good=False, weight=0.12, details={})
    add_check("execution.latency_ms", latency, TH["exec_latency_ms_max"], hi_good=False, weight=0.22, details={})
    add_check("execution.error_rate", err_rate, TH["exec_error_rate_max"], hi_good=False, weight=0.26,
              details={"errors": err_n, "count": cnt})
    add_check("execution.cost_usd", total_cost, TH["exec_total_cost_max"], hi_good=False, weight=0.18, details={})
    add_check("storage.wal_ops", wal_ops, TH["wal_ops_max"], hi_good=False, weight=0.10, details={})
    add_check("index.queue_items", idx_items, TH["index_queue_items_max"], hi_good=False, weight=0.07, details={})
    add_check("guardrails.must_confirm_adhered", 1.0 if confirm_ok else 0.0, 1.0, hi_good=True, weight=0.05,
              details={"must_confirm": must_confirm, "final_move": final_move, "reason": final_reason})

    slo_score = round(score_sum / max(1e-9, total_weight), 4)

    # Alerts for failed checks
    alerts: List[Dict[str, Any]] = []
    for c in checks:
        if c["ok"]:
            continue
        sev = "high" if c["weight"] >= 0.22 else ("medium" if c["weight"] >= 0.12 else "low")
        title = {
            "answer.length": "طول پاسخ زیاد است",
            "execution.latency_ms": "تأخیر اجرا بالا",
            "execution.error_rate": "نرخ خطا بالاست",
            "execution.cost_usd": "هزینه از بودجه بیشتر شد",
            "storage.wal_ops": "تعداد WAL زیاد",
            "index.queue_items": "صف ایندکس بزرگ است",
            "guardrails.must_confirm_adhered": "الزام تأیید رعایت نشد",
        }.get(c["name"], c["name"])
        diff = (c["value"] - c["threshold"]) if c["threshold"] is not None else c["value"]
        summary = f"value={c['value']} threshold={c['threshold']} (Δ={round(diff, 4)})"
        suggest = {
            "answer.length": "خروجی را کوتاه‌تر تولید کن یا خلاصه‌سازی را فعال کن.",
            "execution.latency_ms": "زمان انتظار مهارت‌ها را کاهش بده یا موازی‌سازی محدود اضافه کن.",
            "execution.error_rate": "Retry و مدیریت خطا را بازبینی کن؛ ورودی‌های مشکل‌دار را هندل کن.",
            "execution.cost_usd": "مدل/مهارت کم‌هزینه‌تر یا برش متن/توکن را فعال کن.",
            "storage.wal_ops": "Commit را batch کن و dedupe را در لایهٔ بالاتر بهبود بده.",
            "index.queue_items": "سرعت ایندکس را افزایش بده یا صف را تقسیم کن.",
            "guardrails.must_confirm_adhered": "قبل از اجرا/ارسال، مرحلهٔ تأیید را اجباری کن.",
        }.get(c["name"], "بازبینی پیکربندی توصیه می‌شود.")
        alerts.append(_alert(sev, title, summary, suggest, {"name": c["name"]}))

    return {
        "status": "OK",
        "observability": {
            "slo": {
                "score": slo_score,
                "checks": checks,
                "alerts": alerts,
                "meta": {"source": "B9F3", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    # Minimal demo
    sample = {
        "observability": {
            "telemetry": {
                "metrics": [
                    {"name": "dialog_out_length", "value": 1020, "labels": {"move": "answer"}},
                    {"name": "exec_avg_latency_ms", "value": 1680, "labels": {}},
                    {"name": "exec_total_cost", "value": 0.012, "labels": {}},
                    {"name": "exec_items", "value": 3, "labels": {"ok": 2, "errors": 1}},
                    {"name": "wal_ops", "value": 55, "labels": {}},
                    {"name": "index_queue_items", "value": 1200, "labels": {}},
                    {"name": "plan_must_confirm", "value": 1, "labels": {}}
                ]
            }
        },
        "planner": {"plan": {"guardrails": {"must_confirm": True}}},
        "dialog": {"final": {"move": "answer", "reason": ""}}
    }
    out = b9f3_evaluate_slo(sample)
    print("score:", out["observability"]["slo"]["score"])
    for a in out["observability"]["slo"]["alerts"]:
        print(a["severity"], a["title"], a["summary"])
