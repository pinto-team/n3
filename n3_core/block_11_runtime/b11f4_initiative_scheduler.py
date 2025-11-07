# Folder: n3_core/block_11_runtime
# File:   b11f4_initiative_scheduler.py

from __future__ import annotations
from typing import Any, Dict, List, Optional

__all__ = ["b11f4_initiative_scheduler"]

DEFAULT_INTROSPECTION_COOLDOWN = 15000  # ms
DEFAULT_REFLECTION_COOLDOWN = 20000  # ms


def _now_ms(state: Dict[str, Any]) -> int:
    return int(((state.get("clock") or {}).get("now_ms") or 0))


def _summary(state: Dict[str, Any]) -> Dict[str, Any]:
    obs = state.get("observability") if isinstance(state.get("observability"), dict) else {}
    tel = obs.get("telemetry") if isinstance(obs.get("telemetry"), dict) else {}
    summary = tel.get("summary") if isinstance(tel.get("summary"), dict) else {}
    return summary


def _concept_context(state: Dict[str, Any]) -> Dict[str, str]:
    cg = state.get("concept_graph") if isinstance(state.get("concept_graph"), dict) else {}
    nodes = cg.get("nodes", {}) if isinstance(cg.get("nodes"), dict) else {}
    node_list = nodes.get("nodes") if isinstance(nodes.get("nodes"), list) else []
    return {n.get("id"): n.get("key", n.get("id")) for n in node_list if isinstance(n, dict) and isinstance(n.get("id"), str)}


def _latest_rule(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cg = state.get("concept_graph") if isinstance(state.get("concept_graph"), dict) else {}
    rules = cg.get("rules", {}) if isinstance(cg.get("rules"), dict) else {}
    rule_list = rules.get("rules") if isinstance(rules.get("rules"), list) else []
    if not rule_list:
        return None
    return rule_list[-1]


def _clone_queue(queue: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(queue, list):
        for it in queue:
            if isinstance(it, dict):
                out.append(dict(it))
    return out


def _should_schedule_introspection(summary: Dict[str, Any], now: int, cooldowns: Dict[str, int]) -> bool:
    if not summary.get("needs_introspection"):
        return False
    last = int(cooldowns.get("introspection_ms") or 0)
    cooldown = int(summary.get("introspection_cooldown_ms", DEFAULT_INTROSPECTION_COOLDOWN))
    return (now - last) >= cooldown


def _introspection_message(summary: Dict[str, Any]) -> str:
    u = summary.get("uncertainty", 0.0)
    return "من مطمئن نیستم؛ لطفاً جزئیات بیشتری بده." if u >= 0.75 else "برای ادامه نیاز به اطلاعات بیشتری دارم."


def _reflection_message(state: Dict[str, Any], summary: Dict[str, Any]) -> Optional[str]:
    rule = _latest_rule(state)
    if not isinstance(rule, dict):
        return None
    nodes = _concept_context(state)
    if rule.get("type") == "assoc":
        u = nodes.get(rule.get("u"), rule.get("u"))
        v = nodes.get(rule.get("v"), rule.get("v"))
        return f"I noticed a new association between {u} and {v}. Should I keep it?"
    if rule.get("type") == "synonym":
        a = nodes.get(rule.get("a"), rule.get("a"))
        b = nodes.get(rule.get("b"), rule.get("b"))
        return f"I think {a} and {b} might be synonyms. Does that feel right?"
    if rule.get("type") == "subsumes":
        parent = nodes.get(rule.get("parent"), rule.get("parent"))
        child = nodes.get(rule.get("child"), rule.get("child"))
        return f"It looks like {parent} may include {child}. Should we store that link?"
    return None


def _should_schedule_reflection(summary: Dict[str, Any], now: int, cooldowns: Dict[str, int]) -> bool:
    new_rules = int(summary.get("concept_new_rules", 0))
    if new_rules <= 0:
        return False
    last = int(cooldowns.get("reflection_ms") or 0)
    cooldown = int(summary.get("reflection_cooldown_ms", DEFAULT_REFLECTION_COOLDOWN))
    return (now - last) >= cooldown


def b11f4_initiative_scheduler(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Picks due initiative items and turns them into dialog.final (say) or executor.requests (run_skill).
    Pure; expects time via state.clock.now_ms.
    """
    init = state.get("initiative") if isinstance(state.get("initiative"), dict) else {}
    q = _clone_queue(init.get("queue"))
    cooldowns = dict(init.get("cooldowns", {})) if isinstance(init.get("cooldowns"), dict) else {}
    if not q:
        q = []

    now = _now_ms(state)
    if now <= 0:
        return {"status": "SKIP", "diag": {"reason": "no_clock"}}

    summary = _summary(state)
    if _should_schedule_introspection(summary, now, cooldowns):
        q.append({
            "type": "say",
            "when_ms": now,
            "payload": {"text": _introspection_message(summary)},
            "once": True,
            "cooldown_ms": summary.get("introspection_cooldown_ms", DEFAULT_INTROSPECTION_COOLDOWN)
        })
        cooldowns["introspection_ms"] = now

    if _should_schedule_reflection(summary, now, cooldowns):
        reflect_text = _reflection_message(state, summary)
        if reflect_text:
            q.append({
                "type": "say",
                "when_ms": now,
                "payload": {"text": reflect_text, "move": "reflection"},
                "once": True,
                "cooldown_ms": summary.get("reflection_cooldown_ms", DEFAULT_REFLECTION_COOLDOWN)
            })
            cooldowns["reflection_ms"] = now

    taken = 0
    new_q: List[Dict[str, Any]] = []
    dialog_existing = ((state.get("dialog") or {}).get("final") or {})
    dialog_busy = bool(dialog_existing)
    dialog_out = dialog_existing if dialog_busy else {}

    existing_reqs = []
    exec_block = ((state.get("executor") or {}).get("requests") or [])
    if isinstance(exec_block, list):
        existing_reqs.extend(exec_block)

    new_requests: List[Dict[str, Any]] = []

    for it in q:
        if not isinstance(it, dict):
            continue
        when_ms = int(it.get("when_ms") or 0)
        typ = str(it.get("type") or "")
        once = bool(it.get("once", True))
        cooldown = int(it.get("cooldown_ms") or 0)
        payload = it.get("payload") or {}

        due = when_ms > 0 and when_ms <= now
        if not due:
            new_q.append(it)
            continue

        if typ == "say" and not dialog_busy:
            text = str(payload.get("text") or "")
            if text:
                move = str(payload.get("move") or "answer")
                dialog_out = {"move": move, "text": text, "origin": "initiative"}
                dialog_busy = True
                taken += 1
                if not once and cooldown > 0:
                    it["when_ms"] = now + cooldown
                    new_q.append(it)
                continue

        elif typ == "run_skill":
            req = payload.get("req")
            if isinstance(req, dict):
                new_requests.append(req)
                taken += 1
                if not once and cooldown > 0:
                    it["when_ms"] = now + cooldown
                    new_q.append(it)
                continue

        # if we got here, either invalid payload or blocked; keep the item for later
        new_q.append(it)

    out = {
        "status": "OK",
        "initiative": {"queue": new_q, "stats": {"taken": taken, "remain": len(new_q)}, "cooldowns": cooldowns},
    }
    if dialog_out:
        out.setdefault("dialog", {})["final"] = dialog_out
        out["dialog"]["meta"] = {"clears_previous": True}
    if new_requests:
        out.setdefault("executor", {})["requests"] = existing_reqs + new_requests
    elif existing_reqs:
        out.setdefault("executor", {})["requests"] = existing_reqs
    return out
