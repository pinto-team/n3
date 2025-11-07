# Folder: noema/n3_core/block_12_orchestration
# File:   b12f2_action_enveloper.py

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b12f2_envelope_actions"]

RULES_VERSION = "1.0"
MAX_EMITS = 4
MAX_REQS = 24
MAX_APPLY = 5000
MAX_INDEX = 2000
MAX_TEXT = 1200


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


def _clip_text(s: Optional[str], n: int = MAX_TEXT) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


# ------------------------- envelopes -------------------------

def _env_emit(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    move = action.get("move") if isinstance(action.get("move"), str) else "answer"
    text = _clip_text(action.get("text"))
    if not text:
        return None
    return {
        "transport": {
            "outbound": [{
                "role": "assistant",
                "move": move,
                "text": text,
                "id": _hash({"move": move, "text": text})
            }],
            "meta": {"channel": "default"}
        }
    }


def _env_execute(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    reqs = [r for r in action.get("requests", []) if isinstance(r, dict)][:MAX_REQS]
    if not reqs:
        return None
    limits = action.get("limits") if isinstance(action.get("limits"), dict) else {}
    for r in reqs:
        if "req_id" not in r:
            r["req_id"] = _hash(r)
    return {
        "skills": {
            "batch": reqs,
            "limits": {
                "timeout_ms": int(limits.get("timeout_ms", 30000)),
                "max_inflight": int(limits.get("max_inflight", 4))
            },
            "defer": [str(x) for x in action.get("defer", [])]
        }
    }


def _env_persist(action: Dict[str, Any], ns_hint: Optional[str]) -> Optional[Dict[str, Any]]:
    apply_ops = [op for op in action.get("apply_ops", []) if isinstance(op, dict)][:MAX_APPLY]
    index_items = [it for it in action.get("index_items", []) if isinstance(it, dict)][:MAX_INDEX]
    if not apply_ops and not index_items:
        return None
    ns = ns_hint or "store/noema/default"
    return {
        "storage": {
            "apply": {"namespace": ns, "ops": apply_ops},
            "index": {"queue": index_items}
        }
    }


def _env_delay(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ms = int(action.get("ms", 0)) if isinstance(action.get("ms"), (int, float)) else 0
    if ms <= 0:
        return None
    return {"timers": [{"ms": ms, "reason": "throttle_or_backoff"}]}


# ------------------------- main -------------------------

def b12f2_envelope_actions(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B12F2 — Orchestration.ActionEnveloper (Noema)

    Input:
      {
        "engine": { "actions": [
          { "type":"emit", "move":"answer|confirm", "text": str },
          { "type":"execute", "requests":[...], "limits":{"timeout_ms":int,"max_inflight":int}, "defer":[...] },
          { "type":"persist", "apply_ops":[...], "index_items":[...] },
          { "type":"delay", "ms": int },
          { "type":"noop" }
        ] },
        "storage": { "apply": { "namespace": str } }?   # optional namespace hint
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "driver": {
          "plan": {
            "transport": { "outbound": [ {role,move,text,id}, ... ], "meta": {...} }?,
            "skills": { "batch":[...], "limits":{...}, "defer":[...] }?,
            "storage": { "apply": {namespace, ops[]}, "index": {queue: []} }?,
            "timers": [ {ms, reason} ]?,
            "meta": { "source":"B12F2", "rules_version":"1.0" }
          }
        },
        "diag": { "reason": "ok|no_actions", "counts": { "emit": int, "execute": int, "persist": int, "delay": int } }
      }
    """
    actions = _get(input_json, ["engine", "actions"], [])
    if not isinstance(actions, list) or not actions:
        return {"status": "SKIP", "driver": {"plan": {"meta": {"source": "B12F2", "rules_version": RULES_VERSION}}},
                "diag": {"reason": "no_actions", "counts": {"emit": 0, "execute": 0, "persist": 0, "delay": 0}}}

    ns_hint = _get(input_json, ["storage", "apply", "namespace"], None)

    emit_env: Dict[str, Any] = {"transport": {"outbound": [], "meta": {"channel": "default"}}}
    skills_env: Optional[Dict[str, Any]] = None
    storage_env: Optional[Dict[str, Any]] = None
    timers_env: List[Dict[str, Any]] = []

    c_emit = c_exec = c_persist = c_delay = 0

    for act in actions[: MAX_EMITS + MAX_REQS + 10]:
        t = act.get("type") if isinstance(act, dict) else None
        if t == "emit":
            env = _env_emit(act)
            if env and len(emit_env["transport"]["outbound"]) < MAX_EMITS:
                emit_env["transport"]["outbound"].extend(env["transport"]["outbound"])
                c_emit += 1
        elif t == "execute":
            env = _env_execute(act)
            if env:
                # if multiple execute actions appear, merge batches (bounded)
                if not skills_env:
                    skills_env = env
                else:
                    skills_env["skills"]["batch"].extend(env["skills"]["batch"])
                    skills_env["skills"]["defer"].extend(env["skills"]["defer"])
                c_exec += 1
        elif t == "persist":
            env = _env_persist(act, ns_hint)
            if env:
                if not storage_env:
                    storage_env = env
                else:
                    storage_env["storage"]["apply"]["ops"].extend(env["storage"]["apply"]["ops"])
                    storage_env["storage"]["index"]["queue"].extend(env["storage"]["index"]["queue"])
                c_persist += 1
        elif t == "delay":
            env = _env_delay(act)
            if env:
                timers_env.extend(env["timers"])
                c_delay += 1
        else:
            continue

    # Dedup & cap storage/index if merged
    if storage_env:
        apply_ops = storage_env["storage"]["apply"]["ops"]
        index_q = storage_env["storage"]["index"]["queue"]
        if len(apply_ops) > MAX_APPLY:
            storage_env["storage"]["apply"]["ops"] = apply_ops[:MAX_APPLY]
        if len(index_q) > MAX_INDEX:
            storage_env["storage"]["index"]["queue"] = index_q[:MAX_INDEX]

    # Build final driver plan
    plan: Dict[str, Any] = {"meta": {"source": "B12F2", "rules_version": RULES_VERSION}}
    if emit_env["transport"]["outbound"]:
        plan["transport"] = emit_env["transport"]
    if skills_env:
        plan["skills"] = skills_env["skills"]
    if storage_env:
        plan["storage"] = storage_env["storage"]
    if timers_env:
        plan["timers"] = timers_env

    return {
        "status": "OK",
        "driver": {"plan": plan},
        "diag": {"reason": "ok", "counts": {"emit": c_emit, "execute": c_exec, "persist": c_persist, "delay": c_delay}},
    }


if __name__ == "__main__":
    # Demo
    sample = {
        "engine": {"actions": [
            {"type": "delay", "ms": 180},
            {"type": "emit", "move": "answer", "text": "Done."},
            {"type": "execute",
             "requests": [{"req_id": "r1", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/a"}}],
             "limits": {"timeout_ms": 28000, "max_inflight": 2}, "defer": ["r2"]},
            {"type": "persist", "apply_ops": [{"op": "put", "key": "k/a", "value": {"x": 1}}],
             "index_items": [{"type": "packz", "id": "u1", "ns": "store/noema/t-1"}]}
        ]},
        "storage": {"apply": {"namespace": "store/noema/t-1"}}
    }
    out = b12f2_envelope_actions(sample)
    print(out["diag"])
    print(json.dumps(out["driver"]["plan"], ensure_ascii=False, indent=2)[:350])
