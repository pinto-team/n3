# Folder: noema/n3_core/block_5_planning
# File:   b5f1_intent_router.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Optional

import unicodedata

__all__ = ["b5f1_route_intent"]

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


def _safe_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


# ------------------------- collectors -------------------------

def _collect_prediction(inp: Dict[str, Any]) -> Tuple[str, Dict[str, float]]:
    wm = inp.get("world_model", {}) if isinstance(inp.get("world_model"), dict) else {}
    pred = wm.get("prediction", {}) if isinstance(wm.get("prediction"), dict) else {}
    top = pred.get("top") if isinstance(pred.get("top"), str) else ""
    dist = pred.get("expected_reply") if isinstance(pred.get("expected_reply"), dict) else {}
    return top or "", {str(k): _safe_float(v, 0.0) for k, v in dist.items()}


def _collect_features(inp: Dict[str, Any]) -> Dict[str, Any]:
    feats = _get(inp, ["world_model", "context", "features"], {}) or {}
    return feats if isinstance(feats, dict) else {}


def _collect_terms(inp: Dict[str, Any]) -> List[str]:
    # Prefer concept_graph.nodes (top scores), fallback to packz words
    nodes = _get(inp, ["concept_graph", "nodes", "nodes"], [])
    if isinstance(nodes, list) and nodes:
        sorted_nodes = sorted(nodes, key=lambda n: _safe_float(n.get("score"), 0.0), reverse=True)
        keys = [n.get("key") for n in sorted_nodes[:20] if isinstance(n.get("key"), str)]
        return [k for k in keys if k]
    # Fallback tokens from text
    text = _get(inp, ["perception", "packz", "text"], "") or _get(inp, ["perception", "normalized_text"], "") or _get(
        inp, ["text"], "")
    if not isinstance(text, str):
        return []
    return [t for t in re.split(r"\s+", text.strip()) if t]


def _collect_rules_synonyms(inp: Dict[str, Any]) -> Dict[str, List[str]]:
    rules = _get(inp, ["concept_graph", "rules", "rules"], [])
    syn: Dict[str, List[str]] = {}
    if not isinstance(rules, list):
        return syn
    # Map node-id to key for readability
    nid_to_key = {}
    for n in _get(inp, ["concept_graph", "nodes", "nodes"], []) or []:
        if isinstance(n, dict) and isinstance(n.get("id"), str) and isinstance(n.get("key"), str):
            nid_to_key[n["id"]] = n["key"]
    for r in rules:
        if not isinstance(r, dict) or r.get("type") != "synonym":
            continue
        a = nid_to_key.get(r.get("a"));
        b = nid_to_key.get(r.get("b"))
        if not a or not b:
            continue
        syn.setdefault(a, []).append(b)
        syn.setdefault(b, []).append(a)
    return syn


def _collect_entities(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Best-effort entity list from perception or previous blocks
    ent = _get(inp, ["perception", "entities"], [])
    if isinstance(ent, list) and ent:
        return [e for e in ent if isinstance(e, dict)]
    ent = _get(inp, ["entities"], [])
    return [e for e in ent if isinstance(e, dict)] if isinstance(ent, list) else []


def _collect_manifest(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    # planning.skills is the manifest of available skills
    plan = inp.get("planning", {})
    if isinstance(plan, dict) and isinstance(plan.get("skills"), list):
        return [s for s in plan["skills"] if isinstance(s, dict)]
    # default minimal manifest
    return [
        {
            "id": "skill.answer",
            "name": "Answer Generation",
            "triggers": {"reply": ["direct_answer"], "speech_act": ["question", "statement"]},
            "slots": []
        },
        {
            "id": "skill.execute",
            "name": "Action Executor",
            "triggers": {"reply": ["execute_action"], "speech_act": ["request", "command"]},
            "slots": [{"name": "action", "required": True}, {"name": "object", "required": False}]
        },
        {
            "id": "skill.clarify",
            "name": "Clarification",
            "triggers": {"reply": ["ask_clarification"]},
            "slots": [{"name": "missing_info", "required": True}]
        },
        {
            "id": "skill.ack",
            "name": "Acknowledge",
            "triggers": {"reply": ["acknowledge_only", "small_talk", "closing"]},
            "slots": []
        },
        {
            "id": "skill.safecheck",
            "name": "Safety Check",
            "triggers": {"reply": ["refuse_or_safecheck"]},
            "slots": [{"name": "reason", "required": True}]
        },
    ]


# ------------------------- scoring -------------------------

def _score_skill(skill: Dict[str, Any], reply_top: str, reply_dist: Dict[str, float], sa: Optional[str],
                 terms: List[str], synonyms: Dict[str, List[str]]) -> Tuple[float, List[str]]:
    notes: List[str] = []
    score = 0.0
    trg = skill.get("triggers", {}) if isinstance(skill.get("triggers"), dict) else {}

    # Reply-type alignment (from B2F2)
    if isinstance(trg.get("reply"), list):
        for r in trg["reply"]:
            p = reply_dist.get(r, 0.0)
            if p > 0:
                score += 0.6 * p
        if reply_top in trg["reply"]:
            score += 0.1
            notes.append(f"reply_top={reply_top}")

    # Speech-act alignment
    if sa and isinstance(trg.get("speech_act"), list) and sa in trg["speech_act"]:
        score += 0.15
        notes.append(f"sa={sa}")

    # Term hits (skill.triggers.terms: any match or via synonym)
    term_hits = 0
    trg_terms = [t for t in (trg.get("terms") or []) if isinstance(t, str)]
    if trg_terms:
        cf_terms = set(_cf(t) for t in terms)
        syn_flat = set()
        for t in terms:
            for s in synonyms.get(t, []):
                syn_flat.add(_cf(s))
        for tt in trg_terms:
            if _cf(tt) in cf_terms or _cf(tt) in syn_flat:
                term_hits += 1
        if term_hits:
            bonus = min(0.2, 0.05 * term_hits)
            score += bonus
            notes.append(f"term_hits={term_hits}")

    return round(score, 6), notes


# ------------------------- slot filling -------------------------

def _slot_schema(skill: Dict[str, Any]) -> List[Dict[str, Any]]:
    schema = skill.get("slots") if isinstance(skill.get("slots"), list) else []
    out: List[Dict[str, Any]] = []
    for s in schema:
        if isinstance(s, dict) and isinstance(s.get("name"), str):
            out.append({"name": s["name"], "required": bool(s.get("required", False))})
    return out


def _fill_from_entities(schema: List[Dict[str, Any]], entities: List[Dict[str, Any]]) -> Dict[str, Any]:
    filled: Dict[str, Any] = {}
    # Map entity type/name to slot by casefold equality
    for slot in schema:
        sname = _cf(slot["name"])
        for e in entities:
            etype = _cf(e.get("type", "")) or _cf(e.get("label", ""))
            ename = _cf(e.get("name", ""))
            if etype == sname or ename == sname:
                filled[slot["name"]] = e.get("value", e.get("text", e.get("name")))
                break
    return filled


def _fill_from_text(schema: List[Dict[str, Any]], text: str) -> Dict[str, Any]:
    # Heuristics for common slot names
    filled: Dict[str, Any] = {}
    if not isinstance(text, str) or not text:
        return filled
    url = re.search(r"(https?://\S+)", text)
    email = re.search(r"[\w\.-]+@[\w\.-]+\.\w+", text)
    number = re.search(r"\b\d+(\.\d+)?\b", text)
    for slot in schema:
        name_cf = _cf(slot["name"])
        if name_cf in {"url", "link"} and url and "url" not in filled:
            filled[slot["name"]] = url.group(1)
        elif name_cf in {"email"} and email and "email" not in filled:
            filled[slot["name"]] = email.group(0)
        elif name_cf in {"count", "k", "n"} and number and "count" not in filled:
            filled[slot["name"]] = number.group(0)
    return filled


# ------------------------- main -------------------------

def b5f1_route_intent(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B5F1 — Planning.IntentRouter (Noema)

    Input:
      {
        "world_model": { "prediction": { "top": str, "expected_reply": {...} }, "context": {"features": {"speech_act": str, ...}} },
        "concept_graph": { "nodes": {...}, "rules": {...} }?,
        "perception": { "packz": {"text": str}, "entities": [ {type|label|name, value|text}, ... ] }?,
        "planning": { "skills": [ {id, name, triggers{reply[],speech_act[],terms[]}, slots[{name,required}]}, ... ] }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "planner": {
          "intent": {
            "skill_id": str,
            "skill_name": str,
            "score": float,
            "rationale": [str],
            "slots": {
              "schema": [ {"name": str, "required": bool}, ... ],
              "filled": { slot: value, ... },
              "missing": [ slot, ... ]
            },
            "meta": { "source": "B5F1", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_prediction|no_skills" }
      }
    """
    reply_top, reply_dist = _collect_prediction(input_json)
    feats = _collect_features(input_json)
    sa = feats.get("speech_act") if isinstance(feats.get("speech_act"), str) else None

    if not reply_dist and not reply_top:
        return {
            "status": "SKIP",
            "planner": {"intent": {}},
            "diag": {"reason": "no_prediction"},
        }

    skills = _collect_manifest(input_json)
    if not skills:
        return {
            "status": "SKIP",
            "planner": {"intent": {}},
            "diag": {"reason": "no_skills"},
        }

    terms = _collect_terms(input_json)
    syn = _collect_rules_synonyms(input_json)
    entities = _collect_entities(input_json)
    text = _get(input_json, ["perception", "packz", "text"], "") or _get(input_json, ["perception", "normalized_text"],
                                                                         "") or _get(input_json, ["text"], "")

    # Score each skill
    scored: List[Tuple[float, Dict[str, Any], List[str]]] = []
    for sk in skills:
        s, notes = _score_skill(sk, reply_top, reply_dist, sa, terms, syn)
        scored.append((s, sk, notes))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_score, top_skill, notes = scored[0]

    schema = _slot_schema(top_skill)
    filled = _fill_from_entities(schema, entities)
    # text heuristics as fallback for common slots
    for k, v in _fill_from_text(schema, text).items():
        if k not in filled:
            filled[k] = v

    missing = [s["name"] for s in schema if s["name"] not in filled and s.get("required", False)]

    intent = {
        "skill_id": top_skill.get("id"),
        "skill_name": top_skill.get("name"),
        "score": round(float(top_score), 6),
        "rationale": notes[:8],
        "slots": {
            "schema": schema,
            "filled": filled,
            "missing": missing,
        },
        "meta": {"source": "B5F1", "rules_version": RULES_VERSION},
    }

    return {
        "status": "OK",
        "planner": {"intent": intent},
        "diag": {"reason": "ok"},
    }


if __name__ == "__main__":
    sample = {
        "world_model": {
            "prediction": {
                "top": "execute_action",
                "expected_reply": {
                    "execute_action": 0.56,
                    "ask_clarification": 0.22,
                    "direct_answer": 0.18,
                    "acknowledge_only": 0.02,
                    "small_talk": 0.01,
                    "closing": 0.0,
                    "refuse_or_safecheck": 0.01,
                    "other": 0.0
                }
            },
            "context": {"features": {"speech_act": "request"}}
        },
        "perception": {
            "packz": {"text": "Noema, please summarize https://example.com/report.pdf"},
            "entities": [
                {"type": "action", "value": "summarize"},
                {"type": "url", "value": "https://example.com/report.pdf"}
            ]
        },
        "planning": {
            "skills": [
                {
                    "id": "skill.web_summarize",
                    "name": "Web Document Summarizer",
                    "triggers": {"reply": ["execute_action"], "speech_act": ["request", "command"],
                                 "terms": ["summary", "summarize", "خلاصه"]},
                    "slots": [{"name": "action", "required": True}, {"name": "url", "required": True}]
                },
                {
                    "id": "skill.answer",
                    "name": "Answer Generation",
                    "triggers": {"reply": ["direct_answer", "ask_clarification"],
                                 "speech_act": ["question", "statement"]},
                    "slots": []
                }
            ]
        }
    }
    out = b5f1_route_intent(sample)
    print(out["planner"]["intent"]["skill_id"], out["planner"]["intent"]["slots"])
