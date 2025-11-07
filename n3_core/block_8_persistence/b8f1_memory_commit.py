# Folder: noema/n3_core/block_8_persistence
# File:   b8f1_memory_commit.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b8f1_memory_commit"]

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


def _iso_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha1(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _trim(s: Optional[str], n: int = 2000) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else (s[: n - 1] + "…")


def _packz_like(turn_id: str, text: str, role: str, lang: Optional[str], commit_time: Optional[str],
                dir_hint: Optional[str]) -> Dict[str, Any]:
    return {
        "id": turn_id,
        "text": text,
        "signals": {"direction": (dir_hint or ("rtl" if lang == "fa" else "ltr")), "speech_act": None},
        "meta": {"commit_time": commit_time or _iso_now_z(), "role": role},
        "spans": {}  # left empty; upstream tokenizers may backfill
    }


# ------------------------- collectors -------------------------

def _collect_user(inp: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    pk = _get(inp, ["perception", "packz"], {})
    if isinstance(pk, dict) and isinstance(pk.get("text"), str):
        dir_hint = _get(pk, ["signals", "direction"], None)
        return pk.get("text"), "user", dir_hint
    # fallback
    txt = _get(inp, ["text"], None)
    return (txt if isinstance(txt, str) else None), "user", None


def _collect_assistant(inp: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    # Prefer Safety-filtered final text; fallback to surface/turn content
    final = _get(inp, ["dialog", "final"], {})
    surf = _get(inp, ["dialog", "surface"], {})
    turn = _get(inp, ["dialog", "turn"], {})

    if isinstance(final, dict) and isinstance(final.get("text"), str):
        lang = _get(surf, ["language"], None) or _get(final, ["language"], None)
        move = final.get("move") if isinstance(final.get("move"), str) else _get(turn, ["move"], "")
        return final["text"], (move or "answer"), lang, _get(_get(inp, ["world_model", "context", "features"], {}),
                                                             ["dir"], None)

    if isinstance(surf, dict) and isinstance(surf.get("text"), str):
        move = _get(turn, ["move"], surf.get("move", "answer"))
        return surf["text"], (move if isinstance(move, str) else "answer"), surf.get("language"), _get(
            _get(inp, ["world_model", "context", "features"], {}), ["dir"], None)

    if isinstance(turn, dict) and isinstance(turn.get("content"), str):
        return turn["content"], (turn.get("move") or "answer"), None, _get(
            _get(inp, ["world_model", "context", "features"], {}), ["dir"], None)

    return None, None, None, None


def _collect_result_summary(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    best = _get(inp, ["executor", "results", "best"], {})
    if not isinstance(best, dict) or not best:
        return None
    return {
        "req_id": best.get("req_id"),
        "ok": bool(best.get("ok", True)),
        "kind": best.get("kind"),
        "text": _trim(best.get("text", "")),
        "attachments": best.get("attachments", []) if isinstance(best.get("attachments"), list) else [],
        "usage": best.get("usage", {}) if isinstance(best.get("usage"), dict) else {},
        "duration_ms": best.get("duration_ms", 0),
        "score": best.get("score", 0.0),
    }


def _plan_meta(inp: Dict[str, Any]) -> Dict[str, Any]:
    plan = _get(inp, ["planner", "plan"], {}) or {}
    return {
        "plan_id": plan.get("id"),
        "skill_id": plan.get("skill_id"),
        "skill_name": plan.get("skill_name"),
        "next_move": plan.get("next_move"),
    }


# ------------------------- main -------------------------

def b8f1_memory_commit(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B8F1 — Persistence.MemoryCommit (Noema)
    Produces pure WAL operations to persist the user/assistant turns and (optional) best execution result.

    Input (best-effort):
      - perception.packz.text  (user)
      - dialog.final / dialog.surface / dialog.turn (assistant)
      - executor.results.best  (optional)
      - planner.plan           (meta)

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "memory": {
          "wal": {
            "ops": [
              { "op": "append_turn", "turn": {id, role, text, lang?, move?, time, packz:{...}} },
              { "op": "append_result", "result": {req_id, ok, kind, text, attachments[], usage{}, duration_ms, score}, "link": {"assistant_turn_id": str}? }?,
              { "op": "bump_counters", "keys": {"turns": +1, "assistant_answers": +1, "executions": +1?} }
            ],
            "meta": { "source": "B8F1", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_turns", "counts": { "ops": int } }
      }
    """
    user_text, user_role, user_dir = _collect_user(input_json)
    as_text, as_move, as_lang, as_dir = _collect_assistant(input_json)
    result_best = _collect_result_summary(input_json)
    plan_meta = _plan_meta(input_json)

    ops: List[Dict[str, Any]] = []

    # Nothing to commit?
    if not (user_text or as_text):
        return {
            "status": "SKIP",
            "memory": {"wal": {"ops": [], "meta": {"source": "B8F1", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_turns", "counts": {"ops": 0}},
        }

    # 1) Append user turn (if present)
    if user_text:
        u_time = _iso_now_z()
        u_id = _sha1({"role": user_role, "text": _trim(user_text, 512), "t": u_time})
        u_packz = _packz_like(u_id, user_text, user_role, None, u_time, user_dir)
        ops.append({
            "op": "append_turn",
            "turn": {
                "id": u_id,
                "role": user_role,
                "text": _trim(user_text, 4000),
                "lang": None,
                "move": "user_input",
                "time": u_time,
                "packz": u_packz
            }
        })

    # 2) Append assistant turn (if present)
    a_turn_id = None
    if as_text and as_move:
        a_time = _iso_now_z()
        a_turn_id = _sha1({"role": "assistant", "move": as_move, "text": _trim(as_text, 512), "t": a_time})
        a_packz = _packz_like(a_turn_id, as_text, "assistant", as_lang, a_time, as_dir)
        ops.append({
            "op": "append_turn",
            "turn": {
                "id": a_turn_id,
                "role": "assistant",
                "text": _trim(as_text, 4000),
                "lang": as_lang,
                "move": as_move,
                "time": a_time,
                "packz": a_packz,
                "plan": plan_meta
            }
        })

    # 3) Append best result (optional) and link to assistant turn
    if result_best:
        rec = dict(result_best)
        ops.append({
            "op": "append_result",
            "result": rec,
            "link": {"assistant_turn_id": a_turn_id} if a_turn_id else None
        })

    # 4) Bump counters
    counters = {"turns": 1 if user_text or as_text else 0}
    if as_text:
        counters["assistant_answers"] = 1
    if result_best:
        counters["executions"] = 1
    ops.append({"op": "bump_counters", "keys": counters})

    return {
        "status": "OK",
        "memory": {
            "wal": {
                "ops": ops,
                "meta": {"source": "B8F1", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok", "counts": {"ops": len(ops)}},
    }


if __name__ == "__main__":
    # Minimal demo
    sample = {
        "perception": {"packz": {"text": "سلام نوما، این را ذخیره کن.", "signals": {"direction": "rtl"}}},
        "dialog": {
            "final": {"move": "answer", "text": "انجام شد."},
            "surface": {"language": "fa"},
            "turn": {"move": "answer"}
        },
        "executor": {
            "results": {
                "best": {"req_id": "r1", "ok": True, "kind": "json", "text": "{\"ok\":true}",
                         "usage": {"input_tokens": 10, "output_tokens": 5, "cost": 0.0002}, "duration_ms": 420,
                         "score": 0.7}
            }
        },
        "planner": {"plan": {"id": "plan-42", "skill_id": "skill.web_summarize", "skill_name": "Web Summarizer",
                             "next_move": "execute"}}
    }
    out = b8f1_memory_commit(sample)
    print(out["diag"], len(out["memory"]["wal"]["ops"]))
