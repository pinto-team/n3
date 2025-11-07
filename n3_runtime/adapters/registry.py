# Folder: noema/n3_runtime/adapters
# File:   registry.py

from __future__ import annotations
from typing import Dict, Callable, Optional
import importlib

StepFn = Callable[[dict], dict]
__all__ = ["build_registry"]

def _bind(reg: Dict[str, StepFn], key: str, module: str, attr: str) -> None:
    try:
        mod = importlib.import_module(module)
        fn = getattr(mod, attr, None)
        if callable(fn):
            reg[key] = fn
    except Exception:
        # silent skip; kernel tolerates missing steps
        pass

def build_registry() -> Dict[str, StepFn]:
    """
    Deterministic registry of pure steps. Only binds steps that import cleanly.
    Keys match the kernel order.
    """
    reg: Dict[str, StepFn] = {}

    # --- B8 Persistence (optional in dev) ---
    _bind(reg, "b8f2_plan_apply",     "n3_core.block_8_persistence.b8f2_wal_apply_planner", "b8f2_plan_apply")
    _bind(reg, "b8f3_optimize_apply", "n3_core.block_8_persistence.b8f3_apply_optimizer",   "b8f3_optimize_apply")

    # --- B9 Observability (optional in dev) ---
    _bind(reg, "b9f1_aggregate_telemetry", "n3_core.block_9_observability.b9f1_telemetry_aggregator", "b9f1_aggregate_telemetry")
    _bind(reg, "b9f2_build_trace",         "n3_core.block_9_observability.b9f2_trace_builder",        "b9f2_build_trace")
    _bind(reg, "b9f3_evaluate_slo",        "n3_core.block_9_observability.b9f3_slo_evaluator",        "b9f3_evaluate_slo")

    # --- B4 Concept Graph ---
    _bind(reg, "b4f1_mine_patterns", "n3_core.block_4_concept_graph.b4f1_pattern_miner", "b4f1_mine_patterns")
    _bind(reg, "b4f2_manage_nodes",   "n3_core.block_4_concept_graph.b4f2_node_manager",   "b4f2_manage_nodes")
    _bind(reg, "b4f3_score_edges",    "n3_core.block_4_concept_graph.b4f3_edge_scorer",    "b4f3_score_edges")
    _bind(reg, "b4f4_extract_rules",  "n3_core.block_4_concept_graph.b4f4_rule_extractor", "b4f4_extract_rules")

    # --- B10 Adaptation ---
    _bind(reg, "b10f1_plan_policy_delta",  "n3_core.block_10_adaptation.b10f1_policy_delta_planner",  "b10f1_plan_policy_delta")
    _bind(reg, "b10f2_plan_policy_apply",  "n3_core.block_10_adaptation.b10f2_policy_apply_planner",  "b10f2_plan_policy_apply")
    _bind(reg, "b10f3_stage_policy_apply", "n3_core.block_10_adaptation.b10f3_policy_apply_stager",   "b10f3_stage_policy_apply")

    # --- B11 Runtime (needed for tests) ---
    _bind(reg, "b11f1_activate_config",    "n3_core.block_11_runtime.b11f1_config_activator",         "b11f1_activate_config")
    _bind(reg, "b11f2_gatekeeper",         "n3_core.block_11_runtime.b11f2_runtime_gatekeeper",       "b11f2_gatekeeper")
    _bind(reg, "b11f3_schedule_runtime",   "n3_core.block_11_runtime.b11f3_runtime_scheduler",        "b11f3_schedule_runtime")
    _bind(reg, "b11f4_initiative_scheduler","n3_core.block_11_runtime.b11f4_initiative_scheduler", "b11f4_initiative_scheduler")

    # --- B12 Orchestration (needed for E2E) ---
    _bind(reg, "b12f1_orchestrate",        "n3_core.block_12_orchestration.b12f1_orchestrator_tick",  "b12f1_orchestrate")
    _bind(reg, "b12f2_envelope_actions",   "n3_core.block_12_orchestration.b12f2_action_enveloper",    "b12f2_envelope_actions")
    _bind(reg, "b12f3_build_jobs",         "n3_core.block_12_orchestration.b12f3_driver_job_builder",  "b12f3_build_jobs")

    # --- B13 Drivers (needed for E2E) ---
    _bind(reg, "b13f1_build_protocol",     "n3_core.block_13_drivers.b13f1_driver_protocol_builder",   "b13f1_build_protocol")
    _bind(reg, "b13f2_normalize_driver_replies","n3_core.block_13_drivers.b13f2_driver_reply_normalizer","b13f2_normalize_driver_replies")
    _bind(reg, "b13f3_plan_retry",         "n3_core.block_13_drivers.b13f3_driver_retry_planner",      "b13f3_plan_retry")
    return reg
