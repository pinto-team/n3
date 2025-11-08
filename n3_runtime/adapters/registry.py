# n3_runtime/adapters/registry.py
# A simple registry that binds all block functions by stable names.
# We also expose short aliases for compatibility with earlier code.

from typing import Dict, Callable

# --- B0 Kernel is called directly elsewhere ---

# --- B1: Perception ---
from n3_core.block_1_perception.b1f1_collector import b1f1_collect
from n3_core.block_1_perception.b1f2_normalizer import b1f2_normalize
from n3_core.block_1_perception.b1f3_sentence_splitter import b1f3_split_sentences
from n3_core.block_1_perception.b1f4_tokenizer import b1f4_tokenize
from n3_core.block_1_perception.b1f5_script_tagger import b1f5_script_tagger
from n3_core.block_1_perception.b1f6_addressing import b1f6_addressing
from n3_core.block_1_perception.b1f7_speech_act import b1f7_speech_act
from n3_core.block_1_perception.b1f8_confidence import b1f8_confidence
from n3_core.block_1_perception.b1f9_novelty import b1f9_novelty
from n3_core.block_1_perception.b1f10_packz import b1f10_packz

# --- B2: World Model ---
from n3_core.block_2_world_model.b2f1_context_builder import b2f1_build_context
from n3_core.block_2_world_model.b2f2_predictor import b2f2_predict
from n3_core.block_2_world_model.b2f3_error_computer import b2f3_compute_error
from n3_core.block_2_world_model.b2f4_uncertainty import b2f4_uncertainty

# --- B3: Memory ---
from n3_core.block_3_memory.b3f1_wal_writer import b3f1_wal_write
from n3_core.block_3_memory.b3f2_indexer import b3f2_index
from n3_core.block_3_memory.b3f3_retriever import b3f3_retrieve
from n3_core.block_3_memory.b3f4_context_cache import b3f4_context_cache

# --- B4: Concept Graph ---
from n3_core.block_4_concept_graph.b4f1_pattern_miner import b4f1_mine_patterns
from n3_core.block_4_concept_graph.b4f2_node_manager import b4f2_manage_nodes
from n3_core.block_4_concept_graph.b4f3_edge_scorer import b4f3_score_edges
from n3_core.block_4_concept_graph.b4f4_rule_extractor import b4f4_extract_rules

# --- B5: Planning ---
from n3_core.block_5_planning.b5f1_intent_router import b5f1_route_intent
from n3_core.block_5_planning.b5f2_slot_collector import b5f2_collect_slots
from n3_core.block_5_planning.b5f3_plan_builder import b5f3_build_plan  # note: exported name is b5f3_build_plan

# --- B6: Dialog ---
from n3_core.block_6_dialog.b6f1_turn_realizer import b6f1_realize_turn
from n3_core.block_6_dialog.b6f2_surface_nlg import b6f2_surface_nlg
from n3_core.block_6_dialog.b6f3_safety_filter import b6f3_safety_filter

# --- B7: Execution ---
from n3_core.block_7_execution.b7f1_skill_dispatcher import b7f1_dispatch
from n3_core.block_7_execution.b7f2_result_normalizer import b7f2_normalize_results
from n3_core.block_7_execution.b7f3_result_presenter import b7f3_present_results

# --- B8: Persistence ---
from n3_core.block_8_persistence.b8f1_memory_commit import b8f1_memory_commit
from n3_core.block_8_persistence.b8f2_wal_apply_planner import b8f2_plan_apply
from n3_core.block_8_persistence.b8f3_apply_optimizer import b8f3_optimize_apply

# --- B9: Observability ---
from n3_core.block_9_observability.b9f1_telemetry_aggregator import b9f1_aggregate_telemetry
from n3_core.block_9_observability.b9f2_trace_builder import b9f2_build_trace
from n3_core.block_9_observability.b9f3_slo_evaluator import b9f3_evaluate_slo

# --- B10: Adaptation ---
from n3_core.block_10_adaptation.b10f1_policy_delta_planner import b10f1_plan_policy_delta
from n3_core.block_10_adaptation.b10f2_policy_apply_planner import b10f2_plan_policy_apply
from n3_core.block_10_adaptation.b10f3_policy_apply_stager import b10f3_stage_policy_apply

# --- B11: Runtime ---
from n3_core.block_11_runtime.b11f1_config_activator import b11f1_activate_config
from n3_core.block_11_runtime.b11f2_runtime_gatekeeper import b11f2_gatekeeper
from n3_core.block_11_runtime.b11f3_runtime_scheduler import b11f3_schedule_runtime
from n3_core.block_11_runtime.b11f4_initiative_scheduler import b11f4_initiative_scheduler

# --- B12: Orchestration ---
from n3_core.block_12_orchestration.b12f1_orchestrator_tick import b12f1_orchestrate
from n3_core.block_12_orchestration.b12f2_action_enveloper import b12f2_envelope_actions
from n3_core.block_12_orchestration.b12f3_driver_job_builder import b12f3_build_jobs

# --- B13: Drivers ---
from n3_core.block_13_drivers.b13f1_driver_protocol_builder import b13f1_build_protocol
from n3_core.block_13_drivers.b13f2_driver_reply_normalizer import b13f2_normalize_driver_replies
from n3_core.block_13_drivers.b13f3_driver_retry_planner import b13f3_plan_retry


def build_registry() -> Dict[str, Callable]:
    """
    Return a name->callable map for all blocks.
    Keys use the canonical exported function names and also include short aliases
    for compatibility with previous code (as seen in your /registry output).
    """
    reg: Dict[str, Callable] = {
        # B1
        "b1f1_collector": b1f1_collect,
        "b1f2_normalizer": b1f2_normalize,
        "b1f3_sentence_splitter": b1f3_split_sentences,
        "b1f4_tokenizer": b1f4_tokenize,
        "b1f5_script_tagger": b1f5_script_tagger,
        "b1f6_addressing": b1f6_addressing,
        "b1f7_speech_act": b1f7_speech_act,
        "b1f8_confidence": b1f8_confidence,
        "b1f9_novelty": b1f9_novelty,
        "b1f10_packz": b1f10_packz,

        # B2
        "b2f1_context_builder": b2f1_build_context,
        "b2f2_predictor": b2f2_predict,
        "b2f3_error_computer": b2f3_compute_error,
        "b2f4_uncertainty": b2f4_uncertainty,

        # B3
        "b3f1_wal_writer": b3f1_wal_write,
        "b3f2_indexer": b3f2_index,
        "b3f3_retriever": b3f3_retrieve,
        "b3f4_context_cache": b3f4_context_cache,

        # B4
        "b4f1_mine_patterns": b4f1_mine_patterns,
        "b4f2_manage_nodes": b4f2_manage_nodes,
        "b4f3_score_edges": b4f3_score_edges,
        "b4f4_extract_rules": b4f4_extract_rules,

        # B5
        "b5f1_intent_router": b5f1_route_intent,
        "b5f2_slot_collector": b5f2_collect_slots,
        "b5f3_build_plan": b5f3_build_plan,

        # B6
        "b6f1_turn_realizer": b6f1_realize_turn,
        "b6f2_surface_nlg": b6f2_surface_nlg,
        "b6f3_safety_filter": b6f3_safety_filter,

        # B7
        "b7f1_skill_dispatcher": b7f1_dispatch,
        "b7f2_result_normalizer": b7f2_normalize_results,
        "b7f3_result_presenter": b7f3_present_results,

        # B8
        "b8f1_memory_commit": b8f1_memory_commit,
        "b8f2_plan_apply": b8f2_plan_apply,
        "b8f3_optimize_apply": b8f3_optimize_apply,

        # B9
        "b9f1_aggregate_telemetry": b9f1_aggregate_telemetry,
        "b9f2_build_trace": b9f2_build_trace,
        "b9f3_evaluate_slo": b9f3_evaluate_slo,

        # B10
        "b10f1_plan_policy_delta": b10f1_plan_policy_delta,
        "b10f2_plan_policy_apply": b10f2_plan_policy_apply,
        "b10f3_stage_policy_apply": b10f3_stage_policy_apply,

        # B11 (canonical + short aliases)
        "b11f1_activate_config": b11f1_activate_config,
        "b11f2_runtime_gatekeeper": b11f2_gatekeeper,
        "b11f3_runtime_scheduler": b11f3_schedule_runtime,
        "b11f4_initiative_scheduler": b11f4_initiative_scheduler,
        # short names (compat)
        "b11f2_gatekeeper": b11f2_gatekeeper,
        "b11f3_schedule_runtime": b11f3_schedule_runtime,

        # B12 (canonical + short aliases)
        "b12f1_orchestrator_tick": b12f1_orchestrate,
        "b12f2_action_enveloper": b12f2_envelope_actions,
        "b12f3_driver_job_builder": b12f3_build_jobs,
        # short names (compat)
        "b12f1_orchestrate": b12f1_orchestrate,
        "b12f2_envelope_actions": b12f2_envelope_actions,
        "b12f3_build_jobs": b12f3_build_jobs,

        # B13 (canonical + short aliases)
        "b13f1_driver_protocol_builder": b13f1_build_protocol,
        "b13f2_normalize_driver_replies": b13f2_normalize_driver_replies,
        "b13f3_driver_retry_planner": b13f3_plan_retry,
        # short names (compat)
        "b13f1_build_protocol": b13f1_build_protocol,
        "b13f3_plan_retry": b13f3_plan_retry,
    }

    return reg
