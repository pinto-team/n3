from copy import deepcopy

from n3_core.block_4_concept_graph.b4f1_pattern_miner import b4f1_mine_patterns
from n3_core.block_4_concept_graph.b4f2_node_manager import b4f2_manage_nodes
from n3_core.block_4_concept_graph.b4f3_edge_scorer import b4f3_score_edges
from n3_core.block_4_concept_graph.b4f4_rule_extractor import b4f4_extract_rules


def _merge_state(base: dict, update: dict) -> dict:
    merged = deepcopy(base)
    for k, v in update.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _merge_state(merged[k], v)
        else:
            merged[k] = deepcopy(v)
    return merged


def test_concept_graph_builds_version_and_updates():
    state = {
        "world_model": {
            "trace": {
                "error_history": [
                    {"reward": 0.8, "target": "execute_action", "actual": "execute_action", "top_pred": "execute_action"},
                    {"reward": 0.6, "target": "ask_clarification", "actual": "direct_answer", "top_pred": "direct_answer"},
                    {"reward": 0.9, "target": "execute_action", "actual": "execute_action", "top_pred": "ask_clarification"},
                ]
            }
        }
    }

    patterns = b4f1_mine_patterns(state)
    state = _merge_state(state, patterns)
    nodes = b4f2_manage_nodes(state)
    state = _merge_state(state, nodes)
    edges = b4f3_score_edges(state)
    state = _merge_state(state, edges)
    rules = b4f4_extract_rules(state)

    concept = rules["concept_graph"]
    version = concept["version"]
    updates = concept["updates"]

    assert version["id"]
    assert updates["new_rules"] >= 0
    assert any(str(n["key"]).startswith("intent") for n in state["concept_graph"]["nodes"]["nodes"])
