# Learning Pipeline

Noema blends reinforcement signals from the world model with structural knowledge extracted from
conversation traces. This document summarises the learning loops and how they surface diagnostics.

## Policy Reinforcement (B10F1)

- Consumes `world_model.trace.error_history` entries, each containing `reward`, `target`, `actual`
  and `top_pred` labels.
- Applies a lightweight update rule `w_new = w_old + lr * signal` where the learning rate is scaled
  by uncertainty. The algorithm boosts target labels, penalises incorrect top predictions, and tracks
  the aggregate delta norm.
- Emits `policy.learning` with:
  - `version`: SHA1 hash of the new weight vector plus parent version.
  - `weights`: rounded weight table ready for persistence.
  - `rollback`: previous version snapshot for safe reverts.
  - `summary`: `avg_reward`, `updates`, `confidence`, and `delta_norm` used by observability.
- Publishes `adaptation.policy` summary so telemetry, runtime initiative, and the REST API can expose
  current learning confidence.

## Concept Graph Growth (B4)

1. **Pattern mining** combines mined n-grams with policy trace co-occurrences to generate both textual
   and intent-prefixed terms (e.g., `intent::execute_action`).
2. **Node management** merges new terms into canonical nodes, keeping TF/DF statistics and surfaces.
3. **Edge scoring** factors PMI, co-occurrence strength, node quality, and reward correlation to
   produce weighted edges.
4. **Rule extraction** derives associative/synonym/subsumption rules, attaches reward evidence, and
   issues a concept graph version document with counts.

Each pass emits `concept_graph.version` and `concept_graph.updates`, which are persisted by B8F1 and
 surfaced by observability for dashboards and initiative reflection prompts.

## Training Entry Points

- **`/policy/train`** (FastAPI): runs the concept mining and policy adaptation steps for a thread,
  returning kernel diagnostics, learning summaries, and concept version identifiers.
- **Dashboard**: the UI polls `/introspect/{thread_id}` to chart uncertainty, average reward, and
  policy update counts. This empowers manual supervision without inspecting internal state dumps.

## Persistence & Rollback

- `b8f1_memory_commit` appends `record_concept_version` WAL entries whenever a new concept snapshot is
  produced. `b8f2_plan_apply` persists the version, update payload, and current pointer under the
  session namespace.
- Policy learning keeps `rollback` weights in-memory. Higher-level tooling can checkpoint or roll back
  by promoting stored versions into the runtime config.
