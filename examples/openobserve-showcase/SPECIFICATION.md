# OpenRig Observability Showcase & Reliability — Architectural & Technical Specification

- **Companion Exporter**: [`smota/openrig-telemetry`](https://github.com/smota/openrig-telemetry) (`telemetry-otlp.mjs`)
- **Upstream Contract & Reference PR**: [`mvschwarz/openrig#2589`](https://github.com/mvschwarz/openrig/pull/2589) (`docs/reference/telemetry.md`)
- **Architectural Design Discussion**: [`mvschwarz/openrig#2573`](https://github.com/mvschwarz/openrig/discussions/2573) (*Optional OpenTelemetry integration for queue, tenure and agent-runtime insights*)
- **Target Backend**: OpenObserve `v1.1.2` OSS (V8 native 192-column dashboard schema, DataFusion SQL engine, `/api/v2` folder & alert APIs)

---

## 1. Design Philosophy: Two Layers, Two Personas (SRE + Executive)

When operating multi-agent engineering rigs (`OpenRig` seats, work queues, and autonomous coding harnesses such as `Claude Code`, `Codex`, `Gemini`/`Antigravity`, `Grok`, and local `decider` inference), operators and engineering leaders face two distinct classes of questions:

1. **The OpenRig Rig Layer (Coordination & Control Plane)** — Powered by `openrig-telemetry` (`service_name = 'openrig-telemetry-exporter'`), polling OpenRig's finite queue-transition stream (`GET /api/telemetry/v1/queue-transitions`) and cached seat inventory (`GET /api/rigs`, `GET /api/rigs/:rigId/nodes`).
   - **SRE Operations Lens**: *Which seats are working, idle, awaiting human input, or unavailable right now? Are stopped processes strictly separated from idle capacity? Where are work items dwelling in the queue? Is an apparent quiet period real, or is observation missing due to a daemon restart (`bootEpoch`) or exporter failure?*
   - **Executive Governance Lens**: *What is our active fleet utilization vs. human-gate bottleneck ratio? What is our queue completion throughput? How does a logical seat (`source_namespace + immutable nodeId`) compound across occupant tenure handovers (`seat_tenure_change`)?*

2. **The OpenRig AI Layer (Agent Runtimes, Models, Tooling & Quota)** — Powered by native agent client OTLP streams (`Codex`, `Claude Code`, `Antigravity`/`Gemini`, `Grok`, local `decider`) and hot-path lifecycle hooks (`agent-otel-bridge`).
   - **SRE Operations Lens**: *Which provider subscription quota bucket (`agent_quota_remaining_fraction`) is approaching exhaustion? What is the latency and pipeline depth of tool executions (`execute_tool *`) and sub-agent spawns (`invoke_agent *`)? How do signals correlate across the 3 correlation levels (`L1 Search Correlation`, `L2 OTel Span Links`, `L3 W3C traceparent`)?*
   - **Executive Governance Lens**: *How is engineering effort distributed across workspaces and model tiers? How do we separate producer-reported token counts from subscription quota headroom and dollar invoice costs without conflating subscription usage with billing?*

---

## 2. Folder & Ownership Boundary Architecture

All showcase resources are isolated in dedicated OpenObserve v2 folders and stamped with an explicit ownership marker (`Managed by envctl OpenRig showcase v1.`) so they never collide with host/station SRE folders or manually authored dashboards:

| Folder Key | OpenObserve Folder Name | Folder Type | Purpose |
| --- | --- | --- | --- |
| `openrig-reliability-dashboards` | `50 OpenRig Reliability` | `dashboards` | Houses the two multi-tab V8 visual showcase dashboards (`openrig-rig-layer` and `openrig-ai-layer`). |
| `openrig-reliability-alerts` | `OpenRig Reliability` | `alerts` | Houses the 5 scheduled SQL reliability alerts (`envctl-openrig-alert-*`). |

---

## 3. Complete Requirement Traceability to GitHub Discussion #2573 & PR #2589

Every architectural requirement and non-goal established in [`mvschwarz/openrig#2573`](https://github.com/mvschwarz/openrig/discussions/2573) and [`mvschwarz/openrig#2589`](https://github.com/mvschwarz/openrig/pull/2589) is explicitly implemented and verified across the 2 dashboards (49 panels total: 47 SQL panels + 2 Executive Brief markdown panels) and 5 SQL alerts:

| Discussion #2573 Requirement | Dashboard & Tab | Panel ID(s) | Signal / Stream & Key Attributes | Alert Coverage |
| --- | --- | --- | --- | --- |
| **1. Queue-State Coordination Spans (`openrig.queue.state`)**<br>Completed, observed queue-state intervals (`pending`, `in-progress`, `blocked`) with entry/departure transition IDs | `openrig-rig-layer` → `sre-queues` | `rig_chart_queue_state_spans` (stacked bar)<br>`rig_table_queue_state_spans` (table)<br>`rig_chart_dwell_seconds`<br>`rig_table_dwell_buckets` | **`traces/default`**: `service_name = 'openrig-telemetry-exporter'`, `operation_name = 'openrig.queue.state'`, `openrig_queue_state`, `openrig_queue_departure_state`, `openrig_transition_enter_id`, `openrig_transition_depart_id`, `service_openrig_source_namespace`, `duration`<br>**`metrics`**: `openrig_queue_state_duration_sum`, `openrig_queue_state_duration_bucket` | `envctl-openrig-alert-coverage-gap` |
| **2. Seat-State Timelines, Stopped-vs-Idle Separation & Freshness Guard**<br>Separate `presence` (`running`, `detached`, `exited`, `stopped`) from `activity` (`working`, `idle`, `needs-input`, `unknown`); never classify stopped as `idle`; flag stale departed samples (`sample_age_s > 300`) | `openrig-rig-layer` → `sre-control` | `rig_chart_seat_activity_timeline` (area timeline)<br>`rig_table_seat_roster` (joined presence + activity + freshness table)<br>`rig_chart_presence_by_runtime`<br>`rig_chart_activity_mix` | **`metrics`**: `openrig_seat_presence` (`openrig_seat_presence`), `openrig_seat_activity` (`openrig_seat_state`), `openrig_seat_needs_input` joined on `(service_openrig_source_namespace, openrig_seat_node_id, openrig_seat_runtime)` | `envctl-openrig-alert-seat-human-gate-stall`<br>`envctl-openrig-alert-fleet-bottleneck-high` |
| **3. Logical Seat Identity (`source_namespace + immutable nodeId`) & Tenure Continuity (`seat_tenure_change`)**<br>Follow logical seats across occupant handovers (`prior_generation` → `new_generation`) and distinguish durable `source_namespace` from `bootEpoch` | `openrig-rig-layer` → `exec-rig` | `rig_exec_table_seat_tenure_lineage` (table)<br>`rig_exec_kpi_tenure_changes` (KPI) | **`logs/default`**: `service_name = 'openrig-telemetry-exporter'`, `openrig_record_kind = 'seat_tenure_change'`, `openrig_source_namespace`, `openrig_seat_node_id`, `openrig_tenure_prior_generation`, `openrig_tenure_new_generation`, `openrig_tenure_continuity`, `openrig_source_boot_epoch` | `envctl-openrig-alert-coverage-gap` |
| **4. Quiet Period vs. Missing Observation & Exporter Integrity**<br>Verify whether an apparent quiet period is real or caused by missing observation, boot-epoch resets, or exporter stage failures | `openrig-rig-layer` → `sre-exporter` | `rig_chart_observation_continuity_ts` (area timeline)<br>`rig_chart_failures_by_stage`<br>`rig_chart_gaps_by_code`<br>`rig_table_exporter_health` | **`logs/default`**: `openrig_record_kind` (`queue_transition`, `seat_tenure_change`, `coverage_gap`, `failure`)<br>**`metrics`**: `openrig_telemetry_failure`, `openrig_telemetry_coverage_gap`, `agent_bridge_dropped_total` | `envctl-openrig-alert-exporter-failure`<br>`envctl-openrig-alert-coverage-gap` |
| **5. Token/Cost Provenance, Producer-Reported Waits/Approvals & 3-Level Correlation**<br>Separate subscription quota fraction from provider-reported tokens and invoice costs; surface producer-reported durations/retries/approvals; classify L1/L2/L3 correlation | `openrig-ai-layer` → `sre-tools` & `exec-ai` | `ai_exec_table_token_cost_provenance` (table)<br>`ai_chart_correlation_levels` (stacked bar)<br>`ai_table_producer_waits_approvals` (table) | **`traces/default`**: `gen_ai_usage_input_tokens`, `gen_ai_usage_output_tokens`, `gen_ai_usage_cache_read_input_tokens`, `gen_ai_usage_cost`, `links`, `reference_parent_span_id`<br>**`logs/default`**: `event_name`, `decision`, `attempt`, `TRY_CAST(duration_ms AS DOUBLE)` | `envctl-openrig-alert-ai-quota-low` |

---

## 4. Dashboard 1 Specification: `envctl-openrig: OpenRig Rig Layer (Control Plane, Seats & Queues)`

- **Key**: `openrig-rig-layer`
- **Folder**: `50 OpenRig Reliability` (`openrig-reliability-dashboards`)
- **Schema Version**: `8` (192-column native grid, default window `24h`)
- **Total Panels**: `30` across `4` tabs

### Tab 1: `sre-control` (`SRE: Seat & Daemon Control Plane`) — 8 Panels
1. `rig_kpi_tracked_seats` (`metric`, `x=0, y=0, w=48, h=8`): Total tracked OpenRig seats from `openrig_seat_presence`.
2. `rig_kpi_working_seats` (`metric`, `x=48, y=0, w=48, h=8`): Live seats with `openrig_seat_state = 'working'` and `latest_val > 0`.
3. `rig_kpi_blocked_seats` (`metric`, `x=96, y=0, w=48, h=8`): Live seats with `openrig_seat_needs_input` `latest_val > 0`.
4. `rig_kpi_bottleneck_pct` (`metric`, `x=144, y=0, w=48, h=8`, `unit="percent"`): Latest `agent_fleet_bottleneck_ratio * 100`.
5. `rig_chart_presence_by_runtime` (`stacked`, `x=0, y=8, w=96, h=14`): Seat presence by `openrig_seat_runtime` and `openrig_seat_presence` (`running`, `detached`, `exited`, `stopped`).
6. `rig_chart_activity_mix` (`donut`, `x=96, y=8, w=96, h=14`): Live seat activity distribution (`working`, `idle`, `needs-input`, `unknown`).
7. `rig_chart_seat_activity_timeline` (`area`, `x=0, y=22, w=192, h=14`): Time-series of active seat observations by `openrig_seat_state` over `histogram(_timestamp)`.
8. `rig_table_seat_roster` (`table`, `x=0, y=36, w=192, h=16`): Joined roster of `openrig_seat_presence` + `openrig_seat_activity` + `sample_age_s` enforcing the **Stopped-Seat Guard** (`UNAVAILABLE: Process not running (not idle)`) and **Staleness Guard** (`STALE (>5m): Verify seat/exporter freshness`).

### Tab 2: `sre-queues` (`SRE: Queue State Machine, Spans & Handoff Latency`) — 7 Panels
1. `rig_chart_queue_transitions_ts` (`area`, `x=0, y=0, w=96, h=14`): Queue state transitions over time from `logs/default` (`openrig_record_kind = 'queue_transition'`).
2. `rig_chart_queue_closure` (`stacked`, `x=96, y=0, w=96, h=14`): Cumulative observed transitions by `openrig_queue_state` and `openrig_queue_closure_reason`.
3. `rig_chart_dwell_seconds` (`bar`, `x=0, y=14, w=96, h=14`): Cumulative queue dwell seconds by transition path (`openrig_queue_state -> openrig_queue_departure_state`) from `openrig_queue_state_duration_sum`.
4. `rig_table_dwell_buckets` (`table`, `x=96, y=14, w=96, h=14`): Queue dwell latency histogram buckets (`le` seconds vs. cumulative count) from `openrig_queue_state_duration_bucket`.
5. `rig_table_transition_ledger` (`table`, `x=0, y=28, w=192, h=14`): Audit ledger of `openrig_record_kind`, `openrig_queue_state`, `openrig_coverage_status`, and `openrig_delivery_replayed`.
6. `rig_chart_queue_state_spans` (`stacked`, `x=0, y=42, w=96, h=14`): Mean duration (`ms`) of completed `openrig.queue.state` root coordination spans in `traces/default` by `openrig_queue_state` and `openrig_queue_departure_state`.
7. `rig_table_queue_state_spans` (`table`, `x=96, y=42, w=96, h=14`): Completed `openrig.queue.state` span intervals (`source_namespace`, `queue_state`, `departure_state`, `enter_id`, `depart_id`, `duration_ms`).

### Tab 3: `sre-exporter` (`SRE: Telemetry Exporter, Ledger & Gap Integrity`) — 7 Panels
1. `rig_kpi_exporter_failures` (`metric`, `x=0, y=0, w=64, h=8`): Total exporter failures across stages/categories (`openrig_telemetry_failure`).
2. `rig_kpi_coverage_gaps` (`metric`, `x=64, y=0, w=64, h=8`): Total coverage gaps detected (`openrig_telemetry_coverage_gap`).
3. `rig_kpi_bridge_drops` (`metric`, `x=128, y=0, w=64, h=8`): Total OTel bridge dropped events (`agent_bridge_dropped_total`).
4. `rig_chart_failures_by_stage` (`stacked`, `x=0, y=8, w=96, h=14`): Exporter failures by `openrig_failure_stage` and `openrig_failure_category`.
5. `rig_chart_gaps_by_code` (`bar`, `x=96, y=8, w=96, h=14`): Coverage gaps by `openrig_gap_code` (`boot_boundary_history_unverified`, `page_intervals_suppressed`, `historical_transitions_not_read`, etc.).
6. `rig_chart_observation_continuity_ts` (`area`, `x=0, y=22, w=192, h=14`): Observation Continuity & Quiet-Period Verification Timeline across all `openrig_record_kind` values.
7. `rig_table_exporter_health` (`table`, `x=0, y=36, w=192, h=14`): Self-health matrix by `source_host`, `source_namespace`, `openrig_stream`, `boot_epoch`, and `record_kind`.

### Tab 4: `exec-rig` (`Executive: Fleet Capacity, Lineage & Delivery Velocity`) — 8 Panels
1. `rig_exec_brief` (`markdown`, `x=0, y=0, w=192, h=8`): Executive Brief on fleet capacity, queue completion efficiency, and compounding seat lineage.
2. `rig_exec_kpi_utilization` (`metric`, `x=0, y=8, w=48, h=8`, `unit="percent"`): Fleet active seat utilization percentage.
3. `rig_exec_kpi_completions` (`metric`, `x=48, y=8, w=48, h=8`): Completed queue items reaching terminal state (`done`/`closed`).
4. `rig_exec_kpi_tenure_changes` (`metric`, `x=96, y=8, w=48, h=8`): Total observed seat lineage tenure handover events (`seat_tenure_change`).
5. `rig_exec_kpi_meshloop_ops` (`metric`, `x=144, y=8, w=48, h=8`): Total Meshloop task-graph orchestration operations (`meshloop_operation_count`).
6. `rig_exec_chart_runtime_portfolio` (`donut`, `x=0, y=16, w=96, h=14`): Fleet seat portfolio share by agent runtime harness (`pi`, `claude-code`, `codex`, etc.).
7. `rig_exec_chart_queue_funnel` (`bar`, `x=96, y=16, w=96, h=14`): Work queue lifecycle funnel across queue stages.
8. `rig_exec_table_seat_tenure_lineage` (`table`, `x=0, y=30, w=192, h=16`): Logical Seat Tenure & Occupant Handover Ledger (`source_namespace + seat_node_id`, `runtime`, `prior_generation`, `new_generation`, `tenure_continuity`, `boot_epoch`, `handover_events`).

---

## 5. Dashboard 2 Specification: `envctl-openrig: OpenRig AI Layer (Models, Tooling, Sub-Agents & Quota)`

- **Key**: `openrig-ai-layer`
- **Folder**: `50 OpenRig Reliability` (`openrig-reliability-dashboards`)
- **Schema Version**: `8` (192-column native grid, default window `7d`)
- **Total Panels**: `19` across `3` tabs

### Tab 1: `sre-inference` (`SRE: Model Inference, Latency & Quota Guardrails`) — 7 Panels
1. `ai_kpi_total_spans` (`metric`, `x=0, y=0, w=48, h=8`): Total AI hook and span events across station AI services.
2. `ai_kpi_active_conversations` (`metric`, `x=48, y=0, w=48, h=8`): Distinct `gen_ai_conversation_id` sessions.
3. `ai_kpi_lowest_quota_pct` (`metric`, `x=96, y=0, w=48, h=8`, `unit="percent"`): Lowest remaining subscription quota percentage across all tracked providers (`agent_quota_remaining_fraction`).
4. `ai_kpi_codex_ttft_ms` (`metric`, `x=144, y=0, w=48, h=8`): Mean Codex time-to-first-token (`codex_turn_ttft_duration_ms_sum`).
5. `ai_chart_quota_by_provider` (`bar`, `x=0, y=8, w=96, h=14`): Remaining subscription quota percentage by provider, model group, and window bucket.
6. `ai_chart_spans_over_time` (`area`, `x=96, y=8, w=96, h=14`): AI telemetry throughput over time by provider.
7. `ai_table_quota_guardrails` (`table`, `x=0, y=22, w=192, h=16`): Actionable Provider Quota Guardrails (`CRITICAL: Failover / Conserve` at `< 15%`, `WARN: Conserve Quota` at `< 30%`, `OK: Healthy Headroom`).

### Tab 2: `sre-tools` (`SRE: Tool Execution, Sub-Agents & Hook Pipeline`) — 7 Panels
1. `ai_chart_top_tools` (`bar`, `x=0, y=0, w=96, h=14`): Top 15 agent tool executions (`execute_tool *`, `tool.*`).
2. `ai_chart_hooks_by_agent` (`stacked`, `x=96, y=0, w=96, h=14`): Lifecycle hook events (`PreToolUse`, `PostToolUse`, `PreInvocation`, `PostInvocation`) by agent harness.
3. `ai_chart_tool_archetypes` (`donut`, `x=0, y=14, w=96, h=14`): Shell & tool binary archetype breakdown (`search_retrieval`, `build_test_verify`, `state_mutation`, `inspector_diff`, `filter_compressor`, `generic_exec`).
4. `ai_table_tool_performance` (`table`, `x=96, y=14, w=96, h=14`): Tool execution latency (`avg_duration_us`) and shell pipeline depth (`max_pipeline_depth`).
5. `ai_table_multiagent_and_services` (`table`, `x=0, y=28, w=192, h=14`): Multi-agent runtime and AI service span volume & latency (`agent-otel-bridge`, `codex-app-server`, `decider`, `mtn-signals-digest`).
6. `ai_chart_correlation_levels` (`stacked`, `x=0, y=42, w=96, h=14`): 3-Level Cross-Layer Telemetry Correlation Breakdown (`L1: Search Correlation`, `L2: OTel Span Link`, `L3: W3C Parent-Child Trace`).
7. `ai_table_producer_waits_approvals` (`table`, `x=96, y=42, w=96, h=14`): Producer-reported operation durations (`TRY_CAST(duration_ms AS DOUBLE)`), retry attempts, and approval decisions.

### Tab 3: `exec-ai` (`Executive: Model Portfolio, Workspace Allocation & AI Velocity`) — 5 Panels
1. `ai_exec_brief` (`markdown`, `x=0, y=0, w=192, h=8`): Executive Brief on subscription tier routing, quota protection, and workspace attribution.
2. `ai_exec_chart_model_share` (`donut`, `x=0, y=8, w=96, h=14`): AI model portfolio share by `gen_ai_request_model`.
3. `ai_exec_chart_workspace_allocation` (`stacked`, `x=96, y=8, w=96, h=14`): AI engineering effort by `workspace_project_name` and agent harness.
4. `ai_exec_table_governance` (`table`, `x=0, y=22, w=192, h=16`): Executive AI Portfolio Matrix (`workspace`, `agent_harness`, `provider`, `request_model`, `conversations`, `total_events`).
5. `ai_exec_table_token_cost_provenance` (`table`, `x=0, y=38, w=192, h=16`): Token Usage & Cost Provenance Matrix explicitly separating `Producer-Reported Tokens (Subscription Tier — Not Invoice Amount)`, `Subscription Quota Fraction Only (Token/Invoice Data Unavailable)`, and `Provider/Locally Estimated Cost Reported`.

---

## 6. Reliability Alert Specifications (`OpenRig Reliability` Folder)

All 5 alerts are provisioned into the `OpenRig Reliability` (`alerts`) folder as scheduled SQL alerts:

| Alert Name | Stream (`stream_type` / `stream_name`) | Condition & Threshold | Window / Freq / Silence | Severity | Purpose |
| --- | --- | --- | --- | --- | --- |
| `envctl-openrig-alert-exporter-failure` | `metrics` / `openrig_telemetry_failure` | `failure_count > 0` | `5m` / `5m` / `30m` | `P2` | Detects stage/category failures in `openrig-telemetry-exporter`. |
| `envctl-openrig-alert-coverage-gap` | `metrics` / `openrig_telemetry_coverage_gap` | `gap_count > 0` | `10m` / `5m` / `60m` | `P2` | Detects sequence or window coverage gaps in the queue transition ledger. |
| `envctl-openrig-alert-seat-human-gate-stall` | `metrics` / `openrig_seat_needs_input` | `blocked_seats >= 2` | `15m` / `5m` / `30m` | `P2` | Alerts when multiple OpenRig seats are stalled waiting on human input. |
| `envctl-openrig-alert-fleet-bottleneck-high` | `metrics` / `agent_fleet_bottleneck_ratio` | `bottleneck_ratio >= 0.5` (`50%` wire threshold) | `15m` / `5m` / `30m` | `P2` | Alerts when >= 50% of active seats in the fleet are bottlenecked. |
| `envctl-openrig-alert-ai-quota-low` | `metrics` / `agent_quota_remaining_fraction` | `min_quota_fraction < 0.15` (`15%` wire threshold) | `15m` / `5m` / `60m` | `P1` | Alerts before an AI provider subscription bucket hits rate-limit exhaustion. |
