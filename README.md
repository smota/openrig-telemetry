# openrig-telemetry

Standalone, zero-dependency OTLP/HTTP JSON telemetry exporter (`logs`, `metrics`, and `traces`) for [OpenRig](https://github.com/mvschwarz/openrig)'s finite queue-transition stream (`GET /api/telemetry/v1/queue-transitions`) and cached seat inventory (`GET /api/rigs`, `GET /api/rigs?archived=only`, `GET /api/rigs/:rigId/nodes`).

- **Repository**: [`smota/openrig-telemetry`](https://github.com/smota/openrig-telemetry) (initial full-capability review branch: `feat/coordination-telemetry`)
- **Upstream companion**: [`mvschwarz/openrig#2589`](https://github.com/mvschwarz/openrig/pull/2589) (`docs/reference/telemetry.md` finite-API contract)
- **Runtime**: Node.js `22` or `24` (`^22 || ^24`) on POSIX (Linux and macOS); **zero** runtime npm dependencies (`telemetry-otlp.mjs` uses only Node.js built-ins)
- **License**: Apache-2.0

---

## Scope and Non-Goals

`openrig-telemetry` is an independently maintained **OTLP exporter process**, not a storage backend, time-series database (TSDB), or dashboard implementation.

- **Read-only consumer**: Polls an OpenRig daemon over local HTTP and forwards allowlisted OTLP/HTTP JSON batches to an OpenTelemetry Collector or compatible OTLP/HTTP JSON receiver.
- **No OpenRig daemon or native-client changes**: Running, reconfiguring, upgrading, or removing this exporter never requires restarting the OpenRig daemon (`rig daemon`), altering `~/.openrig`, or touching running agent seats. Native-client telemetry (Claude Code and Codex provider/context usage, status lines, activity hooks, SSE streams, and MCP tools) remains completely unchanged.
- **No model, token, cost, or execution-trace claims**: The exporter emits coordination metadata only (queue state transitions, recorded queue-state durations, coverage gaps, exporter failure diagnostics, and sampled cached seat inventory). It never reads prompts, transcripts, tool arguments, model names, token counts, or billing data, never parses `.env` files, never synthesizes `gen_ai.*` attributes or `traceparent` parentage, and never joins queue transitions to current seat owners, actors, or tenures.

---

## Prerequisites and Compatibility Matrix

### System Requirements

| Requirement | Supported / Verified | Notes |
| --- | --- | --- |
| **Operating System** | Linux, macOS (POSIX) | Requires `0600` atomic file permissions and readable directory descriptors (`open(..., "r")` + `fsync`) on every physical ancestor directory of `--checkpoint` up to `/`. |
| **Node.js** | `22.x` or `24.x` (`^22 \|\| ^24`) | Matches OpenRig's supported Node.js engine range (`npm >= 10` for development checks/tests). |
| **OpenTelemetry Collector** | Tested with `otelcol` **`v0.164.0`** | Any OTLP/HTTP receiver accepting JSON (`Content-Type: application/json`) on `/v1/logs`, `/v1/metrics`, and `/v1/traces` and returning HTTP `200` JSON. |

### OpenRig Version Compatibility

The exporter consumes two distinct OpenRig daemon HTTP surfaces:
1. **Finite queue-transition telemetry (`GET /api/telemetry/v1/queue-transitions`)**: Introduced in OpenRig `v0.6.6` (commit `0ee9d100f`) with `schemaVersion: 1` and `stream: "queue-transitions"`. The finite telemetry engine (`packages/daemon/src/domain/finite-telemetry.ts`) and route (`packages/daemon/src/routes/telemetry.ts`) have zero diff between `v0.7.1` and `main` (`ff49a2c3622c15b4ebe2326ced71f52498f83510`).
2. **Cached rig and node inventory (`GET /api/rigs`, `GET /api/rigs?archived=only`, `GET /api/rigs/:rigId/nodes`)**: Consumes `sessionStatus` (`v0.1.12`), `activityState.display` (`v0.5.5`), `activityState.lastSwap` (`v0.5.5`), and **`arbitratedNeedsInput`** (introduced in commit `c3c8b7904`, first released in **`v0.7.1`**; unchanged in `packages/daemon/src/domain/node-inventory.ts`, `packages/daemon/src/routes/rigs.ts`, and `packages/daemon/src/routes/sessions.ts` through `main` `ff49a2c3622c15b4ebe2326ced71f52498f83510`).

| OpenRig Version | Finite Stream (`/api/telemetry/v1/queue-transitions`) | Inventory `arbitratedNeedsInput` | Compatibility Status | Verification Basis |
| --- | --- | --- | --- | --- |
| `<= v0.6.5` | Missing | Missing | **Incompatible** | Route absent in source |
| `v0.6.6` – `v0.7.0` | Present (`schemaVersion: 1`) | Missing | **Incompatible** | `v0.7.0` lacks `arbitratedNeedsInput` on node inventory |
| **`v0.7.1`** | Present (`schemaVersion: 1`) | Present | **Supported** | **Source-verified + live local native daemon runtime proof** (`rig --version` `0.7.1`) |
| **`v0.7.2` – `v0.7.8`** | Present (`schemaVersion: 1`) | Present | **Supported** | **Source-verified** across each release tag + synthetic wire-contract test suites |
| **`main` (`ff49a2c3622c15b4ebe2326ced71f52498f83510`)** | Present (`schemaVersion: 1`) | Present | **Supported** | **Source-verified + repository contract & Collector interop proof** |
| Future releases (`> v0.7.8`) | Check with capability probe below | Check with capability probe below | **Unverified until probed** | Core node inventory is an internal daemon projection, not a versioned finite-telemetry contract |

> **Important**: Do not assume unbounded compatibility with future OpenRig releases without probing. While `/api/telemetry/v1/queue-transitions` explicitly versions its envelope (`schemaVersion: 1`), `GET /api/rigs/:rigId/nodes` is an unversioned daemon inventory endpoint. Run the capability probe below before enabling the exporter after an OpenRig upgrade.

### Live Capability Probe

Before starting the exporter against a running OpenRig daemon, verify that the daemon exposes the required wire contract. Export `OPENRIG_SOURCE_URL` so the child Node.js process receives it in `process.env`; this read-only probe checks the finite stream and inventory endpoints without modifying daemon state (using `--input-type=module` so top-level `await` executes across the entire Node.js `22.x` range, including early Node 22 versions):

```sh
export OPENRIG_SOURCE_URL="http://127.0.0.1:7433"

node --input-type=module -e '
const base = new URL(process.env.OPENRIG_SOURCE_URL).origin;
const get = async (p) => {
  const r = await fetch(`${base}${p}`, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`${p} -> HTTP ${r.status}`);
  if (!(r.headers.get("content-type") || "").includes("application/json")) throw new Error(`${p} -> non-JSON content-type`);
  return r.json();
};
const t = await get("/api/telemetry/v1/queue-transitions?start=latest&limit=1");
if (
  t.schemaVersion !== 1 ||
  t.stream !== "queue-transitions" ||
  typeof t.observedAt !== "string" ||
  t.source?.sequenceSpaceId !== null ||
  typeof t.source?.bootEpoch !== "string" ||
  !t.source.bootEpoch ||
  !Array.isArray(t.rows) ||
  typeof t.page?.hasMore !== "boolean" ||
  typeof t.page?.nextCursor !== "string" ||
  t.coverage?.historyCompleteness !== "unknown" ||
  !Array.isArray(t.coverage?.gaps)
) {
  throw new Error("Incompatible /api/telemetry/v1/queue-transitions envelope");
}
const rigs = await get("/api/rigs");
const archived = await get("/api/rigs?archived=only");
if (!Array.isArray(rigs) || !Array.isArray(archived)) {
  throw new Error("Expected /api/rigs and /api/rigs?archived=only to return JSON arrays");
}
let nodeCheck = "no-active-rigs (inventory routes reachable; re-run with an active rig to verify node fields)";
const rigWithId = rigs.find((r) => r && typeof r.id === "string");
if (rigWithId) {
  const nodes = await get(`/api/rigs/${encodeURIComponent(rigWithId.id)}/nodes`);
  if (!Array.isArray(nodes)) throw new Error("Expected /api/rigs/:id/nodes to return a JSON array");
  const sample = nodes.find((n) => n.nodeKind === "agent") ?? nodes[0];
  if (sample && (!("arbitratedNeedsInput" in sample) || !("activityState" in sample) || !("sessionStatus" in sample))) {
    throw new Error("Node inventory is missing required fields (arbitratedNeedsInput/activityState/sessionStatus; requires OpenRig >= v0.7.1)");
  }
  nodeCheck = sample ? "verified-agent-node-fields" : "active-rig-with-0-nodes";
}
console.log(JSON.stringify({ ok: true, schemaVersion: t.schemaVersion, stream: t.stream, activeRigs: rigs.length, archivedRigs: archived.length, nodeCheck }));
'
```

---

## Installation and First Verification

No package is published to the npm registry. Install by cloning [`smota/openrig-telemetry`](https://github.com/smota/openrig-telemetry) and checking out the review branch `feat/coordination-telemetry` (or `main` once merged):

```sh
git clone https://github.com/smota/openrig-telemetry.git
cd openrig-telemetry
git checkout feat/coordination-telemetry

# Verify Node.js version (must be v22.x or v24.x)
node --version

# Verify CLI entrypoint (zero runtime dependencies; works before npm ci)
node telemetry-otlp.mjs --help
```

To run the repository's syntax check, TypeScript typecheck, unit test suite (30 portable tests), and real OpenTelemetry Collector interoperability suite:

```sh
npm ci
npm run check
npm test

# Run real Collector interoperability tests (requires otelcol v0.164.0 on PATH or via OTELCOL_BIN)
OTELCOL_BIN=/path/to/otelcol npm run test:interop
```

---

## OpenTelemetry Collector Setup (All 3 Pipelines)

By default (`--signals logs,metrics,traces`), the exporter sends OTLP/HTTP JSON payloads to three sibling endpoints derived from `--endpoint`:
- Logs: `<base>/v1/logs` (the exact URL passed to `--endpoint`)
- Metrics: `<base>/v1/metrics`
- Traces: `<base>/v1/traces`

Your OpenTelemetry Collector (tested with `otelcol` `v0.164.0`) **must configure all three pipelines** (`logs`, `metrics`, and `traces`) unless you restrict `--signals` when creating a new checkpoint.

Save the following minimal configuration as `otelcol-local.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318

processors:
  batch: {}

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

Start the Collector in a separate terminal:

```sh
otelcol --config=file:otelcol-local.yaml
```

---

## Quickstart and First `--once` Run

### 1. First Run at the High Watermark (`--start latest`, Default)

Set explicit variables for your OpenRig daemon URL, Collector `/v1/logs` endpoint, checkpoint file path, and logical namespace, then execute one bounded cycle with `--once`:

```sh
OPENRIG_SOURCE_URL="http://127.0.0.1:7433"
OTLP_LOGS_ENDPOINT="http://127.0.0.1:4318/v1/logs"
CHECKPOINT_PATH="/var/tmp/openrig-telemetry-checkpoint.json"
TELEMETRY_NAMESPACE="local-workstation"

node telemetry-otlp.mjs \
  --source "$OPENRIG_SOURCE_URL" \
  --endpoint "$OTLP_LOGS_ENDPOINT" \
  --checkpoint "$CHECKPOINT_PATH" \
  --namespace "$TELEMETRY_NAMESPACE" \
  --pseudonymize-seats \
  --once
```

**What to expect on a first `--start latest` run**:
- **Quiet transition logs and traces are normal**: `--start latest` opens the cursor at the daemon's current high watermark without reading historical transitions. Until a new queue transition occurs, the first run emits **zero** `openrig.queue.transition` logs and **zero** `openrig.queue.state` spans.
- **Expected initial records**:
  - One coverage gap log (`openrig.telemetry.gap` with `openrig.gap.code = "historical_transitions_not_read"`) and a matching `openrig.telemetry.coverage.gap` counter point (`1`), recording that history prior to checkpoint creation was intentionally not read.
  - Current seat inventory gauges (`openrig.seat.activity`, `openrig.seat.presence`, and evidence-backed `openrig.seat.needs_input`) for active rigs.
  - Initial seat `lastSwap.generation` baselines are persisted in the checkpoint without emitting `openrig.seat.tenure_change.observed` logs (first observation is a baseline, not a handover).

### 2. Draining Retained History (`--start retained`)

If you want a **new** checkpoint to read currently retained historical transitions from the daemon (up to `--limit` rows per page, default `128`), pass `--start retained` when creating the checkpoint:

```sh
OPENRIG_SOURCE_URL="http://127.0.0.1:7433"
OTLP_LOGS_ENDPOINT="http://127.0.0.1:4318/v1/logs"
CHECKPOINT_PATH="/var/tmp/openrig-telemetry-retained.json"
TELEMETRY_NAMESPACE="local-workstation"

node telemetry-otlp.mjs \
  --source "$OPENRIG_SOURCE_URL" \
  --endpoint "$OTLP_LOGS_ENDPOINT" \
  --checkpoint "$CHECKPOINT_PATH" \
  --namespace "$TELEMETRY_NAMESPACE" \
  --start retained \
  --pseudonymize-seats \
  --once
```

> **Note on observed counts**: `openrig.queue.transition.observed` and `openrig.queue.state.duration` count **records collected by this exporter checkpoint since `metricsStartUnixNano`**, never lifetime daemon totals and never "transitions that occurred today." When using `--start retained`, historical transitions increment the cumulative counters as the exporter reads them.

---

## CLI Reference and Configuration Binding

The exporter does **not** load `.env` files or environment variables for its runtime configuration. All options are supplied via explicit CLI flags (or programmatic `runQueueTransitionsExporter` / `exportQueueTransitionsOnce` options).

### CLI Flags (from `node telemetry-otlp.mjs --help`)

| Flag | Bound / Default | Description |
| --- | --- | --- |
| `--source <url>` | **Required** (`http:`/`https:`, no credentials/query/fragment; default daemon port is `7433`) | OpenRig daemon root URL (`http://127.0.0.1:7433`) or exact `/api/telemetry/v1/queue-transitions` URL. Seat inventory origin is derived automatically from `<origin>`. **Bound into checkpoint.** |
| `--endpoint <url>` | **Required** (`http:`/`https:`, path ending in `/v1/logs`, no credentials/query/fragment) | OTLP/HTTP JSON logs receiver URL. Sibling `/v1/metrics` and `/v1/traces` URLs are derived automatically. **Bound into checkpoint.** |
| `--checkpoint <path>` | **Required** (regular file, compact JSON `<= 4 MiB`, mode `0600`) | Path to durable checkpoint and pending-transaction state file. |
| `--namespace <name>` | **Required** (`1..128` ASCII identifier bytes `[A-Za-z0-9._:-]`) | Logical source identity written to `service.namespace` and `openrig.source.namespace`. **Bound into checkpoint.** |
| `--signals <list>` | Default: `logs,metrics,traces` | Comma-separated subset of `logs`, `metrics`, `traces`. **Bound into checkpoint.** |
| `--rig <rigId>` | Optional, repeatable (`1..32` immutable rig IDs, `1..256` `[A-Za-z0-9._:-]` bytes each) | Restrict seat inventory sampling to specific immutable rig IDs (default: all active rigs from `GET /api/rigs`). Deduplicated and sorted. **Bound into checkpoint.** |
| `--pseudonymize-seats` | Optional flag (default: `false`) | HMAC-SHA256 pseudonymize `openrig.seat.node_id` and `openrig.tenure.{prior,new}_generation` using the checkpoint's persistent 32-byte `hmacSalt`. **Bound into checkpoint.** |
| `--start <latest\|retained>` | Default: `latest` | Initial window mode when creating a new checkpoint. Keep identical on resume. **Bound into checkpoint.** |
| `--qitem <id>` | Optional (`1..256` printable non-control bytes) | Filter `/api/telemetry/v1/queue-transitions` to a single `qitemId`. **Bound into checkpoint.** |
| `--include-qitem-id` | Optional flag (default: `false`) | Opt in to exporting `openrig.qitem.id` on transition logs and `openrig.queue.state` spans (never exported on metrics). **Bound into checkpoint.** |
| `--limit <1..128>` | Default: `128` | Maximum source transition rows scanned per page. |
| `--once` | Optional flag (default: `false`) | Run one bounded queue page + configured seat-inventory sample (or replay one pending transaction) and exit. |
| `--poll-ms <10..60000>` | Default: `5000` | Idle queue poll interval in milliseconds in continuous mode when `page.hasMore` is `false`. |
| `--seat-poll-ms <10..600000>` | Default: `30000` | Seat inventory sampling cadence in milliseconds in continuous mode. |
| `--timeout-ms <50..30000>` | Default: `5000` | Per-request HTTP timeout for source `GET` (`<= 128 KiB` transition page, `<= 512 KiB` inventory) and OTLP `POST` (`<= 2 MiB` request, `<= 64 KiB` response). |
| `--retry-ms <1..10000>` | Default: `250` | Base exponential backoff delay in milliseconds for retryable OTLP failures. |
| `--max-retry-ms <10..60000>` | Default: `5000` (`>= --retry-ms`) | Maximum backoff delay and cap for receiver `Retry-After` headers. |
| `--max-retries <0..10>` | Default: `3` | Maximum retries per signal for HTTP `429`/`502`/`503`/`504` and network/timeout errors. |
| `--on-permanent-error <stop\|drop>` | Default: `stop` | `stop` preserves a blocked pending signal batch on non-retryable HTTP `4xx`/`5xx`; `drop` visibly discards a permanently rejected signal payload and records `otlp_dropped_permanent_rejection`. |
| `--help`, `-h` | Optional flag | Print CLI usage instructions and exit `0` without making network connections. |

### Checkpoint Configuration Binding (`configHash`)

When a checkpoint file is first created, the exporter computes a SHA-256 `configHash` over the bound identity fields:
- `stream` (`"queue-transitions"`)
- `--namespace`
- `--source` (normalized to `.../api/telemetry/v1/queue-transitions`)
- `--endpoint` (normalized to `.../v1/logs`)
- `--start` (`latest` or `retained`)
- `--qitem` (`null` or string)
- `--include-qitem-id` (`true` or `false`)
- `--signals` (canonical ordered subset of `["logs", "metrics", "traces"]`)
- `--rig` (sorted array of immutable rig IDs, if specified)
- `--pseudonymize-seats` (when `true`)

Every subsequent run against the same `--checkpoint` file must pass the **exact same bound flags**. Changing any bound flag (for example, changing `--signals` or adding `--rig` against an existing checkpoint) fails immediately with `TelemetryCheckpointError` (`checkpoint_config_mismatch`) and exit code `1` without mutating the checkpoint. Tuning flags (`--limit`, `--poll-ms`, `--seat-poll-ms`, `--timeout-ms`, `--retry-ms`, `--max-retry-ms`, `--max-retries`, `--on-permanent-error`, `--once`) may be adjusted across runs without changing `configHash`.

### Signal Selection Behavior (`--signals`)

- **`logs,metrics,traces` (default)**: Full capability. Tracks open queue intervals, samples seat inventory, and drains the failure ledger.
- **`logs` only (`--signals logs`)**: Exports `openrig.queue.transition`, `openrig.seat.tenure_change.observed`, `openrig.telemetry.gap`, and `openrig.telemetry.failure` logs. Because neither `metrics` nor `traces` is active, queue-state intervals (`checkpoint.queue.open`) are not tracked.
- **`metrics` only (`--signals metrics`)**: Exports all 7 metric families (including `openrig.queue.state.duration` and `openrig.telemetry.failure`) and drains the failure ledger via metric increments.
- **`traces` only (`--signals traces`)**: Exports completed `openrig.queue.state` root spans. Seat inventory is not polled, and failure-ledger entries remain preserved in `checkpoint.ledger` without being silently discarded.

---

## Continuous and Supervised Operation

### Single-Writer Rule and Safe User Execution

1. **One writer per `--checkpoint` file**: The exporter does not acquire cross-process file locks. Never run two exporter processes pointed at the same `--checkpoint` path simultaneously.
2. **Independent user process**: Run the exporter under an ordinary unprivileged user account. Never restart the OpenRig daemon (`rig daemon`) to manage the exporter.
3. **Graceful shutdown (`SIGINT` / `SIGTERM`)**: Sending `SIGINT` (`Ctrl+C`) or `SIGTERM` aborts any in-flight HTTP request or retry backoff timer immediately, leaves any persisted `pending` transaction intact on disk for the next startup without recording a false failure-ledger entry, and exits with code `0`.
4. **Supervisor exit semantics**: In continuous mode (without `--once`), the process loops automatically:
   - When `page.hasMore` is `true`, it fetches the next page immediately without sleeping.
   - When `page.hasMore` is `false`, it sleeps for `--poll-ms` (default `5000` ms) and samples seat inventory every `--seat-poll-ms` (default `30000` ms). Replaying a `pending` transaction on startup does not consume the seat-inventory timer, so fresh seat metrics are sampled on the first source page after recovery.
   - On a source error or after `--max-retries` OTLP retries are exhausted, the process persists the failure in `checkpoint.ledger` (and `checkpoint.pending` if exporting) and exits with code `1` so an external process supervisor can apply restart backoff.

### Foreground Execution (macOS and Linux)

Export the shared source, OTLP logs endpoint, checkpoint path, and namespace environment variables in your current shell before running the exporter. Subsequent recovery and upgrade commands in this guide assume these exported variables are present in your environment:

```sh
export OPENRIG_SOURCE_URL="http://127.0.0.1:7433"
export OTLP_LOGS_ENDPOINT="http://127.0.0.1:4318/v1/logs"
export CHECKPOINT_PATH="$HOME/.local/state/openrig-telemetry/checkpoint.json"
export TELEMETRY_NAMESPACE="workstation-main"

mkdir -p "$(dirname "$CHECKPOINT_PATH")"
chmod 0700 "$(dirname "$CHECKPOINT_PATH")"

node /path/to/openrig-telemetry/telemetry-otlp.mjs \
  --source "$OPENRIG_SOURCE_URL" \
  --endpoint "$OTLP_LOGS_ENDPOINT" \
  --checkpoint "$CHECKPOINT_PATH" \
  --namespace "$TELEMETRY_NAMESPACE" \
  --pseudonymize-seats
```

Stop foreground execution cleanly at any time with `Ctrl+C` (`SIGINT`).

### Optional Linux `systemd --user` Service

No system service installation is required. If you want background supervision on Linux without root privileges, you can optionally create a user unit at `~/.config/systemd/user/openrig-telemetry.service`.

> **Note on systemd PATH and Node.js**: User systemd services do not inherit shell environment managers (such as `mise`, `asdf`, or `nvm`) or custom shell `$PATH` definitions. Determine the absolute path to your desired Node.js binary by running `command -v node` in your shell, and replace `/usr/bin/node` with that absolute path in `ExecStart`. Also replace `/path/to/openrig-telemetry` with your actual repository checkout path:

```ini
[Unit]
Description=OpenRig OTLP Telemetry Exporter (user process)
After=network.target

[Service]
Type=simple
# Replace /usr/bin/node with the absolute path from $(command -v node)
# Replace /path/to/openrig-telemetry with the absolute path to your checkout
ExecStart=/usr/bin/node /path/to/openrig-telemetry/telemetry-otlp.mjs \
  --source http://127.0.0.1:7433 \
  --endpoint http://127.0.0.1:4318/v1/logs \
  --checkpoint %h/.local/state/openrig-telemetry/checkpoint.json \
  --namespace workstation-main \
  --pseudonymize-seats
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=15

[Install]
WantedBy=default.target
```

Ensure `~/.local/state/openrig-telemetry` exists (`mkdir -p ~/.local/state/openrig-telemetry && chmod 0700 ~/.local/state/openrig-telemetry`) before starting the unit with `systemctl --user daemon-reload && systemctl --user start openrig-telemetry.service`.

---

## Exported Signals, Schema, and Semantic Boundaries

### Resource Identity and Instrumentation Scopes

All three signals share a single resource envelope per batch:
- `service.name`: `"openrig-telemetry-exporter"`
- `service.namespace`: `<namespace>`
- `openrig.source.namespace`: `<namespace>`
- `service.instance.id`: Persistent UUID v4 generated when the checkpoint file is created.
- `openrig.source.host_id`: Included only when `envelope.source.hostId` is a safe token (`<= 256` bytes).

**Why `bootEpoch` is excluded from resource attributes**: An OpenRig daemon restart changes `source.bootEpoch`. Keeping `bootEpoch` out of the OTLP resource prevents daemon restarts from splitting cumulative metric counters/histograms and seat gauges into new resource series. Instead, `openrig.source.boot_epoch` is attached as a log record attribute on every emitted log record.

| Signal | OTLP Path | Instrumentation Scope | Version | Max Items / Batch | Max Batch Bytes |
| --- | --- | --- | --- | --- | --- |
| **Logs** | `.../v1/logs` | `openrig.telemetry.queue_transitions` | `"1"` | `448` log records | `2 MiB` (`2,097,152` B) |
| **Metrics** | `.../v1/metrics` | `openrig.telemetry.metrics` | `"1"` | `5,376` data points | `2 MiB` (`2,097,152` B) |
| **Traces** | `.../v1/traces` | `openrig.telemetry.traces` | `"1"` | `128` spans | `2 MiB` (`2,097,152` B) |

### 1. OTLP Logs (`/v1/logs`)

Each logs batch contains up to `448` records (`<= 128` transitions + `<= 33` source gaps/truncation summary + `<= 256` observed seat tenure changes + `<= 3` exporter interval gap notes + `<= 1` ledger-overflow record + `<= 27` drained failure-ledger records):

1. **`openrig.queue.transition` (`INFO`, `severityNumber: 9`)**:
   - `timeUnixNano`: Set **only** when `row.createdAt` is a valid UTC ISO-8601 timestamp (`...Z`). If `row.createdAt` is `null` or invalid, `timeUnixNano` is omitted (never fabricated) and a separate `invalid_timestamp` gap record is emitted.
   - `observedTimeUnixNano`: Set to the page's `observedAt` timestamp.
   - Attributes:
     - `openrig.record.kind = "queue_transition"`
     - `openrig.stream = "queue-transitions"`
     - `openrig.transition.id`: Decimal string (`1..9223372036854775807`), preserved without `Number` conversion.
     - `openrig.queue.state`: Allowlisted (`pending`, `in-progress`, `done`, `blocked`, `failed`, `denied`, `canceled`, `handed-off`).
     - `openrig.identity.provenance`: Allowlisted (`transport:v1`, `relay:v1`, `claimed:v1`, `origin-unknown:v1`, `principal:v1`).
     - `openrig.queue.closure_reason`: Allowlisted (`handed_off_to`, `blocked_on`, `denied`, `canceled`, `no-follow-on`, `escalation`, `superseded`) when present.
     - `openrig.qitem.id`: Included **only** when `--include-qitem-id` is set and `qitemId` is a safe printable token (`<= 256` bytes).
     - `openrig.coverage.status`: `retained_window`, `partial`, or `unavailable`.
     - `openrig.coverage.history_completeness = "unknown"`.
     - `openrig.source.boot_epoch`: Daemon boot epoch string.
     - `openrig.delivery.replayed`: `true` if re-sent from a persisted `pending` transaction, otherwise `false`.
2. **`openrig.seat.tenure_change.observed` (`INFO`, `severityNumber: 9`)**:
   - Derived solely from cached node inventory `activityState.lastSwap` (`{ generation, at } | null`).
   - **Baseline rule**: The first non-null `lastSwap.generation` seen for an immutable `nodeId` establishes its baseline in `checkpoint.seats.known` without emitting a log. A `null` `lastSwap` is unknown and neither clears a known baseline nor emits a log.
   - **Change rule**: When a seat with known baseline `generation A` is later observed with non-null `generation B` (`B !== A`), one log record is emitted with:
     - `openrig.record.kind = "seat_tenure_change"`
     - `openrig.seat.node_id` (HMAC-SHA256 digest when `--pseudonymize-seats` is enabled)
     - `openrig.seat.runtime` (when in the allowlist: `claude-code`, `codex`, `pi`, `omp`, `terminal`, `stub`)
     - `openrig.tenure.prior_generation` and `openrig.tenure.new_generation` (HMAC-SHA256 digests when `--pseudonymize-seats` is enabled)
     - `openrig.tenure.continuity = "intermediate_changes_unknown"` and `openrig.tenure.intermediate_changes_unknown = true`
     - `openrig.source.boot_epoch` and `openrig.delivery.replayed`
   - `lastSwap.generation` is an inventory swap token and is not claimed to equal finite-tenure `generationUuid`.
3. **`openrig.telemetry.gap` (`WARN`, `severityNumber: 13`)**:
   - Carries `observedTimeUnixNano` only (no event `timeUnixNano`).
   - Source gap codes (`openrig.gap.code`): `historical_transitions_not_read`, `boot_boundary_history_unverified`, `watermark_regression_history_unverified`, `before_retained_floor`, `retained_sequence_gap`, `conflicting_transition_id`, `oversized_field`, `invalid_type`, `invalid_timestamp`, `record_byte_limit`, `gap_details_truncated`.
   - Exporter-derived interval gap codes (`openrig.gap.code`): `page_intervals_suppressed`, `open_interval_evicted`, `invalid_interval_boundary`.
4. **`openrig.telemetry.failure` (`ERROR`, `severityNumber: 17`)**:
   - Drained from `checkpoint.ledger` (up to `27` entries per transaction, plus `1` `ledger`/`diagnostic_ledger_overflow` summary if `ledger.dropped > 0`) after a prior source, export, or ledger overflow failure.
   - Attributes: `openrig.record.kind = "failure"`, `openrig.stream = "queue-transitions"`, `openrig.failure.stage` (`source`, `export`, `ledger`), allowlisted `openrig.failure.category`, optional `openrig.signal` (`logs`, `metrics`, `traces`), integer `openrig.failure.items`, `openrig.source.boot_epoch`, `openrig.delivery.replayed`. Drained ledger entries set `timeUnixNano` to `entry.atNano` (`diagnostic_ledger_overflow` carries `observedTimeUnixNano` only).

### 2. OTLP Metrics (`/v1/metrics` — 7 Metric Families)

All integer metric points serialize `asInt` and histogram `count`/`bucketCounts` as **decimal strings** per OTLP/HTTP JSON rules. Cumulative sums and histograms use `aggregationTemporality = 2` (`AGGREGATION_TEMPORALITY_CUMULATIVE`) with the checkpoint's immutable `metricsStartUnixNano` as `startTimeUnixNano`.

| Metric Name | Instrument / Unit | Temporality | Attributes | Semantics & Guards |
| --- | --- | --- | --- | --- |
| `openrig.seat.activity` | Gauge, `{seat}` | Sampled (`0`/`1` one-hot) | `openrig.seat.node_id`, `openrig.seat.state` (`working`, `idle`, `needs-input`, `unknown`), optional `openrig.seat.runtime` | Consumes server-derived `activityState.display` directly. **Stopped-seat guard**: if projected `sessionStatus` is not `"running"` (e.g. `exited`, `stopped`, `detached`, `unknown`), stale `display: "idle"` is mapped to `"unknown"` so a stopped seat is never reported as `idle = 1`. |
| `openrig.seat.presence` | Gauge, `{seat}` | Sampled (`0`/`1` one-hot) | `openrig.seat.node_id`, `openrig.seat.status` (`running`, `exited`, `stopped`, `detached`, `unknown`), optional `openrig.seat.runtime` | Reflects projected inventory `sessionStatus` (never `lifecycleState`). Represents cached daemon inventory projection, **not** independent OS process verification. |
| `openrig.seat.needs_input` | Gauge, `{request}` | Sampled count | `openrig.seat.node_id`, optional `openrig.seat.runtime` | **Authoritative vs. unsupported zero**: Emits `count > 0` when `arbitratedNeedsInput === true`, and `"0"` when `arbitratedNeedsInput === false`. When `arbitratedNeedsInput` is `null` or `undefined` (unsupported runtime or unarbitrated default `count: 0`), **no data point is emitted** so unsupported seats never report a false zero. If a seat that previously exported an observed value later reports `arbitratedNeedsInput: null` while still present, the exporter emits one OTLP `flags = 1` (`NoRecordedValue`) staleness marker on `openrig.seat.needs_input`. |
| `openrig.queue.transition.observed` | Monotonic Sum, `{transition}` | Cumulative (`2`) | `openrig.queue.state`, optional `openrig.queue.closure_reason` | Total transition rows collected by this checkpoint instance. |
| `openrig.queue.state.duration` | Histogram, `s` | Cumulative (`2`) | `openrig.queue.state` (`pending`, `in-progress`, `blocked`), `openrig.queue.departure_state` | Completed recorded queue-state intervals in seconds. Explicit bucket bounds: `[1, 5, 15, 60, 300, 900, 3600, 14400, 86400, 604800]`. |
| `openrig.telemetry.coverage.gap` | Monotonic Sum, `{gap}` | Cumulative (`2`) | `openrig.gap.code` | Cumulative count of source-reported and exporter-derived coverage gaps. |
| `openrig.telemetry.failure` | Monotonic Sum, `{failure}` | Cumulative (`2`) | `openrig.failure.stage`, `openrig.failure.category` | Cumulative count of source, export, and ledger overflow failures drained from the durable ledger. |

### 3. OTLP Traces (`/v1/traces` — Root Queue-State Spans)

- **Span name**: `openrig.queue.state` (`kind: 1` / `SPAN_KIND_INTERNAL`).
- **Root spans only**: `parentSpanId` is always omitted, `links` is empty, and no `gen_ai.*` attributes are emitted. `traceId` (32 hex chars) and `spanId` (16 hex chars) are generated once when the transaction is built and persisted in `checkpoint.pending` so retries replay the exact same IDs.
- **Attributes**: `openrig.queue.state` (`pending`, `in-progress`, or `blocked`), `openrig.queue.departure_state`, decimal string `openrig.transition.enter_id`, decimal string `openrig.transition.depart_id`, and opt-in `openrig.qitem.id` (when `--include-qitem-id` is enabled).
- **What spans represent**: Recorded time between two observed queue-transition rows for the same `qitemId` in an uninterrupted window—**not** agent thinking time, tool execution time, or seat occupancy.

---

## Upstream API Guarantees vs. Exporter Mapping Limits

To keep responsibilities clear, distinguish what OpenRig's finite telemetry API guarantees from how this exporter maps and bounds state:

| Area | OpenRig Finite API Guarantee (`/api/telemetry/v1/queue-transitions`) | `openrig-telemetry` Exporter Mapping & Bound |
| --- | --- | --- |
| **Pagination & Windowing** | Returns at most `128` rows (`<= 128 KiB` response, `<= 4 KiB` per record) in ascending `transitionId` order within a fixed high-watermark window; `coverage.historyCompleteness` is always `"unknown"`. | Polls one page at a time; advances `checkpoint.cursor` only after all enabled OTLP signals resolve. Empty filtered pages advance `cursor` without sending empty OTLP batches. |
| **Queue Intervals (`openrig.queue.state`)** | Provides raw transition rows (`transitionId`, `qitemId`, `state`, `createdAt`) and page coverage gaps. Makes no duration calculation. | Tracks at most **`512` open intervals** (`pending`, `in-progress`, `blocked`) keyed by SHA-256 hash of `qitemId`. Repeated same-state rows preserve the initial entry timestamp. When full, evicts the entry with the lowest `enterId` and records `open_interval_evicted`. |
| **Gap Suppression** | Reports row-local gaps (`oversized_field`, `invalid_type`, `invalid_timestamp`) and window/sequence gaps (`retained_sequence_gap`, `before_retained_floor`, `conflicting_transition_id`, `record_byte_limit`, `gap_details_truncated`, `boot_boundary_history_unverified`, `watermark_regression_history_unverified`). | Row-local gaps invalidate only that item's open interval (`invalid_interval_boundary`). Any sequence/window gap on a page **conservatively suppresses the entire page** and clears all open intervals (`page_intervals_suppressed` if any interval was discarded) so no duration spans missing records. |
| **Seat Inventory & Departures** | `GET /api/rigs`, `GET /api/rigs?archived=only`, and `GET /api/rigs/:rigId/nodes` return current daemon projections. | Queries cached inventory only (**never** passes `?full=true` or `?refresh=true`); caps at **`32` rigs**, **`64` agent seats per rig**, and **`256` known seats total**. Emits one-shot OTLP `flags = 1` (`NoRecordedValue`) staleness markers only after a complete `200 OK` node list omits a seat or `GET /api/rigs?archived=only` confirms rig archive. |
| **Daemon Restart (`bootEpoch`)** | Invalidates prior cursor on boot change or watermark regression, returning `coverage.status = "unavailable"`, a named gap, `rows: []`, and `page.restartCursor`. | Exports the discontinuity gap first, clears open intervals, and adopts `restartCursor` at the new high watermark (skipping the unverified missed interval while keeping resource `service.instance.id` and `metricsStartUnixNano` stable). |

---

## Discontinuities, Retries, Partial Success, and Error Recovery

### Two-Phase Atomic Checkpoint Durability and Preflight

Before sending any network request for a transaction, `telemetry-otlp.mjs` writes `checkpoint.pending` (containing the proposed `nextCursor`, proposed `derived.queue` and `derived.seats` state, and the exact OTLP JSON payloads for each active signal) to disk (`schemaVersion: 2`, `<= 4 MiB`, mode `0600`):
1. **Preflight and physical directory validation**: Resolves the canonical physical path and ancestry of `--checkpoint`. Every physical ancestor directory from the root directory (`/`) down to the checkpoint directory must grant read and traverse permission and support `openSync(dir, "r")` and `fsyncSync(fd)`. If any ancestor directory cannot be opened for reading or synced, preflight fails closed before attempting write or export operations.
2. **Atomic staging**: Loops `writeSync` until all bytes are written to a sibling `0600` temporary file (failing visibly with `checkpoint_short_write` on zero progress while leaving the previous checkpoint untouched).
3. **Safe cleanup of orphaned temporary files**: Ordinary write failures remove the owned temporary file before rename. A crash or forced termination can leave a `.tmp.<pid>.<rand>` file. After stopping all writers, you may remove only orphaned temporary files for this checkpoint; keep the checkpoint and backups.
4. **Rename and ancestor directory sync**: Calls `fsyncSync` on the temporary file, atomically renames it over `--checkpoint`, and calls `fsyncSync` on every physical ancestor directory from `/` down to the checkpoint directory (one temporary-file sync plus a full ancestor sync in both preflight and post-rename phases). If directory synchronization fails, the write operation fails even if the rename already occurred.
5. **Per-signal persistence**: Persists `attempted = true` before each signal's HTTP `POST` and persists each signal's resolved outcome (`exported`, `partial_success`, or `dropped_permanent_rejection`) **before** starting the next signal.
6. **Final adoption**: Adopts `nextCursor`, `derived.queue`, `derived.seats`, and drains reported `ledger` entries **only after every active signal in the transaction has resolved**.

### Transient Failures and Per-Signal Replay

- **Retryable conditions**: HTTP `429`, `502`, `503`, `504`, socket errors, and request timeouts retry up to `--max-retries` (default `3`) with exponential backoff (`--retry-ms` to `--max-retry-ms`), honoring receiver `Retry-After` headers (seconds or HTTP-date) capped at `--max-retry-ms`.
- **Per-signal replay**: While `checkpoint.pending` is non-null, source polling pauses. On the next run, the exporter replays **only** the signals whose `outcome` is still `null`. Already-accepted signals (e.g. `logs` succeeded before `metrics` timed out) are **never** re-sent. Replayed log records set `openrig.delivery.replayed = true`, and replayed traces reuse the persisted `traceId`/`spanId`.

### Protocol `partialSuccess` (`HTTP 200`)

When the OTLP receiver returns HTTP `200` with a JSON `partialSuccess` object (`rejectedLogRecords`, `rejectedDataPoints`, or `rejectedSpans` `> 0`, or a non-empty `errorMessage`):
- The exporter marks the signal resolved as `partial_success` and records an allowlisted `export`/`otlp_partial_success` entry in the durable failure ledger (never echoing raw `errorMessage` text).
- **No whole-batch retry**: Per OTLP specification, a `partialSuccess` batch is not re-sent (re-sending would duplicate accepted records).
- **One-shot staleness markers**: If a `/v1/metrics` batch containing `flags = 1` (`NoRecordedValue`) seat departure markers resolves with `partial_success`, those staleness markers are not retried because `rejectedDataPoints` does not identify which points were rejected.

### Permanent Rejections (`--on-permanent-error stop` vs. `drop`)

All other HTTP `4xx`/`5xx` status codes (`400..599` except `429`/`502`/`503`/`504`—for example HTTP `400 Bad Request` or HTTP `404 Not Found` when a Collector pipeline is not enabled) are non-retryable permanent rejections (`otlp_permanent_rejection`):

- **Default (`--on-permanent-error stop`)**: The exporter persists `pending.signals.<signal>.blocked = { code: "otlp_permanent_rejection", status }`, records `export`/`otlp_permanent_rejection` in the failure ledger, and exits `1`.
- **Fixing the Collector alone does not unblock an already-persisted blocked batch**: While `pending.signals.<signal>.blocked` is non-null on disk, subsequent runs with default `--on-permanent-error stop` check `entry.blocked !== null` before making any network request and immediately fail with `otlp_permanent_rejection` (`Pending <signal> batch is blocked by prior non-retryable otlp_permanent_rejection and will not be automatically resubmitted`, exit code `1`). There is no CLI flag to reset `blocked` in place, and you must not manually edit the checkpoint JSON file (nor can you change `--signals` on an existing checkpoint, which fails with `checkpoint_config_mismatch`).
- **Executable options to recover from a persisted `otlp_permanent_rejection` block**:
  1. **Fix the Collector, then discard the blocked signal batch once with `--on-permanent-error drop --once`**:
     First enable/fix the affected Collector pipeline and restart the Collector so any remaining unattempted signals in `checkpoint.pending` and future pages succeed. Then run one cycle with `--on-permanent-error drop --once` and the **exact same bound flags**:
     ```sh
     node telemetry-otlp.mjs \
       --source "$OPENRIG_SOURCE_URL" \
       --endpoint "$OTLP_LOGS_ENDPOINT" \
       --checkpoint "$CHECKPOINT_PATH" \
       --namespace "$TELEMETRY_NAMESPACE" \
       --pseudonymize-seats \
       --on-permanent-error drop \
       --once
     ```
     Because `pending.signals.<signal>.blocked` is already set, this command does **not** re-send the blocked signal batch: it marks that blocked signal resolved as `dropped_permanent_rejection` (`droppedCount = itemCount`), records `export`/`otlp_dropped_permanent_rejection` in `checkpoint.ledger`, preserves any signals that already succeeded (`outcome !== null`), sends any remaining unattempted signals (`outcome === null && blocked === null`) in the transaction, adopts `nextCursor`, and clears `checkpoint.pending`. Afterwards, resume normal operation with default `--on-permanent-error stop`.
  2. **Fix the Collector, then restore a clean pre-failure checkpoint backup**: If you have a checkpoint backup taken while `pending` was `null` before the rejected transaction started, stop the exporter, fix the Collector, copy that backup over `"$CHECKPOINT_PATH"`, and run the exporter. See [Backup, Restore, and Rollback Consequences](#backup-restore-and-rollback-consequences) for duplicate and metric-rewind trade-offs if other signals in the transaction had already been accepted before the block.
  3. **Fix the Collector, then start with a new `--checkpoint` file path**: Point `--checkpoint` at a new path (or delete the blocked checkpoint file) with `--start latest` or `--start retained`. See [Consequences of Starting a New Checkpoint File](#consequences-of-starting-a-new-checkpoint-file).

### Response Uncertainty, Redirects, and Nonconforming Responses (Never Auto-Dropped)

- **HTTP Redirects (`3xx`) Not Followed**: The exporter does not follow HTTP redirects (`301`, `302`, `307`, `308`). If a gateway or reverse proxy redirects HTTP traffic (for example, redirecting HTTP to HTTPS), the response is treated as `otlp_unexpected_status` and blocks. Configure the exporter's `--endpoint` with the direct final URL.
- **Response Uncertainty**: If the receiver returns a nonconforming response after receiving a request—unexpected `1xx`/`2xx`/`3xx` status other than `200` (`otlp_unexpected_status`), `200` without `Content-Type: application/json` (`otlp_invalid_content_type`), malformed `200` JSON or invalid `partialSuccess` fields (`otlp_invalid_response`), or a response body exceeding `64 KiB` (`otlp_response_too_large`)—delivery state is uncertain because the receiver already received the HTTP `POST` request. The exporter persists `pending.signals.<signal>.blocked = { code, status }` and **refuses to drop or re-send it even if `--on-permanent-error drop` is passed**. Fix the receiver first, then either restore a clean pre-failure checkpoint backup or start with a new `--checkpoint` file path (never manually edit the checkpoint JSON), noting that any batch the receiver actually ingested during the nonconforming response may be duplicated if re-exported.

### Missing Rigs vs. Archived Rigs vs. Inventory Failures

- **Confirmed rig archive**: When a rig disappears from `GET /api/rigs` and appears in `GET /api/rigs?archived=only`, its known seats emit one final batch of `flags = 1` (`NoRecordedValue`) staleness points and are removed from `checkpoint.seats.known` (never logged as a deletion).
- **Missing rig (`configured_rig_not_found`)**: If a configured `--rig` or a rig with previously known seats is absent from **both** `GET /api/rigs` and `GET /api/rigs?archived=only`, the exporter does not infer deletion or archive: it retains those seats' baselines in `checkpoint.seats.known`, emits no false `flags = 1` markers or idle samples, continues sampling other healthy rigs, and records `source`/`configured_rig_not_found` in the failure ledger. If the rig was permanently removed without archiving, start a new `--checkpoint` file to clear the retained seat baseline.
- **Inventory HTTP/bound errors (`source_http_error`, `invalid_inventory_response`, `inventory_limit_exceeded`)**: Recorded in the failure ledger while preserving prior `checkpoint.seats.known` baselines unchanged.

---

## Checkpoints, Backups, Upgrades, Rollback, and Removal

### Checkpoint Schema (`schemaVersion: 2`)

The checkpoint file (`<= 4 MiB`, mode `0600`) stores:
- `schemaVersion: 2`, `configHash`, `config` (`stream: "queue-transitions"`, `namespace`, `sourceUrl`, `endpointUrl`, `start`, `qitemId`, `includeQitemId`, `signals`, `rigs`, `pseudonymizeSeats`), `updatedAt`
- `instanceId` (UUID v4), `metricsStartUnixNano`, `hmacSalt` (64 hex chars / 32 random bytes)
- `cursor`, `source` (`hostId`, `bootEpoch`, `sequenceSpaceId: null`), `coverage`
- `queue` (`open` with up to `512` interval entries, and `agg` containing `transitions`, `hist`, `gaps`, `failures`, and `lastNano`)
- `seats` (`known` with up to `256` seat entries, and `lastNano`)
- `ledger` (`entries` with up to `32` pending failure records, `dropped` overflow count, and `nextSeq`)
- `pending` (`null` when idle, or the in-flight multi-signal transaction)

Any `schemaVersion: 1`, malformed, truncated, or oversized (`> 4 MiB`) checkpoint fails visibly with `TelemetryCheckpointError` and never silently resets.

### Backing Up a Checkpoint

Always stop the exporter process (`SIGTERM` or `SIGINT`, exit code `0`) before copying the checkpoint file so no file write or HTTP request is concurrently in flight:

```sh
cp -p "$CHECKPOINT_PATH" "${CHECKPOINT_PATH}.bak.$(date -u +%Y%m%dT%H%M%SZ)"
```

### Safe Upgrade and Rollback Procedure

1. **Stop the running exporter** (`Ctrl+C` in foreground, or `systemctl --user stop openrig-telemetry.service`) and confirm it exited cleanly.
2. **Back up the checkpoint file**:
   ```sh
   cp -p "$CHECKPOINT_PATH" "${CHECKPOINT_PATH}.pre-upgrade.bak"
   ```
3. **Update the repository checkout**:
   ```sh
   cd /path/to/openrig-telemetry
   git fetch origin
   git checkout feat/coordination-telemetry && git pull --ff-only
   node telemetry-otlp.mjs --help
   ```
4. **Run a single `--once` verification step** with your existing flags:
   ```sh
   node telemetry-otlp.mjs \
     --source "$OPENRIG_SOURCE_URL" \
     --endpoint "$OTLP_LOGS_ENDPOINT" \
     --checkpoint "$CHECKPOINT_PATH" \
     --namespace "$TELEMETRY_NAMESPACE" \
     --pseudonymize-seats \
     --once
   ```
5. **Resume continuous operation**.
6. **Rollback**:
   - **Code-only rollback (recommended when the current checkpoint is healthy and `pending: null`)**: Stop the exporter, check out the previous Git commit (`git checkout <prev-sha>`), and restart against the **current** `"$CHECKPOINT_PATH"` without overwriting it with `${CHECKPOINT_PATH}.pre-upgrade.bak`. Keeping the current checkpoint preserves cursor progress and avoids re-exporting batches sent during Step 4/5.
   - **Checkpoint backup restore (`cp -p "${CHECKPOINT_PATH}.pre-upgrade.bak" "$CHECKPOINT_PATH"`)**: Restore the `.pre-upgrade.bak` file only if the checkpoint file became corrupted or entered an unrecoverable blocked state after the backup was taken, and review the consequences below before restarting.

### Backup, Restore, and Rollback Consequences

Because the exporter operates over HTTP with two-phase local checkpointing (`at-least-once` delivery across uncertain network boundaries; no exactly-once guarantee), backing up or restoring a checkpoint has specific consequences you must account for:

1. **Duplicate exports when restoring an older backup**: If the exporter exported any pages or signals after the backup was taken (for example, during Step 4's `--once` verification run, subsequent continuous polling, or an earlier signal such as `logs` that succeeded before `metrics` or `traces` blocked in the same transaction), overwriting `--checkpoint` with the older backup rewinds `checkpoint.cursor`, `checkpoint.queue`, and `checkpoint.seats`. On restart, the exporter will fetch and export those transition pages and tenure changes **again** from the backup's cursor (`openrig.delivery.replayed = false`, with newly generated `traceId`/`spanId` values for any `openrig.queue.state` spans rebuilt from source rather than replayed from `checkpoint.pending`).
2. **Cumulative metric rewind within the same exporter instance**: A restored backup retains the same `service.instance.id` and `metricsStartUnixNano` as the post-backup runs, but carries older (smaller) cumulative `queue.agg` counter/histogram values and an older `lastNano` timestamp floor. Downstream TSDBs (such as Prometheus) may interpret the lower cumulative totals after rollback as a counter reset or reject/duplicate samples at re-emitted timestamps.
3. **Uncertain acknowledgments on interrupted or blocked `pending` transactions (duplicate attempts possible)**:
   - Before each signal `POST`, the exporter persists `pending.signals.<signal>.attempted = true`. If the exporter is stopped (`SIGINT`/`SIGTERM` exits `0` via `exporter_aborted`), crashes, or times out while an HTTP `POST` is in flight—after `attempted = true` is on disk but before a `200` response is persisted—the exporter cannot know whether the OTLP receiver ingested that request before the connection closed.
   - A backup taken while `checkpoint.pending !== null` preserves that in-flight transaction. When resumed, the exporter replays any signal whose `outcome` is still `null` (setting `openrig.delivery.replayed = true` on logs and reusing persisted `traceId`/`spanId` on spans), which will duplicate that signal batch if the receiver had already ingested the interrupted attempt. Because delivery across network hops is at-least-once, duplicate attempts are possible and no exactly-once processing exists.
   - Likewise, if a transaction stopped on a response-uncertainty block (`otlp_unexpected_status`, `otlp_invalid_content_type`, `otlp_invalid_response`, `otlp_response_too_large`), the receiver already received the HTTP `POST`; restoring an earlier backup or starting a new checkpoint with `--start retained` can re-send records the receiver already accepted.

### Consequences of Starting a New Checkpoint File

Pointing `--checkpoint` at a new file path (or deleting the old checkpoint file) starts a distinct exporter instance:
- Generates a **new `service.instance.id`** UUID and a **new `metricsStartUnixNano`**, starting a new cumulative counter and histogram epoch.
- Generates a **new `hmacSalt`**, which changes all `--pseudonymize-seats` digests for `openrig.seat.node_id` and tenure generation attributes.
- Starts with an **empty open-interval map** (in-progress queue intervals opened before the new checkpoint will not emit durations when they close) and a **fresh seat-tenure baseline** (the first sight of each seat establishes a baseline without emitting a tenure-change log).
- Either skips historical transitions with `historical_transitions_not_read` (`--start latest`) or re-reads retained transitions (`--start retained`, causing overlap with previously exported transition logs/spans).

### Disabling and Removing the Exporter

Because `openrig-telemetry` is completely external to OpenRig:
1. Stop the exporter process (and if you created an optional user service, run `systemctl --user disable --now openrig-telemetry.service` and remove `~/.config/systemd/user/openrig-telemetry.service`).
2. Remove the checkpoint file (`rm -f "$CHECKPOINT_PATH"`) and repository checkout (`rm -rf /path/to/openrig-telemetry`).
3. **Do not restart or reconfigure OpenRig**: `rig daemon` and all running rigs continue operating normally.

---

## Privacy, Freshness, and Backend TTL

### Privacy and Redaction Guarantees

- **Never exported or persisted in checkpoints**: Queue item bodies/summaries, transition notes, `actorSession`, `closureTarget`, raw `originalTimestamp`, prompts, transcripts, file paths (`cwd`), environment variables, model names (`model`), `needsInput.reason`, HTTP response bodies, or receiver `partialSuccess.errorMessage` strings.
- **Seat and tenure pseudonymization (`--pseudonymize-seats`)**: Computes `HMAC-SHA256(hmacSalt, value).slice(0, 32)` (`128-bit` hex digest) for `openrig.seat.node_id`, `openrig.tenure.prior_generation`, and `openrig.tenure.new_generation`. Because `hmacSalt` is stored inside the `0600` checkpoint file, seat series stay consistent across restarts without exposing raw `nodeId` or generation strings to the OTLP backend.
- **Queue item ID isolation**: Internal open intervals hash `qitemId` with SHA-256 (`128-bit` hex key) in the checkpoint. `openrig.qitem.id` is omitted from all exports unless `--include-qitem-id` is explicitly passed, and is **never** placed on metric attributes.

### Sample Timestamps, Staleness Markers, and Backend TTL

- **No backfilled history**: Seat gauges (`openrig.seat.activity`, `openrig.seat.presence`, `openrig.seat.needs_input`) carry the sample cycle's `timeUnixNano` (`observedTimeUnixNano` on `/api/telemetry/v1/queue-transitions`, enforced monotonic per checkpoint). First observation of a seat never backfills prior idle or running points.
- **OTLP `flags = 1` (`NoRecordedValue`)**: When a seat is confirmed departed (omitted from a complete `200 OK` `GET /api/rigs/:rigId/nodes` response or its rig is confirmed in `GET /api/rigs?archived=only`), the exporter emits one final batch of gauge points with `flags = 1` and no `asInt` value. Verified with OpenTelemetry Collector `v0.164.0`.
- **Configure a backend freshness/staleness TTL**: Downstream TSDBs and dashboards (such as Prometheus, Mimir, VictoriaMetrics, or Grafana) **must** configure a series freshness/staleness TTL (for example, a ~5-minute lookback window when using the default `30s` `--seat-poll-ms`). One-shot `flags = 1` departure points are not emitted while the exporter is stopped or if a rig is unreachable (`configured_rig_not_found`), and are not retried if a `/v1/metrics` batch resolves with `partialSuccess` or `--on-permanent-error drop`.
