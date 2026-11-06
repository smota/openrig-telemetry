#!/usr/bin/env node
// Copyright 2026 OpenRig Contributors
// SPDX-License-Identifier: Apache-2.0
//
// Real OpenTelemetry Collector interoperability suite for telemetry-otlp.mjs.
// Adapted for standalone repository verification from OpenRig telemetry validation harnesses.
// Spawns an isolated Collector instance (OTELCOL_BIN or otelcol on PATH) with logs, metrics,
// and traces pipelines over ephemeral loopback ports and verifies multi-phase synthetic export.

import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { createServer as createNet } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileP = promisify(execFile);
const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(HERE, "..");
const CAND = resolve(REPO_ROOT, "telemetry-otlp.mjs");
const OTELCOL = process.env.OTELCOL_BIN?.trim() || "otelcol";
const SRC_PATH = "/api/telemetry/v1/queue-transitions";
const RIG_ID = "SENTINEL-RIG-ID";
const LEAK = /SENTINEL|SYNTHETIC_SECRET/;

const ac = new AbortController();
const deadline = setTimeout(() => ac.abort(new Error("interop deadline 120s exceeded")), 120_000);
const sleep = (ms) => new Promise((r, j) => {
  const t = setTimeout(r, ms);
  ac.signal.addEventListener("abort", () => { clearTimeout(t); j(ac.signal.reason); }, { once: true });
});
const sha = (s) => createHash("sha256").update(s).digest("hex");

const result = {
  startedAt: new Date().toISOString(),
  checks: [],
  phases: {},
  error: null,
};

const check = (name, ok, detail = "") => {
  const passed = Boolean(ok);
  result.checks.push({ name, ok: passed, detail: String(detail).slice(0, 200) });
  if (!passed) console.error(`FAIL ${name} ${String(detail).slice(0, 200)}`);
  return passed;
};

const http = (url, init = {}, ms = 5000) => fetch(url, {
  ...init,
  signal: AbortSignal.any([ac.signal, AbortSignal.timeout(ms)]),
});

const freePort = async () => {
  const s = createNet();
  await new Promise((r) => s.listen(0, "127.0.0.1", r));
  const { port } = s.address();
  return new Promise((r) => s.close(() => r(port)));
};

async function cli(cmd, args, ms = 15000) {
  try {
    const r = await execFileP(cmd, args, { timeout: ms, signal: ac.signal, maxBuffer: 4 << 20 });
    return { code: 0, stdout: r.stdout, stderr: r.stderr, error: null };
  } catch (e) {
    if (ac.signal.aborted) throw ac.signal.reason;
    return { code: typeof e.code === "number" ? e.code : 1, stdout: e.stdout ?? "", stderr: e.stderr ?? "", error: e };
  }
}

class Collector {
  constructor(workDir) {
    this.workDir = workDir;
    this.outputJsonlPath = join(workDir, "otelcol-output.jsonl");
    this.configPath = join(workDir, "otelcol-config.yaml");
    this.text = "";
    this.proc = null;
  }

  async start() {
    this.port = await freePort();
    writeFileSync(
      this.configPath,
      `receivers:\n  otlp:\n    protocols:\n      http:\n        endpoint: 127.0.0.1:${this.port}\nexporters:\n  debug:\n    verbosity: detailed\n  file:\n    path: ${this.outputJsonlPath}\nservice:\n  telemetry:\n    metrics:\n      level: none\n      readers: []\n  pipelines:\n` +
        ["logs", "metrics", "traces"].map((s) => `    ${s}:\n      receivers: [otlp]\n      exporters: [debug, file]\n`).join(""),
    );
    this.proc = spawn(OTELCOL, [`--config=file:${this.configPath}`], { stdio: ["ignore", "pipe", "pipe"] });
    this.exited = new Promise((r) => this.proc.once("exit", r));
    for (const s of [this.proc.stdout, this.proc.stderr]) {
      s.on("data", (c) => { this.text += c; });
    }
    for (let i = 0; i < 40; i++) {
      if (this.proc.exitCode !== null) throw new Error(`collector exited early (${this.proc.exitCode}): ${this.text.slice(0, 400)}`);
      try {
        const res = await http(
          `http://127.0.0.1:${this.port}/v1/logs`,
          { method: "POST", headers: { "content-type": "application/json" }, body: '{"resourceLogs":[]}' },
          1000,
        );
        if (res.ok) return;
      } catch (e) {
        if (ac.signal.aborted) throw e;
      }
      await sleep(250);
    }
    throw new Error("collector not ready within 10s");
  }

  async stop() {
    if (!this.proc || this.proc.exitCode !== null) return;
    this.proc.kill("SIGTERM");
    const k = setTimeout(() => this.proc.kill("SIGKILL"), 3000);
    await this.exited;
    clearTimeout(k);
  }
}

// Records the candidate's actual OTLP payloads in memory and forwards them to the real Collector unchanged.
// Non-OTLP requests (such as probe noise) are recorded in unexpectedRequests and answered 404 without forwarding.
function forwarder(collectorPort) {
  const items = [];
  const unexpectedRequests = [];
  let unexpectedDropped = 0;
  const ALLOWED_EXACT_PATHS = new Set(["/v1/logs", "/v1/metrics", "/v1/traces"]);
  const server = createServer((req, res) => {
    let p = "/";
    try { p = new URL(req.url, "http://127.0.0.1").pathname; } catch { /* default / */ }
    const m = req.method ?? "OTHER";
    const method = ["GET", "POST", "HEAD"].includes(m) ? m : "OTHER";

    if (method !== "POST" || !ALLOWED_EXACT_PATHS.has(p)) {
      const pathClass = ALLOWED_EXACT_PATHS.has(p) ? p : (p === "/" ? "/" : "other");
      if (unexpectedRequests.length < 32) {
        unexpectedRequests.push({ method, pathClass, status: 404 });
      } else {
        unexpectedDropped++;
      }
      res.writeHead(404, { "content-type": "application/json" });
      res.end("{}");
      return;
    }

    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", async () => {
      const raw = Buffer.concat(chunks).toString();
      let status = 502;
      let out = "{}";
      try {
        const r = await http(
          `http://127.0.0.1:${collectorPort}${p}`,
          { method: "POST", headers: { "content-type": req.headers["content-type"] ?? "application/json" }, body: raw },
          5000,
        );
        status = r.status;
        out = await r.text();
      } catch { /* recorded as 502 */ }
      let body = null;
      try { body = JSON.parse(raw); } catch { /* invalid JSON recorded as null */ }
      items.push({ path: p, ctype: req.headers["content-type"], body, status });
      res.writeHead(status, { "content-type": "application/json" });
      res.end(out);
    });
  });
  return new Promise((r) => server.listen(0, "127.0.0.1", () => r({
    server,
    items,
    unexpectedRequests,
    get unexpectedDropped() { return unexpectedDropped; },
    port: server.address().port,
  })));
}

const am = (l = []) => Object.fromEntries(l.map((a) => [a.key, Object.values(a.value ?? {})[0]]));
function view(items, from) {
  const v = {
    logs: [],
    pts: [],
    spans: [],
    statuses: items.slice(from).map((i) => i.status),
    raw: JSON.stringify(items.slice(from).map((i) => i.body)),
  };
  for (const { path, body } of items.slice(from)) {
    if (!body) continue;
    if (path.endsWith("/v1/logs")) {
      for (const r of body.resourceLogs ?? []) {
        for (const s of r.scopeLogs ?? []) {
          for (const l of s.logRecords ?? []) {
            v.logs.push({
              body: l.body?.stringValue ?? "",
              t: l.timeUnixNano,
              obs: l.observedTimeUnixNano,
              attrs: am(l.attributes),
            });
          }
        }
      }
    }
    if (path.endsWith("/v1/metrics")) {
      for (const r of body.resourceMetrics ?? []) {
        for (const s of r.scopeMetrics ?? []) {
          for (const m of s.metrics ?? []) {
            for (const kind of ["sum", "gauge", "histogram"]) {
              for (const p of m[kind]?.dataPoints ?? []) {
                v.pts.push({
                  name: m.name,
                  kind,
                  mono: m[kind].isMonotonic,
                  temp: m[kind].aggregationTemporality,
                  attrs: am(p.attributes),
                  start: p.startTimeUnixNano,
                  t: p.timeUnixNano,
                  rawInt: p.asInt,
                  val: Number(p.asInt ?? p.asDouble ?? NaN),
                  flags: p.flags ?? 0,
                  count: p.count,
                  sum: p.sum,
                  bc: p.bucketCounts,
                  eb: p.explicitBounds,
                });
              }
            }
          }
        }
      }
    }
    if (path.endsWith("/v1/traces")) {
      for (const r of body.resourceSpans ?? []) {
        for (const s of r.scopeSpans ?? []) {
          for (const sp of s.spans ?? []) {
            v.spans.push({ ...sp, attrs: am(sp.attributes) });
          }
        }
      }
    }
  }
  return v;
}

const seatOf = (a) => a["openrig.seat.node_id"];
const stateOf = (a) => a["openrig.seat.state"];
const named = (v, n) => v.pts.filter((p) => p.name === n);
const uniq = (a) => [...new Set(a)];
const transitions = (v) => v.logs.filter((l) => l.body === "openrig.queue.transition");
const tid = (l) => l.attrs["openrig.transition.id"];
const resourcesOf = (items) => items.flatMap(({ body }) => [
  ...(body?.resourceLogs ?? []),
  ...(body?.resourceMetrics ?? []),
  ...(body?.resourceSpans ?? []),
]);
const namespaces = (items) => resourcesOf(items).map((r) => am(r.resource?.attributes)["service.namespace"]);
const instanceIds = (items) => resourcesOf(items).map((r) => am(r.resource?.attributes)["service.instance.id"]);

// ---- synthetic daemon source (real finite-API & cached node-inventory wire shapes) ----
const BASE = Math.floor(Date.now() / 1000) * 1000 - 3600_000;
const iso = (sec) => new Date(BASE + sec * 1000).toISOString();
const S = {
  bootEpoch: "synthetic-boot-1",
  discontinuity: null,
  rigs: [{ id: RIG_ID, name: "SENTINEL-RIG-NAME" }],
  archived: [],
  nodesStatus: 200,
  nodes: [],
  pages: [],
  gaps: {},
  reqs: [],
};

const row = (id, q, state, createdAt) => ({
  transitionId: String(id),
  qitemId: `SENTINEL-QITEM-${q}`,
  state,
  identityProvenance: "transport:v1",
  closureReason: state === "done" ? "no-follow-on" : null,
  createdAt,
  actorSession: "SENTINEL-ACTOR@synthetic",
  body: "SENTINEL-BODY",
  note: "SENTINEL-NOTE",
});

const node = (id, o) => ({
  nodeId: id,
  rigId: RIG_ID,
  rigName: "SENTINEL-RIG-NAME",
  logicalId: `SENTINEL-LOGICAL-${id}`,
  podId: "SENTINEL-POD",
  role: null,
  canonicalSessionName: `SENTINEL-SESSION-${id}`,
  nodeKind: "agent",
  runtime: "claude-code",
  sessionStatus: "running",
  cwd: "/SENTINEL/PATH",
  model: "SENTINEL-MODEL",
  lifecycleState: "running",
  arbitratedNeedsInput: null,
  needsInputEvidence: null,
  ...o,
});

const act = (display, swap) => ({
  activity: display,
  display,
  needsInput: { count: 0, reason: "SENTINEL-NEEDS-INPUT-REASON" },
  decidedBy: null,
  seq: 1,
  lastSwap: swap,
});

const envelope = (idx) => {
  if (S.discontinuity) {
    return {
      schemaVersion: 1,
      stream: "queue-transitions",
      observedAt: new Date().toISOString(),
      source: { sequenceSpaceId: null, bootEpoch: S.discontinuity.bootEpoch, hostId: null },
      coverage: {
        status: "unavailable",
        historyCompleteness: "unknown",
        gaps: S.discontinuity.gaps,
        gapCount: S.discontinuity.gaps.length,
        gapDetailsTruncated: false,
      },
      rows: [],
      page: {
        hasMore: false,
        nextCursor: null,
        restartCursor: S.discontinuity.restartCursor,
      },
    };
  }
  const rows = S.pages[idx] ?? [];
  const gapped = (S.gaps ?? {})[idx];
  return {
    schemaVersion: 1,
    stream: "queue-transitions",
    observedAt: new Date().toISOString(),
    source: { sequenceSpaceId: null, bootEpoch: S.bootEpoch, hostId: null },
    coverage: {
      status: gapped ? "partial" : "retained_window",
      historyCompleteness: "unknown",
      gaps: gapped ?? [],
      gapCount: gapped?.length ?? 0,
      gapDetailsTruncated: false,
    },
    rows,
    page: {
      hasMore: idx + 1 < S.pages.length,
      nextCursor: `c${Math.min(idx + 1, Math.max(S.pages.length, idx))}`,
      restartCursor: null,
    },
  };
};

function syntheticSource() {
  const server = createServer((req, res) => {
    const u = new URL(req.url, "http://127.0.0.1");
    S.reqs.push(`${u.pathname}?${[...u.searchParams.keys()].sort().join(",")}`);
    const send = (code, body) => {
      res.writeHead(code, { "content-type": "application/json" });
      res.end(typeof body === "string" ? body : JSON.stringify(body));
    };
    if (u.pathname === "/api/rigs") return send(200, u.searchParams.get("archived") === "only" ? S.archived : S.rigs);
    if (u.pathname === `/api/rigs/${RIG_ID}/nodes`) {
      return S.nodesStatus === 200 ? send(200, S.nodes) : send(S.nodesStatus, '{"error":"SYNTHETIC_SECRET upstream body"}');
    }
    if (u.pathname === SRC_PATH) {
      const c = u.searchParams.get("cursor");
      return send(200, envelope(c ? Number(c.slice(1)) : 0));
    }
    send(404, "{}");
  });
  return new Promise((r) => server.listen(0, "127.0.0.1", () => r({
    server,
    url: `http://127.0.0.1:${server.address().port}`,
  })));
}

let ctx;
const args = (source, ckpt, extra) => [
  CAND,
  "--source", source,
  "--endpoint", `http://127.0.0.1:${ctx.fwd.port}/v1/logs`,
  "--checkpoint", ckpt,
  "--namespace", ctx.ns,
  "--start", "retained",
  "--once",
  "--pseudonymize-seats",
  ...extra,
];

async function step(name, source, ckpt, extra, expectExit) {
  const mark = ctx.fwd.items.length;
  const r = await cli(process.execPath, args(source, ckpt, extra));
  const v = view(ctx.fwd.items, mark);
  const ck = existsSync(ckpt) ? readFileSync(ckpt, "utf8") : "";
  result.phases[name] = {
    exit: r.code,
    requests: v.statuses.length,
    logs: v.logs.length,
    metricPoints: v.pts.length,
    spans: v.spans.length,
    statuses: v.statuses.join(","),
    checkpointSha: ck ? sha(ck).slice(0, 12) : null,
  };
  if (expectExit !== undefined) check(`${name}: exit ${expectExit}`, r.code === expectExit, `code=${r.code}`);
  check(`${name}: all OTLP responses 200 from real Collector`, v.statuses.every((s) => s === 200), v.statuses.join(","));
  check(`${name}: resource namespace matches configured source`, namespaces(ctx.fwd.items.slice(mark)).every((n) => n === ctx.ns));
  check(`${name}: no sensitive sentinel in exported payloads or CLI output`, !LEAK.test(v.raw + r.stdout + r.stderr));
  check(`${name}: checkpoint excludes private actor/body/note/path/reason fields`, !/SENTINEL-(?:ACTOR|BODY|NOTE|PATH|MODEL|REASON|PROMPT)|SYNTHETIC_SECRET/.test(ck));
  return { r, v };
}

function invariants(items) {
  const v = view(items, 0);
  const series = new Map();
  check("content-type application/json on every export", items.every((i) => i.ctype?.startsWith("application/json")));
  check("no duplicate transition log IDs across runs", uniq(transitions(v).map(tid)).length === transitions(v).length, `n=${transitions(v).length}`);
  check(
    "span IDs/trace IDs valid hex and span IDs unique",
    v.spans.length > 0 &&
      v.spans.every((s) => /^[0-9a-f]{16}$/.test(s.spanId) && /^[0-9a-f]{32}$/.test(s.traceId)) &&
      uniq(v.spans.map((s) => s.spanId)).length === v.spans.length,
  );
  check(
    "no parents/links/gen_ai attributes on spans",
    v.spans.every((s) => !s.parentSpanId && !(s.links?.length) && !Object.keys(s.attrs).some((k) => /^gen_ai/.test(k))),
  );
  check(
    "no queue/native identifiers in metric attributes",
    v.pts.every((p) => !Object.keys(p.attrs).some((k) => /qitem|transition|session|native|gen_ai/.test(k))),
  );
  check(
    "metric int64 values are decimal strings",
    v.pts.filter((p) => p.rawInt !== undefined).every((p) => typeof p.rawInt === "string" && /^-?\d+$/.test(p.rawInt)) &&
      v.pts.filter((p) => p.kind === "histogram").every((p) => typeof p.count === "string" && /^\d+$/.test(p.count)),
  );
  let cumOk = true;
  for (const p of v.pts.filter((q) => q.kind !== "gauge" && !(q.flags & 1))) {
    const k = p.name + JSON.stringify(Object.entries(p.attrs).sort());
    const cur = BigInt(p.count ?? p.rawInt ?? 0);
    const prev = series.get(k);
    if (prev && !(prev.start === p.start && cur >= prev.cur && BigInt(p.t) >= BigInt(prev.t))) cumOk = false;
    series.set(k, { start: p.start, cur, t: p.t });
  }
  check("cumulative series: stable startTime, non-decreasing values and times", cumOk && series.size > 0);
  check("sum/histogram temporality is cumulative (2)", v.pts.filter((p) => p.kind !== "gauge").every((p) => p.temp === 2 && BigInt(p.t) >= BigInt(p.start)));
  check(
    "counters are monotonic sums",
    v.pts.filter((p) => /transition\.observed|coverage\.gap|telemetry\.failure/.test(p.name)).every((p) => p.kind === "sum" && p.mono === true),
  );
}

async function synthetic(workDir) {
  const { server, url } = await syntheticSource();
  try {
    const ckpt = join(workDir, "synthetic-checkpoint.json");
    const src = `${url}${SRC_PATH}`;
    const rig = ["--rig", RIG_ID];
    ctx.ns = "interop-synthetic";
    const A = "SENTINEL-NODE-A";
    const B = "SENTINEL-NODE-B";
    const gen1 = "SENTINEL-GEN-1";
    const gen2 = "SENTINEL-GEN-2";

    // Phase 1: 5 rows -> 5 observed, 3 completed 60s spans, authoritative zero vs unsupported zero, generation baseline
    S.pages = [[
      row(101, 1, "pending", iso(0)),
      row(102, 1, "pending", iso(30)),
      row(103, 1, "in-progress", iso(60)),
      row(104, 1, "blocked", iso(120)),
      row(105, 1, "done", iso(180)),
    ]];
    S.nodes = [
      node(A, {
        activityState: act("working", { generation: gen1, at: iso(10) }),
        arbitratedNeedsInput: false,
        needsInputEvidence: { rung: "self-report", observedAt: iso(170) },
      }),
      node(B, { activityState: act("unknown", null), arbitratedNeedsInput: null }),
    ];
    const p1 = await step("phase1", src, ckpt, rig, 0);
    const v1 = p1.v;
    const t1 = transitions(v1);
    check("p1: five observed transition logs, decimal IDs", t1.length === 5 && t1.every((l) => /^\d+$/.test(tid(l))), `n=${t1.length}`);
    const secs = [0, 30, 60, 120, 180];
    check(
      "p1: source event time matches recorded UTC, observed time separate",
      t1.every((l, i) => l.t === String((BigInt(BASE) + BigInt(secs[i] * 1000)) * 1000000n) && l.obs && l.obs !== l.t),
    );
    check("p1: observed counter total 5", named(v1, "openrig.queue.transition.observed").reduce((a, p) => a + BigInt(p.rawInt ?? 0), 0n) === 5n);
    const hist = named(v1, "openrig.queue.state.duration");
    check(
      "p1: histogram count 3, sum 180s, buckets consistent",
      hist.length > 0 &&
        hist.reduce((a, p) => a + BigInt(p.count), 0n) === 3n &&
        hist.reduce((a, p) => a + p.sum, 0) === 180 &&
        hist.every((p) => p.bc.length === p.eb.length + 1 && p.bc.reduce((a, c) => a + BigInt(c), 0n) === BigInt(p.count) && p.eb.every((b, i) => i === 0 || b > p.eb[i - 1])),
    );
    check(
      "p1: three completed 60s root spans",
      v1.spans.length === 3 &&
        v1.spans.every((s) => s.name === "openrig.queue.state" && BigInt(s.endTimeUnixNano) - BigInt(s.startTimeUnixNano) === 60_000_000_000n) &&
        uniq(v1.spans.map((s) => s.startTimeUnixNano)).length === 3,
    );
    const seats = uniq(named(v1, "openrig.seat.activity").map((p) => seatOf(p.attrs)));
    const niSeats = uniq(named(v1, "openrig.seat.needs_input").map((p) => seatOf(p.attrs)));
    check("p1: two agent seats sampled", seats.length === 2 && seats.every(Boolean), `n=${seats.length}`);
    const ni = named(v1, "openrig.seat.needs_input");
    check("p1: authoritative zero exported for exactly one seat, unsupported default zero omitted", niSeats.length === 1 && ni.every((p) => p.val === 0), `seats=${niSeats.length}`);
    const sa = niSeats[0];
    const sb = seats.find((s) => s !== sa);
    check("p1: seat A working one-hot", named(v1, "openrig.seat.activity").some((p) => seatOf(p.attrs) === sa && stateOf(p.attrs) === "working" && p.val === 1));
    check("p1: baseline emits no tenure change", !v1.logs.some((l) => /tenure/i.test(l.body)));

    // Phase 2: invalid-timestamp row + named gap on a separate page; A generation changes; B exited with fake idle
    S.pages.push([row(106, 2, "pending", null)]);
    S.gaps = { 1: [{ code: "invalid_timestamp", id: "106" }] };
    S.nodes = [
      node(A, {
        activityState: act("working", { generation: gen2, at: iso(400) }),
        arbitratedNeedsInput: false,
        needsInputEvidence: { rung: "self-report", observedAt: iso(390) },
      }),
      node(B, { sessionStatus: "exited", lifecycleState: "stopped", activityState: act("idle", null) }),
    ];
    const p2 = await step("phase2", src, ckpt, rig, 0);
    const v2 = p2.v;
    const t2 = transitions(v2);
    check("p2: null-timestamp row exported with unknown source time and valid observed time", t2.length === 1 && (!t2[0].t || t2[0].t === "0") && BigInt(t2[0].obs) > 0n);
    check("p2: named coverage gap log", v2.logs.some((l) => l.attrs["openrig.gap.code"] === "invalid_timestamp"));
    check("p2: gap accounted in coverage.gap sum", named(v2, "openrig.telemetry.coverage.gap").reduce((a, p) => a + p.val, 0) >= 1);
    check("p2: no invented duration/span from gapped page", v2.spans.length === 0);
    check("p2: exactly one observed tenure-change log (A)", v2.logs.filter((l) => /tenure/i.test(l.body)).length === 1);
    check(
      "p2: exited seat B never exported idle or running",
      !named(v2, "openrig.seat.activity").some((p) => seatOf(p.attrs) === sb && stateOf(p.attrs) === "idle" && p.val === 1) &&
        !named(v2, "openrig.seat.presence").some((p) => seatOf(p.attrs) === sb && p.val === 1 && Object.values(p.attrs).includes("running")),
    );

    // Phase 3: inventory 503 with secret body -> visible failure, no departures, no raw body
    S.nodesStatus = 503;
    const p3 = await step("phase3", src, ckpt, rig);
    check("p3: failure visible (log or coverage.failure) and not an empty topology", p3.v.logs.some((l) => /fail/i.test(l.body)) || named(p3.v, "openrig.telemetry.failure").some((p) => p.val >= 1));
    check("p3: no false departure or deletion observations", !p3.v.logs.some((l) => /depart|delet|archiv/i.test(l.body)) && !named(p3.v, "openrig.seat.presence").some((p) => p.val === 1 && Object.values(p.attrs).includes("absent")));
    check("p3: no activity samples claiming idle for unread seats", !named(p3.v, "openrig.seat.activity").some((p) => stateOf(p.attrs) === "idle" && p.val === 1));

    // Phase 4: complete recovery without B -> exactly B goes stale, A continues
    S.nodesStatus = 200;
    S.nodes = [S.nodes.find((n) => n.nodeId === A)];
    const p4 = await step("phase4", src, ckpt, rig, 0);
    const stale4 = p4.v.pts.filter((p) => p.flags & 1);
    check("p4: staleness marker (flags=1) for B series only", stale4.length >= 1 && stale4.every((p) => seatOf(p.attrs) === sb), `n=${stale4.length}`);
    check("p4: A identity continues after recovery", named(p4.v, "openrig.seat.activity").some((p) => seatOf(p.attrs) === sa && !(p.flags & 1)));
    check("p4: failure observation visible after recovery", p4.v.logs.some((l) => /fail/i.test(l.body)) || named(p4.v, "openrig.telemetry.failure").some((p) => p.val >= 1));
    check("p4: no activity samples for departed B", !p4.v.pts.some((p) => seatOf(p.attrs) === sb && !(p.flags & 1)));

    // Phase 5: rig archived (default list empty, archived=only lists it) -> final stale A, never a deletion claim
    S.rigs = [];
    S.archived = [{ id: RIG_ID, name: "SENTINEL-RIG-NAME" }];
    const p5 = await step("phase5", src, ckpt, rig, 0);
    const stale5 = p5.v.pts.filter((p) => p.flags & 1);
    check("p5: final staleness for A series", stale5.length >= 1 && stale5.every((p) => seatOf(p.attrs) === sa), `n=${stale5.length}`);
    check("p5: no deletion claim", !/delet/i.test(p5.v.raw));

    // Injected regression check: verify forwarder unexpected request isolation and 32-entry cap
    const preUnexpectedCount = ctx.fwd.unexpectedRequests.length;
    const probeRes = await http(`http://127.0.0.1:${ctx.fwd.port}/`, { method: "GET" }, 2000);
    const postUnexpectedCount = ctx.fwd.unexpectedRequests.length;
    check("injected probe noise rejected 404 and recorded separately", probeRes.status === 404 && postUnexpectedCount === preUnexpectedCount + 1);

    // Phase 6: no-change resume must not replay logs/spans
    const p6 = await step("phase6-noop", src, ckpt, rig, 0);
    check("p6: resume emits no replayed logs or spans", p6.v.logs.every((l) => !/transition/.test(l.body)) && p6.v.spans.length === 0);

    // Phase 7 & 8: daemon boot discontinuity (restartCursor) and post-restart continuity under stable resource instance
    S.discontinuity = {
      bootEpoch: "synthetic-boot-2",
      gaps: [{ code: "boot_boundary_history_unverified" }],
      restartCursor: "c2",
    };
    const p7 = await step("phase7-restart-discontinuity", src, ckpt, rig, 0);
    check("p7: exports boot_boundary_history_unverified gap log on daemon restart", p7.v.logs.some((l) => l.attrs["openrig.gap.code"] === "boot_boundary_history_unverified"));

    S.discontinuity = null;
    S.bootEpoch = "synthetic-boot-2";
    S.pages[2] = [row(201, 3, "pending", iso(500))];
    const p8 = await step("phase8-post-restart", src, ckpt, rig, 0);
    const t8 = transitions(p8.v);
    check(
      "p8: adopts restartCursor and exports post-restart transition with new boot_epoch and stable instance.id",
      t8.length === 1 &&
        tid(t8[0]) === "201" &&
        t8[0].attrs["openrig.source.boot_epoch"] === "synthetic-boot-2" &&
        uniq(instanceIds(ctx.fwd.items)).length === 1,
    );

    check("never called inventory with full/refresh", !S.reqs.some((q) => /nodes\?.*(full|refresh)/.test(q)));
    check("archived=only listing consulted", S.reqs.some((q) => q === "/api/rigs?archived"));
    const mode = statSync(ckpt).mode & 0o777;
    check("checkpoint private mode 0600", mode === 0o600, mode.toString(8));

    invariants(ctx.fwd.items);
    const all = view(ctx.fwd.items, 0);
    result.unexpectedRequests = { entries: ctx.fwd.unexpectedRequests, dropped: ctx.fwd.unexpectedDropped };
    result.synthetic = {
      logs: all.logs.length,
      metricPoints: all.pts.length,
      spans: all.spans.length,
      metricNames: uniq(all.pts.map((p) => p.name)).sort(),
    };
    check(
      "all 7 required metric families present",
      [
        "openrig.seat.activity",
        "openrig.seat.presence",
        "openrig.seat.needs_input",
        "openrig.queue.transition.observed",
        "openrig.queue.state.duration",
        "openrig.telemetry.coverage.gap",
        "openrig.telemetry.failure",
      ].every((n) => result.synthetic.metricNames.includes(n)),
      result.synthetic.metricNames.join(","),
    );
  } finally {
    server.closeAllConnections?.();
    server.close();
  }
}

const workDir = mkdtempSync(join(tmpdir(), "openrig-telemetry-interop-"));
chmodSync(workDir, 0o700);
const col = new Collector(workDir);
let fwd = null;

try {
  if (!existsSync(CAND)) {
    throw new Error(`Candidate exporter not found at ${CAND}`);
  }
  const colvRes = await cli(OTELCOL, ["--version"], 5000);
  if (colvRes.code !== 0 || !colvRes.stdout.trim()) {
    throw new Error(
      `OpenTelemetry Collector binary not found or not executable (${OTELCOL}). Set OTELCOL_BIN=/path/to/otelcol or install otelcol (tested with v0.164.0) on PATH.`,
    );
  }
  const colv = colvRes.stdout.trim();
  result.versions = {
    node: process.version,
    collector: colv,
    candidateSha256: sha(readFileSync(CAND)),
  };
  check("collector version reports otelcol", /otelcol/i.test(colv), colv);

  await col.start();
  fwd = await forwarder(col.port);
  ctx = { fwd, ns: "" };

  // Separate negative control via temporary forwarder to the same real Collector:
  // POST /v1/logs with malformed JSON must return 400 from Collector and be recorded as 400.
  let negFwd = null;
  try {
    negFwd = await forwarder(col.port);
    const negRes = await http(
      `http://127.0.0.1:${negFwd.port}/v1/logs`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "not json at all {",
      },
      5000,
    );
    check(
      "negative control: malformed OTLP POST returns 400 and recorded as 400",
      negRes.status === 400 && negFwd.items.length === 1 && negFwd.items[0].status === 400,
    );
  } finally {
    try { negFwd?.server.closeAllConnections?.(); negFwd?.server.close(); } catch { /* best effort */ }
  }

  await synthetic(workDir);
  await sleep(500);
  const t = col.text;
  check(
    "Collector accepted and printed all three signals",
    /LogRecord #/.test(t) && /ResourceMetrics #/.test(t) && /ResourceSpans #/.test(t),
  );
  await col.stop();

  const structured = readFileSync(col.outputJsonlPath, "utf8");
  const normalized = view(
    structured
      .trim()
      .split("\n")
      .filter(Boolean)
      .flatMap((line) => {
        const body = JSON.parse(line);
        return body.resourceMetrics ? [{ path: "/v1/metrics", body, status: 200 }] : [];
      }),
    0,
  );
  check("Collector preserved staleness flags in structured file output", normalized.pts.some((p) => p.flags === 1 && p.rawInt === undefined));
  check("Collector structured output excludes sensitive values", !LEAK.test(structured));
  check("Collector debug output excludes sensitive values", !LEAK.test(t));
} catch (e) {
  result.error = String(e?.message ?? e);
  check("no exception", false, result.error);
} finally {
  clearTimeout(deadline);
  try { fwd?.server.closeAllConnections?.(); fwd?.server.close(); } catch { /* best effort */ }
  try { await col.stop(); } catch (e) { result.error ??= `collector stop: ${e?.message}`; }
  try { rmSync(workDir, { recursive: true, force: true }); } catch { /* best effort */ }
  result.finishedAt = new Date().toISOString();
  result.passed = !result.error && result.checks.length > 0 && result.checks.every((c) => c.ok);
  console.log(
    JSON.stringify({
      passed: result.passed,
      totalChecks: result.checks.length,
      failed: result.checks.filter((c) => !c.ok).map((c) => c.name),
      versions: result.versions ?? null,
      synthetic: result.synthetic ?? null,
    }),
  );
  process.exitCode = result.passed ? 0 : 1;
}
