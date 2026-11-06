// Copyright 2026 Mike Schwarz and contributors
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Modification notice: Extracted from OpenRig (packages/daemon/test/finite-telemetry.test.ts
// in https://github.com/mvschwarz/openrig) into the standalone openrig-telemetry repository;
// removed the 18 internal daemon SQLite/Hono finite-API unit tests and decoupled the 30 exporter
// reference test cases from daemon SQLite/Hono internals using standalone loopback HTTP wire fixtures
// and opaque cursor checks.

import { afterEach, describe, expect, it } from "vitest";
import { execFile, spawn } from "node:child_process";
import fs, { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import * as telemetryOtlp from "../telemetry-otlp.mjs";
import {
    exportQueueTransitionsOnce as exportAllSignalsOnce,
    main as runExporterCli,
    parseExporterArgs,
    runQueueTransitionsExporter,
    TELEMETRY_OTLP_LIMITS,
    TelemetryCheckpointError,
    TelemetryExportError,
    TelemetrySourceError,
} from "../telemetry-otlp.mjs";

// The pre-S2 behavior tests below assert the logs stream only; they opt out of metrics/traces explicitly.
const exportQueueTransitionsOnce = (options: Record<string, unknown>) => exportAllSignalsOnce({ signals: ["logs"], ...options });
const source = { hostId: "host-test", bootEpoch: "boot-test" };
const time = "2026-10-04T09:00:00.000Z";

type FixtureGap = {
    code: string;
    id?: string;
    from?: string;
    through?: string;
    field?: string;
};

type FixtureRow = {
    transitionId: string;
    qitemId: string | null;
    state: string | null;
    actorSession: string | null;
    identityProvenance: string | null;
    closureReason: string | null;
    closureTarget: string | null;
    originalTimestamp: string | null;
    createdAt: string | null;
    gaps?: FixtureGap[];
};

type FixtureStore = {
    rows: FixtureRow[];
    nextAutoId: bigint;
    unavailable: boolean;
};

function toCreatedAtIso(ts: string): string | null {
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(ts)) return ts;
    const m = /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})$/.exec(ts);
    if (!m) return null;
    return `${m[1]}T${m[2]}.000Z`;
}

function fixture(): FixtureStore {
    return { rows: [], nextAutoId: 1n, unavailable: false };
}

function pushFixtureRow(
    store: FixtureStore,
    input: {
        id?: bigint | number;
        qitemId: string | null;
        ts: string;
        state: string | null;
        actorSession?: string | null;
        closureReason?: string | null;
        closureTarget?: string | null;
        identityProvenance?: string | null;
        gaps?: FixtureGap[];
    },
) {
    const id = input.id !== undefined ? BigInt(input.id) : store.nextAutoId;
    if (id >= store.nextAutoId) store.nextAutoId = id + 1n;
    const createdAt = toCreatedAtIso(input.ts);
    store.rows.push({
        transitionId: id.toString(),
        qitemId: input.qitemId,
        state: input.state,
        actorSession: input.actorSession ?? null,
        identityProvenance: input.identityProvenance ?? null,
        closureReason: input.closureReason ?? null,
        closureTarget: input.closureTarget ?? null,
        originalTimestamp: input.ts,
        createdAt,
        ...(input.gaps ? { gaps: input.gaps } : {}),
    });
}

function transitions(store: FixtureStore, count: number) {
    for (let i = 0; i < count; i++) {
        pushFixtureRow(store, {
            qitemId: "q-a",
            ts: time,
            state: "pending",
            actorSession: "seat@rig",
        });
    }
}

afterEach(async () => {
    for (const srv of servers.splice(0))
        await new Promise<void>(resolveClose => srv.close(() => resolveClose()));
    for (const dir of tempDirs.splice(0))
        rmSync(dir, { recursive: true, force: true });
});

const servers: Server[] = [];
const tempDirs: string[] = [];
const exporterScriptPath = resolve(import.meta.dirname, "../telemetry-otlp.mjs");
const execFileAsync = promisify(execFile);

function tempDir() {
    const dir = mkdtempSync(join(tmpdir(), "openrig-telemetry-otlp-"));
    tempDirs.push(dir);
    return dir;
}

async function listen(handler: (req: IncomingMessage, res: ServerResponse) => void): Promise<string> {
    const srv = createServer(handler);
    servers.push(srv);
    await new Promise<void>(resolveListen => srv.listen(0, "127.0.0.1", () => resolveListen()));
    const addr = srv.address();
    if (!addr || typeof addr === "string")
        throw new Error("unexpected server address");
    return `http://127.0.0.1:${addr.port}`;
}

type MockInventoryState = {
    rigs?: Array<{ id: string; name?: string; createdAt?: string; updatedAt?: string }>;
    archivedRigs?: Array<{ id: string; name?: string; createdAt?: string; updatedAt?: string }>;
    nodesByRig?: Record<string, any[]>;
    rigsStatus?: number;
    archivedStatus?: number;
    nodesStatusByRig?: Record<string, number>;
    rawBodyByPath?: Record<string, string>;
};

type OpaqueCursorState = {
    afterId: bigint;
    windowThroughId: bigint;
    bootEpoch: string;
    qitemId: string | null;
};

async function startSourceServer(
    store: FixtureStore,
    getSource: () => { hostId: string | null; bootEpoch: string | null } = () => source,
    inventory: MockInventoryState = {},
) {
    let requests = 0;
    let inventoryRequests = 0;
    let cursorSeq = 0;
    const cursors = new Map<string, OpaqueCursorState>();
    const mintCursor = (state: OpaqueCursorState): string => {
        const token = `opaque_cursor_${++cursorSeq}`;
        cursors.set(token, state);
        return token;
    };
    const reqs: string[] = [];
    const baseUrl = await listen((req, res) => {
        const rawUrl = req.url ?? "/";
        const u = new URL(rawUrl, "http://127.0.0.1");
        // Ignore/reject unrelated GET / root probes without masking real producer requests.
        if (u.pathname === "/" && u.search === "") {
            res.statusCode = 404;
            res.end();
            return;
        }
        reqs.push(`${u.pathname}${u.search}`);
        if (inventory.rawBodyByPath && inventory.rawBodyByPath[u.pathname] !== undefined) {
            inventoryRequests++;
            res.statusCode = 200;
            res.setHeader("content-type", "application/json");
            res.end(inventory.rawBodyByPath[u.pathname]);
            return;
        }
        if (u.pathname === "/api/rigs") {
            inventoryRequests++;
            const isArchivedOnly = u.searchParams.get("archived") === "only";
            const status = isArchivedOnly ? (inventory.archivedStatus ?? 200) : (inventory.rigsStatus ?? 200);
            res.statusCode = status;
            res.setHeader("content-type", "application/json");
            if (status !== 200) {
                res.end(JSON.stringify({ error: "SYNTHETIC_SECRET_RIGS_ERROR" }));
                return;
            }
            res.end(JSON.stringify(isArchivedOnly ? (inventory.archivedRigs ?? []) : (inventory.rigs ?? [])));
            return;
        }
        const nodesMatch = /^\/api\/rigs\/([^/]+)\/nodes$/.exec(u.pathname);
        if (nodesMatch) {
            inventoryRequests++;
            const rigId = decodeURIComponent(nodesMatch[1]!);
            const status = inventory.nodesStatusByRig?.[rigId] ?? 200;
            res.statusCode = status;
            res.setHeader("content-type", "application/json");
            if (status !== 200) {
                res.end(JSON.stringify({ error: `SYNTHETIC_SECRET_NODES_ERROR_${rigId}` }));
                return;
            }
            res.end(JSON.stringify(inventory.nodesByRig?.[rigId] ?? []));
            return;
        }
        requests++;
        if (req.method !== "GET" || u.pathname !== "/api/telemetry/v1/queue-transitions") {
            res.statusCode = 404;
            res.setHeader("content-type", "application/json");
            res.end(JSON.stringify({ code: "not_found" }));
            return;
        }
        if (store.unavailable) {
            res.statusCode = 503;
            res.setHeader("content-type", "application/json");
            res.end(JSON.stringify({
                code: "telemetry_read_unavailable",
                error: "Telemetry history could not be read.",
                coverage: { status: "unavailable", historyCompleteness: "unknown" },
            }));
            return;
        }
        const limitRaw = u.searchParams.get("limit") ?? "128";
        const limit = /^[0-9]+$/.test(limitRaw) ? Number(limitRaw) : NaN;
        const startParam = u.searchParams.get("start") ?? "latest";
        const cursorParam = u.searchParams.get("cursor");
        const qitemId = u.searchParams.get("qitemId");
        if (!Number.isInteger(limit) || limit < 1 || limit > 128 || (cursorParam === null && startParam !== "latest" && startParam !== "retained")) {
            res.statusCode = 400;
            res.setHeader("content-type", "application/json");
            res.end(JSON.stringify({ code: "telemetry_invalid_request" }));
            return;
        }
        const srcMeta = getSource();
        const bootEpoch = srcMeta.bootEpoch ?? "boot-test";
        const sorted = [...store.rows].sort((a, b) => (BigInt(a.transitionId) < BigInt(b.transitionId) ? -1 : BigInt(a.transitionId) > BigInt(b.transitionId) ? 1 : 0));
        const highWatermark = sorted.length > 0 ? BigInt(sorted.at(-1)!.transitionId) : 0n;
        const minRetained = sorted.length > 0 ? sorted[0]!.transitionId : null;

        const sendEnvelope = (payload: Record<string, unknown>) => {
            res.statusCode = 200;
            res.setHeader("content-type", "application/json");
            res.end(JSON.stringify(payload));
        };

        if (cursorParam !== null) {
            const c = cursors.get(cursorParam);
            if (!c || c.qitemId !== qitemId) {
                res.statusCode = 400;
                res.setHeader("content-type", "application/json");
                res.end(JSON.stringify({ code: "telemetry_invalid_request" }));
                return;
            }
            if (c.bootEpoch !== bootEpoch || highWatermark < c.afterId) {
                const gapCode = c.bootEpoch !== bootEpoch ? "boot_boundary_history_unverified" : "watermark_regression_history_unverified";
                const restartCursor = mintCursor({ afterId: highWatermark, windowThroughId: highWatermark, bootEpoch, qitemId });
                sendEnvelope({
                    schemaVersion: 1,
                    stream: "queue-transitions",
                    observedAt: time,
                    source: { hostId: srcMeta.hostId, bootEpoch, sequenceSpaceId: null },
                    rows: [],
                    page: {
                        requestedAfter: c.afterId.toString(),
                        requestedBefore: null,
                        lastScanned: c.afterId.toString(),
                        through: highWatermark.toString(),
                        retainedHighWatermark: highWatermark.toString(),
                        hasMore: false,
                        nextCursor: null,
                        restartCursor,
                        fetched: 0,
                        scanned: 0,
                        filtered: 0,
                        withheld: 0,
                        returned: 0,
                        capReason: null,
                    },
                    coverage: {
                        status: "unavailable",
                        historyCompleteness: "unknown",
                        retainedMinimum: minRetained,
                        gaps: [{ code: gapCode }],
                        gapCount: 1,
                        gapDetailsTruncated: false,
                        eventPayloads: "not_read",
                    },
                });
                return;
            }
            const windowThroughId = c.afterId < c.windowThroughId ? c.windowThroughId : highWatermark;
            const inWindow = sorted.filter(r => BigInt(r.transitionId) > c.afterId && BigInt(r.transitionId) <= windowThroughId);
            const scannedRows = inWindow.slice(0, limit);
            const lastScanned = scannedRows.length > 0 ? BigInt(scannedRows.at(-1)!.transitionId) : windowThroughId;
            const hasMore = inWindow.length > scannedRows.length;
            const nextCursor = mintCursor({ afterId: lastScanned, windowThroughId, bootEpoch, qitemId });
            const pageGaps = scannedRows.flatMap(r => r.gaps ?? []);
            const matchedRows = (qitemId !== null ? scannedRows.filter(r => r.qitemId === qitemId) : scannedRows).map(({ gaps: _g, ...wireRow }) => wireRow);
            sendEnvelope({
                schemaVersion: 1,
                stream: "queue-transitions",
                observedAt: time,
                source: { hostId: srcMeta.hostId, bootEpoch, sequenceSpaceId: null },
                rows: matchedRows,
                page: {
                    requestedAfter: c.afterId.toString(),
                    requestedBefore: null,
                    lastScanned: lastScanned.toString(),
                    through: windowThroughId.toString(),
                    retainedHighWatermark: highWatermark.toString(),
                    hasMore,
                    nextCursor,
                    restartCursor: null,
                    fetched: scannedRows.length,
                    scanned: scannedRows.length,
                    filtered: scannedRows.length - matchedRows.length,
                    withheld: 0,
                    returned: matchedRows.length,
                    capReason: null,
                },
                coverage: {
                    status: pageGaps.length > 0 ? "partial" : "retained_window",
                    historyCompleteness: "unknown",
                    retainedMinimum: minRetained,
                    gaps: pageGaps.slice(0, 32),
                    gapCount: pageGaps.length,
                    gapDetailsTruncated: pageGaps.length > 32,
                    eventPayloads: "not_read",
                },
            });
            return;
        }

        if (startParam === "latest") {
            const nextCursor = mintCursor({ afterId: highWatermark, windowThroughId: highWatermark, bootEpoch, qitemId });
            const gaps = highWatermark > 0n ? [{ code: "historical_transitions_not_read", through: highWatermark.toString() }] : [];
            sendEnvelope({
                schemaVersion: 1,
                stream: "queue-transitions",
                observedAt: time,
                source: { hostId: srcMeta.hostId, bootEpoch, sequenceSpaceId: null },
                rows: [],
                page: {
                    requestedAfter: null,
                    requestedBefore: null,
                    lastScanned: highWatermark.toString(),
                    through: highWatermark.toString(),
                    retainedHighWatermark: highWatermark.toString(),
                    hasMore: false,
                    nextCursor,
                    restartCursor: null,
                    fetched: 0,
                    scanned: 0,
                    filtered: 0,
                    withheld: 0,
                    returned: 0,
                    capReason: null,
                },
                coverage: {
                    status: gaps.length > 0 ? "partial" : "retained_window",
                    historyCompleteness: "unknown",
                    retainedMinimum: minRetained,
                    gaps,
                    gapCount: gaps.length,
                    gapDetailsTruncated: false,
                    eventPayloads: "not_read",
                },
            });
            return;
        }

        const windowThroughId = highWatermark;
        const scannedRows = sorted.slice(0, limit);
        const lastScanned = scannedRows.length > 0 ? BigInt(scannedRows.at(-1)!.transitionId) : 0n;
        const hasMore = sorted.length > scannedRows.length;
        const nextCursor = mintCursor({ afterId: lastScanned, windowThroughId, bootEpoch, qitemId });
        const pageGaps = scannedRows.flatMap(r => r.gaps ?? []);
        const matchedRows = (qitemId !== null ? scannedRows.filter(r => r.qitemId === qitemId) : scannedRows).map(({ gaps: _g, ...wireRow }) => wireRow);
        sendEnvelope({
            schemaVersion: 1,
            stream: "queue-transitions",
            observedAt: time,
            source: { hostId: srcMeta.hostId, bootEpoch, sequenceSpaceId: null },
            rows: matchedRows,
            page: {
                requestedAfter: "0",
                requestedBefore: null,
                lastScanned: lastScanned.toString(),
                through: windowThroughId.toString(),
                retainedHighWatermark: highWatermark.toString(),
                hasMore,
                nextCursor,
                restartCursor: null,
                fetched: scannedRows.length,
                scanned: scannedRows.length,
                filtered: scannedRows.length - matchedRows.length,
                withheld: 0,
                returned: matchedRows.length,
                capReason: null,
            },
            coverage: {
                status: pageGaps.length > 0 ? "partial" : "retained_window",
                historyCompleteness: "unknown",
                retainedMinimum: minRetained,
                gaps: pageGaps.slice(0, 32),
                gapCount: pageGaps.length,
                gapDetailsTruncated: pageGaps.length > 32,
                eventPayloads: "not_read",
            },
        });
    });
    return { baseUrl, getRequests: () => requests, getInventoryRequests: () => inventoryRequests, reqs, inventory };
}
type ReceiverOutcome = {
    status?: number;
    headers?: Record<string, string>;
    body?: string | Record<string, unknown>;
    delayMs?: number;
    dropSocket?: boolean;
};
async function startOtlpReceiver(outcomes: ReceiverOutcome[] = []) {
    const received: Array<{
        path: string;
        contentType: string | undefined;
        rawBody: string;
        json: any;
        records: Array<{
            timeUnixNano?: string;
            observedTimeUnixNano?: string;
            severityText?: string;
            body?: string;
            attributes: Record<string, string | number | boolean>;
        }>;
        resourceAttributes: Record<string, string | number | boolean>;
        scopeName?: string;
    }> = [];
    let attempts = 0;
    const baseUrl = await listen((req, res) => {
        attempts++;
        const chunks: Buffer[] = [];
        req.on("data", chunk => chunks.push(Buffer.from(chunk)));
        req.on("end", () => {
            const rawBody = Buffer.concat(chunks).toString("utf8");
            let json: any = null;
            try {
                json = JSON.parse(rawBody);
            }
            catch {
                json = null;
            }
            const attrMap = (attrs: any[] = []) => Object.fromEntries(attrs.map(a => [
                a.key,
                a.value?.stringValue ?? a.value?.intValue ?? a.value?.boolValue,
            ]));
            const rl = json?.resourceLogs?.[0];
            const sl = rl?.scopeLogs?.[0];
            const records = (sl?.logRecords ?? []).map((r: any) => ({
                timeUnixNano: r.timeUnixNano,
                observedTimeUnixNano: r.observedTimeUnixNano,
                severityText: r.severityText,
                body: r.body?.stringValue,
                attributes: attrMap(r.attributes),
            }));
            received.push({
                path: req.url ?? "",
                contentType: req.headers["content-type"],
                rawBody,
                json,
                records,
                resourceAttributes: attrMap(rl?.resource?.attributes),
                scopeName: sl?.scope?.name,
            });
            const outcome: ReceiverOutcome = outcomes.shift() ?? { status: 200, body: {} };
            const respond = () => {
                if (outcome.dropSocket) {
                    req.socket.destroy();
                    return;
                }
                res.statusCode = outcome.status ?? 200;
                res.setHeader("content-type", "application/json");
                for (const [k, v] of Object.entries(outcome.headers ?? {}))
                    res.setHeader(k, v);
                const payload = typeof outcome.body === "string" ? outcome.body : JSON.stringify(outcome.body ?? {});
                res.end(payload);
            };
            if (outcome.delayMs)
                setTimeout(respond, outcome.delayMs);
            else
                respond();
        });
    });
    return { endpointUrl: `${baseUrl}/v1/logs`, baseUrl, received, getAttempts: () => attempts };
}

describe("reference OTLP queue-transition log exporter", () => {
    it("exports allowlisted queue transitions and gaps, preserves 64-bit decimal IDs, separates event vs observed timestamps, and gates qitemId on opt-in", async () => {
        const db = fixture();
        pushFixtureRow(db, {
            id: 9007199254740993n,
            qitemId: "q-secret-item",
            ts: "2026-10-04 09:00:00",
            state: "done",
            actorSession: "secret-actor@rig",
            closureReason: "handed_off_to",
            closureTarget: "secret-target@rig",
            identityProvenance: "transport:v1",
            gaps: [{ code: "before_retained_floor", from: "1", through: "9007199254740992" }],
        });
        pushFixtureRow(db, {
            id: 9007199254740994n,
            qitemId: "q-secret-item",
            ts: "not-a-timestamp",
            state: "in-progress",
            actorSession: "secret-actor-2@rig",
            closureReason: "unallowlisted_reason",
            closureTarget: "secret-target-2@rig",
            identityProvenance: "unallowlisted_prov",
            gaps: [{ code: "invalid_timestamp", id: "9007199254740994" }],
        });
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver();
        const dir = tempDir();
        const checkpointDefault = join(dir, "checkpoint-default.json");
        const resDefault = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: checkpointDefault,
            namespace: "source-test",
            start: "retained",
        });
        expect(resDefault).toMatchObject({
            outcome: "exported",
            exportedRecords: 4,
            rejectedLogRecords: 0,
            replayedPending: false,
            discontinuity: false,
            hasMore: false,
            source: { hostId: "host-test", bootEpoch: "boot-test", sequenceSpaceId: null },
            coverage: { status: "partial", historyCompleteness: "unknown" },
        });
        expect(otlp.received).toHaveLength(1);
        const batch = otlp.received[0]!;
        expect(batch.path).toBe("/v1/logs");
        expect(batch.contentType).toBe("application/json");
        expect(batch.resourceAttributes).toMatchObject({
            "service.name": "openrig-telemetry-exporter",
            "service.namespace": "source-test",
            "openrig.source.namespace": "source-test",
            "openrig.source.host_id": "host-test",
        });
        // S2: the daemon boot epoch is diagnostic metadata on records, not part of the stable resource identity.
        expect(batch.resourceAttributes["openrig.source.boot_epoch"]).toBeUndefined();
        expect(batch.records.every(r => r.attributes["openrig.source.boot_epoch"] === "boot-test")).toBe(true);
        expect(batch.scopeName).toBe("openrig.telemetry.queue_transitions");
        expect(batch.records).toHaveLength(4);
        const [rec1, rec2, floorGapRec, gapRec] = batch.records;
        expect(rec1).toMatchObject({
            timeUnixNano: String(BigInt(Date.parse(time)) * 1000000n),
            observedTimeUnixNano: String(BigInt(Date.parse(time)) * 1000000n),
            severityText: "INFO",
            body: "openrig.queue.transition",
            attributes: {
                "openrig.record.kind": "queue_transition",
                "openrig.stream": "queue-transitions",
                "openrig.transition.id": "9007199254740993",
                "openrig.queue.state": "done",
                "openrig.identity.provenance": "transport:v1",
                "openrig.queue.closure_reason": "handed_off_to",
                "openrig.coverage.status": "partial",
                "openrig.coverage.history_completeness": "unknown",
                "openrig.delivery.replayed": false,
            },
        });
        expect(rec1!.attributes["openrig.qitem.id"]).toBeUndefined();
        expect(rec2!.timeUnixNano).toBeUndefined();
        expect(rec2!.observedTimeUnixNano).toBe(String(BigInt(Date.parse(time)) * 1000000n));
        expect(rec2!.attributes["openrig.transition.id"]).toBe("9007199254740994");
        expect(rec2!.attributes["openrig.queue.state"]).toBe("in-progress");
        expect(rec2!.attributes["openrig.identity.provenance"]).toBeUndefined();
        expect(rec2!.attributes["openrig.queue.closure_reason"]).toBeUndefined();
        expect(floorGapRec).toMatchObject({
            observedTimeUnixNano: String(BigInt(Date.parse(time)) * 1000000n),
            severityText: "WARN",
            body: "openrig.telemetry.gap",
            attributes: {
                "openrig.record.kind": "coverage_gap",
                "openrig.stream": "queue-transitions",
                "openrig.gap.code": "before_retained_floor",
                "openrig.gap.from": "1",
                "openrig.gap.through": "9007199254740992",
                "openrig.coverage.status": "partial",
                "openrig.coverage.history_completeness": "unknown",
            },
        });
        expect(gapRec).toMatchObject({
            observedTimeUnixNano: String(BigInt(Date.parse(time)) * 1000000n),
            severityText: "WARN",
            body: "openrig.telemetry.gap",
            attributes: {
                "openrig.record.kind": "coverage_gap",
                "openrig.stream": "queue-transitions",
                "openrig.gap.code": "invalid_timestamp",
                "openrig.gap.id": "9007199254740994",
                "openrig.coverage.status": "partial",
                "openrig.coverage.history_completeness": "unknown",
            },
        });
        expect(gapRec!.timeUnixNano).toBeUndefined();
        const serializedAll = batch.rawBody + readFileSync(checkpointDefault, "utf8");
        for (const forbidden of ["PRIVATE_NOTE", "secret-actor", "secret-target", "q-secret-item", "not-a-timestamp", "unallowlisted", "traceId", "spanId"]) {
            expect(serializedAll).not.toContain(forbidden);
        }
        const checkpointOptIn = join(dir, "checkpoint-optin.json");
        await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: checkpointOptIn,
            namespace: "source-test",
            start: "retained",
            includeQitemId: true,
        });
        expect(otlp.received[1]!.records[0]!.attributes["openrig.qitem.id"]).toBe("q-secret-item");
    });
    it("drains finite active+archive windows, advances on empty filtered pages, and summarizes truncated gap details", async () => {
        const db = fixture();
        transitions(db, 200);
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver();
        const dir = tempDir();
        const unfilteredCkpt = join(dir, "unfiltered.json");
        const first = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: unfilteredCkpt,
            namespace: "source-test",
            start: "retained",
        });
        expect(first).toMatchObject({ outcome: "exported", exportedRecords: 128, hasMore: true });
        const second = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: unfilteredCkpt,
            namespace: "source-test",
            start: "retained",
        });
        expect(second).toMatchObject({ outcome: "exported", exportedRecords: 72, hasMore: false });
        expect(otlp.received[0]!.records[0]!.attributes["openrig.transition.id"]).toBe("1");
        expect(otlp.received[1]!.records[71]!.attributes["openrig.transition.id"]).toBe("200");
        const filteredCkpt = join(dir, "filtered.json");
        const f1 = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: filteredCkpt,
            namespace: "source-test",
            start: "retained",
            qitemId: "unmatched-qitem",
        });
        expect(f1).toMatchObject({ outcome: "empty_page_advanced", exportedRecords: 0, hasMore: true });
        expect(otlp.received).toHaveLength(2);
        const f2 = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: filteredCkpt,
            namespace: "source-test",
            start: "retained",
            qitemId: "unmatched-qitem",
        });
        expect(f2).toMatchObject({ outcome: "empty_page_advanced", exportedRecords: 0, hasMore: false });
        expect(f2.cursor).not.toBe(f1.cursor);
        const saved = JSON.parse(readFileSync(filteredCkpt, "utf8"));
        expect(saved.source.sequenceSpaceId).toBeNull();
        expect(saved.coverage.historyCompleteness).toBe("unknown");
        for (const r of db.rows) {
            if (BigInt(r.transitionId) > 150n) {
                r.state = null;
                r.gaps = [{ code: "oversized_field", id: r.transitionId, field: "state" }];
            }
        }
        const truncCkpt = join(dir, "truncated-gaps.json");
        const truncRes = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: truncCkpt,
            namespace: "source-test",
            start: "retained",
            limit: 128,
        });
        expect(truncRes.outcome).toBe("exported");
        const truncRes2 = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: truncCkpt,
            namespace: "source-test",
            start: "retained",
            limit: 128,
        });
        expect(truncRes2.outcome).toBe("exported");
        const lastBatch = otlp.received.at(-1)!;
        const truncGap = lastBatch.records.find(r => r.attributes["openrig.gap.code"] === "gap_details_truncated");
        expect(truncGap).toBeDefined();
        expect(truncGap!.attributes["openrig.gap.total_count"]).toBe("50");
        expect(truncGap!.attributes["openrig.gap.omitted_count"]).toBe("18");
    });
    it("exports named discontinuity gaps before adopting fresh restartCursor on daemon boot change or watermark regression", async () => {
        const db = fixture();
        transitions(db, 2);
        let currentSource = { ...source };
        const src = await startSourceServer(db, () => currentSource);
        const otlp = await startOtlpReceiver();
        const ckpt = join(tempDir(), "discontinuity.json");
        const step1 = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            limit: 1,
        });
        expect(step1).toMatchObject({ outcome: "exported", exportedRecords: 1, hasMore: true });
        transitions(db, 2);
        currentSource = { hostId: "host-test", bootEpoch: "boot-after-restart" };
        const rebootStep = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            limit: 1,
        });
        expect(rebootStep).toMatchObject({
            outcome: "exported",
            discontinuity: true,
            exportedRecords: 1,
            source: { hostId: "host-test", bootEpoch: "boot-after-restart", sequenceSpaceId: null },
        });
        expect(otlp.received[1]!.records).toEqual([
            expect.objectContaining({
                body: "openrig.telemetry.gap",
                attributes: expect.objectContaining({
                    "openrig.gap.code": "boot_boundary_history_unverified",
                    "openrig.coverage.status": "unavailable",
                }),
            }),
        ]);
        transitions(db, 1);
        const postReboot = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
        });
        expect(postReboot.exportedRecords).toBe(1);
        expect(otlp.received[2]!.records[0]!.attributes["openrig.transition.id"]).toBe("5");
        db.rows = db.rows.filter(r => BigInt(r.transitionId) <= 1n);
        const regressedStep = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
        });
        expect(regressedStep).toMatchObject({ outcome: "exported", discontinuity: true, exportedRecords: 1 });
        expect(otlp.received[3]!.records[0]!.attributes["openrig.gap.code"]).toBe("watermark_regression_history_unverified");
    });
    it("persists pending batch atomically before send, pauses source ingestion on receiver failure, and replays pending batch on restart", async () => {
        const db = fixture();
        transitions(db, 2);
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver([
            { dropSocket: true },
            { delayMs: 250, status: 200, body: {} },
            { status: 200, body: {} },
        ]);
        const ckpt = join(tempDir(), "pending-replay.json");
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            timeoutMs: 80,
            retryMs: 10,
            maxRetryMs: 20,
            maxRetries: 1,
        })).rejects.toThrow(TelemetryExportError);
        expect(src.getRequests()).toBe(1);
        expect(otlp.getAttempts()).toBe(2);
        const pendingDisk = JSON.parse(readFileSync(ckpt, "utf8"));
        expect(pendingDisk.cursor).toBeNull();
        expect(pendingDisk.pending).toMatchObject({
            nextCursor: expect.any(String),
            signals: {
                logs: expect.objectContaining({ itemCount: 2, attempted: true, outcome: null }),
            },
        });
        transitions(db, 3);
        const replayRes = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            timeoutMs: 500,
        });
        expect(src.getRequests()).toBe(1);
        expect(replayRes).toMatchObject({
            outcome: "exported",
            replayedPending: true,
            exportedRecords: 2,
        });
        expect(otlp.received.at(-1)!.records.every(r => r.attributes["openrig.delivery.replayed"] === true)).toBe(true);
        const resolvedDisk = JSON.parse(readFileSync(ckpt, "utf8"));
        expect(resolvedDisk.pending).toBeNull();
        expect(resolvedDisk.cursor).toBe(pendingDisk.pending.nextCursor);

        // Verify writeCheckpointAtomically:
        // - completes short writeSync progress (17-byte chunks) into a full parseable checkpoint,
        // - fails visibly on zero-progress or mid-write EIO while preserving prior valid checkpoint and cleaning up only its own temp file,
        // - fsyncs the whole containing ancestor directory chain AFTER renameSync on every write (so a retry after an ancestor fsync EIO
        //   still revisits the ancestor even when newly created subdirectories already exist), closes all descriptors in finally, and
        //   aborts before network export when checkpoint durability fails.
        const syncRoot = fs.realpathSync(tempDir());
        const nestedParentA = join(syncRoot, "created-a");
        const nestedParentB = join(nestedParentA, "created-b");
        const nestedCkpt = join(nestedParentB, "nested-ckpt.json");
        const origOpenSync = fs.openSync;
        const origWriteSync = fs.writeSync;
        const origFsyncSync = fs.fsyncSync;
        const origCloseSync = fs.closeSync;
        const origRenameSync = fs.renameSync;
        const fdPaths = new Map<number, string>();
        const openDirFds = new Set<number>();
        const openTmpFds = new Set<number>();
        const ops: string[] = [];
        let failDirFsyncMatch: string | null = null;
        let failDirFsyncCode = "EIO";
        let failPostSendDirMatch: string | null = null;
        let failPostSendDirAfterVisit = 0;
        let postSendDirVisits = 0;
        let shortWriteChunkBytes = 0;
        let zeroProgressOnChunk = 0;
        let writeEioOnChunk = 0;
        let tmpWriteChunks = 0;
        try {
            fs.openSync = ((p: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode | null) => {
                const fd = origOpenSync(p, flags, mode);
                const pathStr = String(p);
                fdPaths.set(fd, pathStr);
                if (flags === "r" && !pathStr.endsWith(".json") && !pathStr.includes(".tmp.")) {
                    openDirFds.add(fd);
                    if (pathStr === syncRoot || pathStr.startsWith(`${syncRoot}/`)) {
                        ops.push(`open-dir:${pathStr}`);
                    }
                } else if (pathStr.includes(".tmp.")) {
                    openTmpFds.add(fd);
                    ops.push(`open-tmp`);
                }
                return fd;
            }) as typeof fs.openSync;
            fs.writeSync = ((fd: number, buf: NodeJS.ArrayBufferView, offset?: number | null, length?: number | null, position?: number | null) => {
                if (openTmpFds.has(fd) && Buffer.isBuffer(buf) && typeof offset === "number" && typeof length === "number") {
                    tmpWriteChunks++;
                    if (zeroProgressOnChunk > 0 && tmpWriteChunks >= zeroProgressOnChunk) {
                        return 0;
                    }
                    if (writeEioOnChunk > 0 && tmpWriteChunks >= writeEioOnChunk) {
                        throw Object.assign(new Error("injected mid-write EIO"), { code: "EIO" });
                    }
                    if (shortWriteChunkBytes > 0) {
                        const capped = Math.min(length, shortWriteChunkBytes);
                        return origWriteSync(fd, buf, offset, capped, position ?? null);
                    }
                }
                return (origWriteSync as any)(fd, buf, offset, length, position);
            }) as typeof fs.writeSync;
            fs.fsyncSync = ((fd: number) => {
                const pathStr = fdPaths.get(fd) ?? "";
                if (openDirFds.has(fd)) {
                    if (pathStr === syncRoot || pathStr.startsWith(`${syncRoot}/`)) {
                        ops.push(`fsync-dir:${pathStr}`);
                    }
                    if (failPostSendDirMatch !== null && pathStr === failPostSendDirMatch) {
                        postSendDirVisits++;
                        if (failPostSendDirAfterVisit > 0 && postSendDirVisits === failPostSendDirAfterVisit) {
                            throw Object.assign(new Error("injected post-send directory fsync failure"), { code: "EIO" });
                        }
                    }
                    if (failDirFsyncMatch !== null && pathStr === failDirFsyncMatch) {
                        throw Object.assign(new Error(`injected directory fsync failure (${failDirFsyncCode})`), { code: failDirFsyncCode });
                    }
                } else if (openTmpFds.has(fd)) {
                    ops.push(`fsync-tmp`);
                }
                return origFsyncSync(fd);
            }) as typeof fs.fsyncSync;
            fs.closeSync = ((fd: number) => {
                const pathStr = fdPaths.get(fd) ?? "";
                if (openDirFds.has(fd)) {
                    openDirFds.delete(fd);
                    if (pathStr === syncRoot || pathStr.startsWith(`${syncRoot}/`)) {
                        ops.push(`close-dir:${pathStr}`);
                    }
                } else if (openTmpFds.has(fd)) {
                    openTmpFds.delete(fd);
                    ops.push(`close-tmp`);
                }
                fdPaths.delete(fd);
                return origCloseSync(fd);
            }) as typeof fs.closeSync;
            fs.renameSync = ((oldP: fs.PathLike, newP: fs.PathLike) => {
                ops.push(`rename:${String(newP)}`);
                return origRenameSync(oldP, newP);
            }) as typeof fs.renameSync;
            syncBuiltinESMExports();

            // 1. Short writeSync progress (17-byte chunks) loops until the entire serialized buffer is written,
            //    producing a complete, parseable JSON checkpoint on disk.
            const shortWriteDir = join(syncRoot, "short-write");
            const shortWriteCkpt = join(shortWriteDir, "ckpt.json");
            shortWriteChunkBytes = 17;
            tmpWriteChunks = 0;
            (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(shortWriteCkpt, resolvedDisk);
            shortWriteChunkBytes = 0;
            const shortWrittenRaw = readFileSync(shortWriteCkpt, "utf8");
            expect(Buffer.byteLength(shortWrittenRaw, "utf8")).toBeGreaterThan(500);
            expect(tmpWriteChunks).toBeGreaterThan(25);
            const shortWrittenParsed = JSON.parse(shortWrittenRaw);
            expect(shortWrittenParsed.cursor).toBe(resolvedDisk.cursor);
            expect(openTmpFds.size).toBe(0);
            expect(openDirFds.size).toBe(0);

            // 2. Zero progress (written === 0) or mid-write EIO after partial 17-byte chunks fails visibly before rename,
            //    preserves the prior valid checkpoint and unrelated sibling files, closes temp fd, and deletes only the owned temp file.
            const siblingPath = join(shortWriteDir, "unrelated-sibling.txt");
            writeFileSync(siblingPath, "keep-me");
            const mutatedState = { ...resolvedDisk, cursor: "Y3Vyc29yLW11dGF0ZWQ" };
            shortWriteChunkBytes = 17;
            tmpWriteChunks = 0;
            zeroProgressOnChunk = 2;
            expect(() => (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(shortWriteCkpt, mutatedState)).toThrow(
                expect.objectContaining({ name: "TelemetryCheckpointError", code: "checkpoint_short_write" }),
            );
            zeroProgressOnChunk = 0;
            expect(openTmpFds.size).toBe(0);
            expect(readFileSync(shortWriteCkpt, "utf8")).toBe(shortWrittenRaw);
            expect(readFileSync(siblingPath, "utf8")).toBe("keep-me");
            expect(fs.readdirSync(shortWriteDir).sort()).toEqual(["ckpt.json", "unrelated-sibling.txt"]);

            tmpWriteChunks = 0;
            writeEioOnChunk = 3;
            expect(() => (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(shortWriteCkpt, mutatedState)).toThrow(
                expect.objectContaining({ code: "EIO" }),
            );
            writeEioOnChunk = 0;
            shortWriteChunkBytes = 0;
            expect(openTmpFds.size).toBe(0);
            expect(readFileSync(shortWriteCkpt, "utf8")).toBe(shortWrittenRaw);
            expect(readFileSync(siblingPath, "utf8")).toBe("keep-me");
            expect(fs.readdirSync(shortWriteDir).sort()).toEqual(["ckpt.json", "unrelated-sibling.txt"]);

            const attemptsBeforeShortWriteFault = otlp.getAttempts();
            shortWriteChunkBytes = 17;
            tmpWriteChunks = 0;
            zeroProgressOnChunk = 2;
            await expect(exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: otlp.endpointUrl,
                checkpointPath: join(shortWriteDir, "pre-send-short.json"),
                namespace: "source-test",
                start: "retained",
            })).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_short_write" });
            zeroProgressOnChunk = 0;
            shortWriteChunkBytes = 0;
            expect(otlp.getAttempts()).toBe(attemptsBeforeShortWriteFault);
            expect(openTmpFds.size).toBe(0);
            expect(fs.readdirSync(shortWriteDir).sort()).toEqual(["ckpt.json", "unrelated-sibling.txt"]);

            // 3. N1: First write into a missing 2-level parent chain where fsync(syncRoot) fails with EIO leaves
            //    created-a and created-b existing on disk without renaming nestedCkpt; retrying writeCheckpointAtomically
            //    still preflights and post-rename fsyncs syncRoot -> created-a -> created-b, closing every descriptor.
            ops.length = 0;
            failDirFsyncMatch = syncRoot;
            expect(() => (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(nestedCkpt, resolvedDisk)).toThrow(
                expect.objectContaining({ code: "EIO" }),
            );
            expect(existsSync(nestedParentA)).toBe(true);
            expect(existsSync(nestedParentB)).toBe(true);
            expect(existsSync(nestedCkpt)).toBe(false);
            expect(openDirFds.size).toBe(0);
            failDirFsyncMatch = null;

            ops.length = 0;
            (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(nestedCkpt, resolvedDisk);
            expect(ops).toEqual([
                `open-dir:${syncRoot}`,
                `fsync-dir:${syncRoot}`,
                `close-dir:${syncRoot}`,
                `open-dir:${nestedParentA}`,
                `fsync-dir:${nestedParentA}`,
                `close-dir:${nestedParentA}`,
                `open-dir:${nestedParentB}`,
                `fsync-dir:${nestedParentB}`,
                `close-dir:${nestedParentB}`,
                "open-tmp",
                "fsync-tmp",
                "close-tmp",
                `rename:${nestedCkpt}`,
                `open-dir:${syncRoot}`,
                `fsync-dir:${syncRoot}`,
                `close-dir:${syncRoot}`,
                `open-dir:${nestedParentA}`,
                `fsync-dir:${nestedParentA}`,
                `close-dir:${nestedParentA}`,
                `open-dir:${nestedParentB}`,
                `fsync-dir:${nestedParentB}`,
                `close-dir:${nestedParentB}`,
            ]);
            expect(openDirFds.size).toBe(0);

            // 4. Fault injection: directory fsync EIO before network send aborts export before any source or OTLP request is sent
            //    and still closes the directory descriptor in finally.
            const preSendDir = join(syncRoot, "fault-pre-send");
            const preSendCkpt = join(preSendDir, "ckpt.json");
            const srcRequestsBeforeFault = src.getRequests();
            const attemptsBeforeFault = otlp.getAttempts();
            failDirFsyncMatch = preSendDir;
            await expect(exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: otlp.endpointUrl,
                checkpointPath: preSendCkpt,
                namespace: "source-test",
                start: "retained",
            })).rejects.toMatchObject({ code: "EIO" });
            expect(src.getRequests()).toBe(srcRequestsBeforeFault);
            expect(otlp.getAttempts()).toBe(attemptsBeforeFault);
            expect(existsSync(preSendCkpt)).toBe(false);
            expect(openDirFds.size).toBe(0);
            failDirFsyncMatch = null;

            // 5. Fault injection: directory fsync EIO during post-export outcome/adoption write fails visibly,
            //    closes the directory descriptor, and recovers cleanly when retried after the fault clears.
            const postSendDir = join(syncRoot, "fault-post-send");
            const postSendCkpt = join(postSendDir, "ckpt.json");
            const postOtlp = await startOtlpReceiver([{ status: 200, body: {} }, { status: 200, body: {} }]);
            failPostSendDirMatch = postSendDir;
            postSendDirVisits = 0;
            // Visit 1 preflights postSendDir before network; Visits 2-3 sync postSendDir on initial pending write;
            // Visits 4-5 sync postSendDir on attempted=true; Visits 6-7 sync postSendDir after HTTP 200 outcome resolution.
            failPostSendDirAfterVisit = 7;
            await expect(exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: postOtlp.endpointUrl,
                checkpointPath: postSendCkpt,
                namespace: "source-test",
                start: "retained",
            })).rejects.toMatchObject({ code: "EIO" });
            expect(postOtlp.getAttempts()).toBe(1);
            expect(openDirFds.size).toBe(0);
            failPostSendDirMatch = null;
            failPostSendDirAfterVisit = 0;
            const postRecovered = await exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: postOtlp.endpointUrl,
                checkpointPath: postSendCkpt,
                namespace: "source-test",
                start: "retained",
            });
            expect(postRecovered).toMatchObject({
                outcome: "exported",
                replayedPending: true,
                exportedRecords: 5,
            });
            expect(JSON.parse(readFileSync(postSendCkpt, "utf8")).pending).toBeNull();

            // 6. B2 preflight & actionable TelemetryCheckpointError: unreadable (--x / 0311) ancestor directory or
            //    unsupported directory fsync (EINVAL / ENOTSUP) fails with TelemetryCheckpointError (checkpoint_directory_unsyncable)
            //    BEFORE temp file creation, rename, or network I/O, preserving prior checkpoint state and closing all descriptors.
            const chmodAncestor = join(syncRoot, "chmod-0311-ancestor");
            const chmodChild = join(chmodAncestor, "child");
            const chmodExistingCkpt = join(chmodChild, "ckpt.json");
            const chmodFreshCkpt = join(chmodChild, "fresh-ckpt.json");
            (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(chmodExistingCkpt, resolvedDisk);
            const chmodOriginalRaw = readFileSync(chmodExistingCkpt, "utf8");
            if (process.getuid?.() !== 0) {
                fs.chmodSync(chmodAncestor, 0o311);
                try {
                    ops.length = 0;
                    expect(() => (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(chmodExistingCkpt, mutatedState)).toThrow(
                        expect.objectContaining({
                            name: "TelemetryCheckpointError",
                            code: "checkpoint_directory_unsyncable",
                            causeCode: "EACCES",
                        }),
                    );
                    expect(ops).not.toContain("open-tmp");
                    expect(ops).not.toContain(`rename:${chmodExistingCkpt}`);
                    expect(readFileSync(chmodExistingCkpt, "utf8")).toBe(chmodOriginalRaw);
                    expect(fs.readdirSync(chmodChild)).toEqual(["ckpt.json"]);
                    expect(openDirFds.size).toBe(0);
                    expect(openTmpFds.size).toBe(0);

                    const srcBeforePerm = src.getRequests();
                    const otlpBeforePerm = otlp.getAttempts();
                    await expect(exportQueueTransitionsOnce({
                        sourceUrl: src.baseUrl,
                        endpointUrl: otlp.endpointUrl,
                        checkpointPath: chmodFreshCkpt,
                        namespace: "source-test",
                        start: "retained",
                    })).rejects.toMatchObject({
                        name: "TelemetryCheckpointError",
                        code: "checkpoint_directory_unsyncable",
                        causeCode: "EACCES",
                    });
                    expect(src.getRequests()).toBe(srcBeforePerm);
                    expect(otlp.getAttempts()).toBe(otlpBeforePerm);
                    expect(existsSync(chmodFreshCkpt)).toBe(false);
                    expect(openDirFds.size).toBe(0);
                    expect(openTmpFds.size).toBe(0);
                } finally {
                    fs.chmodSync(chmodAncestor, 0o700);
                }
            }

            for (const unsupportedCode of ["EINVAL", "ENOTSUP"] as const) {
                failDirFsyncMatch = shortWriteDir;
                failDirFsyncCode = unsupportedCode;
                ops.length = 0;
                expect(() => (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(shortWriteCkpt, mutatedState)).toThrow(
                    expect.objectContaining({
                        name: "TelemetryCheckpointError",
                        code: "checkpoint_directory_unsyncable",
                        causeCode: unsupportedCode,
                    }),
                );
                expect(ops).not.toContain("open-tmp");
                expect(ops).not.toContain(`rename:${shortWriteCkpt}`);
                expect(readFileSync(shortWriteCkpt, "utf8")).toBe(shortWrittenRaw);
                expect(openDirFds.size).toBe(0);
                expect(openTmpFds.size).toBe(0);

                const srcBeforeUnsup = src.getRequests();
                const otlpBeforeUnsup = otlp.getAttempts();
                await expect(exportQueueTransitionsOnce({
                    sourceUrl: src.baseUrl,
                    endpointUrl: otlp.endpointUrl,
                    checkpointPath: join(shortWriteDir, `unsup-${unsupportedCode}.json`),
                    namespace: "source-test",
                    start: "retained",
                })).rejects.toMatchObject({
                    name: "TelemetryCheckpointError",
                    code: "checkpoint_directory_unsyncable",
                    causeCode: unsupportedCode,
                });
                expect(src.getRequests()).toBe(srcBeforeUnsup);
                expect(otlp.getAttempts()).toBe(otlpBeforeUnsup);
                expect(existsSync(join(shortWriteDir, `unsup-${unsupportedCode}.json`))).toBe(false);
                expect(openDirFds.size).toBe(0);
                failDirFsyncMatch = null;
                failDirFsyncCode = "EIO";
            }

            // 7. Symlinked checkpoint directory resolves realpath so durability preflight and post-rename sync
            //    visit the physical target directory and all of its physical ancestors.
            const realTargetRoot = join(syncRoot, "real-target-root");
            const realTargetSubA = join(realTargetRoot, "real-sub-a");
            const realTargetSubB = join(realTargetSubA, "real-sub-b");
            fs.mkdirSync(realTargetSubB, { recursive: true });
            const symlinkDir = join(syncRoot, "symlink-to-target");
            symlinkSync(realTargetSubB, symlinkDir, "dir");
            const symlinkCkpt = join(symlinkDir, "symlink-ckpt.json");
            ops.length = 0;
            (telemetryOtlp as Record<string, any>).writeCheckpointAtomically(symlinkCkpt, resolvedDisk);
            expect(ops).toEqual([
                `open-dir:${syncRoot}`,
                `fsync-dir:${syncRoot}`,
                `close-dir:${syncRoot}`,
                `open-dir:${realTargetRoot}`,
                `fsync-dir:${realTargetRoot}`,
                `close-dir:${realTargetRoot}`,
                `open-dir:${realTargetSubA}`,
                `fsync-dir:${realTargetSubA}`,
                `close-dir:${realTargetSubA}`,
                `open-dir:${realTargetSubB}`,
                `fsync-dir:${realTargetSubB}`,
                `close-dir:${realTargetSubB}`,
                "open-tmp",
                "fsync-tmp",
                "close-tmp",
                `rename:${symlinkCkpt}`,
                `open-dir:${syncRoot}`,
                `fsync-dir:${syncRoot}`,
                `close-dir:${syncRoot}`,
                `open-dir:${realTargetRoot}`,
                `fsync-dir:${realTargetRoot}`,
                `close-dir:${realTargetRoot}`,
                `open-dir:${realTargetSubA}`,
                `fsync-dir:${realTargetSubA}`,
                `close-dir:${realTargetSubA}`,
                `open-dir:${realTargetSubB}`,
                `fsync-dir:${realTargetSubB}`,
                `close-dir:${realTargetSubB}`,
            ]);
            expect(openDirFds.size).toBe(0);
            expect(openTmpFds.size).toBe(0);
            expect(JSON.parse(readFileSync(symlinkCkpt, "utf8")).cursor).toBe(resolvedDisk.cursor);
        } finally {
            fs.openSync = origOpenSync;
            fs.writeSync = origWriteSync;
            fs.fsyncSync = origFsyncSync;
            fs.closeSync = origCloseSync;
            fs.renameSync = origRenameSync;
            syncBuiltinESMExports();
        }
    });
    it("fails visibly without resetting on malformed, oversized, or config-mismatched checkpoints", async () => {
        const db = fixture();
        transitions(db, 1);
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver();
        const dir = tempDir();
        const malformedPath = join(dir, "malformed.json");
        writeFileSync(malformedPath, "{not-valid-json");
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: malformedPath,
            namespace: "source-test",
        })).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_malformed" });
        expect(readFileSync(malformedPath, "utf8")).toBe("{not-valid-json");
        const oversizedPath = join(dir, "oversized.json");
        writeFileSync(oversizedPath, "x".repeat(TELEMETRY_OTLP_LIMITS.checkpointBytes + 1));
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: oversizedPath,
            namespace: "source-test",
        })).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_exceeds_byte_limit" });
        const validPath = join(dir, "valid.json");
        await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: validPath,
            namespace: "source-test",
            start: "retained",
        });
        const originalSnapshot = readFileSync(validPath, "utf8");
        for (const override of [
            { namespace: "other-ns" },
            { start: "latest" as const },
            { qitemId: "q-other" },
            { includeQitemId: true },
            { endpointUrl: `${otlp.baseUrl}/other/v1/logs` },
        ]) {
            await expect(exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: otlp.endpointUrl,
                checkpointPath: validPath,
                namespace: "source-test",
                start: "retained",
                ...override,
            })).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_config_mismatch" });
            expect(readFileSync(validPath, "utf8")).toBe(originalSnapshot);
        }
        const tamperedPendingPath = join(dir, "tampered-pending.json");
        const tamperedObj = JSON.parse(originalSnapshot);
        tamperedObj.pending = {
            nextCursor: tamperedObj.cursor,
            discontinuity: false,
            hasMore: false,
            source: tamperedObj.source,
            coverage: tamperedObj.coverage,
            diagnostics: [],
            signals: {
                logs: {
                    itemCount: 1,
                    attempted: true,
                    outcome: null,
                    rejectedCount: 0,
                    partialSuccessWarning: false,
                    droppedCount: 0,
                    status: null,
                    blocked: null,
                    payload: { resourceLogs: [{ scopeLogs: [{ scope: { name: "openrig.telemetry.queue_transitions" }, logRecords: [{ traceId: "invented", body: { stringValue: "openrig.queue.transition" }, attributes: [] }] }] }] },
                },
                metrics: null,
                traces: null,
            },
        };
        const tamperedText = JSON.stringify(tamperedObj);
        writeFileSync(tamperedPendingPath, tamperedText);
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: tamperedPendingPath,
            namespace: "source-test",
            start: "retained",
        })).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_malformed" });
        expect(readFileSync(tamperedPendingPath, "utf8")).toBe(tamperedText);
    });
    it("handles retryable statuses with bounded Retry-After, partialSuccess without retry or body leak, permanent stop/drop, and response uncertainty", async () => {
        const db = fixture();
        transitions(db, 3);
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver([
            { status: 429, headers: { "retry-after": "60" }, body: "RATE_LIMIT_RAW_SECRET" },
            { status: 503, body: "UNAVAILABLE_RAW_SECRET" },
            { status: 200, body: { partialSuccess: { rejectedLogRecords: "1", errorMessage: "COLLECTOR_INTERNAL_SECRET" } } },
            { status: 400, body: "PERMANENT_BAD_REQUEST_SECRET" },
            { status: 400, body: "PERMANENT_BAD_REQUEST_SECRET_2" },
        ]);
        const dir = tempDir();
        const ckpt = join(dir, "outcomes.json");
        const diagnostics: Array<Record<string, unknown>> = [];
        const step1 = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            limit: 2,
            retryMs: 5,
            maxRetryMs: 25,
            maxRetries: 2,
            onDiagnostic: (d: Record<string, unknown>) => diagnostics.push(d),
        });
        expect(step1).toMatchObject({
            outcome: "partial_success",
            exportedRecords: 2,
            rejectedLogRecords: 1,
            partialSuccessWarning: true,
            retryDelaysMs: [25, 10],
        });
        expect(otlp.getAttempts()).toBe(3);
        expect(JSON.stringify([step1, diagnostics])).not.toContain("COLLECTOR_INTERNAL_SECRET");
        const invalidRespRx = await startOtlpReceiver([
            { status: 200, body: "<html>not-json</html>" },
            { status: 200, body: {} },
        ]);
        const invalidRespCkpt = join(dir, "invalid-resp.json");
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: invalidRespRx.endpointUrl,
            checkpointPath: invalidRespCkpt,
            namespace: "source-test",
            start: "retained",
            limit: 2,
            maxRetries: 2,
            onPermanentError: "drop",
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_invalid_response", status: 200 });
        expect(invalidRespRx.getAttempts()).toBe(1);
        const invalidRespSaved = JSON.parse(readFileSync(invalidRespCkpt, "utf8"));
        expect(invalidRespSaved.cursor).toBeNull();
        expect(invalidRespSaved.pending?.signals?.logs?.blocked).toEqual({ code: "otlp_invalid_response", status: 200 });
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: invalidRespRx.endpointUrl,
            checkpointPath: invalidRespCkpt,
            namespace: "source-test",
            start: "retained",
            limit: 2,
            onPermanentError: "drop",
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_invalid_response", status: 200 });
        expect(invalidRespRx.getAttempts()).toBe(1);
        let stopErr: unknown;
        try {
            await exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: otlp.endpointUrl,
                checkpointPath: ckpt,
                namespace: "source-test",
                start: "retained",
                limit: 2,
                onPermanentError: "stop",
            });
        }
        catch (err) {
            stopErr = err;
        }
        expect(stopErr).toMatchObject({ name: "TelemetryExportError", code: "otlp_permanent_rejection", status: 400 });
        expect(String((stopErr as Error)?.message)).not.toContain("PERMANENT_BAD_REQUEST_SECRET");
        expect(JSON.parse(readFileSync(ckpt, "utf8")).pending).not.toBeNull();
        const attemptsAfterStop = otlp.getAttempts();
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            limit: 2,
            onPermanentError: "stop",
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_permanent_rejection", status: 400 });
        expect(otlp.getAttempts()).toBe(attemptsAfterStop);
        const dropped = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            limit: 2,
            onPermanentError: "drop",
            onDiagnostic: (d: Record<string, unknown>) => diagnostics.push(d),
        });
        // S2: the batch now also carries the durable failure-ledger log for the earlier partial-success outcome.
        expect(dropped).toMatchObject({
            outcome: "dropped_permanent_rejection",
            droppedRecords: 2,
            status: 400,
        });
        expect(otlp.received.at(-1)!.records.map(r => r.body)).toEqual(["openrig.queue.transition", "openrig.telemetry.failure"]);
        expect(otlp.getAttempts()).toBe(attemptsAfterStop);
        expect(JSON.parse(readFileSync(ckpt, "utf8")).pending).toBeNull();
        expect(JSON.stringify(diagnostics)).not.toContain("PERMANENT_BAD_REQUEST_SECRET");
        for (const nonRetryable5xx of [500, 501]) {
            transitions(db, 1);
            const rx5xx = await startOtlpReceiver([
                { status: nonRetryable5xx, body: `RAW_SERVER_ERROR_SECRET_${nonRetryable5xx}` },
                { status: 200, body: {} },
            ]);
            const ckpt5xx = join(dir, `outcomes-${nonRetryable5xx}.json`);
            const diags5xx: Array<Record<string, unknown>> = [];
            let firstErr: unknown;
            try {
                await exportQueueTransitionsOnce({
                    sourceUrl: src.baseUrl,
                    endpointUrl: rx5xx.endpointUrl,
                    checkpointPath: ckpt5xx,
                    namespace: "source-test",
                    start: "retained",
                    limit: 1,
                    maxRetries: 2,
                    onPermanentError: "stop",
                    onDiagnostic: (d: Record<string, unknown>) => diags5xx.push(d),
                });
            }
            catch (err) {
                firstErr = err;
            }
            expect(firstErr).toMatchObject({ name: "TelemetryExportError", code: "otlp_permanent_rejection", status: nonRetryable5xx });
            expect(String((firstErr as Error)?.message)).not.toContain(`RAW_SERVER_ERROR_SECRET_${nonRetryable5xx}`);
            expect(rx5xx.getAttempts()).toBe(1);
            expect(JSON.parse(readFileSync(ckpt5xx, "utf8")).pending?.signals?.logs?.blocked).toEqual({
                code: "otlp_permanent_rejection",
                status: nonRetryable5xx,
            });
            await expect(exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: rx5xx.endpointUrl,
                checkpointPath: ckpt5xx,
                namespace: "source-test",
                start: "retained",
                limit: 1,
                onPermanentError: "stop",
            })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_permanent_rejection", status: nonRetryable5xx });
            expect(rx5xx.getAttempts()).toBe(1);
            const dropped5xx = await exportQueueTransitionsOnce({
                sourceUrl: src.baseUrl,
                endpointUrl: rx5xx.endpointUrl,
                checkpointPath: ckpt5xx,
                namespace: "source-test",
                start: "retained",
                limit: 1,
                onPermanentError: "drop",
                onDiagnostic: (d: Record<string, unknown>) => diags5xx.push(d),
            });
            expect(dropped5xx).toMatchObject({
                outcome: "dropped_permanent_rejection",
                droppedRecords: 1,
                status: nonRetryable5xx,
            });
            expect(rx5xx.getAttempts()).toBe(1);
            expect(JSON.parse(readFileSync(ckpt5xx, "utf8")).pending).toBeNull();
            expect(JSON.stringify(diags5xx)).not.toContain(`RAW_SERVER_ERROR_SECRET_${nonRetryable5xx}`);
        }
    });
    it("exports and checkpoints a maximum 128-row + 33-gap (161-record) batch compactly below byte bounds", async () => {
        const db = fixture();
        const maxQitem = "q-" + "a".repeat(250);
        for (let i = 0; i < 128; i++) {
            pushFixtureRow(db, {
                qitemId: maxQitem,
                ts: time,
                state: "done",
                actorSession: i < 40 ? null : "seat@rig",
                closureReason: "handed_off_to",
                closureTarget: "target",
                identityProvenance: "transport:v1",
                gaps: i < 40 ? [{ code: "oversized_field", id: String(i + 1), field: "actorSession" }] : [],
            });
        }
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver([{ dropSocket: true }, { status: 200, body: {} }]);
        const ckpt = join(tempDir(), "max-batch-ckpt.json");
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            includeQitemId: true,
            maxRetries: 0,
        })).rejects.toThrow(TelemetryExportError);
        const rawCkpt = readFileSync(ckpt, "utf8");
        expect(Buffer.byteLength(rawCkpt)).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.checkpointBytes);
        expect(rawCkpt.trim().split("\n")).toHaveLength(1);
        const parsedCkpt = JSON.parse(rawCkpt);
        expect(parsedCkpt.pending.signals.logs.itemCount).toBe(161);
        expect(parsedCkpt.pending.signals.logs.payload.resourceLogs[0].scopeLogs[0].logRecords).toHaveLength(161);
        const res = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            includeQitemId: true,
        });
        expect(res).toMatchObject({ outcome: "exported", exportedRecords: 161, replayedPending: true });
    });
    it("rejects source errors, non-JSON content-types, non-200 OTLP responses, oversized responses without auto-retry, and supports CLI --help and --once", async () => {
        const db = fixture();
        transitions(db, 2);
        const src = await startSourceServer(db);
        const otlp = await startOtlpReceiver();
        const dir = tempDir();
        expect(() => parseExporterArgs(["--source", "http://user:pass@127.0.0.1:7433", "--endpoint", otlp.endpointUrl, "--checkpoint", join(dir, "c.json"), "--namespace", "ns"])).toThrow();
        expect(() => parseExporterArgs(["--source", src.baseUrl, "--endpoint", `${otlp.baseUrl}/v1/traces`, "--checkpoint", join(dir, "c.json"), "--namespace", "ns"])).toThrow();
        expect(parseExporterArgs(["--help"])).toMatchObject({ help: true });
        const helpRun = await execFileAsync(process.execPath, [exporterScriptPath, "--help"], { encoding: "utf8" });
        expect(helpRun.stdout).toContain("7433");
        expect(helpRun.stdout).toContain("example-source");
        const cliCkpt = join(dir, "cli-ckpt.json");
        const latestCkpt = join(dir, "latest-ckpt.json");
        const latestInit = await exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: latestCkpt,
            namespace: "example-source",
        });
        expect(latestInit).toMatchObject({ outcome: "exported", exportedRecords: 1, hasMore: false });
        expect(otlp.received[0]!.records[0]!.attributes).toMatchObject({
            "openrig.gap.code": "historical_transitions_not_read",
            "openrig.gap.through": "2",
        });
        const run1 = await execFileAsync(process.execPath, [
            exporterScriptPath,
            "--source", src.baseUrl,
            "--endpoint", otlp.endpointUrl,
            "--checkpoint", cliCkpt,
            "--namespace", "example-source",
            "--start", "retained",
            "--signals", "logs",
            "--once",
        ], { encoding: "utf8" });
        expect(run1.stderr).toBe("");
        expect(otlp.received).toHaveLength(2);
        expect(otlp.received[1]!.records.map(r => r.attributes["openrig.transition.id"])).toEqual(["1", "2"]);
        transitions(db, 1);
        const code = await runExporterCli([
            "--source", src.baseUrl,
            "--endpoint", otlp.endpointUrl,
            "--checkpoint", cliCkpt,
            "--namespace", "example-source",
            "--start", "retained",
            "--signals", "logs",
            "--once",
        ]);
        expect(code).toBe(0);
        expect(otlp.received).toHaveLength(3);
        expect(otlp.received[2]!.records.map(r => r.attributes["openrig.transition.id"])).toEqual(["3"]);
        const proxyMimeOtlp = await startOtlpReceiver([
            { status: 200, headers: { "content-type": "text/html" }, body: "{}" },
            { status: 200, body: {} },
        ]);
        const proxyMimeCkpt = join(dir, "proxy-mime-otlp.json");
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: proxyMimeOtlp.endpointUrl,
            checkpointPath: proxyMimeCkpt,
            namespace: "example-source",
            start: "retained",
            maxRetries: 2,
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_invalid_content_type", status: 200 });
        expect(proxyMimeOtlp.getAttempts()).toBe(1);
        const savedMimeCkpt = JSON.parse(readFileSync(proxyMimeCkpt, "utf8"));
        expect(savedMimeCkpt.cursor).toBeNull();
        expect(savedMimeCkpt.pending?.signals?.logs?.blocked).toEqual({ code: "otlp_invalid_content_type", status: 200 });
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: proxyMimeOtlp.endpointUrl,
            checkpointPath: proxyMimeCkpt,
            namespace: "example-source",
            start: "retained",
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_invalid_content_type", status: 200 });
        expect(proxyMimeOtlp.getAttempts()).toBe(1);
        const proxyStatusOtlp = await startOtlpReceiver([
            { status: 202, headers: { "content-type": "application/json" }, body: "{}" },
            { status: 200, body: {} },
        ]);
        const proxyStatusCkpt = join(dir, "proxy-status-otlp.json");
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: proxyStatusOtlp.endpointUrl,
            checkpointPath: proxyStatusCkpt,
            namespace: "example-source",
            start: "retained",
            maxRetries: 2,
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_unexpected_status", status: 202 });
        expect(proxyStatusOtlp.getAttempts()).toBe(1);
        const savedStatusCkpt = JSON.parse(readFileSync(proxyStatusCkpt, "utf8"));
        expect(savedStatusCkpt.cursor).toBeNull();
        expect(savedStatusCkpt.pending?.signals?.logs?.blocked).toEqual({ code: "otlp_unexpected_status", status: 202 });
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: proxyStatusOtlp.endpointUrl,
            checkpointPath: proxyStatusCkpt,
            namespace: "example-source",
            start: "retained",
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_unexpected_status", status: 202 });
        expect(proxyStatusOtlp.getAttempts()).toBe(1);
        let oversizedOtlpCalls = 0;
        const oversizedOtlpUrl = await listen((_req, res) => {
            oversizedOtlpCalls++;
            res.statusCode = 200;
            res.setHeader("content-type", "application/json");
            res.end("x".repeat(TELEMETRY_OTLP_LIMITS.otlpResponseBytes + 1024));
        });
        const oversizedOtlpCkpt = join(dir, "oversized-otlp.json");
        const oversizedDiags: Array<Record<string, unknown>> = [];
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: `${oversizedOtlpUrl}/v1/logs`,
            checkpointPath: oversizedOtlpCkpt,
            namespace: "example-source",
            start: "retained",
            maxRetries: 2,
            onDiagnostic: (d: Record<string, unknown>) => oversizedDiags.push(d),
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_response_too_large" });
        expect(oversizedOtlpCalls).toBe(1);
        expect(oversizedDiags).toContainEqual(expect.objectContaining({ type: "otlp_response_too_large" }));
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: `${oversizedOtlpUrl}/v1/logs`,
            checkpointPath: oversizedOtlpCkpt,
            namespace: "example-source",
            start: "retained",
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_response_too_large" });
        expect(oversizedOtlpCalls).toBe(1);
        const htmlSourceUrl = await listen((_req, res) => {
            res.statusCode = 200;
            res.setHeader("content-type", "text/html; charset=utf-8");
            res.end("{}");
        });
        await expect(exportQueueTransitionsOnce({
            sourceUrl: htmlSourceUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: join(dir, "html-src.json"),
            namespace: "example-source",
        })).rejects.toMatchObject({ name: "TelemetrySourceError", code: "source_invalid_content_type" });
        const oversizedSourceUrl = await listen((_req, res) => {
            res.statusCode = 200;
            res.setHeader("content-type", "application/json");
            res.end("x".repeat(TELEMETRY_OTLP_LIMITS.sourceResponseBytes + 1024));
        });
        await expect(exportQueueTransitionsOnce({
            sourceUrl: oversizedSourceUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: join(dir, "oversized-src.json"),
            namespace: "example-source",
        })).rejects.toMatchObject({ name: "TelemetrySourceError", code: "source_response_too_large" });
        const reqsBeforeProbe = src.getRequests();
        const reqListBeforeProbe = [...src.reqs];
        const rootNoiseRes = await fetch(`${src.baseUrl}/`);
        expect(rootNoiseRes.status).toBe(404);
        expect(src.getRequests()).toBe(reqsBeforeProbe);
        expect(src.reqs).toEqual(reqListBeforeProbe);
        const badCursorRes = await fetch(`${src.baseUrl}/api/telemetry/v1/queue-transitions?cursor=unknown_opaque_cursor`);
        expect(badCursorRes.status).toBe(400);
        expect(src.getRequests()).toBe(reqsBeforeProbe + 1);
        db.unavailable = true;
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: otlp.endpointUrl,
            checkpointPath: cliCkpt,
            namespace: "example-source",
            start: "retained",
        })).rejects.toMatchObject({ name: "TelemetrySourceError", status: 503 });
    });
    it("persists checkpoint v2 with instanceId/metricsStartUnixNano, refuses v1 and blocked-mismatch checkpoints, and tracks independent per-signal pending states across logs, metrics, and traces", async () => {
        const exportTransaction = (telemetryOtlp as Record<string, any>).exportPendingTransaction;
        const createPending = (telemetryOtlp as Record<string, any>).createPendingTransaction;
        const readCkpt = (telemetryOtlp as Record<string, any>).readCheckpoint;
        const normalizeCfg = (telemetryOtlp as Record<string, any>).normalizeConfig;
        expect(typeof exportTransaction).toBe("function");
        expect(typeof createPending).toBe("function");
        expect(typeof readCkpt).toBe("function");
        expect(typeof normalizeCfg).toBe("function");
        const dir = tempDir();
        let beforeMetricsDisk: any = null;
        const signalQueues: Record<string, ReceiverOutcome[]> = {
            "/v1/logs": [{ status: 200, body: { partialSuccess: { rejectedLogRecords: "1" } } }],
            "/v1/metrics": [
                { status: 503, body: "METRICS_TEMP_DOWN" },
                { status: 200, body: { partialSuccess: { rejectedDataPoints: 2 } } },
            ],
            "/v1/traces": [
                { status: 200, body: { partialSuccess: { rejectedSpans: "1" } } },
            ],
        };
        const signalAttempts: Record<string, number> = { "/v1/logs": 0, "/v1/metrics": 0, "/v1/traces": 0 };
        const signalReceived: Array<{ path: string; json: any }> = [];
        const baseUrl = await listen((req, res) => {
            const path = req.url ?? "";
            signalAttempts[path] = (signalAttempts[path] ?? 0) + 1;
            const chunks: Buffer[] = [];
            req.on("data", chunk => chunks.push(Buffer.from(chunk)));
            req.on("end", () => {
                if (path === "/v1/metrics" && signalAttempts[path] === 1) {
                    beforeMetricsDisk = JSON.parse(readFileSync(join(dir, "multi-signal-v2.json"), "utf8"));
                }
                const rawBody = Buffer.concat(chunks).toString("utf8");
                signalReceived.push({ path, json: JSON.parse(rawBody) });
                const outcome: ReceiverOutcome = signalQueues[path]?.shift() ?? { status: 200, body: {} };
                if (outcome.dropSocket) {
                    req.socket.destroy();
                    return;
                }
                const respond = () => {
                    res.statusCode = outcome.status ?? 200;
                    res.setHeader("content-type", "application/json");
                    for (const [k, v] of Object.entries(outcome.headers ?? {})) res.setHeader(k, v);
                    res.end(typeof outcome.body === "string" ? outcome.body : JSON.stringify(outcome.body ?? {}));
                };
                if (outcome.delayMs) setTimeout(respond, outcome.delayMs);
                else respond();
            });
        });
        const ckptPath = join(dir, "multi-signal-v2.json");
        const config = normalizeCfg({
            sourceUrl: "http://127.0.0.1:7433",
            endpointUrl: `${baseUrl}/v1/logs`,
            checkpointPath: ckptPath,
            namespace: "source-test",
            start: "retained",
            maxRetries: 0,
        });
        expect(config.endpoints).toEqual({
            logs: `${baseUrl}/v1/logs`,
            metrics: `${baseUrl}/v1/metrics`,
            traces: `${baseUrl}/v1/traces`,
        });
        const v1Path = join(dir, "legacy-v1.json");
        writeFileSync(v1Path, JSON.stringify({
            schemaVersion: 1,
            configHash: config.configHash,
            config: {
                stream: "queue-transitions",
                namespace: "source-test",
                sourceUrl: config.sourceUrl,
                endpointUrl: config.endpointUrl,
                start: "retained",
                qitemId: null,
                includeQitemId: false,
            },
            cursor: null,
            source: { hostId: null, bootEpoch: null, sequenceSpaceId: null },
            coverage: { status: "retained_window", historyCompleteness: "unknown" },
            pending: null,
            updatedAt: time,
        }));
        expect(() => readCkpt(normalizeCfg({ ...config, checkpointPath: v1Path }))).toThrow(
            expect.objectContaining({ name: "TelemetryCheckpointError", code: "checkpoint_unsupported_version" }),
        );
        const initialCkpt = readCkpt(config);
        expect(initialCkpt.schemaVersion).toBe(2);
        expect(initialCkpt.instanceId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
        expect(initialCkpt.metricsStartUnixNano).toMatch(/^[1-9][0-9]{18}$/);
        const logsPayload = {
            resourceLogs: [{
                resource: { attributes: [{ key: "service.name", value: { stringValue: "openrig-telemetry-exporter" } }] },
                scopeLogs: [{
                    scope: { name: "openrig.telemetry.queue_transitions", version: "1" },
                    logRecords: [
                        { observedTimeUnixNano: "1759568400000000000", severityNumber: 9, severityText: "INFO", body: { stringValue: "openrig.queue.transition" }, attributes: [{ key: "openrig.transition.id", value: { stringValue: "1" } }, { key: "openrig.delivery.replayed", value: { boolValue: false } }] },
                        { observedTimeUnixNano: "1759568400000000000", severityNumber: 9, severityText: "INFO", body: { stringValue: "openrig.queue.transition" }, attributes: [{ key: "openrig.transition.id", value: { stringValue: "2" } }, { key: "openrig.delivery.replayed", value: { boolValue: false } }] },
                    ],
                }],
            }],
        };
        const metricsPayload = {
            resourceMetrics: [{
                resource: { attributes: [{ key: "service.name", value: { stringValue: "openrig-telemetry-exporter" } }] },
                scopeMetrics: [{
                    scope: { name: "openrig.telemetry.metrics", version: "1" },
                    metrics: [{
                        name: "openrig.queue.transition.observed",
                        unit: "{transition}",
                        sum: {
                            aggregationTemporality: 2,
                            isMonotonic: true,
                            dataPoints: [
                                { startTimeUnixNano: initialCkpt.metricsStartUnixNano, timeUnixNano: "1759568400000000000", asInt: "1", attributes: [{ key: "openrig.queue.state", value: { stringValue: "pending" } }] },
                                { startTimeUnixNano: initialCkpt.metricsStartUnixNano, timeUnixNano: "1759568400000000000", asInt: "1", attributes: [{ key: "openrig.queue.state", value: { stringValue: "done" } }] },
                                { startTimeUnixNano: initialCkpt.metricsStartUnixNano, timeUnixNano: "1759568400000000000", asInt: "1", attributes: [{ key: "openrig.queue.state", value: { stringValue: "blocked" } }] },
                            ],
                        },
                    }],
                }],
            }],
        };
        const fixedTraceId = "0123456789abcdef0123456789abcdef";
        const fixedSpanId = "0123456789abcdef";
        const tracesPayload = {
            resourceSpans: [{
                resource: { attributes: [{ key: "service.name", value: { stringValue: "openrig-telemetry-exporter" } }] },
                scopeSpans: [{
                    scope: { name: "openrig.telemetry.traces", version: "1" },
                    spans: [
                        { traceId: fixedTraceId, spanId: fixedSpanId, name: "openrig.queue.state", kind: 1, startTimeUnixNano: "1759568400000000000", endTimeUnixNano: "1759568405000000000", attributes: [{ key: "openrig.queue.state", value: { stringValue: "in-progress" } }] },
                        { traceId: "fedcba9876543210fedcba9876543210", spanId: "fedcba9876543210", name: "openrig.queue.state", kind: 1, startTimeUnixNano: "1759568405000000000", endTimeUnixNano: "1759568410000000000", attributes: [{ key: "openrig.queue.state", value: { stringValue: "done" } }] },
                    ],
                }],
            }],
        };
        await expect(exportTransaction(config, initialCkpt, {
            nextCursor: "Y3Vyc29yLXYyLTFf",
            discontinuity: false,
            hasMore: true,
            source: { hostId: "host-test", bootEpoch: "boot-test", sequenceSpaceId: null },
            coverage: { status: "retained_window", historyCompleteness: "unknown" },
            signals: {
                logs: { itemCount: 2, payload: logsPayload },
                metrics: { itemCount: 3, payload: metricsPayload },
                traces: { itemCount: 2, payload: tracesPayload },
            },
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "otlp_retry_exhausted", signal: "metrics", status: 503 });
        expect(signalAttempts).toEqual({ "/v1/logs": 1, "/v1/metrics": 1, "/v1/traces": 0 });
        expect(beforeMetricsDisk?.pending?.signals?.metrics).toMatchObject({
            attempted: true,
            outcome: null,
        });
        expect(beforeMetricsDisk?.pending?.signals?.logs).toMatchObject({
            attempted: true,
            outcome: "partial_success",
        });
        expect(beforeMetricsDisk?.pending?.signals?.traces).toMatchObject({
            attempted: false,
            outcome: null,
        });
        const midDisk = JSON.parse(readFileSync(ckptPath, "utf8"));
        expect(midDisk.cursor).toBeNull();
        expect(midDisk.instanceId).toBe(initialCkpt.instanceId);
        expect(midDisk.metricsStartUnixNano).toBe(initialCkpt.metricsStartUnixNano);
        expect(midDisk.pending.signals.logs).toMatchObject({
            itemCount: 2,
            attempted: true,
            outcome: "partial_success",
            rejectedCount: 1,
            partialSuccessWarning: false,
            blocked: null,
        });
        expect(midDisk.pending.signals.metrics).toMatchObject({
            itemCount: 3,
            attempted: true,
            outcome: null,
            rejectedCount: 0,
            blocked: null,
        });
        expect(midDisk.pending.signals.traces).toMatchObject({
            itemCount: 2,
            attempted: false,
            outcome: null,
            rejectedCount: 0,
            blocked: null,
        });
        expect(midDisk.pending.diagnostics).toContainEqual(expect.objectContaining({
            type: "otlp_partial_success",
            signal: "logs",
            rejectedLogRecords: 1,
        }));
        const resumedState = readCkpt(config);
        const finalRes = await exportTransaction(config, resumedState, null);
        expect(signalAttempts).toEqual({ "/v1/logs": 1, "/v1/metrics": 2, "/v1/traces": 1 });
        expect(finalRes).toMatchObject({
            outcome: "partial_success",
            replayedPending: true,
            exportedRecords: 2,
            rejectedLogRecords: 1,
            exportedDataPoints: 3,
            rejectedDataPoints: 2,
            exportedSpans: 2,
            rejectedSpans: 1,
            cursor: "Y3Vyc29yLXYyLTFf",
        });
        expect(finalRes.diagnostics).toEqual(expect.arrayContaining([
            expect.objectContaining({ type: "otlp_partial_success", signal: "logs", rejectedLogRecords: 1 }),
            expect.objectContaining({ type: "otlp_partial_success", signal: "metrics", rejectedDataPoints: 2 }),
            expect.objectContaining({ type: "otlp_partial_success", signal: "traces", rejectedSpans: 1 }),
        ]));
        const tracesReq = signalReceived.find(r => r.path === "/v1/traces");
        expect(tracesReq?.json.resourceSpans[0].scopeSpans[0].spans[0]).toMatchObject({
            traceId: fixedTraceId,
            spanId: fixedSpanId,
            startTimeUnixNano: "1759568400000000000",
            endTimeUnixNano: "1759568405000000000",
        });
        const resolvedDisk = JSON.parse(readFileSync(ckptPath, "utf8"));
        expect(resolvedDisk.pending).toBeNull();
        expect(resolvedDisk.cursor).toBe("Y3Vyc29yLXYyLTFf");
        const mismatchPath = join(dir, "blocked-mismatch.json");
        const mismatchObj = {
            ...midDisk,
            pending: {
                ...midDisk.pending,
                signals: {
                    ...midDisk.pending.signals,
                    metrics: {
                        ...midDisk.pending.signals.metrics,
                        outcome: "exported",
                        blocked: { code: "otlp_permanent_rejection", status: 400 },
                    },
                },
            },
        };
        writeFileSync(mismatchPath, JSON.stringify(mismatchObj));
        expect(() => readCkpt(normalizeCfg({ ...config, checkpointPath: mismatchPath }))).toThrow(
            expect.objectContaining({ name: "TelemetryCheckpointError", code: "checkpoint_malformed" }),
        );
        const worstLogs = Array.from({ length: 161 }, (_, i) => ({
            timeUnixNano: "1759568400000000000",
            observedTimeUnixNano: "1759568400000000000",
            severityNumber: 9,
            severityText: "INFO",
            body: { stringValue: "openrig.queue.transition" },
            attributes: [
                { key: "openrig.record.kind", value: { stringValue: "queue_transition" } },
                { key: "openrig.stream", value: { stringValue: "queue-transitions" } },
                { key: "openrig.transition.id", value: { stringValue: String(i + 1) } },
                { key: "openrig.queue.state", value: { stringValue: "in-progress" } },
                { key: "openrig.identity.provenance", value: { stringValue: "transport:v1" } },
                { key: "openrig.coverage.status", value: { stringValue: "retained_window" } },
                { key: "openrig.coverage.history_completeness", value: { stringValue: "unknown" } },
                { key: "openrig.delivery.replayed", value: { boolValue: false } },
            ],
        }));
        const worstMetrics = [{
            name: "openrig.queue.transition.observed",
            unit: "{transition}",
            sum: {
                aggregationTemporality: 2,
                isMonotonic: true,
                dataPoints: Array.from({ length: 64 }, (_, i) => ({
                    startTimeUnixNano: initialCkpt.metricsStartUnixNano,
                    timeUnixNano: "1759568400000000000",
                    asInt: String(i + 1),
                    attributes: [{ key: "openrig.queue.state", value: { stringValue: "pending" } }],
                })),
            },
        }];
        const worstSpans = Array.from({ length: 128 }, (_, i) => ({
            traceId: (i + 1).toString(16).padStart(32, "0"),
            spanId: (i + 1).toString(16).padStart(16, "0"),
            name: "openrig.queue.state",
            kind: 1,
            startTimeUnixNano: "1759568400000000000",
            endTimeUnixNano: "1759568405000000000",
            attributes: [{ key: "openrig.queue.state", value: { stringValue: "in-progress" } }],
        }));
        const worstPending = createPending({
            nextCursor: "Y3Vyc29yLXdvcnN0",
            discontinuity: false,
            hasMore: true,
            source: { hostId: "host-test", bootEpoch: "boot-test", sequenceSpaceId: null },
            coverage: { status: "retained_window", historyCompleteness: "unknown" },
            signals: {
                logs: { itemCount: 161, payload: { resourceLogs: [{ resource: { attributes: [] }, scopeLogs: [{ scope: { name: "openrig.telemetry.queue_transitions", version: "1" }, logRecords: worstLogs }] }] } },
                metrics: { itemCount: 64, payload: { resourceMetrics: [{ resource: { attributes: [] }, scopeMetrics: [{ scope: { name: "openrig.telemetry.metrics", version: "1" }, metrics: worstMetrics }] }] } },
                traces: { itemCount: 128, payload: { resourceSpans: [{ resource: { attributes: [] }, scopeSpans: [{ scope: { name: "openrig.telemetry.traces", version: "1" }, spans: worstSpans }] }] } },
            },
        });
        const worstCkptText = JSON.stringify({ ...initialCkpt, pending: worstPending }) + "\n";
        expect(Buffer.byteLength(worstCkptText)).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.checkpointBytes);
    });
    it("aborts in-flight HTTP and retry backoffs promptly without further retries while preserving pending state, and executes CLI --help through a symlink", async () => {
        const db = fixture();
        transitions(db, 1);
        const src = await startSourceServer(db);
        let receiverCalls = 0;
        const slowUrl = await listen((_req, res) => {
            receiverCalls++;
            setTimeout(() => {
                res.statusCode = 503;
                res.setHeader("content-type", "application/json");
                res.end("{}");
            }, 5000);
        });
        const dir = tempDir();
        const ckpt = join(dir, "abort-pending.json");
        const ac = new AbortController();
        setTimeout(() => ac.abort(), 60);
        const started = Date.now();
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: `${slowUrl}/v1/logs`,
            checkpointPath: ckpt,
            namespace: "source-test",
            start: "retained",
            timeoutMs: 10000,
            retryMs: 5000,
            maxRetryMs: 10000,
            maxRetries: 3,
            signal: ac.signal,
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "exporter_aborted" });
        const elapsed = Date.now() - started;
        expect(elapsed).toBeLessThan(1500);
        expect(receiverCalls).toBe(1);
        const savedAfterAbort = JSON.parse(readFileSync(ckpt, "utf8"));
        expect(savedAfterAbort.cursor).toBeNull();
        expect(savedAfterAbort.pending).not.toBeNull();
        expect(savedAfterAbort.pending.signals.logs.attempted).toBe(true);
        expect(savedAfterAbort.pending.signals.logs.outcome).toBeNull();
        let retryCalls = 0;
        const retryUrl = await listen((_req, res) => {
            retryCalls++;
            res.statusCode = 503;
            res.setHeader("content-type", "application/json");
            res.end("{}");
        });
        const retryCkpt = join(dir, "abort-backoff.json");
        const backoffAc = new AbortController();
        setTimeout(() => backoffAc.abort(), 60);
        const backoffStarted = Date.now();
        await expect(exportQueueTransitionsOnce({
            sourceUrl: src.baseUrl,
            endpointUrl: `${retryUrl}/v1/logs`,
            checkpointPath: retryCkpt,
            namespace: "source-test",
            start: "retained",
            timeoutMs: 5000,
            retryMs: 5000,
            maxRetryMs: 10000,
            maxRetries: 3,
            signal: backoffAc.signal,
        })).rejects.toMatchObject({ name: "TelemetryExportError", code: "exporter_aborted" });
        expect(Date.now() - backoffStarted).toBeLessThan(1500);
        expect(retryCalls).toBe(1);
        const symlinkPath = join(dir, "telemetry-otlp-symlink.mjs");
        symlinkSync(exporterScriptPath, symlinkPath);
        const symHelp = await execFileAsync(process.execPath, [symlinkPath, "--help"], { encoding: "utf8" });
        expect(symHelp.stdout).toContain("usage: node telemetry-otlp.mjs");
        expect(symHelp.stdout).toContain("--endpoint");

        // L1: real child process receiving SIGTERM (and SIGINT) during in-flight OTLP request exits cleanly with code 0,
        // preserves pending state on disk with attempted=true, writes no failure-ledger entry, and does not retry;
        // genuine CLI errors still exit with code 1.
        let sigtermReceiverCalls = 0;
        const sigtermCkpt = join(dir, "child-sigterm.json");
        let notifyInFlight!: () => void;
        const inFlightPromise = new Promise<void>((r) => { notifyInFlight = r; });
        const sigtermReceiverUrl = await listen((_req, res) => {
            sigtermReceiverCalls++;
            notifyInFlight();
            setTimeout(() => {
                res.statusCode = 200;
                res.setHeader("content-type", "application/json");
                res.end("{}");
            }, 10_000);
        });
        const runChildSignal = async (signalName: NodeJS.Signals, childCkpt: string, receiverBaseUrl: string, waitForRequest = true) => {
            const child = spawn(process.execPath, [
                symlinkPath,
                "--source", src.baseUrl,
                "--endpoint", `${receiverBaseUrl}/v1/logs`,
                "--checkpoint", childCkpt,
                "--namespace", "source-test",
                "--start", "retained",
                "--signals", "logs",
                "--timeout-ms", "10000",
                "--retry-ms", "5000",
                "--max-retry-ms", "10000",
                "--max-retries", "3",
                "--once",
            ], { stdio: ["ignore", "pipe", "pipe"] });
            let stdout = "";
            let stderr = "";
            child.stdout.on("data", (d) => { stdout += String(d); });
            child.stderr.on("data", (d) => { stderr += String(d); });
            if (waitForRequest) await inFlightPromise;
            child.kill(signalName);
            const [code, termSig] = await new Promise<[number | null, NodeJS.Signals | null]>((resolveExit) => {
                child.once("exit", (c, s) => resolveExit([c, s]));
            });
            return { code, termSig, stdout, stderr };
        };
        const sigtermRes = await runChildSignal("SIGTERM", sigtermCkpt, sigtermReceiverUrl, true);
        expect(sigtermRes.code).toBe(0);
        expect(sigtermRes.termSig).toBeNull();
        expect(sigtermRes.stderr).toBe("");
        expect(sigtermReceiverCalls).toBe(1);
        expect(existsSync(sigtermCkpt)).toBe(true);
        const sigtermSaved = JSON.parse(readFileSync(sigtermCkpt, "utf8"));
        expect(sigtermSaved.cursor).toBeNull();
        expect(sigtermSaved.pending).not.toBeNull();
        expect(sigtermSaved.pending.signals.logs).toMatchObject({ attempted: true, outcome: null });
        expect(sigtermSaved.ledger.entries).toEqual([]);

        // Also verify SIGINT during slow source request exits 0 cleanly without error output,
        // synchronizing on the child's actual GET /api/telemetry/v1/queue-transitions request
        // (ignoring unrelated GET / probes) with bounded readiness and child cleanup.
        let slowSourceCalls = 0;
        let notifySourceInFlight!: () => void;
        const sourceInFlight = new Promise<void>((r) => { notifySourceInFlight = r; });
        const slowSourceUrl = await listen((req, res) => {
            const reqUrl = new URL(req.url ?? "/", "http://127.0.0.1");
            if (req.method === "GET" && reqUrl.pathname === "/api/telemetry/v1/queue-transitions") {
                slowSourceCalls++;
                notifySourceInFlight();
                setTimeout(() => {
                    res.statusCode = 200;
                    res.setHeader("content-type", "application/json");
                    res.end("{}");
                }, 10_000);
                return;
            }
            res.statusCode = 404;
            res.end();
        });
        const probeRes = await fetch(`${slowSourceUrl}/`);
        expect(probeRes.status).toBe(404);
        expect(slowSourceCalls).toBe(0);
        const sigintCkpt = join(dir, "child-sigint.json");
        const sigintChild = spawn(process.execPath, [
            symlinkPath,
            "--source", slowSourceUrl,
            "--endpoint", `${sigtermReceiverUrl}/v1/logs`,
            "--checkpoint", sigintCkpt,
            "--namespace", "source-test",
            "--once",
        ], { stdio: ["ignore", "pipe", "pipe"] });
        let sigintStderr = "";
        sigintChild.stderr.on("data", (d) => { sigintStderr += String(d); });
        const sigintExit = new Promise<[number | null, NodeJS.Signals | null]>((resolveExit) => {
            sigintChild.once("exit", (c, s) => resolveExit([c, s]));
        });
        try {
            let readinessTimer: ReturnType<typeof setTimeout> | undefined;
            try {
                await Promise.race([
                    sourceInFlight,
                    sigintExit.then(([c, s]) => {
                        throw new Error(`sigintChild exited prematurely before source request (code=${String(c)}, signal=${String(s)}, stderr=${sigintStderr})`);
                    }),
                    new Promise<never>((_, reject) => {
                        readinessTimer = setTimeout(() => reject(new Error("Timed out waiting for sigintChild source GET request")), 5_000);
                    }),
                ]);
            } finally {
                if (readinessTimer !== undefined) clearTimeout(readinessTimer);
            }
            sigintChild.kill("SIGINT");
            const [sigintCode, sigintSig] = await sigintExit;
            expect(sigintCode).toBe(0);
            expect(sigintSig).toBeNull();
            expect(sigintStderr).toBe("");
            expect(slowSourceCalls).toBe(1);
            expect(existsSync(sigintCkpt)).toBe(false);
        } finally {
            if (sigintChild.exitCode === null && sigintChild.signalCode === null) {
                sigintChild.kill("SIGKILL");
                await sigintExit;
            }
        }

        // Genuine CLI failure (without SIGINT/SIGTERM) still exits 1 with [telemetry-otlp] error output.
        expect(await runExporterCli(["--source", "not-a-url", "--endpoint", `${sigtermReceiverUrl}/v1/logs`, "--checkpoint", join(dir, "bad.json"), "--namespace", "source-test", "--once"])).toBe(1);
    });
});

// ---------------------------------------------------------------------------------------------
// S2: observed queue counters, recorded state-duration histograms, completed-interval spans,
// source gaps/eviction and the durable failure ledger, over a loopback HTTP source fixture and a three-signal HTTP receiver.
// ---------------------------------------------------------------------------------------------
type SignalPath = "/v1/logs" | "/v1/metrics" | "/v1/traces";
async function startSignalReceiver() {
    const received: Array<{ path: SignalPath; raw: string; json: any }> = [];
    const outcomes: Record<string, ReceiverOutcome[]> = { "/v1/logs": [], "/v1/metrics": [], "/v1/traces": [] };
    const failAll = new Set<string>();
    const allowedPaths = new Set(["/v1/logs", "/v1/metrics", "/v1/traces"]);
    const baseUrl = await listen((req, res) => {
        const u = new URL(req.url ?? "/", "http://127.0.0.1");
        if (req.method !== "POST" || !allowedPaths.has(u.pathname)) {
            res.statusCode = allowedPaths.has(u.pathname) ? 405 : 404;
            res.setHeader("content-type", "application/json");
            res.end("{}");
            return;
        }
        const chunks: Buffer[] = [];
        req.on("data", chunk => chunks.push(Buffer.from(chunk)));
        req.on("end", () => {
            const raw = Buffer.concat(chunks).toString("utf8");
            const path = u.pathname as SignalPath;
            let parsedJson: any;
            try {
                parsedJson = JSON.parse(raw);
            } catch {
                res.statusCode = 400;
                res.setHeader("content-type", "application/json");
                res.end(JSON.stringify({ code: "otlp_malformed_json" }));
                return;
            }
            received.push({ path, raw, json: parsedJson });
            const outcome = failAll.has(path) ? { status: 503, body: "DOWN" } : (outcomes[path]?.shift() ?? { status: 200, body: {} });
            res.statusCode = outcome.status ?? 200;
            res.setHeader("content-type", "application/json");
            res.end(typeof outcome.body === "string" ? outcome.body : JSON.stringify(outcome.body ?? {}));
        });
    });
    return {
        baseUrl, endpointUrl: `${baseUrl}/v1/logs`, received, outcomes, failAll,
        by: (path: SignalPath) => received.filter(r => r.path === path),
    };
}
const attrMapOf = (attrs: any[] = []) => Object.fromEntries(attrs.map(a => [a.key, a.value?.stringValue ?? a.value?.intValue ?? a.value?.boolValue ?? a.value?.doubleValue]));
function metricPoints(batches: Array<{ json: any }>) {
    const out: Array<{ name: string; kind: "sum" | "histogram" | "gauge"; metric: any; point: any; attrs: Record<string, any> }> = [];
    for (const b of batches)
        for (const rm of b.json.resourceMetrics ?? [])
            for (const sm of rm.scopeMetrics ?? [])
                for (const metric of sm.metrics ?? []) {
                    const kind = metric.sum ? "sum" : metric.histogram ? "histogram" : "gauge";
                    for (const point of metric[kind].dataPoints) out.push({ name: metric.name, kind, metric: metric[kind], point, attrs: attrMapOf(point.attributes) });
                }
    return out;
}
const lastBatchPoints = (r: { by: (p: SignalPath) => Array<{ json: any }> }) => metricPoints([r.by("/v1/metrics").at(-1)!]);
const spansOf = (batches: Array<{ json: any }>) => batches.flatMap(b => b.json.resourceSpans.flatMap((rs: any) => rs.scopeSpans.flatMap((ss: any) => ss.spans)));
const logRecordsOf = (batches: Array<{ json: any }>) => batches.flatMap(b => (b.json.resourceLogs ?? []).flatMap((rl: any) => rl.scopeLogs.flatMap((sl: any) => sl.logRecords.map((r: any) => ({ body: r.body.stringValue, attrs: attrMapOf(r.attributes), rec: r })))));
const readJson = (path: string) => JSON.parse(readFileSync(path, "utf8"));
const nano = (iso: string) => String(BigInt(Date.parse(iso)) * 1_000_000n);
function qt(db: FixtureStore, id: number, qitem: string, ts: string, state: string, closure: string | null = null, gaps?: FixtureGap[]) {
    const createdAt = toCreatedAtIso(ts);
    const autoGaps = gaps ?? (createdAt === null ? [{ code: "invalid_timestamp", id: String(id) }] : undefined);
    pushFixtureRow(db, {
        id,
        qitemId: qitem,
        ts,
        state,
        actorSession: "actor-secret@rig",
        closureReason: closure,
        closureTarget: closure ? "target-secret@rig" : null,
        identityProvenance: "transport:v1",
        ...(autoGaps ? { gaps: autoGaps } : {}),
    });
}
async function drain(options: Record<string, unknown>, maxPages = 40) {
    let last: any = null;
    for (let i = 0; i < maxPages; i++) {
        last = await exportAllSignalsOnce(options);
        if (!last.hasMore) return last;
    }
    throw new Error("drain did not complete");
}
async function intervalHarness(opts: { getSource?: () => { hostId: string | null; bootEpoch: string | null }; inventory?: MockInventoryState } = {}) {
    const db = fixture();
    const src = await startSourceServer(db, opts.getSource, opts.inventory);
    const rx = await startSignalReceiver();
    const dir = tempDir();
    const checkpointPath = join(dir, "ckpt.json");
    const options: Record<string, unknown> = { sourceUrl: src.baseUrl, endpointUrl: rx.endpointUrl, checkpointPath, namespace: "source-test", start: "retained", maxRetries: 0, retryMs: 1 };
    return { db, src, rx, dir, checkpointPath, options };
}

describe("reference OTLP queue-state intervals, counters, spans and failure ledger", () => {
    it("completes recorded intervals across pages, keeps the original entry on repeated state, and exports stable cumulative counters, histogram and root INTERNAL spans", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-secret-1", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-secret-1", "2026-10-04 09:00:05", "pending");
        qt(h.db, 3, "q-secret-1", "2026-10-04 09:00:30", "in-progress");
        qt(h.db, 4, "q-secret-1", "2026-10-04 09:01:30", "done", "no-follow-on");
        const first = await exportAllSignalsOnce({ ...h.options, limit: 2 });
        expect(first).toMatchObject({ outcome: "exported", hasMore: true });
        expect(h.rx.by("/v1/traces")).toHaveLength(0);
        const afterFirst = readJson(h.checkpointPath);
        expect(Object.values(afterFirst.queue.open)).toEqual([{ s: "pending", t: nano("2026-10-04T09:00:00Z"), id: "1" }]);
        expect(Object.keys(afterFirst.queue.open)[0]).toMatch(/^[0-9a-f]{32}$/);
        expect(JSON.stringify(afterFirst)).not.toContain("q-secret-1");
        await exportAllSignalsOnce({ ...h.options, limit: 2 });
        const ckpt = readJson(h.checkpointPath);
        expect(ckpt.queue.open).toEqual({});
        // counters: cumulative monotonic sums with temporality 2 and decimal int64 values, stable start time.
        const final = lastBatchPoints(h.rx);
        const counters = final.filter(p => p.name === "openrig.queue.transition.observed");
        expect(counters.every(p => p.kind === "sum" && p.metric.aggregationTemporality === 2 && p.metric.isMonotonic === true)).toBe(true);
        const byKey = Object.fromEntries(counters.map(p => [`${p.attrs["openrig.queue.state"]}|${p.attrs["openrig.queue.closure_reason"] ?? "-"}`, p.point.asInt]));
        expect(byKey).toEqual({ "pending|-": "2", "in-progress|-": "1", "done|no-follow-on": "1" });
        for (const p of final) {
            expect(p.point.startTimeUnixNano).toBe(ckpt.metricsStartUnixNano);
            expect(BigInt(p.point.timeUnixNano)).toBeGreaterThanOrEqual(BigInt(ckpt.metricsStartUnixNano));
        }
        const hist = final.filter(p => p.name === "openrig.queue.state.duration");
        expect(hist).toHaveLength(2);
        expect(hist[0]!.metric.aggregationTemporality).toBe(2);
        expect(hist[0]!.metric).toBeDefined();
        const pendingToProgress = hist.find(p => p.attrs["openrig.queue.state"] === "pending")!;
        expect(pendingToProgress.attrs).toEqual({ "openrig.queue.state": "pending", "openrig.queue.departure_state": "in-progress" });
        expect(pendingToProgress.point.count).toBe("1");
        expect(pendingToProgress.point.sum).toBe(30);
        expect(pendingToProgress.point.explicitBounds).toEqual([1, 5, 15, 60, 300, 900, 3600, 14400, 86400, 604800]);
        expect(pendingToProgress.point.bucketCounts).toEqual(["0", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0"]);
        expect(final.find(p => p.name === "openrig.queue.state.duration" && p.attrs["openrig.queue.state"] === "in-progress")!.point.sum).toBe(60);
        expect(final.find(p => p.name === "openrig.queue.state.duration")!.metric.aggregationTemporality).toBe(2);
        const unit = (n: string) => h.rx.by("/v1/metrics").at(-1)!.json.resourceMetrics[0].scopeMetrics[0].metrics.find((m: any) => m.name === n).unit;
        expect(unit("openrig.queue.state.duration")).toBe("s");
        expect(unit("openrig.queue.transition.observed")).toBe("{transition}");
        // spans: root INTERNAL, generated ids, no parent/links, source times, transition ids; no queue id by default.
        const spans = spansOf(h.rx.by("/v1/traces"));
        expect(spans).toHaveLength(2);
        const span = spans.find((s: any) => attrMapOf(s.attributes)["openrig.queue.state"] === "pending");
        expect(span).toMatchObject({ name: "openrig.queue.state", kind: 1, startTimeUnixNano: nano("2026-10-04T09:00:00Z"), endTimeUnixNano: nano("2026-10-04T09:00:30Z") });
        expect(span.traceId).toMatch(/^[0-9a-f]{32}$/);
        expect(span.spanId).toMatch(/^[0-9a-f]{16}$/);
        expect(span.parentSpanId).toBeUndefined();
        expect(span.links).toBeUndefined();
        expect(attrMapOf(span.attributes)).toEqual({
            "openrig.record.kind": "queue_state_interval",
            "openrig.queue.state": "pending",
            "openrig.queue.departure_state": "in-progress",
            "openrig.transition.enter_id": "1",
            "openrig.transition.depart_id": "3",
        });
        expect(new Set(spans.map((s: any) => s.traceId)).size).toBe(2);
        // identity: stable resource across batches, no boot epoch in resource identity, boot epoch kept on log records.
        const resources = h.rx.received.map(r => (r.json.resourceMetrics ?? r.json.resourceSpans ?? r.json.resourceLogs)[0].resource);
        const attrsOfResource = resources.map(r => attrMapOf(r.attributes));
        for (const a of attrsOfResource) {
            expect(a).toMatchObject({ "service.name": "openrig-telemetry-exporter", "service.namespace": "source-test", "service.instance.id": ckpt.instanceId });
            expect(a["openrig.source.boot_epoch"]).toBeUndefined();
        }
        expect(logRecordsOf(h.rx.by("/v1/logs")).every(r => r.attrs["openrig.source.boot_epoch"] === "boot-test")).toBe(true);
        // privacy: no actor, note, closure target or raw queue id anywhere on the wire.
        const wire = h.rx.received.map(r => r.raw).join("\n");
        for (const secret of ["actor-secret", "target-secret", "PRIVATE_NOTE", "q-secret-1"]) expect(wire).not.toContain(secret);
    });

    it("exports the queue id on spans only when opted in and never as a metric label", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-visible-1", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-visible-1", "2026-10-04 09:00:10", "in-progress");
        await drain({ ...h.options, includeQitemId: true });
        expect(attrMapOf(spansOf(h.rx.by("/v1/traces"))[0].attributes)["openrig.qitem.id"]).toBe("q-visible-1");
        expect(metricPoints(h.rx.by("/v1/metrics")).some(p => JSON.stringify(p.point.attributes).includes("q-visible-1"))).toBe(false);
        const allowed = new Set(["openrig.queue.state", "openrig.queue.closure_reason", "openrig.queue.departure_state", "openrig.gap.code", "openrig.failure.stage", "openrig.failure.category"]);
        for (const p of metricPoints(h.rx.by("/v1/metrics"))) for (const key of Object.keys(p.attrs)) expect(allowed.has(key)).toBe(true);
    });

    it("never spans a real source gap inside one page and recovers on the next page without clearing then recreating a cross-gap duration", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-b", "2026-10-04 09:00:00", "pending");
        qt(h.db, 5, "q-a", "2026-10-04 09:00:10", "in-progress", null, [{ code: "retained_sequence_gap", from: "3", through: "4" }]); // ids 3 and 4 are absent: retained_sequence_gap
        qt(h.db, 6, "q-b", "2026-10-04 09:00:20", "in-progress");
        const page1 = await exportAllSignalsOnce({ ...h.options });
        expect(page1.hasMore).toBe(false);
        expect(logRecordsOf(h.rx.by("/v1/logs")).map(r => r.attrs["openrig.gap.code"]).filter(Boolean)).toEqual(["retained_sequence_gap", "page_intervals_suppressed"]);
        expect(h.rx.by("/v1/traces")).toHaveLength(0);
        expect(metricPoints(h.rx.by("/v1/metrics")).filter(p => p.name === "openrig.queue.state.duration")).toHaveLength(0);
        const ckpt = readJson(h.checkpointPath);
        expect(ckpt.queue.open).toEqual({}); // rows of a gap page open nothing: no entry may later complete across the unknown rows
        const gaps = Object.fromEntries(lastBatchPoints(h.rx).filter(p => p.name === "openrig.telemetry.coverage.gap").map(p => [p.attrs["openrig.gap.code"], p.point.asInt]));
        expect(gaps).toEqual({ retained_sequence_gap: "1", page_intervals_suppressed: "1" });
        const transitionTotal = lastBatchPoints(h.rx).filter(p => p.name === "openrig.queue.transition.observed").reduce((n, p) => n + Number(p.point.asInt), 0);
        expect(transitionTotal).toBe(4); // observed records are still counted
        qt(h.db, 7, "q-c", "2026-10-04 09:01:00", "pending");
        qt(h.db, 8, "q-c", "2026-10-04 09:01:40", "in-progress");
        qt(h.db, 9, "q-a", "2026-10-04 09:02:00", "done");
        await drain(h.options);
        const spans = spansOf(h.rx.by("/v1/traces"));
        expect(spans).toHaveLength(1);
        expect(attrMapOf(spans[0].attributes)).toMatchObject({ "openrig.queue.state": "pending", "openrig.transition.enter_id": "7", "openrig.transition.depart_id": "8" });
    });

    it("never invents durations for null, reversed or missing boundaries and records the invalid boundary", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-null", "2026-10-04 09:00:10", "pending");
        qt(h.db, 2, "q-null", "not-a-timestamp", "in-progress");
        qt(h.db, 3, "q-rev", "2026-10-04 09:00:20", "pending");
        qt(h.db, 4, "q-rev", "2026-10-04 09:00:10", "in-progress"); // reversed
        qt(h.db, 5, "q-rev", "2026-10-04 09:00:40", "done");
        qt(h.db, 6, "q-first", "2026-10-04 09:00:50", "done"); // no open boundary
        qt(h.db, 7, "q-mid", "2026-10-04 09:00:00", "in-progress"); // first observation is not "pending"
        qt(h.db, 8, "q-mid", "2026-10-04 09:00:07", "done");
        await drain(h.options);
        const spans = spansOf(h.rx.by("/v1/traces"));
        // q-rev: pending->in-progress is reversed (invalid); in-progress(09:00:10, own valid entry) -> done(09:00:40) = 30s; q-mid in-progress->done = 7s.
        expect(spans.map((s: any) => [attrMapOf(s.attributes)["openrig.queue.state"], attrMapOf(s.attributes)["openrig.transition.enter_id"], attrMapOf(s.attributes)["openrig.transition.depart_id"]])).toEqual([
            ["in-progress", "4", "5"],
            ["in-progress", "7", "8"],
        ]);
        const gaps = Object.fromEntries(lastBatchPoints(h.rx).filter(p => p.name === "openrig.telemetry.coverage.gap").map(p => [p.attrs["openrig.gap.code"], p.point.asInt]));
        expect(gaps.invalid_interval_boundary).toBe("2");
        expect(gaps.invalid_timestamp).toBe("1");
        expect(gaps.page_intervals_suppressed).toBeUndefined(); // row-local gaps do not suppress valid intervals
    });

    it("drops open intervals at a daemon boot discontinuity without resetting cumulative series or the metric resource", async () => {
        let boot = "boot-1";
        const h = await intervalHarness({ getSource: () => ({ hostId: "host-test", bootEpoch: boot }) });
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        await drain(h.options);
        const before = readJson(h.checkpointPath);
        expect(Object.keys(before.queue.open)).toHaveLength(1);
        boot = "boot-2";
        await exportAllSignalsOnce(h.options); // discontinuity: named gap, fresh restart cursor
        const mid = readJson(h.checkpointPath);
        expect(mid.queue.open).toEqual({});
        expect(mid.instanceId).toBe(before.instanceId);
        expect(mid.metricsStartUnixNano).toBe(before.metricsStartUnixNano);
        qt(h.db, 2, "q-a", "2026-10-04 09:00:30", "in-progress");
        await drain(h.options);
        expect(h.rx.by("/v1/traces")).toHaveLength(0);
        const counters = lastBatchPoints(h.rx).filter(p => p.name === "openrig.queue.transition.observed");
        expect(counters.reduce((n, p) => n + Number(p.point.asInt), 0)).toBe(2); // pending(1) stays cumulative across the discontinuity + in-progress(1)
        const gapCodes = lastBatchPoints(h.rx).filter(p => p.name === "openrig.telemetry.coverage.gap").map(p => p.attrs["openrig.gap.code"]);
        expect(gapCodes).toContain("boot_boundary_history_unverified");
        const metricResources = h.rx.by("/v1/metrics").map(r => JSON.stringify(r.json.resourceMetrics[0].resource));
        expect(new Set(metricResources).size).toBe(1);
        expect(new Set(h.rx.by("/v1/logs").map(r => JSON.stringify(r.json.resourceLogs[0].resource))).size).toBe(1);
        expect(logRecordsOf(h.rx.by("/v1/logs")).map(r => r.attrs["openrig.source.boot_epoch"])).toContain("boot-2");
    });

    it("bounds open intervals at 512, evicts the oldest with a visible diagnostic, and never completes an evicted interval", async () => {
        const h = await intervalHarness();
        expect(TELEMETRY_OTLP_LIMITS.maxOpenIntervals).toBe(512);
        for (let i = 0; i < 513; i++) qt(h.db, i + 1, `q-${i}`, "2026-10-04 09:00:00", "pending");
        await drain(h.options);
        const ckpt = readJson(h.checkpointPath);
        expect(Object.keys(ckpt.queue.open)).toHaveLength(512);
        expect(ckpt.queue.agg.gaps.open_interval_evicted).toBe(1);
        const evictedLog = logRecordsOf(h.rx.by("/v1/logs")).find(r => r.attrs["openrig.gap.code"] === "open_interval_evicted")!;
        expect(evictedLog.attrs["openrig.gap.count"]).toBe("1");
        qt(h.db, 514, "q-0", "2026-10-04 09:00:30", "in-progress"); // evicted (oldest)
        qt(h.db, 515, "q-512", "2026-10-04 09:00:40", "in-progress"); // retained
        await drain(h.options);
        const spans = spansOf(h.rx.by("/v1/traces"));
        expect(spans).toHaveLength(1);
        expect(attrMapOf(spans[0].attributes)["openrig.transition.enter_id"]).toBe("513");
    });

    it("keeps source cursor and derived state unchanged while any signal is unresolved, replays only unresolved signals with identical span ids, then adopts once", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-a", "2026-10-04 09:00:30", "in-progress");
        h.rx.outcomes["/v1/metrics"]!.push({ status: 200, body: { partialSuccess: { rejectedDataPoints: 1 } } });
        h.rx.outcomes["/v1/traces"]!.push({ status: 503, body: "TRACES_DOWN" });
        await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ code: "otlp_retry_exhausted", signal: "traces" });
        const failedDisk = readJson(h.checkpointPath);
        expect(failedDisk.cursor).toBeNull();
        expect(failedDisk.queue).toEqual({ open: {}, agg: { transitions: {}, hist: {}, gaps: {}, failures: {}, lastNano: "0" } });
        expect(failedDisk.pending.derived.queue.agg.transitions).toEqual({ "pending|-": 1, "in-progress|-": 1 });
        expect(failedDisk.pending.signals.logs.outcome).toBe("exported");
        expect(failedDisk.pending.signals.metrics.outcome).toBe("partial_success");
        expect(failedDisk.pending.signals.traces).toMatchObject({ attempted: true, outcome: null });
        expect(failedDisk.ledger.entries.map((e: any) => [e.stage, e.category, e.signal])).toEqual([["export", "otlp_partial_success", "metrics"], ["export", "otlp_retry_exhausted", "traces"]]);
        const firstSpanIds = spansOf(h.rx.by("/v1/traces")).map((s: any) => `${s.traceId}/${s.spanId}`);
        const requestsBefore = h.src.getRequests();
        const logsBefore = h.rx.by("/v1/logs").length;
        const metricsBefore = h.rx.by("/v1/metrics").length;
        const resumed = await exportAllSignalsOnce(h.options);
        expect(resumed).toMatchObject({ replayedPending: true, outcome: "partial_success" });
        expect(h.src.getRequests()).toBe(requestsBefore); // ingestion paused while pending
        expect(h.rx.by("/v1/logs")).toHaveLength(logsBefore);
        expect(h.rx.by("/v1/metrics")).toHaveLength(metricsBefore);
        expect(spansOf(h.rx.by("/v1/traces")).map((s: any) => `${s.traceId}/${s.spanId}`)).toEqual([...firstSpanIds, ...firstSpanIds]);
        const adopted = readJson(h.checkpointPath);
        expect(adopted.pending).toBeNull();
        expect(adopted.cursor).not.toBeNull();
        expect(adopted.queue.agg.transitions).toEqual({ "pending|-": 1, "in-progress|-": 1 });
        expect(adopted.ledger.entries).toHaveLength(2); // failures observed during this transaction survive adoption
        // The next page reports each failure exactly once, as a log and a cumulative counter, and the ledger is then empty.
        qt(h.db, 3, "q-a", "2026-10-04 09:01:00", "done");
        await exportAllSignalsOnce(h.options);
        const failureLogs = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.telemetry.failure");
        expect(failureLogs.map(r => [r.attrs["openrig.failure.stage"], r.attrs["openrig.failure.category"], r.attrs["openrig.signal"]])).toEqual([["export", "otlp_partial_success", "metrics"], ["export", "otlp_retry_exhausted", "traces"]]);
        expect(failureLogs[0]!.attrs["openrig.failure.items"]).toBe("1");
        const failures = Object.fromEntries(lastBatchPoints(h.rx).filter(p => p.name === "openrig.telemetry.failure").map(p => [`${p.attrs["openrig.failure.stage"]}|${p.attrs["openrig.failure.category"]}`, p.point.asInt]));
        expect(failures).toEqual({ "export|otlp_partial_success": "1", "export|otlp_retry_exhausted": "1" });
        expect(readJson(h.checkpointPath).ledger).toMatchObject({ entries: [], dropped: 0 });
        await exportAllSignalsOnce(h.options);
        qt(h.db, 4, "q-z", "2026-10-04 09:02:00", "pending");
        await exportAllSignalsOnce(h.options);
        expect(Object.fromEntries(lastBatchPoints(h.rx).filter(p => p.name === "openrig.telemetry.failure").map(p => [`${p.attrs["openrig.failure.stage"]}|${p.attrs["openrig.failure.category"]}`, p.point.asInt]))).toEqual(failures);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.telemetry.failure")).toHaveLength(2);
    });

    it("accounts permanent rejection once (stop does not re-count on blocked reruns) and records dropped items when explicitly dropped", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-a", "2026-10-04 09:00:30", "in-progress");
        h.rx.outcomes["/v1/traces"]!.push({ status: 400, body: "RAW_BODY_MUST_NOT_LEAK" });
        await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ code: "otlp_permanent_rejection", signal: "traces" });
        await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ code: "otlp_permanent_rejection" });
        const blocked = readJson(h.checkpointPath);
        expect(blocked.cursor).toBeNull();
        expect(blocked.queue.agg.transitions).toEqual({});
        expect(blocked.ledger.entries.map((e: any) => e.category)).toEqual(["otlp_permanent_rejection"]);
        const dropped = await exportAllSignalsOnce({ ...h.options, onPermanentError: "drop" });
        expect(dropped).toMatchObject({ droppedSpans: 1 });
        const after = readJson(h.checkpointPath);
        expect(after.pending).toBeNull();
        expect(after.queue.agg.transitions).toEqual({ "pending|-": 1, "in-progress|-": 1 }); // cumulative state is adopted so later metrics heal the loss
        expect(after.ledger.entries.map((e: any) => [e.category, e.items])).toEqual([["otlp_permanent_rejection", 0], ["otlp_dropped_permanent_rejection", 1]]);
        expect(JSON.stringify(after)).not.toContain("RAW_BODY_MUST_NOT_LEAK");
    });

    it("bounds the failure ledger at 32 entries with an explicit overflow count that is reported without loss or double count", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        h.rx.failAll.add("/v1/logs");
        for (let i = 0; i < 33; i++) await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ code: "otlp_retry_exhausted" });
        const saturated = readJson(h.checkpointPath);
        expect(saturated.ledger.entries).toHaveLength(32);
        expect(saturated.ledger.dropped).toBe(1);
        h.rx.failAll.clear();
        await exportAllSignalsOnce(h.options); // resolves the original pending page
        qt(h.db, 2, "q-a", "2026-10-04 09:00:10", "in-progress");
        await drain(h.options);
        await drain(h.options);
        await drain(h.options);
        const final = readJson(h.checkpointPath);
        expect(final.ledger).toMatchObject({ entries: [], dropped: 0 });
        expect(final.queue.agg.failures).toEqual({ "export|otlp_retry_exhausted": 32, "ledger|diagnostic_ledger_overflow": 1 });
        const failureLogs = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.telemetry.failure");
        expect(failureLogs).toHaveLength(33); // 32 entries + 1 overflow summary
        expect(failureLogs.every((r, i, all) => i === 0 || BigInt(r.rec.observedTimeUnixNano) >= BigInt(all[i - 1]!.rec.observedTimeUnixNano))).toBe(true);
        expect(TELEMETRY_OTLP_LIMITS.maxRecordsPerBatch).toBeGreaterThanOrEqual(161 + 31);
    });

    it("records source failures in the durable ledger without advancing the cursor and reports them after recovery", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        h.db.unavailable = true;
        await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ name: "TelemetrySourceError", status: 503 });
        const failed = readJson(h.checkpointPath);
        expect(failed.cursor).toBeNull();
        expect(failed.ledger.entries.map((e: any) => [e.stage, e.category])).toEqual([["source", "source_http_error"]]);
        h.db.unavailable = false;
        await drain(h.options);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.telemetry.failure").map(r => r.attrs["openrig.failure.category"])).toEqual(["source_http_error"]);
        expect(readJson(h.checkpointPath).queue.agg.failures).toEqual({ "source|source_http_error": 1 });
    });

    it("fits worst-case pending plus derived state inside the tested checkpoint bound and metric point budget", async () => {
        const L = telemetryOtlp as Record<string, any>;
        expect(TELEMETRY_OTLP_LIMITS.checkpointBytes).toBe(4 * 1024 * 1024);
        expect(TELEMETRY_OTLP_LIMITS.exportBatchBytes).toBe(2 * 1024 * 1024);
        expect(TELEMETRY_OTLP_LIMITS.maxMetricsPerBatch).toBe(5376);
        expect(TELEMETRY_OTLP_LIMITS.maxRecordsPerBatch).toBe(448);
        const dir = tempDir();
        const config = L.normalizeConfig({ sourceUrl: "http://127.0.0.1:7433", endpointUrl: "http://127.0.0.1:4318/v1/logs", checkpointPath: join(dir, "w.json"), namespace: "n".repeat(128), includeQitemId: true });
        const ckpt = L.readCheckpoint(config);
        const states = L.TELEMETRY_OTLP_ALLOWLISTS.states as string[];
        const closures = L.TELEMETRY_OTLP_ALLOWLISTS.closureReasons as string[];
        const agg = ckpt.queue.agg;
        for (const s of states) for (const c of [...closures, "-"]) agg.transitions[`${s}|${c}`] = Number.MAX_SAFE_INTEGER - 1;
        for (const s of L.QUEUE_INTERVAL_STATES as string[]) for (const d of states) if (d !== s) agg.hist[`${s}|${d}`] = { n: 8800000000000, sum: 1e15, b: Array(11).fill(800000000000) };
        for (const c of [...L.TELEMETRY_OTLP_ALLOWLISTS.gapCodes, ...L.EXPORTER_GAP_CODES]) agg.gaps[c] = 123456789;
        for (const [stage, cats] of Object.entries(L.FAILURE_CATEGORIES as Record<string, string[]>)) for (const c of cats) agg.failures[`${stage}|${c}`] = 123456789;
        agg.lastNano = "9223372036854775807";
        for (let i = 0; i < 512; i++) ckpt.queue.open[i.toString(16).padStart(32, "0")] = { s: "in-progress", t: "9223372036854775806", id: "9223372036854775807" };
        for (let i = 0; i < 256; i++) {
            ckpt.seats.known[i.toString(16).padStart(32, "0")] = {
                id: `s-${String(i).padStart(3, "0")}-${"x".repeat(122)}`,
                rig: (i % 32).toString(16).padStart(32, "0"),
                rt: "claude-code",
                gen: "g".repeat(256),
                ni: true,
            };
        }
        ckpt.seats.lastNano = "9223372036854775807";
        const built = L.buildQueueMetricsPayload(config, ckpt, agg, "9223372036854775807");
        const active256 = Array.from({ length: 256 }, (_, i) => ({
            id: `s-${String(i).padStart(3, "0")}-${"x".repeat(122)}`,
            rt: "claude-code",
            activity: "needs-input",
            presence: "detached",
            needsInputCount: 999999,
        }));
        const stale256 = Array.from({ length: 256 }, (_, i) => ({
            id: `p-${String(i).padStart(3, "0")}-${"y".repeat(122)}`,
            rt: "claude-code",
            activity: true,
            presence: true,
            needsInput: true,
        }));
        const seatBuilt = L.buildSeatMetrics(active256, stale256, "9223372036854775807", "0");
        const totalMetricPoints = built.itemCount + seatBuilt.itemCount;
        expect(totalMetricPoints).toBe(122 + 5120);
        expect(totalMetricPoints).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.maxMetricsPerBatch);
        const combinedMetricsPayload = {
            resourceMetrics: [{
                resource: built.payload.resourceMetrics[0].resource,
                scopeMetrics: [{
                    scope: built.payload.resourceMetrics[0].scopeMetrics[0].scope,
                    metrics: [...built.payload.resourceMetrics[0].scopeMetrics[0].metrics, ...seatBuilt.metrics],
                }],
            }],
        };
        expect(Buffer.byteLength(JSON.stringify(combinedMetricsPayload))).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.exportBatchBytes);
        const spans = Array.from({ length: 128 }, (_, i) => ({ startNano: "9223372036854775806", endNano: "9223372036854775807", state: "in-progress", departure: "blocked", enterId: "9223372036854775806", departId: "9223372036854775807", qitemId: "q".repeat(256) }));
        const traces = L.buildQueueSpansPayload(config, ckpt, spans);
        expect(Buffer.byteLength(JSON.stringify(traces.payload))).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.exportBatchBytes);
        const logRecords = Array.from({ length: TELEMETRY_OTLP_LIMITS.maxRecordsPerBatch }, (_, i) => ({ observedTimeUnixNano: "9223372036854775807", severityNumber: 9, severityText: "INFO", body: { stringValue: "openrig.seat.tenure_change.observed" }, attributes: [{ key: "openrig.seat.node_id", value: { stringValue: "s".repeat(128) } }, { key: "openrig.tenure.prior_generation", value: { stringValue: "p".repeat(256) } }, { key: "openrig.tenure.new_generation", value: { stringValue: "n".repeat(256) } }] }));
        const logsPayload = { resourceLogs: [{ resource: { attributes: [] }, scopeLogs: [{ scope: { name: "openrig.telemetry.queue_transitions", version: "1" }, logRecords }] }] };
        expect(Buffer.byteLength(JSON.stringify(logsPayload))).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.exportBatchBytes);
        const pending = L.createPendingTransaction({
            nextCursor: "A".repeat(2048), discontinuity: false, hasMore: false,
            source: { hostId: "h".repeat(256), bootEpoch: "b".repeat(128), sequenceSpaceId: null },
            coverage: { status: "partial", historyCompleteness: "unknown" },
            signals: { logs: { itemCount: logRecords.length, payload: logsPayload }, metrics: { itemCount: totalMetricPoints, payload: combinedMetricsPayload }, traces: { itemCount: spans.length, payload: traces.payload } },
            derived: { queue: JSON.parse(JSON.stringify(ckpt.queue)), seats: JSON.parse(JSON.stringify(ckpt.seats)), ledgerThrough: 0, ledgerDroppedThrough: 0 },
        });
        for (let i = 1; i <= 32; i++) ckpt.ledger.entries.push({ seq: i, stage: "export", category: "otlp_retry_exhausted", signal: "traces", atNano: "9223372036854775807", items: 999999 });
        ckpt.ledger.nextSeq = 33;
        const state = { ...ckpt, pending };
        L.writeCheckpointAtomically(config.checkpointPath, state);
        const bytes = readFileSync(config.checkpointPath).byteLength;
        expect(bytes).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.checkpointBytes);
        const reread = L.readCheckpoint(config);
        expect(reread.pending.derived.queue.open).toBeDefined();
        expect(Object.keys(reread.pending.derived.seats.known)).toHaveLength(256);
    });

    it("fails visibly on a checkpoint whose derived queue state or pending derived state is malformed", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        h.rx.outcomes["/v1/logs"]!.push({ status: 503 });
        await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ code: "otlp_retry_exhausted" });
        const good = readJson(h.checkpointPath);
        const tamper = (mutate: (c: any) => void) => {
            const c = JSON.parse(JSON.stringify(good));
            mutate(c);
            writeFileSync(h.checkpointPath, JSON.stringify(c));
        };
        for (const mutate of [
            (c: any) => { delete c.queue; },
            (c: any) => { c.queue.open = { "not-a-hash": { s: "pending", t: "1", id: "1" } }; },
            (c: any) => { c.queue.open = { [`${"a".repeat(32)}`]: { s: "done", t: "1", id: "1" } }; },
            (c: any) => { c.queue.agg.transitions = { "bogus|-": 1 }; },
            (c: any) => { c.queue.agg.failures = { "export|raw http body": 1 }; },
            (c: any) => { c.ledger.entries = [{ seq: 1, stage: "export", category: "free text", signal: null, atNano: "1", items: 0 }]; },
            (c: any) => { c.pending.derived.queue.agg.hist = { "pending|in-progress": { n: 1, sum: 1, b: [1] } }; },
            (c: any) => { delete c.pending.derived; },
            (c: any) => { delete c.hmacSalt; },
        ]) {
            tamper(mutate);
            await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_malformed" });
        }
    });

    it("emits logs, metrics and traces from the default CLI and honors --signals opt-out", async () => {
        const h = await intervalHarness();
        qt(h.db, 1, "q-a", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-a", "2026-10-04 09:00:30", "in-progress");
        const argv = (ckpt: string, extra: string[] = []) => ["--source", h.src.baseUrl, "--endpoint", h.rx.endpointUrl, "--checkpoint", ckpt, "--namespace", "example-source", "--start", "retained", "--once", ...extra];
        expect(await runExporterCli(argv(join(h.dir, "all.json")))).toBe(0);
        expect(h.rx.received.map(r => r.path).sort()).toEqual(["/v1/logs", "/v1/metrics", "/v1/traces"]);
        const before = h.rx.received.length;
        expect(await runExporterCli(argv(join(h.dir, "logs-only.json"), ["--signals", "logs"]))).toBe(0);
        expect(h.rx.received.slice(before).map(r => r.path)).toEqual(["/v1/logs"]);
        expect(() => parseExporterArgs(argv(join(h.dir, "x.json"), ["--signals", "logs,profiles"]))).toThrow();
        expect(() => parseExporterArgs(argv(join(h.dir, "x.json"), ["--signals", ""]))).toThrow();
        // signals are bound into the checkpoint configuration: resuming with a different set fails visibly.
        await expect(exportAllSignalsOnce({ ...h.options, signals: ["logs"], checkpointPath: join(h.dir, "all.json"), namespace: "example-source" })).rejects.toMatchObject({ code: "checkpoint_config_mismatch" });

        // Unrelated noise regression: GET / probe and GET /v1/logs must be rejected with 404/405 without counting as OTLP.
        const rxBefore = h.rx.received.length;
        const rootNoise = await fetch(`${h.rx.baseUrl}/`);
        expect(rootNoise.status).toBe(404);
        const getSignalNoise = await fetch(`${h.rx.baseUrl}/v1/logs`);
        expect(getSignalNoise.status).toBe(405);
        expect(h.rx.received.length).toBe(rxBefore);

        // Malformed POST body to valid signal path must receive 400 and not count as valid OTLP batch.
        const malformedPost = await fetch(`${h.rx.baseUrl}/v1/logs`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "invalid-json{",
        });
        expect(malformedPost.status).toBe(400);
        expect(h.rx.received.length).toBe(rxBefore);
    });
});

// ---------------------------------------------------------------------------------------------
// S3: cached seat inventory one-hot activity/presence metrics, evidence-backed needs_input count,
// complete-inventory-only departure reconciliation with OTLP flags=1 staleness markers,
// observed seat tenure-change logs, and HMAC seat/tenure pseudonymization.
// ---------------------------------------------------------------------------------------------
function mockSeatNode(nodeId: string, rigId: string, overrides: Record<string, any> = {}) {
    return {
        nodeId,
        rigId,
        rigName: `SECRET-RIG-NAME-${rigId}`,
        logicalId: `SECRET-LOGICAL-${nodeId}`,
        podId: `SECRET-POD-${nodeId}`,
        podNamespace: "SECRET-POD-NS",
        role: "SECRET-ROLE",
        canonicalSessionName: `SECRET-CANONICAL-${nodeId}`,
        nodeKind: "agent",
        runtime: "claude-code",
        sessionStatus: "running",
        cwd: `/secret/path/${nodeId}`,
        model: "claude-secret-model",
        lifecycleState: "running",
        resumeToken: `SECRET-RESUME-${nodeId}`,
        arbitratedNeedsInput: null,
        needsInputEvidence: null,
        activityState: {
            activity: "working",
            display: "working",
            needsInput: { count: 0, reason: "SECRET-NEEDS-INPUT-REASON" },
            decidedBy: "hook",
            seq: 1,
            lastSwap: null,
        },
        ...overrides,
    };
}

describe("reference OTLP seat inventory metrics, departure reconciliation, tenure logs and pseudonymization", () => {
    it("samples server-derived seat activity, projected session presence, and evidence-backed needs_input while overriding stopped-seat idle and omitting unsupported zero counts", async () => {
        const h = await intervalHarness({
            inventory: {
                rigs: [{ id: "rig-1", name: "SECRET-RIG-1" }],
                nodesByRig: {
                    "rig-1": [
                        mockSeatNode("seat-working", "rig-1", {
                            runtime: "claude-code",
                            sessionStatus: "running",
                            arbitratedNeedsInput: false,
                            needsInputEvidence: { rung: "self-report", observedAt: time },
                            activityState: {
                                activity: "working",
                                display: "working",
                                needsInput: { count: 0, reason: "SECRET-REASON-0" },
                                lastSwap: null,
                            },
                        }),
                        mockSeatNode("seat-needs-input", "rig-1", {
                            runtime: "codex",
                            sessionStatus: "running",
                            arbitratedNeedsInput: true,
                            needsInputEvidence: { rung: "hook", observedAt: time },
                            activityState: {
                                activity: "needs-input",
                                display: "needs-input",
                                needsInput: { count: 3, reason: "SECRET-REASON-3" },
                                lastSwap: null,
                            },
                        }),
                        mockSeatNode("seat-stopped-stale-idle", "rig-1", {
                            runtime: "pi",
                            sessionStatus: "stopped",
                            lifecycleState: "running", // must be ignored; sessionStatus is authoritative for presence
                            arbitratedNeedsInput: null,
                            activityState: {
                                activity: "idle",
                                display: "idle", // stale/malformed idle on a stopped seat must never emit idle=1
                                needsInput: { count: 0, reason: "SECRET-REASON-UNSUPPORTED" },
                                lastSwap: null,
                            },
                        }),
                        mockSeatNode("seat-null-activity", "rig-1", {
                            runtime: "agy", // community harness outside built-in closed allowlist: omit openrig.seat.runtime
                            sessionStatus: "detached",
                            arbitratedNeedsInput: null,
                            activityState: null,
                        }),
                    ],
                },
            },
        });

        const res1 = await exportAllSignalsOnce(h.options);
        expect(res1.outcome).toBe("exported");
        expect(h.src.reqs.some(r => /nodes\?.*(full|refresh)/.test(r))).toBe(false);

        const pts1 = lastBatchPoints(h.rx);
        const activityFor = (id: string) => Object.fromEntries(
            pts1.filter(p => p.name === "openrig.seat.activity" && p.attrs["openrig.seat.node_id"] === id)
                .map(p => [p.attrs["openrig.seat.state"], p.point.asInt]),
        );
        const presenceFor = (id: string) => Object.fromEntries(
            pts1.filter(p => p.name === "openrig.seat.presence" && p.attrs["openrig.seat.node_id"] === id)
                .map(p => [p.attrs["openrig.seat.status"], p.point.asInt]),
        );

        expect(activityFor("seat-working")).toEqual({ working: "1", idle: "0", "needs-input": "0", unknown: "0" });
        expect(presenceFor("seat-working")).toEqual({ running: "1", exited: "0", stopped: "0", detached: "0", unknown: "0" });

        expect(activityFor("seat-needs-input")).toEqual({ working: "0", idle: "0", "needs-input": "1", unknown: "0" });
        expect(presenceFor("seat-needs-input")).toEqual({ running: "1", exited: "0", stopped: "0", detached: "0", unknown: "0" });

        // Confirmed stopped seat overrides stale display:"idle" to unknown, and uses sessionStatus ("stopped"), never lifecycleState ("running").
        expect(activityFor("seat-stopped-stale-idle")).toEqual({ working: "0", idle: "0", "needs-input": "0", unknown: "1" });
        expect(presenceFor("seat-stopped-stale-idle")).toEqual({ running: "0", exited: "0", stopped: "1", detached: "0", unknown: "0" });

        expect(activityFor("seat-null-activity")).toEqual({ working: "0", idle: "0", "needs-input": "0", unknown: "1" });
        expect(presenceFor("seat-null-activity")).toEqual({ running: "0", exited: "0", stopped: "0", detached: "1", unknown: "0" });

        // runtime attribute emitted only for closed built-in allowlist; omitted for community harness "agy".
        const runtimeFor = (id: string) => pts1.find(p => p.name === "openrig.seat.activity" && p.attrs["openrig.seat.node_id"] === id)?.attrs["openrig.seat.runtime"];
        expect(runtimeFor("seat-working")).toBe("claude-code");
        expect(runtimeFor("seat-needs-input")).toBe("codex");
        expect(runtimeFor("seat-stopped-stale-idle")).toBe("pi");
        expect(runtimeFor("seat-null-activity")).toBeUndefined();

        // needs_input emitted ONLY for authoritative zero ("seat-working") and positive ("seat-needs-input"); omitted when null/unsupported.
        const needsInputPts1 = pts1.filter(p => p.name === "openrig.seat.needs_input");
        expect(Object.fromEntries(needsInputPts1.map(p => [p.attrs["openrig.seat.node_id"], p.point.asInt]))).toEqual({
            "seat-working": "0",
            "seat-needs-input": "3",
        });

        // If a seat's previously observed needs_input becomes unsupported (arbitratedNeedsInput: null) while seat stays present,
        // emit a flags=1 NoRecordedValue marker on openrig.seat.needs_input once, then omit on subsequent samples.
        h.src.inventory.nodesByRig!["rig-1"]![1] = mockSeatNode("seat-needs-input", "rig-1", {
            runtime: "codex",
            sessionStatus: "running",
            arbitratedNeedsInput: null,
            needsInputEvidence: null,
            activityState: { activity: "idle", display: "idle", needsInput: { count: 0, reason: "SECRET" }, lastSwap: null },
        });
        await exportAllSignalsOnce(h.options);
        const pts2 = lastBatchPoints(h.rx);
        const niSeat2 = pts2.find(p => p.name === "openrig.seat.needs_input" && p.attrs["openrig.seat.node_id"] === "seat-needs-input")!;
        expect(niSeat2.point.flags).toBe(1);
        expect(niSeat2.point.asInt).toBeUndefined();

        await exportAllSignalsOnce(h.options);
        const pts3 = lastBatchPoints(h.rx);
        expect(pts3.some(p => p.name === "openrig.seat.needs_input" && p.attrs["openrig.seat.node_id"] === "seat-needs-input")).toBe(false);
    });

    it("reconciles departures only after complete inventory reads, emits OTLP flags=1 staleness markers once, and starts fresh history on same-name recreation with a new nodeId", async () => {
        const h = await intervalHarness({
            inventory: {
                rigs: [{ id: "rig-1", name: "SECRET-RIG-1" }],
                nodesByRig: {
                    "rig-1": [
                        mockSeatNode("seat-a", "rig-1", {
                            logicalId: "worker",
                            arbitratedNeedsInput: false,
                            activityState: { activity: "working", display: "working", needsInput: { count: 0 }, lastSwap: { generation: "gen-a-1", at: time } },
                        }),
                        mockSeatNode("seat-b-v1", "rig-1", {
                            logicalId: "lead",
                            arbitratedNeedsInput: true,
                            activityState: { activity: "needs-input", display: "needs-input", needsInput: { count: 2 }, lastSwap: { generation: "gen-b-1", at: time } },
                        }),
                    ],
                },
            },
        });

        await exportAllSignalsOnce(h.options);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed")).toHaveLength(0);

        // Seat B departs; same logicalId ("lead") is recreated with a new immutable nodeId ("seat-b-v2") and new generation ("gen-b-2").
        h.src.inventory.nodesByRig!["rig-1"] = [
            mockSeatNode("seat-a", "rig-1", {
                logicalId: "worker",
                arbitratedNeedsInput: false,
                activityState: { activity: "working", display: "working", needsInput: { count: 0 }, lastSwap: { generation: "gen-a-1", at: time } },
            }),
            mockSeatNode("seat-b-v2", "rig-1", {
                logicalId: "lead",
                arbitratedNeedsInput: false,
                activityState: { activity: "idle", display: "idle", needsInput: { count: 0 }, lastSwap: { generation: "gen-b-2", at: time } },
            }),
        ];
        await exportAllSignalsOnce(h.options);
        const pts2 = lastBatchPoints(h.rx);
        const departedPts = pts2.filter(p => p.attrs["openrig.seat.node_id"] === "seat-b-v1");
        expect(departedPts).toHaveLength(10); // 4 activity + 5 presence + 1 needs_input
        expect(departedPts.every(p => p.point.flags === 1 && p.point.asInt === undefined)).toBe(true);

        const newSeatPts = pts2.filter(p => p.attrs["openrig.seat.node_id"] === "seat-b-v2");
        expect(newSeatPts).toHaveLength(10);
        expect(newSeatPts.every(p => !p.point.flags && typeof p.point.asInt === "string")).toBe(true);
        // Same-name recreation with new nodeId starts a new baseline without a false tenure-change log.
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed")).toHaveLength(0);

        // Next steady sample emits zero points for departed seat-b-v1.
        await exportAllSignalsOnce(h.options);
        const pts3 = lastBatchPoints(h.rx);
        expect(pts3.some(p => p.attrs["openrig.seat.node_id"] === "seat-b-v1")).toBe(false);
        expect(pts3.some(p => p.attrs["openrig.seat.node_id"] === "seat-b-v2" && !p.point.flags)).toBe(true);
    });

    it("distinguishes archived rigs, missing configured rigs, and inventory read failures from node deletion, preserving baselines and recording allowlisted failures", async () => {
        const h = await intervalHarness({
            inventory: {
                rigs: [{ id: "rig-1", name: "SECRET-RIG-1" }],
                archivedRigs: [],
                nodesByRig: {
                    "rig-1": [
                        mockSeatNode("seat-a", "rig-1", {
                            activityState: { activity: "working", display: "working", needsInput: { count: 0 }, lastSwap: { generation: "gen-1", at: time } },
                        }),
                        mockSeatNode("seat-b", "rig-1", {
                            activityState: { activity: "working", display: "working", needsInput: { count: 0 }, lastSwap: null },
                        }),
                    ],
                },
                nodesStatusByRig: { "rig-1": 200 },
            },
        });
        const rigOpts = { ...h.options, rigs: ["rig-1"] };

        await exportAllSignalsOnce(rigOpts);
        expect(Object.keys(readJson(h.checkpointPath).seats.known)).toHaveLength(2);

        // Inventory 503 failure on /api/rigs/rig-1/nodes: must record failure, emit no false departures or idle samples, and preserve baseline.
        h.src.inventory.nodesStatusByRig!["rig-1"] = 503;
        const failStep = await exportAllSignalsOnce(rigOpts);
        expect(failStep.outcome).toBe("exported");
        expect(Object.keys(readJson(h.checkpointPath).seats.known)).toHaveLength(2);
        const failLogs = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.telemetry.failure");
        expect(failLogs.some(r => r.attrs["openrig.failure.stage"] === "source")).toBe(true);
        const failPts = lastBatchPoints(h.rx);
        expect(failPts.some(p => p.name === "openrig.telemetry.failure" && Number(p.point.asInt) >= 1)).toBe(true);
        expect(failPts.some(p => p.name.startsWith("openrig.seat."))).toBe(false);
        expect(h.rx.received.map(r => r.raw).join("\n")).not.toContain("SYNTHETIC_SECRET_NODES_ERROR");

        // Recovery with seat-b absent reconciles seat-b departure with flags=1 while seat-a continues.
        h.src.inventory.nodesStatusByRig!["rig-1"] = 200;
        h.src.inventory.nodesByRig!["rig-1"] = [
            mockSeatNode("seat-a", "rig-1", {
                activityState: { activity: "working", display: "working", needsInput: { count: 0 }, lastSwap: { generation: "gen-1", at: time } },
            }),
        ];
        await exportAllSignalsOnce(rigOpts);
        const recPts = lastBatchPoints(h.rx);
        expect(recPts.filter(p => p.attrs["openrig.seat.node_id"] === "seat-b").every(p => p.point.flags === 1)).toBe(true);
        expect(recPts.filter(p => p.attrs["openrig.seat.node_id"] === "seat-a").every(p => !p.point.flags)).toBe(true);

        // Rig 1 is archived (removed from /api/rigs, present in /api/rigs?archived=only): consults ?archived=only and marks seat-a stale once.
        h.src.inventory.rigs = [];
        h.src.inventory.archivedRigs = [{ id: "rig-1", name: "SECRET-RIG-1" }];
        await exportAllSignalsOnce(rigOpts);
        expect(h.src.reqs).toContain("/api/rigs?archived=only");
        const archPts = lastBatchPoints(h.rx);
        const archStale = archPts.filter(p => p.attrs["openrig.seat.node_id"] === "seat-a");
        expect(archStale.length).toBeGreaterThan(0);
        expect(archStale.every(p => p.point.flags === 1 && p.point.asInt === undefined)).toBe(true);

        // Subsequent no-op run on archived rig emits no duplicate staleness markers or transition logs.
        const beforeNoop = h.rx.received.length;
        const noopRes = await exportAllSignalsOnce(rigOpts);
        expect(noopRes.outcome).toBe("empty_page_advanced");
        expect(h.rx.received).toHaveLength(beforeNoop);

        // Configured rig absent from both active and archived lists records configured_rig_not_found failure without false deletion.
        h.src.inventory.archivedRigs = [];
        await exportAllSignalsOnce(rigOpts);
        const missingLogs = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.telemetry.failure");
        expect(missingLogs.some(r => r.attrs["openrig.failure.category"] === "configured_rig_not_found")).toBe(true);

        // L3 + F1: In unscoped mode (rigs: null) with >= 2 rigs, when one rig with previously known seats disappears from
        // BOTH active (/api/rigs) AND archived (/api/rigs?archived=only) lists while a second rig remains healthy:
        // - do not query /api/rigs/:rigId/nodes for the missing rig,
        // - retain the missing rig's seat baseline with no false flags=1 departure markers or invented execution/presence points,
        // - CONTINUE sampling the healthy rig's activity, presence, observed needs_input count, and tenure-change logs alongside queue logs,
        // - emit allowlisted source/configured_rig_not_found in the failure ledger and logs/metrics (without raw rig IDs),
        // - do not mutate live checkpoint.seats before all signal outcomes resolve,
        // - and resume the same missing seat identity and generation baseline when the missing rig reappears.
        const unscopedCkptPath = join(h.dir, "unscoped-missing-rig.json");
        const unscopedDiags: Array<Record<string, unknown>> = [];
        const unscopedOpts = {
            ...h.options,
            checkpointPath: unscopedCkptPath,
            rigs: null,
            onDiagnostic: (d: Record<string, unknown>) => unscopedDiags.push(d),
        };
        h.src.inventory.rigs = [
            { id: "rig-unscoped-missing", name: "SECRET-UNSCOPED-MISSING" },
            { id: "rig-unscoped-healthy", name: "SECRET-UNSCOPED-HEALTHY" },
        ];
        h.src.inventory.archivedRigs = [];
        h.src.inventory.nodesStatusByRig!["rig-unscoped-missing"] = 200;
        h.src.inventory.nodesStatusByRig!["rig-unscoped-healthy"] = 200;
        h.src.inventory.nodesByRig!["rig-unscoped-missing"] = [
            mockSeatNode("seat-unscoped-1", "rig-unscoped-missing", {
                arbitratedNeedsInput: false,
                activityState: {
                    activity: "working",
                    display: "working",
                    needsInput: { count: 0 },
                    lastSwap: { generation: "gen-u-1", at: time },
                },
            }),
        ];
        h.src.inventory.nodesByRig!["rig-unscoped-healthy"] = [
            mockSeatNode("seat-healthy-1", "rig-unscoped-healthy", {
                arbitratedNeedsInput: true,
                needsInputEvidence: { rung: "hook", observedAt: time },
                activityState: {
                    activity: "needs-input",
                    display: "needs-input",
                    needsInput: { count: 2, reason: "SECRET" },
                    lastSwap: { generation: "gen-h-1", at: time },
                },
            }),
        ];
        await exportAllSignalsOnce(unscopedOpts);
        expect(Object.keys(readJson(unscopedCkptPath).seats.known)).toHaveLength(2);

        // Rig "rig-unscoped-missing" disappears from BOTH active and archived lists while "rig-unscoped-healthy" stays active,
        // updates its needs_input count to 5 and swaps generation gen-h-1 -> gen-h-2, alongside a new queue transition row.
        // Inject a one-time 503 on /v1/metrics to prove live checkpoint.seats is not mutated before all signal outcomes resolve.
        h.src.inventory.rigs = [{ id: "rig-unscoped-healthy", name: "SECRET-UNSCOPED-HEALTHY" }];
        h.src.inventory.archivedRigs = [];
        h.src.inventory.nodesByRig!["rig-unscoped-healthy"] = [
            mockSeatNode("seat-healthy-1", "rig-unscoped-healthy", {
                arbitratedNeedsInput: true,
                needsInputEvidence: { rung: "hook", observedAt: time },
                activityState: {
                    activity: "needs-input",
                    display: "needs-input",
                    needsInput: { count: 5, reason: "SECRET" },
                    lastSwap: { generation: "gen-h-2", at: "2026-10-04T09:00:10.000Z" },
                },
            }),
        ];
        qt(h.db, 10, "q-during-partial", "2026-10-04 09:00:10", "pending");
        h.src.reqs.length = 0;
        h.rx.outcomes["/v1/metrics"]!.push({ status: 503, body: "METRICS_TEMP_503" });
        await expect(exportAllSignalsOnce(unscopedOpts)).rejects.toMatchObject({ code: "otlp_retry_exhausted", signal: "metrics" });

        // Live checkpoint.seats on disk still holds gen-h-1 while pending.derived.seats holds gen-h-2 (and preserves gen-u-1).
        const midPartialCkpt = readJson(unscopedCkptPath);
        const midLiveById = Object.fromEntries(Object.values(midPartialCkpt.seats.known).map((e: any) => [e.id, e.gen]));
        expect(midLiveById).toEqual({ "seat-unscoped-1": "gen-u-1", "seat-healthy-1": "gen-h-1" });
        const midDerivedById = Object.fromEntries(Object.values(midPartialCkpt.pending.derived.seats.known).map((e: any) => [e.id, e.gen]));
        expect(midDerivedById).toEqual({ "seat-unscoped-1": "gen-u-1", "seat-healthy-1": "gen-h-2" });

        // Replay resolves /v1/metrics without re-fetching source or resending /v1/logs.
        await exportAllSignalsOnce(unscopedOpts);

        expect(h.src.reqs).toContain("/api/rigs");
        expect(h.src.reqs).toContain("/api/rigs?archived=only");
        expect(h.src.reqs.some(r => r.includes("/api/rigs/rig-unscoped-missing/nodes"))).toBe(false);
        expect(h.src.reqs.some(r => r.includes("/api/rigs/rig-unscoped-healthy/nodes"))).toBe(true);

        // Missing seat baseline is retained intact with zero points emitted for it; healthy seat continues sampling all 10 points + tenure log!
        const unscopedDuringOutage = readJson(unscopedCkptPath);
        expect(Object.keys(unscopedDuringOutage.seats.known)).toHaveLength(2);
        const adoptedById = Object.fromEntries(Object.values(unscopedDuringOutage.seats.known).map((e: any) => [e.id, e.gen]));
        expect(adoptedById).toEqual({ "seat-unscoped-1": "gen-u-1", "seat-healthy-1": "gen-h-2" });

        const outagePts = lastBatchPoints(h.rx);
        // Healthy rig seat sampled: 4 activity + 5 presence + 1 needs_input (count=5)
        const healthyPts = outagePts.filter(p => p.attrs["openrig.seat.node_id"] === "seat-healthy-1");
        expect(healthyPts).toHaveLength(10);
        expect(healthyPts.every(p => !p.point.flags && typeof p.point.asInt === "string")).toBe(true);
        expect(healthyPts.find(p => p.name === "openrig.seat.needs_input")?.point.asInt).toBe("5");
        // Missing rig seat: neither flags=1 departure markers nor invented active/idle/presence points
        expect(outagePts.some(p => p.attrs["openrig.seat.node_id"] === "seat-unscoped-1")).toBe(false);
        // Failure metric and failure/transition/tenure logs emitted in the same transaction, with no raw rig IDs leaked:
        expect(outagePts.some(p => p.name === "openrig.telemetry.failure" && p.attrs["openrig.failure.category"] === "configured_rig_not_found")).toBe(true);
        const outageBatchLogs = logRecordsOf([h.rx.by("/v1/logs").at(-1)!]);
        expect(outageBatchLogs.some(r => r.body === "openrig.queue.transition" && r.attrs["openrig.transition.id"] === "10")).toBe(true);
        expect(outageBatchLogs.some(r => r.body === "openrig.seat.tenure_change.observed" && r.attrs["openrig.seat.node_id"] === "seat-healthy-1" && r.attrs["openrig.tenure.prior_generation"] === "gen-h-1" && r.attrs["openrig.tenure.new_generation"] === "gen-h-2")).toBe(true);
        expect(outageBatchLogs.some(r => r.body === "openrig.telemetry.failure" && r.attrs["openrig.failure.category"] === "configured_rig_not_found")).toBe(true);
        expect(JSON.stringify(unscopedDiags)).not.toContain("rig-unscoped-missing");
        expect(h.rx.by("/v1/logs").at(-1)!.raw).not.toContain("rig-unscoped-missing");

        // Missing rig reappears with the same seat and same generation: resumes active sampling under the same identity with zero false tenure-change logs.
        h.src.inventory.rigs = [
            { id: "rig-unscoped-missing", name: "SECRET-UNSCOPED-MISSING" },
            { id: "rig-unscoped-healthy", name: "SECRET-UNSCOPED-HEALTHY" },
        ];
        const tenureLogsBeforeReappear = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed").length;
        await exportAllSignalsOnce(unscopedOpts);
        const reappearPts = lastBatchPoints(h.rx);
        const reappearSeatPts = reappearPts.filter(p => p.attrs["openrig.seat.node_id"] === "seat-unscoped-1");
        expect(reappearSeatPts).toHaveLength(10);
        expect(reappearSeatPts.every(p => !p.point.flags && typeof p.point.asInt === "string")).toBe(true);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed")).toHaveLength(tenureLogsBeforeReappear);

        // Subsequent generation change from preserved gen-u-1 -> gen-u-2 emits openrig.seat.tenure_change.observed.
        h.src.inventory.nodesByRig!["rig-unscoped-missing"]![0].activityState.lastSwap = { generation: "gen-u-2", at: time };
        await exportAllSignalsOnce(unscopedOpts);
        const latestTenureLogs = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed");
        expect(latestTenureLogs).toHaveLength(tenureLogsBeforeReappear + 1);
        expect(latestTenureLogs.at(-1)!.attrs).toMatchObject({
            "openrig.seat.node_id": "seat-unscoped-1",
            "openrig.tenure.prior_generation": "gen-u-1",
            "openrig.tenure.new_generation": "gen-u-2",
        });

        // Explicit multi --rig scope: a missing configured rig must not block another healthy configured rig,
        // while a newly never-known unavailable configured rig still emits source/configured_rig_not_found.
        const multiRigCkptPath = join(h.dir, "multi-rig-partial.json");
        h.src.inventory.rigs = [{ id: "rig-unscoped-healthy", name: "SECRET-UNSCOPED-HEALTHY" }];
        h.src.inventory.archivedRigs = [];
        await exportAllSignalsOnce({
            ...h.options,
            checkpointPath: multiRigCkptPath,
            rigs: ["rig-unscoped-healthy", "rig-never-known-missing"],
        });
        const multiRigPts = lastBatchPoints(h.rx);
        expect(multiRigPts.filter(p => p.attrs["openrig.seat.node_id"] === "seat-healthy-1")).toHaveLength(10);
        expect(multiRigPts.some(p => p.name === "openrig.telemetry.failure" && p.attrs["openrig.failure.category"] === "configured_rig_not_found")).toBe(true);
    });

    it("emits openrig.seat.tenure_change.observed logs only on observed generation changes and replays multi-signal seat transactions without duplicating tenure logs or losing queue state", async () => {
        const h = await intervalHarness({
            inventory: {
                rigs: [{ id: "rig-1", name: "SECRET-RIG-1" }],
                nodesByRig: {
                    "rig-1": [
                        mockSeatNode("seat-a", "rig-1", {
                            activityState: {
                                activity: "working",
                                display: "working",
                                needsInput: { count: 0 },
                                lastSwap: { generation: "gen-1", at: "2026-10-04T08:50:00.000Z" },
                            },
                        }),
                    ],
                },
            },
        });

        // 1. First non-null observation establishes baseline; no tenure-change log emitted.
        await exportAllSignalsOnce(h.options);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed")).toHaveLength(0);

        // 2. Same generation with refreshed timestamp is not a new tenure; null lastSwap is unknown and preserves baseline.
        h.src.inventory.nodesByRig!["rig-1"]![0].activityState.lastSwap = { generation: "gen-1", at: "2026-10-04T08:55:00.000Z" };
        await exportAllSignalsOnce(h.options);
        h.src.inventory.nodesByRig!["rig-1"]![0].activityState.lastSwap = null;
        await exportAllSignalsOnce(h.options);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed")).toHaveLength(0);

        // 3. Generation changes from gen-1 to gen-2 alongside a queue transition while /v1/metrics fails with 503.
        qt(h.db, 1, "q-1", "2026-10-04 09:00:00", "pending");
        h.src.inventory.nodesByRig!["rig-1"]![0].activityState.lastSwap = { generation: "gen-2", at: "2026-10-04T09:00:05.000Z" };
        h.rx.outcomes["/v1/metrics"]!.push({ status: 503, body: "METRICS_TEMP_DOWN" });

        await expect(exportAllSignalsOnce(h.options)).rejects.toMatchObject({ code: "otlp_retry_exhausted", signal: "metrics" });
        const midCkpt = readJson(h.checkpointPath);
        // Live seats baseline on disk still holds gen-1 while pending.derived.seats holds gen-2 and pending.derived.queue holds the queue transition.
        expect(Object.values(midCkpt.seats.known)[0]).toMatchObject({ gen: "gen-1" });
        expect(Object.values(midCkpt.pending.derived.seats.known)[0]).toMatchObject({ gen: "gen-2" });
        expect(midCkpt.pending.derived.queue.agg.transitions).toEqual({ "pending|-": 1 });
        expect(midCkpt.pending.signals.logs.outcome).toBe("exported");
        expect(midCkpt.pending.signals.metrics.outcome).toBeNull();

        const logsCountAfterFirstAttempt = h.rx.by("/v1/logs").length;
        const resumed = await exportAllSignalsOnce(h.options);
        expect(resumed).toMatchObject({ outcome: "exported", replayedPending: true });
        expect(h.rx.by("/v1/logs")).toHaveLength(logsCountAfterFirstAttempt); // logs not resent

        const tenureLogs = logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed");
        expect(tenureLogs).toHaveLength(1);
        expect(tenureLogs[0]!.rec.timeUnixNano).toBe(nano("2026-10-04T09:00:05.000Z"));
        expect(tenureLogs[0]!.attrs).toMatchObject({
            "openrig.record.kind": "seat_tenure_change",
            "openrig.seat.node_id": "seat-a",
            "openrig.seat.runtime": "claude-code",
            "openrig.tenure.prior_generation": "gen-1",
            "openrig.tenure.new_generation": "gen-2",
            "openrig.tenure.continuity": "intermediate_changes_unknown",
            "openrig.tenure.intermediate_changes_unknown": true,
        });

        // Adopted checkpoint has both updated seat baseline and queue state; next cycle with gen-2 does not re-emit tenure log.
        const adopted = readJson(h.checkpointPath);
        expect(adopted.pending).toBeNull();
        expect(Object.values(adopted.seats.known)[0]).toMatchObject({ gen: "gen-2" });
        expect(adopted.queue.agg.transitions).toEqual({ "pending|-": 1 });

        await exportAllSignalsOnce(h.options);
        expect(logRecordsOf(h.rx.by("/v1/logs")).filter(r => r.body === "openrig.seat.tenure_change.observed")).toHaveLength(1);
    });

    it("pseudonymizes seat and tenure identifiers with persistent checkpoint HMAC salt when --pseudonymize-seats is set, binds --rig and --pseudonymize-seats to checkpoint config, and strips all private inventory fields", async () => {
        const h = await intervalHarness({
            inventory: {
                rigs: [{ id: "SENTINEL-RIG-1", name: "SENTINEL-RIG-NAME" }],
                nodesByRig: {
                    "SENTINEL-RIG-1": [
                        mockSeatNode("SENTINEL-NODE-1", "SENTINEL-RIG-1", {
                            arbitratedNeedsInput: false,
                            activityState: {
                                activity: "working",
                                display: "working",
                                needsInput: { count: 0, reason: "SENTINEL-REASON" },
                                lastSwap: { generation: "SENTINEL-GEN-1", at: time },
                            },
                        }),
                    ],
                },
            },
        });
        const pseudoOpts = { ...h.options, rigs: ["SENTINEL-RIG-1"], pseudonymizeSeats: true };
        await exportAllSignalsOnce(pseudoOpts);

        h.src.inventory.nodesByRig!["SENTINEL-RIG-1"]![0].activityState.lastSwap = { generation: "SENTINEL-GEN-2", at: time };
        await exportAllSignalsOnce(pseudoOpts);

        const ckpt = readJson(h.checkpointPath);
        const L = telemetryOtlp as Record<string, any>;
        const expectedSeatHash = L.pseudonymize(ckpt.hmacSalt, "seat-node", "SENTINEL-NODE-1");
        const expectedGen1Hash = L.pseudonymize(ckpt.hmacSalt, "seat-gen", "SENTINEL-GEN-1");
        const expectedGen2Hash = L.pseudonymize(ckpt.hmacSalt, "seat-gen", "SENTINEL-GEN-2");

        const pts = lastBatchPoints(h.rx);
        expect(pts.some(p => p.name === "openrig.seat.activity" && p.attrs["openrig.seat.node_id"] === expectedSeatHash)).toBe(true);
        const tenureLog = logRecordsOf(h.rx.by("/v1/logs")).find(r => r.body === "openrig.seat.tenure_change.observed")!;
        expect(tenureLog.attrs).toMatchObject({
            "openrig.seat.node_id": expectedSeatHash,
            "openrig.tenure.prior_generation": expectedGen1Hash,
            "openrig.tenure.new_generation": expectedGen2Hash,
        });

        const wireAll = h.rx.received.map(r => r.raw).join("\n");
        for (const forbidden of [
            "SENTINEL-RIG-1",
            "SENTINEL-RIG-NAME",
            "SENTINEL-NODE-1",
            "SENTINEL-GEN-1",
            "SENTINEL-GEN-2",
            "SENTINEL-REASON",
            "SECRET-LOGICAL",
            "SECRET-POD",
            "SECRET-CANONICAL",
            "/secret/path",
            "claude-secret-model",
            "SECRET-RESUME",
        ]) {
            expect(wireAll).not.toContain(forbidden);
        }
        const rawCkpt = readFileSync(h.checkpointPath, "utf8");
        for (const forbiddenCkpt of [
            "SENTINEL-RIG-NAME",
            "SENTINEL-NODE-1",
            "SENTINEL-GEN-1",
            "SENTINEL-GEN-2",
            "SENTINEL-REASON",
            "SECRET-LOGICAL",
            "SECRET-POD",
            "SECRET-CANONICAL",
            "/secret/path",
            "claude-secret-model",
            "SECRET-RESUME",
        ]) {
            expect(rawCkpt).not.toContain(forbiddenCkpt);
        }

        // Changing --rig or --pseudonymize-seats fails visibly with checkpoint_config_mismatch.
        await expect(exportAllSignalsOnce({ ...pseudoOpts, pseudonymizeSeats: false })).rejects.toMatchObject({ code: "checkpoint_config_mismatch" });
        await expect(exportAllSignalsOnce({ ...pseudoOpts, rigs: ["OTHER-RIG"] })).rejects.toMatchObject({ code: "checkpoint_config_mismatch" });
        await expect(exportAllSignalsOnce({ ...pseudoOpts, rigs: null })).rejects.toMatchObject({ code: "checkpoint_config_mismatch" });

        // Malformed seats state in checkpoint fails visibly with checkpoint_malformed.
        const badCkpt = JSON.parse(rawCkpt);
        badCkpt.seats.known = { "not-a-hash": { id: "x", rig: "y", rt: null, gen: null, ni: false } };
        writeFileSync(h.checkpointPath, JSON.stringify(badCkpt));
        await expect(exportAllSignalsOnce(pseudoOpts)).rejects.toMatchObject({ name: "TelemetryCheckpointError", code: "checkpoint_malformed" });
    });

    it("fits 64-seat metrics plus queue state inside the metric point, export batch, and checkpoint bounds, and samples seats on repeated CLI --once invocations", async () => {
        const nodes64 = Array.from({ length: 64 }, (_, i) => mockSeatNode(`seat-node-${String(i + 1).padStart(3, "0")}`, "rig-max", {
            runtime: "claude-code",
            sessionStatus: "running",
            arbitratedNeedsInput: true,
            needsInputEvidence: { rung: "hook", observedAt: time },
            activityState: {
                activity: "needs-input",
                display: "needs-input",
                needsInput: { count: 1, reason: "SECRET" },
                lastSwap: { generation: `gen-${i + 1}`, at: time },
            },
        }));
        const h = await intervalHarness({
            inventory: {
                rigs: [{ id: "rig-max", name: "SECRET-RIG-MAX" }],
                nodesByRig: { "rig-max": nodes64 },
            },
        });
        qt(h.db, 1, "q-1", "2026-10-04 09:00:00", "pending");
        qt(h.db, 2, "q-1", "2026-10-04 09:00:20", "in-progress");

        const cliArgs = [
            "--source", h.src.baseUrl,
            "--endpoint", h.rx.endpointUrl,
            "--checkpoint", h.checkpointPath,
            "--namespace", "source-test",
            "--start", "retained",
            "--rig", "rig-max",
            "--pseudonymize-seats",
            "--once",
        ];
        expect(await runExporterCli(cliArgs)).toBe(0);
        const firstMetricsBatch = h.rx.by("/v1/metrics").at(-1)!;
        expect(Buffer.byteLength(firstMetricsBatch.raw)).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.exportBatchBytes);
        const firstPts = metricPoints([firstMetricsBatch]);
        expect(firstPts.length).toBeGreaterThanOrEqual(64 * 10);
        expect(firstPts.length).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.maxMetricsPerBatch);
        expect(readFileSync(h.checkpointPath).byteLength).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.checkpointBytes);

        // Immediately repeated CLI --once invocation must sample the full configured seat cycle again.
        const metricsCountBefore = h.rx.by("/v1/metrics").length;
        expect(await runExporterCli(cliArgs)).toBe(0);
        expect(h.rx.by("/v1/metrics").length).toBe(metricsCountBefore + 1);

        // L4: In continuous mode (runQueueTransitionsExporter), replaying a pending transaction on startup/recovery
        // must NOT consume the seat sampling cadence (lastSeatSampleMs), so the very next fresh cycle samples seat
        // inventory immediately even when --seat-poll-ms is large (600,000 ms).
        const contCkptPath = join(h.dir, "continuous-cadence.json");
        const contBaseOpts = {
            ...h.options,
            checkpointPath: contCkptPath,
            rigs: ["rig-max"],
            pseudonymizeSeats: false,
            pollMs: 15,
            seatPollMs: 600_000,
        };
        qt(h.db, 3, "q-cont", "2026-10-04 09:01:00", "pending");
        h.rx.outcomes["/v1/metrics"]!.push({ status: 503, body: "METRICS_TEMP_503" });
        await expect(exportAllSignalsOnce(contBaseOpts)).rejects.toMatchObject({ code: "otlp_retry_exhausted", signal: "metrics" });
        expect(readJson(contCkptPath).pending).not.toBeNull();

        // Update seat-node-001 needsInput count to 99 while pending batch (which has count=1) waits for replay.
        h.src.inventory.nodesByRig!["rig-max"]![0] = mockSeatNode("seat-node-001", "rig-max", {
            runtime: "claude-code",
            sessionStatus: "running",
            arbitratedNeedsInput: true,
            needsInputEvidence: { rung: "hook", observedAt: time },
            activityState: {
                activity: "needs-input",
                display: "needs-input",
                needsInput: { count: 99, reason: "SECRET" },
                lastSwap: { generation: "gen-1", at: time },
            },
        });
        const metricsBeforeCont = h.rx.by("/v1/metrics").length;
        const contAc = new AbortController();
        const checkDoneTimer = setInterval(() => {
            // Wait for 1) the pending replay metrics post + 2) the immediate fresh seat-sample metrics post and adoption
            if (h.rx.by("/v1/metrics").length >= metricsBeforeCont + 2 && readJson(contCkptPath).pending === null) {
                contAc.abort();
            }
        }, 10);
        const fallbackTimer = setTimeout(() => contAc.abort(), 1500);
        try {
            await runQueueTransitionsExporter({ ...contBaseOpts, once: false, signal: contAc.signal });
        } catch (err: any) {
            if (err?.code !== "exporter_aborted") throw err;
        } finally {
            clearInterval(checkDoneTimer);
            clearTimeout(fallbackTimer);
        }
        expect(h.rx.by("/v1/metrics").length).toBeGreaterThanOrEqual(metricsBeforeCont + 2);
        const freshPts = lastBatchPoints(h.rx);
        const freshSeat1Ni = freshPts.find(p => p.name === "openrig.seat.needs_input" && p.attrs["openrig.seat.node_id"] === "seat-node-001");
        expect(freshSeat1Ni?.point.asInt).toBe("99");
    });

    it("exports and replays worst-case 256-seat inventory (5120 active+stale points, 256 tenure logs, 128-byte unpseudonymized seat IDs, 128 transitions/spans, 512 open intervals, 32 ledger failures) within consistent bounds and retains ledger entries under --signals traces", async () => {
        // 1. Coordinator repro check: 256 active seats with short IDs emit 2560 points (~660 KiB > 256 KiB).
        const L = telemetryOtlp as Record<string, any>;
        const shortActive = Array.from({ length: 256 }, (_, i) => ({
            id: `seat-${i + 1}`,
            rt: "claude-code",
            activity: "working",
            presence: "running",
            needsInputCount: 0,
        }));
        const shortSeatMetrics = L.buildSeatMetrics(shortActive, [], "1759568400000000000", "0");
        expect(shortSeatMetrics.itemCount).toBe(2560);
        expect(shortSeatMetrics.itemCount).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.maxMetricsPerBatch);
        expect(Buffer.byteLength(JSON.stringify(shortSeatMetrics.metrics))).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.exportBatchBytes);

        // 2. Full end-to-end 4 rigs x 64 seats = 256 seats with max 256-byte rigIds, 128-byte unpseudonymized seat IDs, and 256-byte generations.
        const rigIds = Array.from({ length: 4 }, (_, r) => {
            const head = `rig-alpha-${r}-`;
            return head + "r".repeat(256 - head.length);
        });
        const makeSeatId = (prefix: string, rIdx: number, sIdx: number) => {
            const head = `${prefix}-r${rIdx}-s${String(sIdx).padStart(2, "0")}-`;
            return head + "x".repeat(128 - head.length);
        };
        const makeGen = (ver: string, rIdx: number, sIdx: number) => {
            const head = `gen-${ver}-r${rIdx}-s${String(sIdx).padStart(2, "0")}-`;
            return head + "g".repeat(256 - head.length);
        };
        const nodesByRigV1: Record<string, any[]> = {};
        for (let r = 0; r < 4; r++) {
            const rigId = rigIds[r]!;
            nodesByRigV1[rigId] = Array.from({ length: 64 }, (_, s) => mockSeatNode(makeSeatId("seat-v1", r, s), rigId, {
                runtime: "claude-code",
                sessionStatus: "running",
                arbitratedNeedsInput: true,
                needsInputEvidence: { rung: "hook", observedAt: time },
                activityState: {
                    activity: "needs-input",
                    display: "needs-input",
                    needsInput: { count: 4, reason: "SECRET-REASON" },
                    lastSwap: { generation: makeGen("v1", r, s), at: "2026-10-04T08:50:00.000Z" },
                },
            }));
        }

        const h = await intervalHarness({
            inventory: {
                rigs: rigIds.map(id => ({ id, name: `SECRET-${id}` })),
                nodesByRig: nodesByRigV1,
            },
        });
        const maxOpts = {
            ...h.options,
            includeQitemId: true,
            pseudonymizeSeats: false,
            rigs: rigIds,
        };

        // Seed 512 open intervals across 4 pages (512 items entering "pending") + initial 256-seat baseline.
        for (let i = 1; i <= 512; i++) {
            const qid = `q-max-${String(i).padStart(4, "0")}-${"q".repeat(240)}`;
            qt(h.db, i, qid, "2026-10-04 09:00:00", "pending");
        }
        await drain(maxOpts);
        const baselineCkpt = readJson(h.checkpointPath);
        expect(Object.keys(baselineCkpt.seats.known)).toHaveLength(256);
        expect(Object.keys(baselineCkpt.queue.open)).toHaveLength(512);

        // Saturate failure ledger with 32 entries + 2 overflow drops by injecting 34 source 503 errors.
        h.db.unavailable = true;
        for (let i = 0; i < 34; i++) {
            await expect(exportAllSignalsOnce(maxOpts)).rejects.toMatchObject({ name: "TelemetrySourceError", status: 503 });
        }
        h.db.unavailable = false;
        const saturatedLedgerCkpt = readJson(h.checkpointPath);
        expect(saturatedLedgerCkpt.ledger.entries).toHaveLength(32);
        expect(saturatedLedgerCkpt.ledger.dropped).toBe(2);

        // Next page: 128 of those 512 open items transition to "in-progress" (completing 128 spans while keeping 512 open intervals),
        // with 32 row-local oversized_field gaps (actor_session > 512 bytes), while all 256 seats change runtime ("claude-code" -> "codex")
        // AND swap generation (gen-v1 -> gen-v2), producing:
        // - 5120 seat metric points (2560 active + 2560 stale flags=1) + queue cumulative metrics
        // - 128 transition logs + 32 gap logs + 256 tenure-change logs + 1 overflow log + 27 drained failure logs = 444 log records
        // - 128 spans
        for (let i = 1; i <= 128; i++) {
            const qid = `q-max-${String(i).padStart(4, "0")}-${"q".repeat(240)}`;
            pushFixtureRow(h.db, {
                id: BigInt(512 + i),
                qitemId: qid,
                ts: "2026-10-04 09:01:00",
                state: "in-progress",
                actorSession: i <= 32 ? null : "seat@rig",
                closureReason: "handed_off_to",
                closureTarget: "target-secret",
                identityProvenance: "transport:v1",
                gaps: i <= 32 ? [{ code: "oversized_field", id: String(512 + i), field: "actorSession" }] : [],
            });
        }

        for (let r = 0; r < 4; r++) {
            const rigId = rigIds[r]!;
            h.src.inventory.nodesByRig![rigId] = Array.from({ length: 64 }, (_, s) => mockSeatNode(makeSeatId("seat-v1", r, s), rigId, {
                runtime: "codex",
                sessionStatus: "running",
                arbitratedNeedsInput: true,
                needsInputEvidence: { rung: "hook", observedAt: time },
                activityState: {
                    activity: "working",
                    display: "working",
                    needsInput: { count: 2, reason: "SECRET-REASON" },
                    lastSwap: { generation: makeGen("v2", r, s), at: "2026-10-04T09:01:00.000Z" },
                },
            }));
        }

        // Fail /v1/traces once so the maximal pending transaction (with logs + metrics + traces + derived queue/seats) is persisted and replayed.
        h.rx.outcomes["/v1/traces"]!.push({ status: 503, body: "TRACES_BUSY" });
        await expect(exportAllSignalsOnce(maxOpts)).rejects.toMatchObject({ code: "otlp_retry_exhausted", signal: "traces" });

        const pendingBytes = readFileSync(h.checkpointPath).byteLength;
        expect(pendingBytes).toBeLessThanOrEqual(TELEMETRY_OTLP_LIMITS.checkpointBytes);
        const pendingCkpt = readJson(h.checkpointPath);
        expect(pendingCkpt.pending.signals.logs.outcome).toBe("exported");
        expect(pendingCkpt.pending.signals.logs.itemCount).toBe(128 + 32 + 256 + 1 + 27); // 444 records
        expect(pendingCkpt.pending.signals.metrics.outcome).toBe("exported");
        expect(pendingCkpt.pending.signals.metrics.itemCount).toBeGreaterThanOrEqual(5120);
        expect(pendingCkpt.pending.signals.traces.itemCount).toBe(128);
        expect(pendingCkpt.pending.signals.traces.outcome).toBeNull();

        const logsCountBeforeReplay = h.rx.by("/v1/logs").length;
        const metricsCountBeforeReplay = h.rx.by("/v1/metrics").length;
        const replayed = await exportAllSignalsOnce(maxOpts);
        expect(replayed).toMatchObject({
            outcome: "exported",
            replayedPending: true,
            exportedRecords: 444,
            exportedSpans: 128,
        });
        expect(replayed.exportedDataPoints).toBeGreaterThanOrEqual(5120);
        expect(h.rx.by("/v1/logs")).toHaveLength(logsCountBeforeReplay);
        expect(h.rx.by("/v1/metrics")).toHaveLength(metricsCountBeforeReplay);

        // Replace all 256 seats with 256 brand-new 128-byte seat IDs in the next cycle:
        // 256 departed seats (2560 flags=1 points) + 256 new active seats (2560 points) = 5120 seat points.
        for (let r = 0; r < 4; r++) {
            const rigId = rigIds[r]!;
            h.src.inventory.nodesByRig![rigId] = Array.from({ length: 64 }, (_, s) => mockSeatNode(makeSeatId("seat-v2", r, s), rigId, {
                runtime: "claude-code",
                sessionStatus: "running",
                arbitratedNeedsInput: false,
                needsInputEvidence: { rung: "self-report", observedAt: time },
                activityState: {
                    activity: "idle",
                    display: "idle",
                    needsInput: { count: 0, reason: "SECRET-REASON" },
                    lastSwap: { generation: makeGen("v3", r, s), at: "2026-10-04T09:02:00.000Z" },
                },
            }));
        }
        const replacementRes = await exportAllSignalsOnce(maxOpts);
        expect(replacementRes.outcome).toBe("exported");
        expect(replacementRes.exportedDataPoints).toBeGreaterThanOrEqual(5120);
        const replacementPts = lastBatchPoints(h.rx);
        expect(replacementPts.filter(p => p.point.flags === 1)).toHaveLength(2560);

        // Next cycle: 128 transitions with 40 oversized_field gaps (32 gap details + 1 gap_details_truncated = 161 source records
        // + 1 page_intervals_suppressed note) while all 256 seats swap generation again (256 tenure logs) and the failure ledger
        // is saturated again (1 overflow + 27 drained failure logs = 446 log records in a single live transaction).
        h.db.unavailable = true;
        for (let i = 0; i < 33; i++) {
            await expect(exportAllSignalsOnce(maxOpts)).rejects.toMatchObject({ name: "TelemetrySourceError", status: 503 });
        }
        h.db.unavailable = false;
        for (let i = 1; i <= 128; i++) {
            const qid = `q-max-${String(128 + i).padStart(4, "0")}-${"q".repeat(240)}`;
            pushFixtureRow(h.db, {
                id: BigInt(640 + i),
                qitemId: qid,
                ts: "2026-10-04 09:03:00",
                state: "in-progress",
                actorSession: i <= 40 ? null : "seat@rig",
                closureReason: "handed_off_to",
                closureTarget: "target-secret",
                identityProvenance: "transport:v1",
                gaps: i <= 40 ? [{ code: "oversized_field", id: String(640 + i), field: "actorSession" }] : [],
            });
        }
        for (let r = 0; r < 4; r++) {
            const rigId = rigIds[r]!;
            h.src.inventory.nodesByRig![rigId] = Array.from({ length: 64 }, (_, s) => mockSeatNode(makeSeatId("seat-v2", r, s), rigId, {
                runtime: "claude-code",
                sessionStatus: "running",
                arbitratedNeedsInput: false,
                needsInputEvidence: { rung: "self-report", observedAt: time },
                activityState: {
                    activity: "working",
                    display: "working",
                    needsInput: { count: 0, reason: "SECRET-REASON" },
                    lastSwap: { generation: makeGen("v4", r, s), at: "2026-10-04T09:03:00.000Z" },
                },
            }));
        }
        const maxLogsRes = await exportAllSignalsOnce(maxOpts);
        expect(maxLogsRes).toMatchObject({
            outcome: "exported",
            exportedRecords: 128 + 33 + 256 + 1 + 1 + 27, // 446 records
        });

        // 3. Trace-only (--signals traces) must not silently drain failure-ledger entries without an export channel.
        const traceOnlyCkptPath = join(h.dir, "traces-only.json");
        const traceOnlyOpts = {
            ...h.options,
            checkpointPath: traceOnlyCkptPath,
            signals: ["traces"],
        };
        h.db.unavailable = true;
        await expect(exportQueueTransitionsOnce(traceOnlyOpts)).rejects.toMatchObject({ name: "TelemetrySourceError", status: 503 });
        h.db.unavailable = false;
        expect(readJson(traceOnlyCkptPath).ledger.entries).toHaveLength(1);
        await drain(traceOnlyOpts);
        expect(readJson(traceOnlyCkptPath).ledger.entries).toHaveLength(1);
    });
});
