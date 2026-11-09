#!/usr/bin/env python3
"""Offline unit and behavioral tests for OpenRig Showcase & Reliability (openrig_showcase.py).

Verifies:
- Ownership constants and strict isolation from SRE (sre.py) and legacy station provisioning
- folders_catalog() structure for dashboards and alerts folders
- V8 normalized dashboards (openrig-rig-layer & openrig-ai-layer), tabs, visual chart types,
  and non-overlapping 192-column V8 grid geometry
- DataFusion SQL compatibility (no MAX_BY; uses last_value(... ORDER BY _timestamp))
- alerts_catalog() 5 reliability alerts in OpenRig Reliability alert folder
- Two-phase plan/apply/idempotence/validate-queries/rollback lifecycle on PinnedWireFakeApi
"""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import openrig_showcase as ors
p = ors.p
sre = ors.sre
sm = ors.sre_model


class ShowcaseFakeApi:
    """In-memory OpenObserve 1.1.2 OSS API simulator supporting folders, dashboards, alerts, and _search."""

    def __init__(self):
        self.folders = {
            "dashboards": {
                "default": {"folderId": "default", "name": "default", "description": "Default", "icon": None},
            },
            "alerts": {
                "default": {"folderId": "default", "name": "default", "description": "Default", "icon": None},
            },
        }
        self.dashboards = {}
        self.alerts = {}
        self.views = {}
        self.destinations = [{"name": "envctl-sre-alert-webhook", "url": "http://127.0.0.1:9999/hook"}]
        self.seq = 200

    def _next_id(self, prefix):
        self.seq += 1
        return f"{prefix}-{self.seq}"

    def call(self, method, path, body=None):
        from urllib.parse import parse_qs, unquote, urlsplit

        parsed = urlsplit(path)
        route = parsed.path
        query = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
        body = copy.deepcopy(body)

        if route == "/config" and method == "GET":
            return {"commit_hash": p.COMMIT, "build_type": "opensource"}

        if route == "/api/default/alerts/destinations" and method == "GET":
            return copy.deepcopy(self.destinations)

        if route == "/api/default/_search" and method == "POST":
            sql = (body or {}).get("query", {}).get("sql", "")
            if "MAX_BY(" in sql.upper():
                raise p.SafeError("DataFusion unknown function max_by")
            return {"hits": [{"y_axis_1": 1}]}

        if route.startswith("/api/v2/default/folders/"):
            parts = [unquote(x) for x in route.split("/")[5:]]
            ftype = parts[0]
            bucket = self.folders[ftype]
            if len(parts) == 1:
                if method == "GET":
                    return {"list": list(bucket.values())}
                if method == "POST":
                    fid = self._next_id("fld-" + ftype[:4])
                    rec = {
                        "folderId": fid,
                        "name": body["name"],
                        "description": body.get("description", ""),
                        "icon": None,
                    }
                    bucket[fid] = rec
                    return copy.deepcopy(rec)
            elif len(parts) == 2:
                fid = parts[1]
                if fid not in bucket:
                    raise p.SafeError("Folder not found")
                if method == "GET":
                    return copy.deepcopy(bucket[fid])
                if method == "PUT":
                    bucket[fid]["name"] = body["name"]
                    bucket[fid]["description"] = body.get("description", "")
                    return {"code": 200}
                if method == "DELETE":
                    del bucket[fid]
                    return {"code": 200}

        if route == "/api/default/dashboards":
            folder = query.get("folder", "default")
            if method == "GET":
                return {
                    "dashboards": [
                        {"v8": copy.deepcopy(r["body"]), "hash": r["hash"]}
                        for r in self.dashboards.values()
                        if r["folderId"] == folder
                    ]
                }
            if method == "POST":
                did = self._next_id("dash")
                stored = copy.deepcopy(body)
                stored["dashboardId"] = did
                stored["owner"] = "operator@station.local"
                h = self._next_id("hash")
                self.dashboards[did] = {"body": stored, "hash": h, "folderId": folder}
                return {"v8": copy.deepcopy(stored), "hash": h}

        if route.startswith("/api/default/dashboards/"):
            did = unquote(route.split("/")[-1])
            folder = query.get("folder", "default")
            rec = self.dashboards.get(did)
            if not rec or rec["folderId"] != folder:
                raise p.SafeError("Dashboard not found")
            if method == "GET":
                return {"v8": copy.deepcopy(rec["body"]), "hash": rec["hash"]}
            if method == "PUT":
                if query.get("hash") != rec["hash"]:
                    raise p.SafeError("Hash conflict")
                stored = copy.deepcopy(body)
                stored["dashboardId"] = did
                stored["owner"] = rec["body"].get("owner", "operator@station.local")
                h = self._next_id("hash")
                rec["body"] = stored
                rec["hash"] = h
                return {"v8": copy.deepcopy(stored), "hash": h}
            if method == "DELETE":
                del self.dashboards[did]
                return {"code": 200}

        if route == "/api/v2/default/alerts":
            if method == "GET":
                return {
                    "list": [
                        {
                            "alert_id": aid,
                            "folder_id": r["folderId"],
                            "name": r["body"]["name"],
                            "description": r["body"].get("description", ""),
                            "stream_name": r["body"].get("stream_name"),
                            "stream_type": r["body"].get("stream_type"),
                            "enabled": r["body"].get("enabled", False),
                        }
                        for aid, r in self.alerts.items()
                    ]
                }
            if method == "POST":
                folder = query.get("folder", "default")
                if not body.get("destinations"):
                    raise p.SafeError("Alert destination is required")
                aid = self._next_id("alert")
                stored = copy.deepcopy(body)
                stored.pop("folderId", None)
                stored.pop("folder_id", None)
                stored["id"] = aid
                stored["owner"] = "operator@station.local"
                self.alerts[aid] = {"body": stored, "folderId": folder}
                return {"code": 200, "id": aid, "name": stored["name"]}

        if route.startswith("/api/v2/default/alerts/"):
            aid = unquote(route.split("/")[-1])
            rec = self.alerts.get(aid)
            if not rec:
                raise p.SafeError("Alert not found")
            if method == "GET":
                detail = copy.deepcopy(rec["body"])
                detail.pop("folderId", None)
                detail.pop("folder_id", None)
                return detail
            if method == "PUT":
                stored = copy.deepcopy(body)
                stored.pop("folderId", None)
                stored.pop("folder_id", None)
                stored["id"] = aid
                rec["body"] = stored
                return {"code": 200, "id": aid}
            if method == "DELETE":
                del self.alerts[aid]
                return {"code": 200}

        raise p.SafeError(f"Unhandled route {method} {path}")


class TestOpenRigShowcaseCatalog(unittest.TestCase):
    def test_constants_and_folders_catalog(self):
        self.assertEqual(ors.OPENRIG_SHOWCASE_MARKER, "Managed by envctl OpenRig showcase v1.")
        self.assertEqual(ors.DASHBOARD_PREFIX, "envctl-openrig: ")
        self.assertEqual(ors.ALERT_PREFIX, "envctl-openrig-alert-")
        self.assertEqual(
            ors.FOLDER_KEYS,
            ("openrig-reliability-dashboards", "openrig-reliability-alerts"),
        )
        self.assertEqual(ors.DASHBOARD_KEYS, ("openrig-rig-layer", "openrig-ai-layer"))
        self.assertEqual(len(ors.KNOWN_ALERT_NAMES), 5)

        folders = ors.folders_catalog()
        self.assertEqual(len(folders), 2)
        by_key = {f["key"]: f for f in folders}
        self.assertEqual(by_key["openrig-reliability-dashboards"]["name"], "50 OpenRig Reliability")
        self.assertEqual(by_key["openrig-reliability-dashboards"]["folderType"], "dashboards")
        self.assertIn(ors.OPENRIG_SHOWCASE_MARKER, by_key["openrig-reliability-dashboards"]["description"])
        self.assertEqual(by_key["openrig-reliability-alerts"]["name"], "OpenRig Reliability")
        self.assertEqual(by_key["openrig-reliability-alerts"]["folderType"], "alerts")
        self.assertIn(ors.OPENRIG_SHOWCASE_MARKER, by_key["openrig-reliability-alerts"]["description"])

    def test_dashboards_v8_schema_and_non_overlapping_layout(self):
        dashboards = {d["key"]: d for d in ors.dashboards_catalog()}
        self.assertEqual(set(dashboards.keys()), {"openrig-rig-layer", "openrig-ai-layer"})

        rig = dashboards["openrig-rig-layer"]["body"]
        ai = dashboards["openrig-ai-layer"]["body"]
        self.assertEqual(rig["version"], 8)
        self.assertEqual(ai["version"], 8)
        self.assertEqual([t["tabId"] for t in rig["tabs"]], ["sre-control", "sre-queues", "sre-exporter", "exec-rig"])
        self.assertEqual([t["tabId"] for t in ai["tabs"]], ["sre-inference", "sre-tools", "exec-ai"])

        for dkey, dspec in dashboards.items():
            self.assertEqual(dspec["folderKey"], "openrig-reliability-dashboards")
            body = dspec["body"]
            seen_ids = set()
            for tab in body["tabs"]:
                rects = []
                for pan in tab["panels"]:
                    pid = pan["id"]
                    self.assertNotIn(pid, seen_ids, f"Duplicate panel ID {pid} in {dkey}")
                    seen_ids.add(pid)
                    lay = pan["layout"]
                    x, y, w, h = lay["x"], lay["y"], lay["w"], lay["h"]
                    self.assertGreaterEqual(x, 0)
                    self.assertLess(x, 192)
                    self.assertGreaterEqual(w, 1)
                    self.assertLessEqual(x + w, 192)
                    self.assertGreaterEqual(y, 0)
                    self.assertGreaterEqual(h, 2)
                    for ox, oy, ow, oh, opid in rects:
                        overlap = not (x + w <= ox or ox + ow <= x or y + h <= oy or oy + oh <= y)
                        self.assertFalse(
                            overlap,
                            f"Panel {pid} ({x},{y},{w},{h}) overlaps {opid} ({ox},{oy},{ow},{oh}) in {dkey}/{tab['tabId']}",
                        )
                    rects.append((x, y, w, h, pid))

                    if pan["type"] != "markdown":
                        self.assertTrue(pan["queries"])
                        q = pan["queries"][0]
                        self.assertNotIn("MAX_BY(", q["query"].upper())
                        fields = q["fields"]
                        for axis_name in ("x", "y", "breakdown"):
                            for item in fields.get(axis_name, []):
                                self.assertEqual(item.get("type"), "custom")
                                self.assertEqual(item.get("args"), [])
                                self.assertEqual(item.get("functionName"), "count")

    def test_alerts_catalog_specs(self):
        alerts = ors.alerts_catalog(folder_id="fld-alerts-1", destinations=["envctl-sre-alert-webhook"], enabled=True)
        self.assertEqual(len(alerts), 5)
        names = [a["name"] for a in alerts]
        self.assertEqual(tuple(names), ors.KNOWN_ALERT_NAMES)
        for a in alerts:
            self.assertEqual(a["folderKey"], "openrig-reliability-alerts")
            b = a["body"]
            self.assertEqual(b["folder_id"], "fld-alerts-1")
            self.assertEqual(b["folderId"], "fld-alerts-1")
            self.assertEqual(b["destinations"], ["envctl-sre-alert-webhook"])
            self.assertTrue(b["enabled"])
            self.assertNotIn("MAX_BY(", b["query_condition"]["sql"].upper())

    def test_two_phase_apply_idempotence_validate_and_rollback(self):
        api = ShowcaseFakeApi()
        with tempfile.TemporaryDirectory() as tmp:
            res_f = ors.apply_phase(api, "folders", tmp)
            self.assertTrue(res_f["ok"])
            self.assertEqual(res_f["create"], 2)
            self.assertEqual(len(res_f["folderIds"]), 2)

            res_c = ors.apply_phase(api, "catalog", tmp)
            self.assertTrue(res_c["ok"])
            self.assertEqual(res_c["create"], 7)  # 2 dashboards + 5 alerts
            self.assertEqual(len(res_c["dashboardIds"]), 2)
            self.assertEqual(len(res_c["alertIds"]), 5)

            # Re-planning catalog after apply must be 100% idempotent
            inv_after = ors.inventory(api)
            plan_after = ors.build_plan(inv_after, phase="catalog")
            self.assertEqual(plan_after["create"], 0)
            self.assertEqual(plan_after["update"], 0)
            self.assertEqual(plan_after["unchanged"], 9)
            self.assertEqual(len(plan_after["actions"]), 0)

            # Query validation must accept all 52 SQL queries (47 dashboard panels + 5 alerts)
            val_report = ors.validate_queries(api)
            self.assertEqual(val_report["totalQueries"], 52)
            self.assertEqual(val_report["queriesAccepted"], 52)
            self.assertEqual(val_report["queriesRejected"], 0)
            self.assertTrue(all(r["sqlAccepted"] for r in val_report["results"]))

            # Verify Discussion #2573 explicit panel coverage
            panel_ids = {r["id"] for r in val_report["results"] if r["source"] == "dashboard"}
            for required_pid in (
                "rig_chart_seat_activity_timeline",
                "rig_table_seat_roster",
                "rig_chart_queue_state_spans",
                "rig_table_queue_state_spans",
                "rig_chart_observation_continuity_ts",
                "rig_exec_table_seat_tenure_lineage",
                "ai_chart_correlation_levels",
                "ai_table_producer_waits_approvals",
                "ai_exec_table_token_cost_provenance",
            ):
                self.assertIn(required_pid, panel_ids)

            # Rollback must cleanly remove all created showcase resources
            rb = ors.rollback_backup_dir(api, tmp)
            self.assertTrue(rb["ok"])
            inv_final = ors.inventory(api)
            self.assertEqual(len(inv_final["folders"]), 0)
            self.assertEqual(len(inv_final["dashboards"]), 0)
            self.assertEqual(len(inv_final["alerts"]), 0)


if __name__ == "__main__":
    unittest.main()
