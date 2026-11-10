#!/usr/bin/env python3
"""Standalone OpenRig Showcase & Reliability Provisioning Suite for OpenObserve.

Zero-dependency Python 3 provisioning CLI and portable V8 catalog generator for
`openrig-telemetry` (https://github.com/smota/openrig-telemetry).

Manages:
- Folders:
  - `50 OpenRig Reliability` (dashboards folder)
  - `OpenRig Reliability` (alerts folder)
- V8 Visual Showcase Dashboards (SRE + Executive Dual-Lens):
  - `envctl-openrig: OpenRig Rig Layer (Control Plane, Seats & Queues)` (4 tabs, 30 panels)
  - `envctl-openrig: OpenRig AI Layer (Models, Tooling, Sub-Agents & Quota)` (3 tabs, 19 panels)
- Actionable SQL Scheduled Alerts (5 rules):
  - `envctl-openrig-alert-exporter-failure` (P2)
  - `envctl-openrig-alert-coverage-gap` (P2)
  - `envctl-openrig-alert-seat-human-gate-stall` (P2)
  - `envctl-openrig-alert-fleet-bottleneck-high` (P2)
  - `envctl-openrig-alert-ai-quota-low` (P1)

Commands:
  plan             Build deterministic read-only plan (--phase folders|catalog)
  apply            Apply two-phase provisioning with backup & rollback journal
  validate-queries Execute all 52 SQL queries against live OpenObserve
  export-json      Export portable V8 dashboard and alert JSON artifacts
  rollback         Roll back applied journals in reverse phase order
"""
import argparse
import base64
import contextlib
import copy
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys
import time
from urllib.parse import quote, urlsplit


class _StandaloneProvisionNamespace:
    COMMIT = "d16d693ba99bc20621a58b948ecbcad7ec7fa882"
    ORG = os.environ.get("OPENOBSERVE_ORG", "default")
    BASE = f"/api/{ORG}"
    ALERTS = f"/api/v2/{ORG}/alerts"
    FOLDERS = f"/api/v2/{ORG}/folders"
    LIMIT = 4 * 1024 * 1024

    class SafeError(Exception):
        pass

    @staticmethod
    def digest(value):
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @classmethod
    def private_write(cls, path, value):
        path = Path(path).absolute()
        for parent in [path.parent, *path.parents]:
            if parent.is_symlink():
                raise cls.SafeError("Artifact parent must not be a symlink")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists() or path.is_symlink():
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise cls.SafeError("Existing artifact must be owned regular mode 0600")
        temporary = path.with_name(path.name + ".new")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()

    @classmethod
    def private_read(cls, path):
        info = Path(path).lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size > 16 * cls.LIMIT
        ):
            raise cls.SafeError("Input artifact must be owned regular mode 0600 and bounded")
        return json.loads(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def dashboard(cls, value):
        for version in range(8, 0, -1):
            if isinstance(value.get("v" + str(version)), dict):
                return value["v" + str(version)]
        if "version" in value and "title" in value:
            return value
        raise cls.SafeError("Unsupported dashboard response schema")

    @classmethod
    def subset(cls, actual, wanted):
        if isinstance(wanted, dict):
            return isinstance(actual, dict) and all(
                k in actual and cls.subset(actual[k], v) for k, v in wanted.items()
            )
        if isinstance(wanted, list):
            return (
                isinstance(actual, list)
                and len(actual) == len(wanted)
                and all(cls.subset(a, w) for a, w in zip(actual, wanted))
            )
        return actual == wanted

    @classmethod
    def mutation_guard(cls):
        if (
            platform.system() not in ("Linux", "Darwin")
            or os.environ.get("APPX_PACKAGE_FAMILY_NAME")
            or os.environ.get("PACKAGE_FAMILY_NAME")
        ):
            raise cls.SafeError("Provisioning mutations require an unpackaged POSIX process")

    class Api:
        def __init__(self):
            username = os.environ.get("ZO_ROOT_USER_EMAIL", "")
            password = os.environ.get("ZO_ROOT_USER_PASSWORD", "")
            if (
                not username
                or not password
                or ":" in username
                or any(c in username + password for c in "\r\n")
            ):
                raise _StandaloneProvisionNamespace.SafeError(
                    "Private backend credentials unavailable or invalid"
                )
            self.auth = (
                "Basic " + base64.b64encode((username + ":" + password).encode("utf-8")).decode("ascii")
            )
            raw_url = os.environ.get("OPENOBSERVE_URL", "http://127.0.0.1:5080")
            parsed = urlsplit(raw_url)
            self.scheme = parsed.scheme or "http"
            self.host = parsed.hostname or "127.0.0.1"
            self.port = parsed.port or (443 if self.scheme == "https" else 5080)

        def call(self, method, path, body=None):
            if not path.startswith("/api/") and path != "/config":
                raise _StandaloneProvisionNamespace.SafeError("API path outside provisioning boundary")
            conn_cls = (
                http.client.HTTPSConnection
                if self.scheme == "https"
                else http.client.HTTPConnection
            )
            connection = conn_cls(self.host, self.port, timeout=4)
            try:
                data = None if body is None else json.dumps(body).encode("utf-8")
                connection.request(
                    method,
                    path,
                    data,
                    {"Authorization": self.auth, "Content-Type": "application/json"},
                )
                deadline = time.monotonic() + 8
                response = connection.getresponse()
                chunks, size = [], 0
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise _StandaloneProvisionNamespace.SafeError(
                            "Backend response deadline exceeded"
                        )
                    if connection.sock is not None:
                        connection.sock.settimeout(min(4, remaining))
                    chunk = response.read1(
                        min(65536, _StandaloneProvisionNamespace.LIMIT + 1 - size)
                    )
                    if not chunk:
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > _StandaloneProvisionNamespace.LIMIT:
                        raise _StandaloneProvisionNamespace.SafeError(
                            "Backend response exceeded limit"
                        )
                raw_bytes = b"".join(chunks)
                if len(raw_bytes) > _StandaloneProvisionNamespace.LIMIT or not (
                    200 <= response.status < 300
                ):
                    raise _StandaloneProvisionNamespace.SafeError(
                        "Backend request rejected or exceeded limit; response suppressed"
                    )
                return json.loads(raw_bytes) if raw_bytes else {}
            except _StandaloneProvisionNamespace.SafeError:
                raise
            except Exception:
                raise _StandaloneProvisionNamespace.SafeError(
                    "Backend request failed; details suppressed"
                ) from None
            finally:
                connection.close()


p = _StandaloneProvisionNamespace()


class _StandaloneSreModelNamespace:
    V8_FIELD_AXES = (
        "x", "y", "z", "breakdown",
        "latitude", "longitude", "weight",
        "source", "target", "value", "name", "value_for_maps",
    )

    @staticmethod
    def tab(tab_id, name, panels):
        return {"tabId": tab_id, "name": name, "panels": panels}

    @staticmethod
    def normalize_panel_layout(layout):
        if not isinstance(layout, dict):
            return layout
        normalized = dict(layout)
        raw_w = normalized.get("w", 192)
        raw_x = normalized.get("x", 0)
        raw_h = normalized.get("h", 8)
        raw_y = normalized.get("y", 0)
        x = max(0, min(191, int(raw_x)))
        w = max(1, min(192 - x, int(raw_w)))
        h = max(1, int(raw_h) * 2)
        y = max(0, int(raw_y) * 2)
        normalized["x"] = x
        normalized["w"] = w
        normalized["h"] = h
        normalized["y"] = y
        return normalized

    @staticmethod
    def _migrate_v7_field_to_v8(field_item, is_custom_query=True):
        if not isinstance(field_item, dict):
            return
        is_histogram = field_item.get("aggregationFunction") == "histogram"
        args = field_item.get("args")
        if not args or not is_histogram:
            field_item["args"] = []
        else:
            for arg in field_item["args"]:
                if isinstance(arg, dict) and not arg.get("type"):
                    arg["type"] = "histogramInterval"
        if is_custom_query:
            field_item["type"] = "custom"
        else:
            field_item["type"] = "build"
            field_item["args"].insert(
                0,
                {
                    "type": "field",
                    "value": {"field": field_item.get("column"), "streamAlias": None},
                },
            )
            field_item.pop("column", None)
        if field_item.get("aggregationFunction"):
            field_item["functionName"] = field_item.pop("aggregationFunction")
        else:
            field_item.pop("aggregationFunction", None)
            field_item.pop("functionName", None)

    @classmethod
    def _migrate_fields(cls, fields, is_custom_query=True):
        if not fields:
            return
        if isinstance(fields, list):
            for f in fields:
                cls._migrate_v7_field_to_v8(f, is_custom_query)
        elif isinstance(fields, dict):
            cls._migrate_v7_field_to_v8(fields, is_custom_query)

    @classmethod
    def _migrate_filter_conditions(cls, filter_obj):
        if not isinstance(filter_obj, dict):
            return filter_obj
        conditions = filter_obj.get("conditions")
        if isinstance(conditions, list):
            migrated = []
            for cond in conditions:
                if not isinstance(cond, dict):
                    migrated.append(cond)
                    continue
                if cond.get("filterType") == "group":
                    migrated.append(cls._migrate_filter_conditions(cond))
                else:
                    c = dict(cond)
                    col = c.get("column")
                    if isinstance(col, str):
                        c["column"] = {"streamAlias": None, "field": col}
                    migrated.append(c)
            filter_obj["conditions"] = migrated
        return filter_obj

    @classmethod
    def normalize_dashboard(cls, dashboard_body):
        if not isinstance(dashboard_body, dict):
            return dashboard_body
        if dashboard_body.get("version") == 8:
            return copy.deepcopy(dashboard_body)
        cloned = copy.deepcopy(dashboard_body)
        cloned["version"] = 8
        desc = str(cloned.get("description", ""))
        match = re.search(r"key=([A-Za-z0-9_-]+)", desc)
        dash_key = match.group(1) if match else "dashboard"

        def _normalize_panel(pan, panel_id=None):
            if not isinstance(pan, dict):
                return
            if panel_id:
                pan["id"] = panel_id
            if "layout" in pan and isinstance(pan["layout"], dict):
                pan["layout"] = cls.normalize_panel_layout(pan["layout"])
            for q in pan.get("queries", []):
                if not isinstance(q, dict):
                    continue
                is_custom = bool(q.get("customQuery", True))
                fields = q.get("fields")
                if isinstance(fields, dict):
                    for axis in cls.V8_FIELD_AXES:
                        if axis in fields:
                            cls._migrate_fields(fields[axis], is_custom)
                    if "filter" in fields:
                        fields["filter"] = cls._migrate_filter_conditions(fields["filter"])

        for t in cloned.get("tabs", []):
            tab_id = t.get("tabId", "default")
            for idx, pan in enumerate(t.get("panels", []), 1):
                _normalize_panel(pan, f"envctl-sre-{dash_key}-{tab_id}-{idx}")
        for idx, pan in enumerate(cloned.get("panels", []), 1):
            _normalize_panel(pan, f"envctl-sre-{dash_key}-default-{idx}")
        return cloned


sre_model = _StandaloneSreModelNamespace()


class _StandaloneSreNamespace:
    @staticmethod
    def lock_file_path():
        custom = os.environ.get("ENVCTL_SRE_LOCK_PATH")
        if custom:
            return Path(custom).absolute()
        return (Path.home() / ".local/state/openrig-telemetry/mutation.lock").absolute()

    @classmethod
    @contextlib.contextmanager
    def global_lock(cls):
        path = cls.lock_file_path()
        for parent in [path.parent, *path.parents]:
            if parent.is_symlink():
                raise p.SafeError("Lock parent must not be a symlink; HOLD")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.is_symlink():
            raise p.SafeError("Lock path must not be a symlink; HOLD")
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise p.SafeError("Lock file must be owned regular mode 0600; HOLD")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise p.SafeError("Concurrent provisioning mutation lock held; HOLD") from None
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)

    @staticmethod
    def identifier(kind, value):
        if kind == "folder":
            raw = value.get("folderId") or value.get("folder_id")
        elif kind == "dashboard":
            raw = value.get("dashboardId")
        else:
            raw = value.get("id") or value.get("alert_id")
        if not isinstance(raw, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", raw):
            raise p.SafeError("Unsupported backend resource identifier; HOLD")
        return raw

    @classmethod
    def verify_dashboard_folder_membership(cls, api, dashboard_id, expected_folder_id):
        dash_list = api.call(
            "GET", p.BASE + "/dashboards?folder=" + quote(expected_folder_id, safe="")
        ).get("dashboards", [])
        if not isinstance(dash_list, list):
            raise p.SafeError("Invalid dashboards listing response; HOLD")
        for raw in dash_list:
            summary = p.dashboard(raw)
            did = cls.identifier("dashboard", summary)
            if did == dashboard_id:
                explicit_fid = (
                    raw.get("folderId")
                    or raw.get("folder_id")
                    or summary.get("folderId")
                    or summary.get("folder_id")
                )
                if explicit_fid and explicit_fid != expected_folder_id:
                    raise p.SafeError("Dashboard summary folder disagreement in folder listing; HOLD")
                return True
        return False

    @staticmethod
    def verify_alert_folder_membership(api, alert_id, expected_folder_id):
        alert_list = api.call("GET", p.ALERTS).get("list", [])
        if not isinstance(alert_list, list):
            raise p.SafeError("Invalid alerts listing response; HOLD")
        matches = [
            item for item in alert_list if (item.get("alert_id") or item.get("id")) == alert_id
        ]
        if len(matches) != 1:
            raise p.SafeError("Alert listing readback ambiguous or not found; HOLD")
        actual_fid = matches[0].get("folder_id") or matches[0].get("folderId") or "default"
        if actual_fid != expected_folder_id:
            raise p.SafeError("Alert folder_id in backend listing does not match destination folder; HOLD")
        return actual_fid

    @classmethod
    def read_resource(cls, api, kind, key, folder_id=None, folder_type=None):
        if kind == "folder":
            if folder_type not in ("dashboards", "alerts"):
                raise p.SafeError("Folder read requires dashboards or alerts folder_type; HOLD")
            raw = api.call("GET", p.FOLDERS + "/" + folder_type + "/" + quote(key, safe=""))
            fid = cls.identifier("folder", raw)
            body = copy.deepcopy(raw)
            body["folderId"] = fid
            return {"body": body, "folderId": fid, "folderType": folder_type}
        if kind == "dashboard":
            fid = folder_id or "default"
            raw = api.call(
                "GET",
                p.BASE + "/dashboards/" + quote(key, safe="") + "?folder=" + quote(fid, safe=""),
            )
            return {
                "body": p.dashboard(raw),
                "hash": raw.get("hash"),
                "folderId": fid,
                "folderType": "dashboards",
            }
        if kind == "alert":
            if folder_id:
                actual_fid = cls.verify_alert_folder_membership(api, key, folder_id)
            else:
                alert_list = api.call("GET", p.ALERTS).get("list", [])
                matches = [
                    item for item in alert_list if (item.get("alert_id") or item.get("id")) == key
                ]
                if len(matches) != 1:
                    raise p.SafeError("Alert folder resolution ambiguous on readback; HOLD")
                actual_fid = matches[0].get("folder_id") or matches[0].get("folderId") or "default"
            path = p.ALERTS + "/" + quote(key, safe="") + "?folder=" + quote(actual_fid, safe="")
            detail = api.call("GET", path)
            return {"body": detail, "hash": None, "folderId": actual_fid, "folderType": "alerts"}
        raise p.SafeError("Unsupported resource kind; HOLD")

    @staticmethod
    def expected(body, kind):
        body = copy.deepcopy(body)
        if kind == "folder":
            for key in ("folderId", "folder_id", "icon"):
                body.pop(key, None)
        elif kind == "dashboard":
            for key in ("dashboardId", "owner", "created", "folderId", "folder_id"):
                body.pop(key, None)
        elif kind == "alert":
            for key in (
                "id", "alert_id", "folderId", "folder_id", "owner",
                "last_edited_by", "updated_at", "last_triggered_at", "last_satisfied_at",
            ):
                body.pop(key, None)
        return body

    @classmethod
    def body_matches(cls, actual_body, desired_body, kind):
        return p.subset(actual_body, cls.expected(desired_body, kind))

    @classmethod
    def move_resource(cls, api, kind, resource_id, from_folder_id, dst_folder_id):
        if not from_folder_id or not dst_folder_id:
            raise p.SafeError("Move requires source and destination folder IDs; HOLD")
        if kind == "dashboard":
            api.call(
                "PATCH",
                p.BASE + "/dashboards/move?folder=" + quote(from_folder_id, safe=""),
                {"dashboard_ids": [resource_id], "dst_folder_id": dst_folder_id},
            )
            if not cls.verify_dashboard_folder_membership(api, resource_id, dst_folder_id):
                raise p.SafeError(
                    "Dashboard move verification failed: not found in destination folder listing; HOLD"
                )
            if from_folder_id != dst_folder_id and cls.verify_dashboard_folder_membership(
                api, resource_id, from_folder_id
            ):
                raise p.SafeError(
                    "Dashboard move verification failed: still listed in source folder; HOLD"
                )
            moved = cls.read_resource(api, "dashboard", resource_id, folder_id=dst_folder_id)
        elif kind == "alert":
            api.call(
                "PATCH",
                p.ALERTS + "/move?folder=" + quote(from_folder_id, safe=""),
                {"alert_ids": [resource_id], "anomaly_config_ids": [], "dst_folder_id": dst_folder_id},
            )
            actual_fid = cls.verify_alert_folder_membership(api, resource_id, dst_folder_id)
            moved = cls.read_resource(api, "alert", resource_id, folder_id=actual_fid)
        else:
            raise p.SafeError("Move unsupported for resource kind; HOLD")
        if moved.get("folderId") != dst_folder_id or cls.identifier(kind, moved["body"]) != resource_id:
            raise p.SafeError("Resource move readback verification failed; HOLD")
        return moved

    @classmethod
    def write_resource(cls, api, action, current_state):
        kind = action["kind"]
        body = copy.deepcopy(action["desired"])
        target_fid = action.get("targetFolderId")
        ftype = action.get("folderType")
        if current_state is None:
            if kind == "folder":
                response = api.call(
                    "POST",
                    p.FOLDERS + "/" + ftype,
                    {"name": body["name"], "description": body["description"]},
                )
                fid = cls.identifier("folder", response)
                return cls.read_resource(api, "folder", fid, folder_type=ftype)
            if kind == "dashboard":
                wire = copy.deepcopy(body)
                wire.pop("folderId", None)
                wire.pop("folder_id", None)
                response = api.call(
                    "POST", p.BASE + "/dashboards?folder=" + quote(target_fid, safe=""), wire
                )
                did = cls.identifier("dashboard", p.dashboard(response))
                return cls.read_resource(api, "dashboard", did, folder_id=target_fid)
        res_id = cls.identifier(kind, current_state["body"])
        if kind == "folder":
            if res_id == "default":
                raise p.SafeError("Refusing to mutate default folder; HOLD")
            wire = {"folderId": res_id, "name": body["name"], "description": body["description"]}
            api.call("PUT", p.FOLDERS + "/" + ftype + "/" + quote(res_id, safe=""), wire)
            return cls.read_resource(api, "folder", res_id, folder_type=ftype)
        if kind == "dashboard":
            fid = current_state.get("folderId") or target_fid or "default"
            if not current_state.get("hash"):
                raise p.SafeError("Dashboard concurrency hash missing; HOLD")
            wire = copy.deepcopy(body)
            wire.pop("folderId", None)
            wire.pop("folder_id", None)
            wire["dashboardId"] = res_id
            wire["owner"] = current_state["body"].get("owner", "")
            wire["created"] = current_state["body"].get("created", wire.get("created", ""))
            wire["role"] = current_state["body"].get("role", wire.get("role", ""))
            api.call(
                "PUT",
                p.BASE
                + "/dashboards/"
                + quote(res_id, safe="")
                + "?folder="
                + quote(fid, safe="")
                + "&hash="
                + quote(str(current_state["hash"]), safe=""),
                wire,
            )
            return cls.read_resource(api, "dashboard", res_id, folder_id=fid)
        raise p.SafeError("Unsupported resource kind; HOLD")

    @classmethod
    def delete_resource(cls, api, kind, current_state):
        res_id = cls.identifier(kind, current_state["body"])
        if kind == "folder":
            if res_id == "default":
                raise p.SafeError("Never delete default folder; HOLD")
            ftype = current_state["folderType"]
            api.call("DELETE", p.FOLDERS + "/" + ftype + "/" + quote(res_id, safe=""))
        elif kind == "dashboard":
            fid = current_state.get("folderId") or "default"
            api.call(
                "DELETE",
                p.BASE + "/dashboards/" + quote(res_id, safe="") + "?folder=" + quote(fid, safe=""),
            )
        elif kind == "alert":
            fid = current_state.get("folderId") or "default"
            api.call(
                "DELETE",
                p.ALERTS + "/" + quote(res_id, safe="") + "?folder=" + quote(fid, safe=""),
            )
        else:
            raise p.SafeError("Unsupported resource kind for delete; HOLD")


sre = _StandaloneSreNamespace()

OPENRIG_SHOWCASE_MARKER = "Managed by envctl OpenRig showcase v1."
DASHBOARD_PREFIX = "envctl-openrig: "
ALERT_PREFIX = "envctl-openrig-alert-"

FOLDER_KEYS = (
    "openrig-reliability-dashboards",
    "openrig-reliability-alerts",
)
DASHBOARD_KEYS = (
    "openrig-rig-layer",
    "openrig-ai-layer",
)
KNOWN_ALERT_NAMES = (
    "envctl-openrig-alert-exporter-failure",
    "envctl-openrig-alert-coverage-gap",
    "envctl-openrig-alert-seat-human-gate-stall",
    "envctl-openrig-alert-fleet-bottleneck-high",
    "envctl-openrig-alert-ai-quota-low",
)

FOLDERS_API = "/api/v2/default/folders"
PREFERRED_WEBHOOK_DEST = "envctl-sre-alert-webhook"


def ensure_env_credentials():
    """Load OpenObserve credentials from canonical station env files if not in os.environ."""
    if os.environ.get("ZO_ROOT_USER_EMAIL") and os.environ.get("ZO_ROOT_USER_PASSWORD"):
        return
    candidates = [
        Path.home() / ".config/observability/openobserve.env",
        Path.home() / ".config/mtn/openobserve.env",
    ]
    for env_path in candidates:
        if env_path.is_file():
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            if os.environ.get("ZO_ROOT_USER_EMAIL") and os.environ.get("ZO_ROOT_USER_PASSWORD"):
                return


def _axis(col, label=None, color=None):
    axis = {
        "label": label or col,
        "alias": col,
        "column": col,
        "color": color,
        "aggregationFunction": "count",
    }
    return axis


def _base_panel(pid, title, desc, panel_type, x, y, w, h, config=None, queries=None, markdown_content=""):
    cfg = {
        "title": title,
        "description": desc,
        "show_legends": True,
        "no_value_replacement": "",
    }
    if config:
        cfg.update(config)
    return {
        "id": pid,
        "title": title,
        "description": desc,
        "type": panel_type,
        "config": cfg,
        "queryType": "sql",
        "layout": {
            "x": int(x),
            "y": int(y),
            "w": int(w),
            "h": int(h),
            "i": 1,
        },
        "queries": queries if queries is not None else [],
        "htmlContent": "",
        "markdownContent": markdown_content,
        "customChartContent": "",
    }


def _sql_query(stream_type, stream, sql, x_axes, y_axes, breakdown_axes=None):
    return {
        "query": sql.strip(),
        "vrlFunctionQuery": "",
        "customQuery": True,
        "fields": {
            "stream": stream,
            "stream_type": stream_type,
            "x": list(x_axes),
            "y": list(y_axes),
            "z": [],
            "breakdown": list(breakdown_axes) if breakdown_axes else [],
            "filter": {
                "filterType": "group",
                "logicalOperator": "AND",
                "conditions": [],
            },
        },
        "config": {
            "promql_legend": "",
        },
    }


def metric_panel(pid, title, desc, stream_type, stream, sql, x, y, w=48, h=4, unit=None, color="#5960b2"):
    cfg = {
        "title": title,
        "description": desc,
        "show_legends": False,
        "unit": unit,
    }
    q = _sql_query(
        stream_type=stream_type,
        stream=stream,
        sql=sql,
        x_axes=[],
        y_axes=[_axis("y_axis_1", title, color)],
        breakdown_axes=[],
    )
    return _base_panel(pid, title, desc, "metric", x, y, w, h, config=cfg, queries=[q])


def bar_panel(
    pid,
    title,
    desc,
    stream_type,
    stream,
    sql,
    x,
    y,
    w=96,
    h=7,
    x_label="Category",
    y_label="Value",
    color="#5960b2",
    stacked=False,
    breakdown_col=None,
    breakdown_label="Series",
):
    panel_type = "stacked" if stacked else "bar"
    b_axes = [_axis(breakdown_col, breakdown_label)] if breakdown_col else []
    q = _sql_query(
        stream_type=stream_type,
        stream=stream,
        sql=sql,
        x_axes=[_axis("x_axis_1", x_label)],
        y_axes=[_axis("y_axis_1", y_label, color)],
        breakdown_axes=b_axes,
    )
    return _base_panel(
        pid,
        title,
        desc,
        panel_type,
        x,
        y,
        w,
        h,
        config={"title": title, "description": desc, "show_legends": True},
        queries=[q],
    )


def timeseries_panel(
    pid,
    title,
    desc,
    stream_type,
    stream,
    sql,
    x,
    y,
    w=96,
    h=7,
    y_label="Value",
    color="#5960b2",
    chart_type="area",
    breakdown_col=None,
    breakdown_label="Series",
):
    b_axes = [_axis(breakdown_col, breakdown_label)] if breakdown_col else []
    q = _sql_query(
        stream_type=stream_type,
        stream=stream,
        sql=sql,
        x_axes=[_axis("x_axis_1", "Time")],
        y_axes=[_axis("y_axis_1", y_label, color)],
        breakdown_axes=b_axes,
    )
    return _base_panel(
        pid,
        title,
        desc,
        chart_type,
        x,
        y,
        w,
        h,
        config={"title": title, "description": desc, "show_legends": True},
        queries=[q],
    )


def donut_panel(
    pid,
    title,
    desc,
    stream_type,
    stream,
    sql,
    x,
    y,
    w=96,
    h=7,
    x_label="Segment",
    y_label="Count",
    color="#5960b2",
):
    q = _sql_query(
        stream_type=stream_type,
        stream=stream,
        sql=sql,
        x_axes=[_axis("x_axis_1", x_label)],
        y_axes=[_axis("y_axis_1", y_label, color)],
        breakdown_axes=[],
    )
    return _base_panel(
        pid,
        title,
        desc,
        "donut",
        x,
        y,
        w,
        h,
        config={"title": title, "description": desc, "show_legends": True},
        queries=[q],
    )


def table_panel(pid, title, desc, stream_type, stream, sql, cols, x, y, w=192, h=8):
    if not cols:
        raise ValueError("table_panel requires at least one column in cols")
    q = _sql_query(
        stream_type=stream_type,
        stream=stream,
        sql=sql,
        x_axes=[_axis(cols[0])],
        y_axes=[_axis(c) for c in cols[1:]],
        breakdown_axes=[],
    )
    return _base_panel(
        pid,
        title,
        desc,
        "table",
        x,
        y,
        w,
        h,
        config={"title": title, "description": desc, "show_legends": False},
        queries=[q],
    )


def markdown_panel(pid, title, desc, content, x, y, w=192, h=4):
    return _base_panel(
        pid,
        title,
        desc,
        "markdown",
        x,
        y,
        w,
        h,
        config={"title": title, "description": desc, "show_legends": False},
        queries=[],
        markdown_content=content,
    )


def normalize_showcase_dashboard(v5_dashboard):
    """Normalize a V5 showcase dashboard to OpenObserve V8 while preserving explicit panel IDs."""
    tab_panel_ids = [
        [pan.get("id") for pan in t.get("panels", [])]
        for t in v5_dashboard.get("tabs", [])
    ]
    normalized = sre_model.normalize_dashboard(v5_dashboard)
    for t_idx, t in enumerate(normalized.get("tabs", [])):
        for p_idx, pan in enumerate(t.get("panels", []), 1):
            orig_id = tab_panel_ids[t_idx][p_idx - 1]
            if orig_id:
                pan["id"] = orig_id
            if isinstance(pan.get("layout"), dict):
                pan["layout"]["i"] = p_idx
    return normalized


def folders_catalog():
    """Return the two OpenRig Reliability folders (dashboards + alerts)."""
    dash_desc = (
        f"OpenRig Rig Layer, AI Layer, and SRE/Executive reliability showcase dashboards. "
        f"{OPENRIG_SHOWCASE_MARKER}"
    )
    alert_desc = (
        f"Dedicated OpenRig reliability alerts covering telemetry exporter health, coverage gaps, "
        f"human-gate seat stalls, fleet bottlenecks, and AI provider quota exhaustion. "
        f"{OPENRIG_SHOWCASE_MARKER}"
    )
    return [
        {
            "kind": "folder",
            "key": "openrig-reliability-dashboards",
            "folderKey": "openrig-reliability-dashboards",
            "folderType": "dashboards",
            "name": "50 OpenRig Reliability",
            "description": dash_desc,
            "body": {
                "name": "50 OpenRig Reliability",
                "description": dash_desc,
            },
        },
        {
            "kind": "folder",
            "key": "openrig-reliability-alerts",
            "folderKey": "openrig-reliability-alerts",
            "folderType": "alerts",
            "name": "OpenRig Reliability",
            "description": alert_desc,
            "body": {
                "name": "OpenRig Reliability",
                "description": alert_desc,
            },
        },
    ]


def build_rig_layer_dashboard():
    """Dashboard 1: envctl-openrig: OpenRig Rig Layer (Control Plane, Seats & Queues)."""
    tab1_panels = [
        metric_panel(
            "rig_kpi_tracked_seats",
            "Tracked OpenRig Seats",
            "Total distinct OpenRig seats observed across seat presence telemetry.",
            "metrics",
            "openrig_seat_presence",
            'SELECT COUNT(DISTINCT openrig_seat_node_id) AS "y_axis_1" FROM "openrig_seat_presence"',
            x=0,
            y=0,
            w=48,
            h=4,
            color="#5960b2",
        ),
        metric_panel(
            "rig_kpi_working_seats",
            "Active Working Seats (Live)",
            "Seats currently in the 'working' execution state based on latest activity gauge.",
            "metrics",
            "openrig_seat_activity",
            (
                'SELECT COALESCE(SUM(latest_val), 0) AS "y_axis_1" FROM ('
                "SELECT openrig_seat_node_id, last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "openrig_seat_activity" WHERE openrig_seat_state = \'working\' '
                "GROUP BY openrig_seat_node_id)"
            ),
            x=48,
            y=0,
            w=48,
            h=4,
            color="#10b981",
        ),
        metric_panel(
            "rig_kpi_blocked_seats",
            "Seats Blocked on Human Input",
            "Seats currently waiting on a human approval or input gate.",
            "metrics",
            "openrig_seat_needs_input",
            (
                'SELECT COALESCE(SUM(latest_val), 0) AS "y_axis_1" FROM ('
                "SELECT openrig_seat_node_id, last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "openrig_seat_needs_input" GROUP BY openrig_seat_node_id)'
            ),
            x=96,
            y=0,
            w=48,
            h=4,
            color="#ef4444",
        ),
        metric_panel(
            "rig_kpi_bottleneck_pct",
            "Fleet Bottleneck Ratio (%)",
            "Percentage of active fleet seats stalled or bottlenecked.",
            "metrics",
            "agent_fleet_bottleneck_ratio",
            'SELECT ROUND(COALESCE(last_value(value ORDER BY _timestamp), 0) * 100.0, 1) AS "y_axis_1" FROM "agent_fleet_bottleneck_ratio"',
            x=144,
            y=0,
            w=48,
            h=4,
            unit="percent",
            color="#f59e0b",
        ),
        bar_panel(
            "rig_chart_presence_by_runtime",
            "Seat Presence by Runtime & Lifecycle Status",
            "Active seat presence grouped by agent runtime harness and lifecycle status.",
            "metrics",
            "openrig_seat_presence",
            (
                'SELECT COALESCE(openrig_seat_runtime, \'unknown\') AS "x_axis_1", '
                'SUM(latest_val) AS "y_axis_1", '
                'COALESCE(openrig_seat_status, \'unknown\') AS "breakdown_1" '
                "FROM (SELECT openrig_seat_node_id, openrig_seat_runtime, openrig_seat_status, "
                "last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "openrig_seat_presence" '
                "GROUP BY openrig_seat_node_id, openrig_seat_runtime, openrig_seat_status) "
                "WHERE latest_val > 0 GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=4,
            w=96,
            h=7,
            x_label="Runtime",
            y_label="Seats",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Status",
        ),
        donut_panel(
            "rig_chart_activity_mix",
            "Live Seat Activity State Distribution",
            "Proportional breakdown of live seat states (working, idle, needs-input).",
            "metrics",
            "openrig_seat_activity",
            (
                'SELECT COALESCE(openrig_seat_state, \'unknown\') AS "x_axis_1", '
                'SUM(latest_val) AS "y_axis_1" '
                "FROM (SELECT openrig_seat_node_id, openrig_seat_state, "
                "last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "openrig_seat_activity" '
                "GROUP BY openrig_seat_node_id, openrig_seat_state) "
                "WHERE latest_val > 0 GROUP BY x_axis_1 ORDER BY y_axis_1 DESC"
            ),
            x=96,
            y=4,
            w=96,
            h=7,
            x_label="Activity State",
            y_label="Seats",
        ),
        timeseries_panel(
            "rig_chart_seat_activity_timeline",
            "Seat Activity Timeline Over Time (Working vs Idle vs Needs-Input vs Unknown)",
            "Seat-state timeline over time from openrig_seat_activity samples (value > 0) for quiet-period vs active-work analysis.",
            "metrics",
            "openrig_seat_activity",
            (
                'SELECT histogram(_timestamp) AS "x_axis_1", '
                'SUM(value) AS "y_axis_1", '
                'COALESCE(openrig_seat_state, \'unknown\') AS "breakdown_1" '
                'FROM "openrig_seat_activity" '
                "WHERE value > 0 "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY x_axis_1 ASC"
            ),
            x=0,
            y=11,
            w=192,
            h=7,
            y_label="Active Seat Samples",
            chart_type="area",
            breakdown_col="breakdown_1",
            breakdown_label="Seat State",
        ),
        table_panel(
            "rig_table_seat_roster",
            "Actionable Seat Roster: Logical Identity, Presence vs Activity (Stopped-Seat Guard) & Freshness",
            "Joined presence + activity roster keyed by source_namespace + seat_node_id; never classifies stopped/exited/detached processes as idle and flags stale samples.",
            "metrics",
            "openrig_seat_activity",
            (
                "SELECT COALESCE(p.openrig_source_namespace, a.openrig_source_namespace, 'default') AS source_namespace, "
                "COALESCE(p.openrig_seat_node_id, a.openrig_seat_node_id) AS seat_node_id, "
                "COALESCE(p.openrig_seat_runtime, a.openrig_seat_runtime, 'unknown') AS runtime, "
                "COALESCE(p.presence_status, 'unknown') AS presence_status, "
                "CASE WHEN COALESCE(p.presence_status, 'unknown') != 'running' AND a.active_state = 'idle' "
                "THEN 'unknown (stopped-seat guard)' "
                "ELSE COALESCE(a.active_state, 'unknown') END AS activity_state, "
                "ROUND((date_part('epoch', now()) * 1000000 - GREATEST(COALESCE(p.last_ts, 0), COALESCE(a.last_ts, 0))) / 1000000.0, 1) AS sample_age_s, "
                "CASE WHEN (date_part('epoch', now()) * 1000000 - GREATEST(COALESCE(p.last_ts, 0), COALESCE(a.last_ts, 0))) > 300000000 "
                "THEN 'STALE: Departed or missing observation (>300s)' "
                "WHEN a.active_state = 'needs-input' "
                "THEN 'ACTION: Unblock human gate' "
                "WHEN COALESCE(p.presence_status, 'unknown') != 'running' "
                "THEN 'UNAVAILABLE: Process not running (not idle)' "
                "WHEN a.active_state = 'working' "
                "THEN 'OK: Executing' "
                "ELSE 'IDLE: Running & ready for dispatch' END AS sre_action "
                "FROM ("
                "SELECT openrig_source_namespace, openrig_seat_node_id, openrig_seat_runtime, "
                "last_value(openrig_seat_status ORDER BY _timestamp) AS presence_status, "
                "MAX(_timestamp) AS last_ts "
                'FROM "openrig_seat_presence" WHERE value > 0 '
                "GROUP BY openrig_source_namespace, openrig_seat_node_id, openrig_seat_runtime"
                ") p FULL OUTER JOIN ("
                "SELECT openrig_source_namespace, openrig_seat_node_id, openrig_seat_runtime, "
                "last_value(openrig_seat_state ORDER BY _timestamp) AS active_state, "
                "MAX(_timestamp) AS last_ts "
                'FROM "openrig_seat_activity" WHERE value > 0 '
                "GROUP BY openrig_source_namespace, openrig_seat_node_id, openrig_seat_runtime"
                ") a ON p.openrig_seat_node_id = a.openrig_seat_node_id "
                "AND p.openrig_source_namespace = a.openrig_source_namespace "
                "ORDER BY activity_state DESC, seat_node_id ASC"
            ),
            cols=["source_namespace", "seat_node_id", "runtime", "presence_status", "activity_state", "sample_age_s", "sre_action"],
            x=0,
            y=18,
            w=192,
            h=8,
        ),
    ]

    tab2_panels = [
        timeseries_panel(
            "rig_chart_queue_transitions_ts",
            "Queue State Transitions Over Time",
            "Time-series rate of OpenRig queue state transitions emitted by openrig-telemetry-exporter.",
            "logs",
            "default",
            (
                'SELECT histogram(_timestamp) AS "x_axis_1", COUNT(*) AS "y_axis_1", '
                'COALESCE(openrig_queue_state, \'unknown\') AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' AND openrig_record_kind = 'queue_transition' "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY x_axis_1 ASC"
            ),
            x=0,
            y=0,
            w=96,
            h=7,
            y_label="Transitions",
            chart_type="area",
            breakdown_col="breakdown_1",
            breakdown_label="Queue State",
        ),
        bar_panel(
            "rig_chart_queue_closure",
            "Queue Transitions by State & Closure Reason",
            "Observed queue transitions segmented by target state and closure reason.",
            "metrics",
            "openrig_queue_transition_observed",
            (
                'SELECT COALESCE(openrig_queue_state, \'unknown\') AS "x_axis_1", '
                'MAX(value) AS "y_axis_1", '
                'COALESCE(openrig_queue_closure_reason, \'active\') AS "breakdown_1" '
                'FROM "openrig_queue_transition_observed" '
                "GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=96,
            y=0,
            w=96,
            h=7,
            x_label="Queue State",
            y_label="Observed Transitions",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Closure Reason",
        ),
        bar_panel(
            "rig_chart_dwell_seconds",
            "Cumulative Queue Dwell Time (Seconds) by State Transition",
            "Total time items spent dwelling in each queue state before transitioning.",
            "metrics",
            "openrig_queue_state_duration_sum",
            (
                "SELECT CONCAT(COALESCE(openrig_queue_state, 'unknown'), ' -> ', "
                'COALESCE(openrig_queue_departure_state, \'unknown\')) AS "x_axis_1", '
                'ROUND(MAX(value), 2) AS "y_axis_1" '
                'FROM "openrig_queue_state_duration_sum" '
                "GROUP BY openrig_queue_state, openrig_queue_departure_state ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=7,
            w=96,
            h=7,
            x_label="Transition Path",
            y_label="Duration (s)",
            color="#f59e0b",
        ),
        table_panel(
            "rig_table_dwell_buckets",
            "Queue Dwell Latency Histogram Buckets (SLO Distribution)",
            "Cumulative histogram bucket distribution for queue state dwell duration.",
            "metrics",
            "openrig_queue_state_duration_bucket",
            (
                "SELECT COALESCE(openrig_queue_state, 'unknown') AS queue_state, "
                "COALESCE(openrig_queue_departure_state, 'unknown') AS departure_state, "
                "COALESCE(le, '+Inf') AS le_seconds, "
                "MAX(value) AS cumulative_count "
                'FROM "openrig_queue_state_duration_bucket" '
                "GROUP BY openrig_queue_state, openrig_queue_departure_state, le "
                "ORDER BY queue_state ASC, cumulative_count ASC"
            ),
            cols=["queue_state", "departure_state", "le_seconds", "cumulative_count"],
            x=96,
            y=7,
            w=96,
            h=7,
        ),
        table_panel(
            "rig_table_transition_ledger",
            "OpenRig Queue Transition & Seat Tenure Audit Ledger",
            "Audit ledger of queue transitions and seat tenure events with coverage and replay flags.",
            "logs",
            "default",
            (
                "SELECT COALESCE(openrig_record_kind, 'unknown') AS record_kind, "
                "COALESCE(openrig_queue_state, 'n/a') AS queue_state, "
                "COALESCE(openrig_coverage_status, 'complete') AS coverage_status, "
                "COALESCE(CAST(openrig_delivery_replayed AS VARCHAR), 'false') AS replayed, "
                "COUNT(*) AS events "
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' "
                "GROUP BY openrig_record_kind, openrig_queue_state, openrig_coverage_status, openrig_delivery_replayed "
                "ORDER BY events DESC"
            ),
            cols=["record_kind", "queue_state", "coverage_status", "replayed", "events"],
            x=0,
            y=14,
            w=192,
            h=7,
        ),
        bar_panel(
            "rig_chart_queue_state_spans",
            "Completed Queue-State Coordination Spans (openrig.queue.state Mean Duration ms)",
            "Mean duration (ms) of completed observed queue-state root spans (pending, in-progress, blocked) by departure state.",
            "traces",
            "default",
            (
                'SELECT COALESCE(openrig_queue_state, \'unknown\') AS "x_axis_1", '
                'ROUND(AVG(duration) / 1000.0, 2) AS "y_axis_1", '
                'COALESCE(openrig_queue_departure_state, \'unknown\') AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' AND operation_name = 'openrig.queue.state' "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=21,
            w=96,
            h=7,
            x_label="Queue State Interval",
            y_label="Mean Duration (ms)",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Departure State",
        ),
        table_panel(
            "rig_table_queue_state_spans",
            "Root Queue-State Span Intervals (openrig.queue.state Enter -> Depart Audit)",
            "Completed queue-state coordination spans from traces/default with enter/depart transition IDs and duration.",
            "traces",
            "default",
            (
                "SELECT COALESCE(service_openrig_source_namespace, 'default') AS source_namespace, "
                "COALESCE(openrig_queue_state, 'unknown') AS queue_state, "
                "COALESCE(openrig_queue_departure_state, 'unknown') AS departure_state, "
                "COALESCE(openrig_transition_enter_id, 'n/a') AS enter_id, "
                "COALESCE(openrig_transition_depart_id, 'n/a') AS depart_id, "
                "ROUND(MAX(duration) / 1000.0, 2) AS duration_ms "
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' AND operation_name = 'openrig.queue.state' "
                "GROUP BY service_openrig_source_namespace, openrig_queue_state, openrig_queue_departure_state, "
                "openrig_transition_enter_id, openrig_transition_depart_id "
                "ORDER BY duration_ms DESC LIMIT 25"
            ),
            cols=["source_namespace", "queue_state", "departure_state", "enter_id", "depart_id", "duration_ms"],
            x=96,
            y=21,
            w=96,
            h=7,
        ),
    ]

    tab3_panels = [
        metric_panel(
            "rig_kpi_exporter_failures",
            "Telemetry Exporter Failures",
            "Total exporter failures across stages and categories.",
            "metrics",
            "openrig_telemetry_failure",
            (
                'SELECT COALESCE(SUM(max_val), 0) AS "y_axis_1" FROM ('
                "SELECT openrig_failure_stage, openrig_failure_category, MAX(value) AS max_val "
                'FROM "openrig_telemetry_failure" '
                "GROUP BY openrig_failure_stage, openrig_failure_category)"
            ),
            x=0,
            y=0,
            w=64,
            h=4,
            color="#ef4444",
        ),
        metric_panel(
            "rig_kpi_coverage_gaps",
            "Telemetry Coverage Gaps Detected",
            "Total telemetry coverage gaps recorded across gap codes.",
            "metrics",
            "openrig_telemetry_coverage_gap",
            (
                'SELECT COALESCE(SUM(max_val), 0) AS "y_axis_1" FROM ('
                "SELECT openrig_gap_code, MAX(value) AS max_val "
                'FROM "openrig_telemetry_coverage_gap" GROUP BY openrig_gap_code)'
            ),
            x=64,
            y=0,
            w=64,
            h=4,
            color="#f59e0b",
        ),
        metric_panel(
            "rig_kpi_bridge_drops",
            "OTel Bridge Dropped Events",
            "Cumulative events dropped by agent-otel-bridge.",
            "metrics",
            "agent_bridge_dropped_total",
            'SELECT COALESCE(MAX(value), 0) AS "y_axis_1" FROM "agent_bridge_dropped_total"',
            x=128,
            y=0,
            w=64,
            h=4,
            color="#10b981",
        ),
        bar_panel(
            "rig_chart_failures_by_stage",
            "Exporter Failures by Stage & Category",
            "Breakdown of telemetry exporter failures by pipeline stage and error category.",
            "metrics",
            "openrig_telemetry_failure",
            (
                'SELECT COALESCE(openrig_failure_stage, \'none\') AS "x_axis_1", '
                'MAX(value) AS "y_axis_1", '
                'COALESCE(openrig_failure_category, \'none\') AS "breakdown_1" '
                'FROM "openrig_telemetry_failure" '
                "GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=4,
            w=96,
            h=7,
            x_label="Failure Stage",
            y_label="Failures",
            color="#ef4444",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Category",
        ),
        bar_panel(
            "rig_chart_gaps_by_code",
            "Telemetry Coverage Gaps by Gap Code",
            "Observed coverage gaps classified by OpenRig gap code.",
            "metrics",
            "openrig_telemetry_coverage_gap",
            (
                'SELECT COALESCE(openrig_gap_code, \'none\') AS "x_axis_1", '
                'MAX(value) AS "y_axis_1" '
                'FROM "openrig_telemetry_coverage_gap" '
                "GROUP BY x_axis_1 ORDER BY y_axis_1 DESC"
            ),
            x=96,
            y=4,
            w=96,
            h=7,
            x_label="Gap Code",
            y_label="Gap Count",
            color="#f59e0b",
        ),
        timeseries_panel(
            "rig_chart_observation_continuity_ts",
            "Observation Continuity & Quiet-Period Verification Timeline (Transitions vs Tenure vs Gaps vs Failures)",
            "Distinguishes true quiet periods from missing observation, coverage gaps, or boot-epoch discontinuities.",
            "logs",
            "default",
            (
                'SELECT histogram(_timestamp) AS "x_axis_1", '
                'COUNT(*) AS "y_axis_1", '
                'COALESCE(openrig_record_kind, \'unknown\') AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY x_axis_1 ASC"
            ),
            x=0,
            y=11,
            w=192,
            h=7,
            y_label="Emitted Records",
            chart_type="area",
            breakdown_col="breakdown_1",
            breakdown_label="Record Kind",
        ),
        table_panel(
            "rig_table_exporter_health",
            "Exporter & Bridge Self-Health Matrix (Epochs, Streams & Delivery)",
            "Self-health matrix tracking exporter boot epochs, streams, and emitted record counts.",
            "logs",
            "default",
            (
                "SELECT COALESCE(openrig_source_host_id, 'unknown') AS source_host, "
                "COALESCE(openrig_source_namespace, 'default') AS source_namespace, "
                "COALESCE(openrig_stream, 'default') AS openrig_stream, "
                "COALESCE(openrig_source_boot_epoch, 'unknown') AS boot_epoch, "
                "COALESCE(openrig_record_kind, 'unknown') AS record_kind, "
                "COUNT(*) AS records_emitted "
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' "
                "GROUP BY openrig_source_host_id, openrig_source_namespace, openrig_stream, "
                "openrig_source_boot_epoch, openrig_record_kind "
                "ORDER BY records_emitted DESC"
            ),
            cols=["source_host", "source_namespace", "openrig_stream", "boot_epoch", "record_kind", "records_emitted"],
            x=0,
            y=18,
            w=192,
            h=7,
        ),
    ]

    exec_brief_md = (
        "### Executive Brief: OpenRig Fleet Operations & Governance\n"
        "- **Active Seat Utilization vs. Human Gate Bottlenecks**: Tracks real-time productive seat execution "
        "against seats paused on human decisions (`openrig_seat_needs_input`).\n"
        "- **Queue Completion Efficiency**: Measures end-to-end work item flow (`queued -> notified -> closed`) "
        "and highlights dwell-time bottlenecks.\n"
        "- **Compounding Seat Lineage**: Audits planned seat handovers (`seat_tenure_change`) across logical seats "
        "(`source_namespace + immutable nodeId`) and Meshloop orchestration throughput.\n"
        "- **Cross-Layer Links**: Pair with **envctl-openrig: OpenRig AI Layer** (Folder `50 OpenRig Reliability`) "
        "and the **OpenRig Reliability** alert folder for end-to-end autonomous engineering governance."
    )

    tab4_panels = [
        markdown_panel(
            "rig_exec_brief",
            "Executive Brief: OpenRig Fleet Operations & Governance",
            "Executive guide to reading OpenRig fleet capacity, queue velocity, and seat lineage.",
            exec_brief_md,
            x=0,
            y=0,
            w=192,
            h=4,
        ),
        metric_panel(
            "rig_exec_kpi_utilization",
            "Fleet Active Seat Utilization (%)",
            "Percentage of active OpenRig seats currently executing work.",
            "metrics",
            "openrig_seat_activity",
            (
                "SELECT ROUND(100.0 * COALESCE(SUM(CASE WHEN openrig_seat_state = 'working' "
                'THEN latest_val ELSE 0 END), 0) / NULLIF(SUM(latest_val), 0), 1) AS "y_axis_1" '
                "FROM (SELECT openrig_seat_node_id, openrig_seat_state, "
                "last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "openrig_seat_activity" '
                "GROUP BY openrig_seat_node_id, openrig_seat_state) WHERE latest_val > 0"
            ),
            x=0,
            y=4,
            w=48,
            h=4,
            unit="percent",
            color="#5960b2",
        ),
        metric_panel(
            "rig_exec_kpi_completions",
            "Completed Queue Items",
            "Cumulative work queue items reaching the 'done' terminal state.",
            "metrics",
            "openrig_queue_transition_observed",
            (
                'SELECT COALESCE(SUM(max_val), 0) AS "y_axis_1" FROM ('
                "SELECT openrig_queue_closure_reason, MAX(value) AS max_val "
                'FROM "openrig_queue_transition_observed" '
                "WHERE openrig_queue_state IN ('done', 'closed') GROUP BY openrig_queue_closure_reason)"
            ),
            x=48,
            y=4,
            w=48,
            h=4,
            color="#10b981",
        ),
        metric_panel(
            "rig_exec_kpi_tenure_changes",
            "Seat Lineage Tenure Events",
            "Total seat lineage handover and tenure transition events recorded.",
            "logs",
            "default",
            (
                'SELECT COUNT(*) AS "y_axis_1" FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' AND openrig_record_kind = 'seat_tenure_change'"
            ),
            x=96,
            y=4,
            w=48,
            h=4,
            color="#5960b2",
        ),
        metric_panel(
            "rig_exec_kpi_meshloop_ops",
            "Meshloop Orchestration Operations",
            "Total Meshloop task-graph orchestration operations observed.",
            "metrics",
            "meshloop_operation_count",
            (
                'SELECT COALESCE(SUM(max_val), 0) AS "y_axis_1" FROM ('
                'SELECT operation, MAX(value) AS max_val FROM "meshloop_operation_count" GROUP BY operation)'
            ),
            x=144,
            y=4,
            w=48,
            h=4,
            color="#5960b2",
        ),
        donut_panel(
            "rig_exec_chart_runtime_portfolio",
            "Fleet Seat Portfolio by Agent Harness Runtime",
            "Distribution of distinct OpenRig seats across agent runtimes (Claude, Codex, Agy, etc.).",
            "metrics",
            "openrig_seat_presence",
            (
                'SELECT COALESCE(openrig_seat_runtime, \'unknown\') AS "x_axis_1", '
                'COUNT(DISTINCT openrig_seat_node_id) AS "y_axis_1" '
                'FROM "openrig_seat_presence" '
                "GROUP BY x_axis_1 ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=8,
            w=96,
            h=7,
            x_label="Runtime Harness",
            y_label="Seats",
        ),
        bar_panel(
            "rig_exec_chart_queue_funnel",
            "Work Queue Lifecycle Funnel (Queued → Notified → Closed)",
            "Transition volume across each stage of the OpenRig work queue lifecycle.",
            "metrics",
            "openrig_queue_transition_observed",
            (
                'SELECT COALESCE(openrig_queue_state, \'unknown\') AS "x_axis_1", '
                'SUM(max_val) AS "y_axis_1" '
                "FROM (SELECT openrig_queue_state, openrig_queue_closure_reason, MAX(value) AS max_val "
                'FROM "openrig_queue_transition_observed" '
                "GROUP BY openrig_queue_state, openrig_queue_closure_reason) "
                "GROUP BY x_axis_1 ORDER BY y_axis_1 DESC"
            ),
            x=96,
            y=8,
            w=96,
            h=7,
            x_label="Queue Stage",
            y_label="Transitions",
            color="#10b981",
        ),
        table_panel(
            "rig_exec_table_seat_tenure_lineage",
            "Logical Seat Tenure & Occupant Handover Ledger (source_namespace + node_id Across Occupant Generations)",
            "Follows logical seats (durable source_namespace + immutable node_id) across occupant swaps (seat_tenure_change) separately from transient boot_epoch.",
            "logs",
            "default",
            (
                "SELECT COALESCE(openrig_source_namespace, 'default') AS source_namespace, "
                "COALESCE(openrig_seat_node_id, 'fleet-baseline') AS seat_node_id, "
                "COALESCE(openrig_seat_runtime, 'multi-runtime') AS runtime, "
                "COALESCE(openrig_tenure_prior_generation, 'baseline') AS prior_generation, "
                "COALESCE(openrig_tenure_new_generation, 'current_stable') AS new_generation, "
                "COALESCE(openrig_tenure_continuity, 'continuous_no_swap_in_window') AS tenure_continuity, "
                "COALESCE(openrig_source_boot_epoch, 'unknown') AS boot_epoch, "
                "SUM(CASE WHEN openrig_record_kind = 'seat_tenure_change' THEN 1 ELSE 0 END) AS handover_events "
                'FROM "default" '
                "WHERE service_name = 'openrig-telemetry-exporter' "
                "AND openrig_record_kind IN ('seat_tenure_change', 'queue_transition') "
                "GROUP BY openrig_source_namespace, openrig_seat_node_id, openrig_seat_runtime, "
                "openrig_tenure_prior_generation, openrig_tenure_new_generation, openrig_tenure_continuity, openrig_source_boot_epoch "
                "ORDER BY handover_events DESC, seat_node_id ASC"
            ),
            cols=[
                "source_namespace",
                "seat_node_id",
                "runtime",
                "prior_generation",
                "new_generation",
                "tenure_continuity",
                "boot_epoch",
                "handover_events",
            ],
            x=0,
            y=15,
            w=192,
            h=8,
        ),
    ]

    v5_body = {
        "version": 5,
        "dashboardId": "",
        "title": DASHBOARD_PREFIX + "OpenRig Rig Layer (Control Plane, Seats & Queues)",
        "description": (
            f"{OPENRIG_SHOWCASE_MARKER} key=openrig-rig-layer; "
            "OpenRig control plane, seat activity, queue state machine, exporter integrity, and executive capacity."
        ),
        "role": "",
        "owner": "",
        "created": "2026-11-09T00:00:00Z",
        "tabs": [
            sre_model.tab("sre-control", "SRE: Seat & Daemon Control Plane", tab1_panels),
            sre_model.tab("sre-queues", "SRE: Queue State Machine, Spans & Handoff Latency", tab2_panels),
            sre_model.tab("sre-exporter", "SRE: Telemetry Exporter, Ledger & Gap Integrity", tab3_panels),
            sre_model.tab("exec-rig", "Executive: Fleet Capacity, Lineage & Delivery Velocity", tab4_panels),
        ],
        "variables": {"list": []},
        "defaultDatetimeDuration": {"type": "relative", "relativeTimePeriod": "24h"},
    }
    return normalize_showcase_dashboard(v5_body)


def build_ai_layer_dashboard():
    """Dashboard 2: envctl-openrig: OpenRig AI Layer (Models, Tooling, Sub-Agents & Quota)."""
    tab1_panels = [
        metric_panel(
            "ai_kpi_total_spans",
            "Total AI Hook & Span Events",
            "Total AI runtime, hook, and inference spans across station AI services.",
            "traces",
            "default",
            (
                'SELECT COUNT(*) AS "y_axis_1" FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'decider', 'codex-app-server', 'mtn-signals-digest')"
            ),
            x=0,
            y=0,
            w=48,
            h=4,
            color="#5960b2",
        ),
        metric_panel(
            "ai_kpi_active_conversations",
            "Distinct AI Agent Sessions",
            "Distinct AI agent conversation/session IDs observed by agent-otel-bridge.",
            "traces",
            "default",
            (
                'SELECT COUNT(DISTINCT gen_ai_conversation_id) AS "y_axis_1" FROM "default" '
                "WHERE service_name = 'agent-otel-bridge' AND gen_ai_conversation_id IS NOT NULL"
            ),
            x=48,
            y=0,
            w=48,
            h=4,
            color="#10b981",
        ),
        metric_panel(
            "ai_kpi_lowest_quota_pct",
            "Lowest Provider Quota Remaining (%)",
            "Minimum remaining quota percentage across all tracked AI subscription providers and windows.",
            "metrics",
            "agent_quota_remaining_fraction",
            (
                'SELECT ROUND(MIN(latest_val) * 100.0, 1) AS "y_axis_1" FROM ('
                'SELECT gen_ai_provider_name, "group", bucket, '
                "last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "agent_quota_remaining_fraction" '
                'GROUP BY gen_ai_provider_name, "group", bucket)'
            ),
            x=96,
            y=0,
            w=48,
            h=4,
            unit="percent",
            color="#f59e0b",
        ),
        metric_panel(
            "ai_kpi_codex_ttft_ms",
            "Codex Mean TTFT Latency (ms)",
            "Observed Codex turn time-to-first-token cumulative latency.",
            "metrics",
            "codex_turn_ttft_duration_ms_sum",
            'SELECT ROUND(COALESCE(MAX(value), 0), 1) AS "y_axis_1" FROM "codex_turn_ttft_duration_ms_sum"',
            x=144,
            y=0,
            w=48,
            h=4,
            color="#5960b2",
        ),
        bar_panel(
            "ai_chart_quota_by_provider",
            "Provider Quota Headroom (%) by Model Group & Window",
            "Remaining subscription quota percentage per provider, model group, and rate-limit window.",
            "metrics",
            "agent_quota_remaining_fraction",
            (
                "SELECT CONCAT(COALESCE(gen_ai_provider_name, 'unknown'), ':', "
                'COALESCE("group", \'default\'), \' (\', COALESCE(bucket, \'win\'), \')\') AS "x_axis_1", '
                'ROUND(last_value(value ORDER BY _timestamp) * 100.0, 1) AS "y_axis_1" '
                'FROM "agent_quota_remaining_fraction" '
                'GROUP BY gen_ai_provider_name, "group", bucket ORDER BY y_axis_1 ASC'
            ),
            x=0,
            y=4,
            w=96,
            h=7,
            x_label="Provider / Group (Window)",
            y_label="Remaining (%)",
            color="#10b981",
        ),
        timeseries_panel(
            "ai_chart_spans_over_time",
            "AI Telemetry Throughput Over Time by Provider",
            "Span volume over time segmented by AI provider or service.",
            "traces",
            "default",
            (
                'SELECT histogram(_timestamp) AS "x_axis_1", COUNT(*) AS "y_axis_1", '
                'COALESCE(gen_ai_provider_name, service_name) AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'decider', 'codex-app-server', 'mtn-signals-digest') "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY x_axis_1 ASC"
            ),
            x=96,
            y=4,
            w=96,
            h=7,
            y_label="Spans",
            chart_type="area",
            breakdown_col="breakdown_1",
            breakdown_label="Provider",
        ),
        table_panel(
            "ai_table_quota_guardrails",
            "Actionable Provider Quota Guardrails & Reset Forecast",
            "Per-provider quota table with automated SRE guardrail posture (Failover, Throttle, or Healthy).",
            "metrics",
            "agent_quota_remaining_fraction",
            (
                "SELECT COALESCE(gen_ai_provider_name, 'unknown') AS provider, "
                'COALESCE("group", \'default\') AS model_group, '
                "COALESCE(bucket, 'default') AS window_bucket, "
                "ROUND(last_value(value ORDER BY _timestamp) * 100.0, 1) AS remaining_pct, "
                "CASE WHEN last_value(value ORDER BY _timestamp) < 0.15 "
                "THEN 'CRITICAL: Failover / Conserve' "
                "WHEN last_value(value ORDER BY _timestamp) < 0.30 "
                "THEN 'WARN: Throttle Sub-Agents' "
                "ELSE 'OK: Healthy Headroom' END AS sre_guardrail_status "
                'FROM "agent_quota_remaining_fraction" '
                'GROUP BY gen_ai_provider_name, "group", bucket ORDER BY remaining_pct ASC'
            ),
            cols=["provider", "model_group", "window_bucket", "remaining_pct", "sre_guardrail_status"],
            x=0,
            y=11,
            w=192,
            h=8,
        ),
    ]

    tab2_panels = [
        bar_panel(
            "ai_chart_top_tools",
            "Top Agent Tool Executions by Operation",
            "Most frequent tool invocations across agent harnesses.",
            "traces",
            "default",
            (
                'SELECT COALESCE(operation_name, \'unknown\') AS "x_axis_1", '
                'COUNT(*) AS "y_axis_1" FROM "default" '
                "WHERE service_name = 'agent-otel-bridge' "
                "AND (operation_name LIKE 'execute_tool%' OR operation_name LIKE 'tool.%') "
                "GROUP BY x_axis_1 ORDER BY y_axis_1 DESC LIMIT 15"
            ),
            x=0,
            y=0,
            w=96,
            h=7,
            x_label="Tool Operation",
            y_label="Invocations",
            color="#5960b2",
        ),
        bar_panel(
            "ai_chart_hooks_by_agent",
            "Lifecycle Hook Events by Agent Harness & Stage",
            "Agent lifecycle hook and span events grouped by harness and hook event stage.",
            "traces",
            "default",
            (
                'SELECT COALESCE(gen_ai_agent_name, \'unknown\') AS "x_axis_1", '
                'COUNT(*) AS "y_axis_1", '
                'COALESCE(agent_hook_event, \'span\') AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name = 'agent-otel-bridge' "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=96,
            y=0,
            w=96,
            h=7,
            x_label="Agent Harness",
            y_label="Events",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Hook Event",
        ),
        donut_panel(
            "ai_chart_tool_archetypes",
            "Shell & Tool Binary Archetype Breakdown",
            "Classification of tool executions by archetype (search, inspector, build/test, filter).",
            "traces",
            "default",
            (
                'SELECT COALESCE(agent_tool_archetype, operation_name, \'other\') AS "x_axis_1", '
                'COUNT(*) AS "y_axis_1" FROM "default" '
                "WHERE service_name = 'agent-otel-bridge' "
                "AND (operation_name LIKE 'execute_tool%' OR operation_name LIKE 'tool.%') "
                "GROUP BY x_axis_1 ORDER BY y_axis_1 DESC LIMIT 12"
            ),
            x=0,
            y=7,
            w=96,
            h=7,
            x_label="Tool Archetype",
            y_label="Calls",
        ),
        table_panel(
            "ai_table_tool_performance",
            "Tool Execution Latency & Shell Pipeline Depth",
            "Latency and pipeline depth metrics per tool operation and underlying binary.",
            "traces",
            "default",
            (
                "SELECT COALESCE(operation_name, 'unknown') AS operation, "
                "COALESCE(agent_tool_binary, 'builtin') AS tool_binary, "
                "COUNT(*) AS calls, "
                "ROUND(AVG(duration), 1) AS avg_duration_us, "
                "MAX(COALESCE(agent_tool_pipeline_depth, 0)) AS max_pipeline_depth "
                'FROM "default" '
                "WHERE service_name = 'agent-otel-bridge' "
                "AND (operation_name LIKE 'execute_tool%' OR operation_name LIKE 'tool.%') "
                "GROUP BY operation_name, agent_tool_binary ORDER BY calls DESC LIMIT 20"
            ),
            cols=["operation", "tool_binary", "calls", "avg_duration_us", "max_pipeline_depth"],
            x=96,
            y=7,
            w=96,
            h=7,
        ),
        table_panel(
            "ai_table_multiagent_and_services",
            "AI Services & Multi-Agent Runtime Telemetry (Bridge, Codex, Decider, MTN Digest)",
            "Cross-service span volume and mean latency across station AI services.",
            "traces",
            "default",
            (
                "SELECT COALESCE(service_name, 'unknown') AS service_name, "
                "COALESCE(operation_name, 'unknown') AS operation_name, "
                "COALESCE(gen_ai_provider_name, 'internal') AS provider, "
                "COALESCE(gen_ai_request_model, 'n/a') AS model, "
                "COUNT(*) AS spans, "
                "ROUND(AVG(duration), 1) AS avg_duration_us "
                'FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'decider', 'codex-app-server', 'mtn-signals-digest') "
                "GROUP BY service_name, operation_name, gen_ai_provider_name, gen_ai_request_model "
                "ORDER BY spans DESC LIMIT 25"
            ),
            cols=["service_name", "operation_name", "provider", "model", "spans", "avg_duration_us"],
            x=0,
            y=14,
            w=192,
            h=7,
        ),
        bar_panel(
            "ai_chart_correlation_levels",
            "3-Level Cross-Layer Telemetry Correlation Breakdown (Search vs Span Links vs Parent-Child)",
            "Classifies telemetry across Discussion #2573 correlation levels: L1 Search Correlation, L2 OTel Span Links, and L3 W3C Parent-Child Traces.",
            "traces",
            "default",
            (
                "SELECT CASE WHEN service_name = 'openrig-telemetry-exporter' "
                "THEN 'L1: Search Correlation (openrig.queue.state)' "
                "WHEN links IS NOT NULL AND links != '' AND links != '[]' "
                "THEN 'L2: OTel Span Link (Causal Handoff)' "
                "WHEN reference_parent_span_id IS NOT NULL AND reference_parent_span_id != '' "
                "THEN 'L3: W3C Parent-Child Trace (Tool/Sub-Agent)' "
                "ELSE 'L1: Search Correlation (Root Agent Span)' END AS \"x_axis_1\", "
                'COUNT(*) AS "y_axis_1", '
                'COALESCE(service_name, \'unknown\') AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name IN ('openrig-telemetry-exporter', 'agent-otel-bridge', 'codex-app-server', 'decider', 'mtn-signals-digest') "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=21,
            w=96,
            h=7,
            x_label="Correlation Level",
            y_label="Spans",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Service",
        ),
        table_panel(
            "ai_table_producer_waits_approvals",
            "Producer-Reported Operation Durations, Retries & Approval Decisions",
            "Explicitly producer-reported operation durations, retry attempts, and human approval gate decisions.",
            "logs",
            "default",
            (
                "SELECT COALESCE(service_name, 'unknown') AS service_name, "
                "COALESCE(event_name, openrig_record_kind, 'log_event') AS event_name, "
                "COALESCE(decision, approval_policy, 'n/a') AS approval_decision, "
                "COUNT(*) AS events, "
                "MAX(COALESCE(attempt, 0)) AS max_retry_attempt, "
                "ROUND(AVG(COALESCE(TRY_CAST(duration_ms AS DOUBLE), 0)), 2) AS avg_duration_ms "
                'FROM "default" '
                "WHERE service_name IN ('codex-app-server', 'openrig-telemetry-exporter', 'mtn-spine') "
                "GROUP BY service_name, event_name, openrig_record_kind, decision, approval_policy "
                "ORDER BY events DESC LIMIT 20"
            ),
            cols=["service_name", "event_name", "approval_decision", "events", "max_retry_attempt", "avg_duration_ms"],
            x=96,
            y=21,
            w=96,
            h=7,
        ),
    ]

    exec_ai_md = (
        "### Executive Brief: OpenRig AI Layer & Model Economics\n"
        "- **Subscription Tier Routing**: Governs allocation across `claude` (conserved for architecture & "
        "Second-Model Gate), `codex` (multi-file autonomous engineering), `gemini`/`agy` (discovery & synthesis), "
        "`grok` (fast iteration & adversarial review), and local `decider` inference.\n"
        "- **Token Usage & Cost Provenance**: Provider-reported token observations, locally estimated cost, and "
        "subscription quota fractions remain strictly distinguishable (subscription usage is never conflated with "
        "invoice amounts).\n"
        "- **3-Level Correlation**: Distinguishes Level 1 (Search correlation), Level 2 (OTel span links), and "
        "Level 3 (W3C `traceparent` parent-child spans)."
    )

    tab3_panels = [
        markdown_panel(
            "ai_exec_brief",
            "Executive Brief: OpenRig AI Layer & Model Economics",
            "Executive overview of multi-provider routing, quota conservation, and workspace attribution.",
            exec_ai_md,
            x=0,
            y=0,
            w=192,
            h=4,
        ),
        donut_panel(
            "ai_exec_chart_model_share",
            "AI Model Portfolio Share (By Request Model)",
            "Proportional share of AI activity across requested models and inference services.",
            "traces",
            "default",
            (
                'SELECT COALESCE(gen_ai_request_model, service_name, \'unspecified\') AS "x_axis_1", '
                'COUNT(*) AS "y_axis_1" FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'decider', 'codex-app-server') "
                "GROUP BY x_axis_1 ORDER BY y_axis_1 DESC"
            ),
            x=0,
            y=4,
            w=96,
            h=7,
            x_label="Model",
            y_label="Events",
        ),
        bar_panel(
            "ai_exec_chart_workspace_allocation",
            "AI Engineering Effort by Workspace Project & Harness",
            "Distribution of agent engineering spans across workspace projects and agent harnesses.",
            "traces",
            "default",
            (
                'SELECT COALESCE(workspace_project_name, \'station-global\') AS "x_axis_1", '
                'COUNT(*) AS "y_axis_1", '
                'COALESCE(gen_ai_agent_name, service_name) AS "breakdown_1" '
                'FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'decider', 'codex-app-server', 'mtn-signals-digest') "
                "GROUP BY x_axis_1, breakdown_1 ORDER BY y_axis_1 DESC"
            ),
            x=96,
            y=4,
            w=96,
            h=7,
            x_label="Workspace Project",
            y_label="Events",
            stacked=True,
            breakdown_col="breakdown_1",
            breakdown_label="Agent Harness",
        ),
        table_panel(
            "ai_exec_table_governance",
            "Executive AI Portfolio Matrix: Workspace, Agent Harness, Model Tier & Session Volume",
            "Comprehensive portfolio breakdown by workspace, harness, provider, model, and session count.",
            "traces",
            "default",
            (
                "SELECT COALESCE(workspace_project_name, 'station-global') AS workspace, "
                "COALESCE(gen_ai_agent_name, service_name) AS agent_harness, "
                "COALESCE(gen_ai_provider_name, 'internal') AS provider, "
                "COALESCE(gen_ai_request_model, 'default') AS request_model, "
                "COUNT(DISTINCT gen_ai_conversation_id) AS conversations, "
                "COUNT(*) AS total_events "
                'FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'decider', 'codex-app-server', 'mtn-signals-digest') "
                "GROUP BY workspace_project_name, gen_ai_agent_name, service_name, "
                "gen_ai_provider_name, gen_ai_request_model "
                "ORDER BY total_events DESC"
            ),
            cols=["workspace", "agent_harness", "provider", "request_model", "conversations", "total_events"],
            x=0,
            y=11,
            w=192,
            h=8,
        ),
        table_panel(
            "ai_exec_table_token_cost_provenance",
            "Token Usage & Cost Provenance Matrix (Provider-Reported Tokens vs Subscription Quota vs Invoice Cost)",
            "Separates producer-reported token counts from subscription quota fractions and invoice cost so subscription usage is never conflated with billing.",
            "traces",
            "default",
            (
                "SELECT COALESCE(service_name, 'unknown') AS service_name, "
                "COALESCE(gen_ai_provider_name, 'subscription-cli') AS provider, "
                "COALESCE(gen_ai_request_model, 'default') AS model, "
                "SUM(COALESCE(gen_ai_usage_input_tokens, 0)) AS input_tokens, "
                "SUM(COALESCE(gen_ai_usage_output_tokens, 0)) AS output_tokens, "
                "SUM(COALESCE(gen_ai_usage_cache_read_input_tokens, 0)) AS cache_read_tokens, "
                "CASE WHEN SUM(COALESCE(gen_ai_usage_cost, 0)) > 0 "
                "THEN 'Provider/Locally Estimated Cost Reported' "
                "WHEN SUM(COALESCE(gen_ai_usage_input_tokens, 0)) + SUM(COALESCE(gen_ai_usage_output_tokens, 0)) > 0 "
                "THEN 'Producer-Reported Tokens (Subscription Tier — Not Invoice Amount)' "
                "ELSE 'Subscription Quota Fraction Only (Token/Invoice Data Unavailable)' END AS cost_provenance "
                'FROM "default" '
                "WHERE service_name IN ('agent-otel-bridge', 'codex-app-server', 'Codex Desktop', 'codex_exec', 'decider', 'mtn-signals-digest') "
                "GROUP BY service_name, gen_ai_provider_name, gen_ai_request_model "
                "ORDER BY input_tokens DESC, output_tokens DESC"
            ),
            cols=[
                "service_name",
                "provider",
                "model",
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cost_provenance",
            ],
            x=0,
            y=19,
            w=192,
            h=8,
        ),
    ]

    v5_body = {
        "version": 5,
        "dashboardId": "",
        "title": DASHBOARD_PREFIX + "OpenRig AI Layer (Models, Tooling, Sub-Agents & Quota)",
        "description": (
            f"{OPENRIG_SHOWCASE_MARKER} key=openrig-ai-layer; "
            "OpenRig AI model inference, provider quota guardrails, tool execution archetypes, and portfolio governance."
        ),
        "role": "",
        "owner": "",
        "created": "2026-11-09T00:00:00Z",
        "tabs": [
            sre_model.tab("sre-inference", "SRE: Model Inference, Latency & Quota Guardrails", tab1_panels),
            sre_model.tab("sre-tools", "SRE: Tool Execution, Sub-Agents & Hook Pipeline", tab2_panels),
            sre_model.tab("exec-ai", "Executive: Model Portfolio, Workspace Allocation & AI Velocity", tab3_panels),
        ],
        "variables": {"list": []},
        "defaultDatetimeDuration": {"type": "relative", "relativeTimePeriod": "7d"},
    }
    return normalize_showcase_dashboard(v5_body)


def dashboards_catalog():
    """Return the two V8 normalized OpenRig Showcase dashboard specs."""
    rig_body = build_rig_layer_dashboard()
    ai_body = build_ai_layer_dashboard()
    return [
        {
            "kind": "dashboard",
            "key": "openrig-rig-layer",
            "folderKey": "openrig-reliability-dashboards",
            "folderType": "dashboards",
            "title": rig_body["title"],
            "body": rig_body,
        },
        {
            "kind": "dashboard",
            "key": "openrig-ai-layer",
            "folderKey": "openrig-reliability-dashboards",
            "folderType": "dashboards",
            "title": ai_body["title"],
            "body": ai_body,
        },
    ]


def alert_spec(
    name,
    stream_type,
    stream_name,
    sql,
    column,
    operator,
    threshold,
    period,
    frequency,
    silence,
    severity,
    service,
    summary,
    runbook,
    folder_key="openrig-reliability-alerts",
    folder_id="default",
    destinations=None,
    enabled=False,
):
    """Build an OpenRig Reliability SQL scheduled alert specification."""
    if not name.startswith(ALERT_PREFIX):
        name = ALERT_PREFIX + name
    short_key = name[len(ALERT_PREFIX):] if name.startswith(ALERT_PREFIX) else name
    dest_list = list(destinations) if destinations else []
    wire_threshold = (
        int(round(threshold * 100))
        if isinstance(threshold, float) and 0 < abs(threshold) < 1
        else int(threshold)
    )
    body = {
        "name": name,
        "description": f"{OPENRIG_SHOWCASE_MARKER} key={short_key}; severity={severity} {summary}",
        "org_id": "default",
        "folder_id": folder_id,
        "folderId": folder_id,
        "stream_type": stream_type,
        "stream_name": stream_name,
        "is_real_time": False,
        "query_condition": {
            "type": "sql",
            "sql": sql.strip(),
            "aggregation": {
                "group_by": [],
                "function": "max",
                "having": {
                    "column": column,
                    "operator": operator,
                    "value": wire_threshold,
                },
            },
        },
        "trigger_condition": {
            "period": int(period),
            "operator": operator,
            "threshold": wire_threshold,
            "frequency": int(frequency),
            "frequency_type": "minutes",
            "silence": int(silence),
        },
        "destinations": dest_list,
        "enabled": bool(enabled),
        "context_attributes": {
            "owner": "station-operator",
            "severity": severity,
            "service": service,
            "column": column,
            "operator": operator,
            "threshold": str(threshold),
            "summary": summary,
            "runbook": runbook,
            "dedupe_key": f"envctl-openrig:{short_key}",
        },
    }
    return {
        "kind": "alert",
        "key": name,
        "name": name,
        "column": column,
        "operator": operator,
        "threshold": threshold,
        "folderKey": folder_key,
        "folderType": "alerts",
        "body": body,
    }


def alerts_catalog(folder_id="default", destinations=None, enabled=False):
    """Return the 5 OpenRig Reliability SQL scheduled alert specs."""
    return [
        alert_spec(
            name="envctl-openrig-alert-exporter-failure",
            stream_type="metrics",
            stream_name="openrig_telemetry_failure",
            sql='SELECT COALESCE(MAX(value), 0) AS failure_count FROM "openrig_telemetry_failure"',
            column="failure_count",
            operator=">",
            threshold=0,
            period=5,
            frequency=5,
            silence=30,
            severity="P2",
            service="openrig-telemetry-exporter",
            summary="OpenRig telemetry exporter recorded stage/category failures",
            runbook="docs/user/daily-guide.md#openrig-telemetry",
            folder_id=folder_id,
            destinations=destinations,
            enabled=enabled,
        ),
        alert_spec(
            name="envctl-openrig-alert-coverage-gap",
            stream_type="metrics",
            stream_name="openrig_telemetry_coverage_gap",
            sql='SELECT COALESCE(MAX(value), 0) AS gap_count FROM "openrig_telemetry_coverage_gap"',
            column="gap_count",
            operator=">",
            threshold=0,
            period=10,
            frequency=5,
            silence=60,
            severity="P2",
            service="openrig-telemetry-exporter",
            summary="OpenRig telemetry coverage gap detected in transition ledger",
            runbook="docs/user/daily-guide.md#openrig-telemetry",
            folder_id=folder_id,
            destinations=destinations,
            enabled=enabled,
        ),
        alert_spec(
            name="envctl-openrig-alert-seat-human-gate-stall",
            stream_type="metrics",
            stream_name="openrig_seat_needs_input",
            sql=(
                "SELECT COALESCE(SUM(latest_val), 0) AS blocked_seats FROM ("
                "SELECT openrig_seat_node_id, last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "openrig_seat_needs_input" GROUP BY openrig_seat_node_id)'
            ),
            column="blocked_seats",
            operator=">=",
            threshold=2,
            period=15,
            frequency=5,
            silence=30,
            severity="P2",
            service="openrig-fleet",
            summary="Multiple OpenRig seats are stalled waiting on human input",
            runbook="docs/maintainer/rig-operating-manual.md",
            folder_id=folder_id,
            destinations=destinations,
            enabled=enabled,
        ),
        alert_spec(
            name="envctl-openrig-alert-fleet-bottleneck-high",
            stream_type="metrics",
            stream_name="agent_fleet_bottleneck_ratio",
            sql='SELECT COALESCE(last_value(value ORDER BY _timestamp), 0) AS bottleneck_ratio FROM "agent_fleet_bottleneck_ratio"',
            column="bottleneck_ratio",
            operator=">=",
            threshold=0.5,
            period=15,
            frequency=5,
            silence=30,
            severity="P2",
            service="openrig-fleet",
            summary="OpenRig fleet bottleneck ratio >= 50% of active seats",
            runbook="docs/maintainer/rig-operating-manual.md",
            folder_id=folder_id,
            destinations=destinations,
            enabled=enabled,
        ),
        alert_spec(
            name="envctl-openrig-alert-ai-quota-low",
            stream_type="metrics",
            stream_name="agent_quota_remaining_fraction",
            sql=(
                "SELECT COALESCE(MIN(latest_val), 1.0) AS min_quota_fraction FROM ("
                'SELECT gen_ai_provider_name, "group", bucket, '
                "last_value(value ORDER BY _timestamp) AS latest_val "
                'FROM "agent_quota_remaining_fraction" '
                'GROUP BY gen_ai_provider_name, "group", bucket)'
            ),
            column="min_quota_fraction",
            operator="<",
            threshold=0.15,
            period=15,
            frequency=5,
            silence=60,
            severity="P1",
            service="openrig-ai-layer",
            summary="AI provider subscription quota headroom dropped below 15%",
            runbook="docs/user/daily-guide.md#quota-governance",
            folder_id=folder_id,
            destinations=destinations,
            enabled=enabled,
        ),
    ]


def is_owned_folder(body, folder_type=None):
    if not isinstance(body, dict):
        return False
    desc = str(body.get("description") or "")
    if OPENRIG_SHOWCASE_MARKER not in desc:
        return False
    expected = {f["name"]: f["folderType"] for f in folders_catalog()}
    name = body.get("name", "")
    if name not in expected:
        return False
    if folder_type and expected[name] != folder_type:
        return False
    return True


def is_owned_dashboard(body):
    if not isinstance(body, dict):
        return False
    title = str(body.get("title") or "")
    desc = str(body.get("description") or "")
    return title.startswith(DASHBOARD_PREFIX) and OPENRIG_SHOWCASE_MARKER in desc


def is_owned_alert(body):
    if not isinstance(body, dict):
        return False
    name = str(body.get("name") or "")
    desc = str(body.get("description") or "")
    return name in KNOWN_ALERT_NAMES and OPENRIG_SHOWCASE_MARKER in desc


def resolve_alert_destinations(api):
    """Inspect OpenObserve alert destinations; bind envctl-sre-alert-webhook if present, or safe fallback."""
    raw = api.call("GET", p.BASE + "/alerts/destinations")
    if not isinstance(raw, list):
        return [], False
    names = sorted(
        item.get("name")
        for item in raw
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    )
    if PREFERRED_WEBHOOK_DEST in names:
        return [PREFERRED_WEBHOOK_DEST], True
    sre_dests = [n for n in names if n.startswith(("envctl-sre-", "envctl-station-"))]
    if sre_dests:
        # OpenObserve 1.1.2 OSS requires a non-empty destinations array on alert POST/PUT;
        # bind existing station destination with enabled=False so no mismatched template fires.
        return [sre_dests[0]], False
    return [], False


def inventory(api):
    """Discover OpenRig Showcase folders, dashboards, and alerts without touching SRE resources."""
    config = api.call("GET", "/config")
    if config.get("commit_hash") != p.COMMIT or config.get("build_type") != "opensource":
        raise p.SafeError("Backend differs from inspected OpenObserve 1.1.2 OSS build; HOLD")

    folder_specs_by_name = {f["name"]: f for f in folders_catalog()}
    folders = {}
    all_dashboard_folder_ids = ["default"]

    for folder_type in ("dashboards", "alerts"):
        flist = api.call("GET", FOLDERS_API + "/" + folder_type).get("list", [])
        if not isinstance(flist, list):
            raise p.SafeError("Invalid folder listing response; HOLD")
        for raw in flist:
            fid = sre.identifier("folder", raw)
            if folder_type == "dashboards" and fid not in all_dashboard_folder_ids:
                all_dashboard_folder_ids.append(fid)
            name = raw.get("name", "")
            desc = str(raw.get("description") or "")
            if OPENRIG_SHOWCASE_MARKER in desc or name in folder_specs_by_name:
                if not is_owned_folder(raw, folder_type=folder_type):
                    raise p.SafeError(f"Folder collision on '{name}'; unrelated state preserved; HOLD")
                f_spec = folder_specs_by_name[name]
                body = copy.deepcopy(raw)
                body["folderId"] = fid
                folders[f_spec["key"]] = {
                    "folderId": fid,
                    "folderType": folder_type,
                    "body": body,
                }

    dashboards = {}
    for fid in all_dashboard_folder_ids:
        dlist = api.call("GET", p.BASE + "/dashboards?folder=" + quote(fid, safe="")).get("dashboards", [])
        if not isinstance(dlist, list):
            raise p.SafeError("Invalid dashboard listing response; HOLD")
        for raw in dlist:
            summary = p.dashboard(raw)
            explicit_fid = (
                raw.get("folderId")
                or raw.get("folder_id")
                or summary.get("folderId")
                or summary.get("folder_id")
            )
            if explicit_fid and explicit_fid != fid and explicit_fid in all_dashboard_folder_ids:
                continue
            title = str(summary.get("title") or "")
            desc = str(summary.get("description") or "")
            if not title.startswith(DASHBOARD_PREFIX) and OPENRIG_SHOWCASE_MARKER not in desc:
                continue
            if not is_owned_dashboard(summary):
                raise p.SafeError(f"Dashboard collision on '{title}'; unrelated state preserved; HOLD")
            did = sre.identifier("dashboard", summary)
            detail = sre.read_resource(api, "dashboard", did, folder_id=fid)
            if not is_owned_dashboard(detail["body"]):
                raise p.SafeError("Dashboard detail ownership mismatch; HOLD")
            match = re.search(r"key=([A-Za-z0-9_-]+)", str(detail["body"].get("description") or ""))
            dkey = match.group(1) if match else title
            dashboards[dkey] = detail

    alerts = {}
    alist = api.call("GET", p.ALERTS).get("list", [])
    if not isinstance(alist, list):
        raise p.SafeError("Invalid alert listing response; HOLD")
    for raw in alist:
        name = str(raw.get("name") or "")
        desc = str(raw.get("description") or "")
        if not name.startswith(ALERT_PREFIX) and OPENRIG_SHOWCASE_MARKER not in desc:
            continue
        if not is_owned_alert(raw):
            raise p.SafeError(f"Alert collision on '{name}'; unrelated state preserved; HOLD")
        aid = sre.identifier("alert", raw)
        fid = raw.get("folder_id") or raw.get("folderId") or "default"
        detail = sre.read_resource(api, "alert", aid, folder_id=fid)
        if not is_owned_alert(detail["body"]):
            raise p.SafeError("Alert detail ownership mismatch; HOLD")
        alerts[name] = detail

    dests, enabled = resolve_alert_destinations(api)
    return {
        "backendCommit": p.COMMIT,
        "folders": folders,
        "dashboards": dashboards,
        "alerts": alerts,
        "alertDestinations": dests,
        "alertsEnabled": enabled,
    }


def _expected_for_match(desired_body, kind):
    cleaned = copy.deepcopy(desired_body)
    if kind == "dashboard":
        for tab in cleaned.get("tabs", []):
            for pan in tab.get("panels", []):
                cfg = pan.get("config")
                if isinstance(cfg, dict):
                    for stripped_key in ("title", "description", "unit"):
                        cfg.pop(stripped_key, None)
    return cleaned


def showcase_body_matches(actual_body, desired_body, kind):
    return sre.body_matches(actual_body, _expected_for_match(desired_body, kind), kind)


def build_plan(inv, phase="catalog"):
    """Build deterministic plan for 'folders' or 'catalog' phase."""
    if phase not in ("folders", "catalog"):
        raise p.SafeError(f"Unsupported phase '{phase}'; expected 'folders' or 'catalog'")

    actions = []
    unchanged = 0

    for f_spec in folders_catalog():
        key = f_spec["key"]
        prior = inv.get("folders", {}).get(key)
        desired_body = copy.deepcopy(f_spec["body"])
        if prior and showcase_body_matches(prior["body"], desired_body, "folder"):
            unchanged += 1
            continue
        actions.append({
            "op": "update" if prior else "create",
            "kind": "folder",
            "key": key,
            "name": desired_body["name"],
            "folderKey": key,
            "folderType": f_spec["folderType"],
            "targetFolderId": prior["folderId"] if prior else None,
            "moveFromFolderId": None,
            "before": copy.deepcopy(prior),
            "desired": desired_body,
        })

    if phase == "catalog":
        dash_folder = inv.get("folders", {}).get("openrig-reliability-dashboards")
        alert_folder = inv.get("folders", {}).get("openrig-reliability-alerts")
        if not dash_folder or not alert_folder:
            raise p.SafeError(
                "OpenRig Reliability folders missing in inventory; run 'apply --phase folders' first; HOLD"
            )
        dash_fid = dash_folder["folderId"]
        alert_fid = alert_folder["folderId"]

        for d_spec in dashboards_catalog():
            key = d_spec["key"]
            prior = inv.get("dashboards", {}).get(key)
            desired_body = copy.deepcopy(d_spec["body"])
            move_from = None
            if prior:
                did = sre.identifier("dashboard", prior["body"])
                desired_body["dashboardId"] = did
                desired_body["owner"] = prior["body"].get("owner", "")
                desired_body["created"] = prior["body"].get("created", desired_body.get("created", ""))
                desired_body["role"] = prior["body"].get("role", desired_body.get("role", ""))
                prior_fid = prior.get("folderId") or "default"
                if prior_fid != dash_fid:
                    move_from = prior_fid
                if move_from is None and showcase_body_matches(prior["body"], desired_body, "dashboard"):
                    unchanged += 1
                    continue
            actions.append({
                "op": "update" if prior else "create",
                "kind": "dashboard",
                "key": key,
                "name": desired_body["title"],
                "folderKey": d_spec["folderKey"],
                "folderType": "dashboards",
                "targetFolderId": dash_fid,
                "moveFromFolderId": move_from,
                "before": copy.deepcopy(prior),
                "desired": desired_body,
            })

        dests = inv.get("alertDestinations") or []
        enabled = bool(inv.get("alertsEnabled", False))
        for a_spec in alerts_catalog(folder_id=alert_fid, destinations=dests, enabled=enabled):
            name = a_spec["name"]
            prior = inv.get("alerts", {}).get(name)
            desired_body = copy.deepcopy(a_spec["body"])
            if prior and prior["body"].get("destinations") and not dests:
                desired_body["destinations"] = copy.deepcopy(prior["body"]["destinations"])
                desired_body["enabled"] = bool(prior["body"].get("enabled", False))
            move_from = None
            if prior:
                if prior["body"].get("owner") is not None:
                    desired_body["owner"] = prior["body"]["owner"]
                prior_fid = prior.get("folderId") or "default"
                if prior_fid != alert_fid:
                    move_from = prior_fid
                if move_from is None and showcase_body_matches(prior["body"], desired_body, "alert"):
                    unchanged += 1
                    continue
            actions.append({
                "op": "update" if prior else "create",
                "kind": "alert",
                "key": name,
                "name": name,
                "folderKey": a_spec["folderKey"],
                "folderType": "alerts",
                "targetFolderId": alert_fid,
                "moveFromFolderId": move_from,
                "before": copy.deepcopy(prior),
                "desired": desired_body,
            })

    creates = sum(1 for a in actions if a["op"] == "create")
    updates = sum(1 for a in actions if a["op"] == "update")
    return {
        "version": 1,
        "backendCommit": p.COMMIT,
        "phase": phase,
        "inventoryDigest": p.digest(inv),
        "create": creates,
        "update": updates,
        "unchanged": unchanged,
        "actions": actions,
    }


def _write_showcase_resource(api, action, working_state):
    kind = action["kind"]
    body = copy.deepcopy(action["desired"])
    target_fid = action.get("targetFolderId")

    if kind in ("folder", "dashboard"):
        return sre.write_resource(api, action, working_state)

    if kind == "alert":
        wire = copy.deepcopy(body)
        wire["folder_id"] = target_fid
        wire["folderId"] = target_fid
        if working_state is None:
            response = api.call("POST", p.ALERTS + "?folder=" + quote(target_fid, safe=""), wire)
            aid = response.get("id") or response.get("alert_id")
            if not aid:
                values = api.call("GET", p.ALERTS).get("list", [])
                matches = [v for v in values if v.get("name") == wire["name"]]
                if len(matches) != 1:
                    raise p.SafeError("OpenRig alert creation readback ambiguous; HOLD")
                aid = matches[0].get("alert_id") or matches[0].get("id")
            return sre.read_resource(api, "alert", sre.identifier("alert", {"id": aid}), folder_id=target_fid)

        res_id = sre.identifier("alert", working_state["body"])
        fid = working_state.get("folderId") or target_fid or "default"
        wire["folder_id"] = fid
        wire["folderId"] = fid
        for preserved_key in ("owner", "updated_at", "last_triggered_at", "last_satisfied_at"):
            if working_state["body"].get(preserved_key) is not None:
                wire[preserved_key] = working_state["body"][preserved_key]
        api.call("PUT", p.ALERTS + "/" + quote(res_id, safe="") + "?folder=" + quote(fid, safe=""), wire)
        return sre.read_resource(api, "alert", res_id, folder_id=fid)

    raise p.SafeError(f"Unsupported resource kind '{kind}'; HOLD")


def _verify_owned_after_write(kind, body, folder_type=None):
    if kind == "folder":
        return is_owned_folder(body, folder_type=folder_type)
    if kind == "dashboard":
        return is_owned_dashboard(body)
    if kind == "alert":
        return is_owned_alert(body)
    return False


def apply_phase(api, phase, backup_dir):
    """Apply 'folders' or 'catalog' phase with durable backup and journal in backup_dir."""
    p.mutation_guard()
    backup_root = Path(backup_dir).expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True, mode=0o700)

    with sre.global_lock():
        inv = inventory(api)
        plan = build_plan(inv, phase=phase)
        p.private_write(backup_root / f"inventory-{phase}.json", inv)
        p.private_write(backup_root / f"plan-{phase}.json", plan)

        journal_path = backup_root / f"journal-{phase}.json"
        if journal_path.exists():
            journal_path.unlink()

        journal = {
            "version": 1,
            "phase": phase,
            "plan": plan,
            "actions": [],
            "complete": False,
        }
        p.private_write(journal_path, journal)

        for action in plan["actions"]:
            kind = action["kind"]
            prior = action["before"]
            record = {**action, "afterMove": None, "after": None}
            journal["actions"].append(record)
            p.private_write(journal_path, journal)

            working_state = None
            if prior is not None:
                res_id = sre.identifier(kind, prior["body"])
                live = sre.read_resource(
                    api,
                    kind,
                    res_id,
                    folder_id=prior.get("folderId"),
                    folder_type=prior.get("folderType"),
                )
                if live != prior:
                    raise p.SafeError("OpenRig resource changed after plan; HOLD")
                working_state = live

                if action.get("moveFromFolderId"):
                    moved = sre.move_resource(
                        api,
                        kind,
                        res_id,
                        action["moveFromFolderId"],
                        action["targetFolderId"],
                    )
                    record["afterMove"] = moved
                    p.private_write(journal_path, journal)
                    working_state = moved

            if working_state is not None and showcase_body_matches(working_state["body"], action["desired"], kind):
                result = working_state
            else:
                result = _write_showcase_resource(api, action, working_state)

            if (
                not _verify_owned_after_write(kind, result["body"], folder_type=action.get("folderType"))
                or not showcase_body_matches(result["body"], action["desired"], kind)
            ):
                raise p.SafeError("OpenRig resource readback differs after write; HOLD")
            if action.get("targetFolderId") and result.get("folderId") != action["targetFolderId"]:
                raise p.SafeError("OpenRig resource folder placement readback differs; HOLD")

            record["after"] = result
            p.private_write(journal_path, journal)

        journal["complete"] = True
        p.private_write(journal_path, journal)

        post_inv = inventory(api)
        return {
            "ok": True,
            "phase": phase,
            "create": plan["create"],
            "update": plan["update"],
            "unchanged": plan["unchanged"],
            "mutations": len(journal["actions"]),
            "folderIds": {k: v["folderId"] for k, v in post_inv["folders"].items()},
            "dashboardIds": {
                k: sre.identifier("dashboard", v["body"]) for k, v in post_inv["dashboards"].items()
            },
            "alertIds": {
                k: sre.identifier("alert", v["body"]) for k, v in post_inv["alerts"].items()
            },
        }


def rollback_backup_dir(api, backup_dir):
    """Roll back applied OpenRig Showcase journals in reverse phase order (catalog, then folders)."""
    p.mutation_guard()
    backup_root = Path(backup_dir).expanduser().resolve()
    rolled_phases = []

    with sre.global_lock():
        for phase in ("catalog", "folders"):
            journal_path = backup_root / f"journal-{phase}.json"
            if not journal_path.exists():
                continue
            journal = p.private_read(journal_path)
            for action in reversed(journal.get("actions", [])):
                if action.get("rolledBack"):
                    continue
                kind = action["kind"]
                before = action.get("before")
                after = action.get("after")
                if after is None and before is None:
                    action["rolledBack"] = True
                    p.private_write(journal_path, journal)
                    continue
                if after is None:
                    raise p.SafeError("Unverified partial OpenRig mutation; HOLD")

                if before is None:
                    sre.delete_resource(api, kind, after)
                else:
                    res_id = sre.identifier(kind, after["body"])
                    working = sre.read_resource(
                        api,
                        kind,
                        res_id,
                        folder_id=after.get("folderId"),
                        folder_type=after.get("folderType"),
                    )
                    if action.get("moveFromFolderId") and working.get("folderId") != before.get("folderId"):
                        working = sre.move_resource(
                            api,
                            kind,
                            res_id,
                            working["folderId"],
                            before["folderId"],
                        )
                    restore_action = {
                        "kind": kind,
                        "desired": before["body"],
                        "targetFolderId": before.get("folderId"),
                        "folderType": before.get("folderType") or action.get("folderType"),
                    }
                    _write_showcase_resource(api, restore_action, working)

                action["rolledBack"] = True
                p.private_write(journal_path, journal)
            rolled_phases.append(phase)

    return {"ok": True, "rollback": "verified", "phases": rolled_phases}


def collect_all_showcase_queries():
    """Extract all SQL queries from both OpenRig Showcase dashboards and all 5 alerts."""
    items = []
    for d_spec in dashboards_catalog():
        d_key = d_spec["key"]
        body = d_spec["body"]
        for t in body.get("tabs", []):
            tab_id = t.get("tabId", "default")
            for pan in t.get("panels", []):
                if pan.get("type") == "markdown" or not pan.get("queries"):
                    continue
                for q in pan.get("queries", []):
                    fields = q.get("fields", {})
                    items.append({
                        "source": "dashboard",
                        "dashboardKey": d_key,
                        "tabId": tab_id,
                        "id": pan.get("id"),
                        "title": pan.get("title"),
                        "streamType": fields.get("stream_type", "metrics"),
                        "stream": fields.get("stream", "default"),
                        "sql": q.get("query", ""),
                    })
    for a_spec in alerts_catalog():
        body = a_spec["body"]
        items.append({
            "source": "alert",
            "dashboardKey": None,
            "tabId": None,
            "id": body["name"],
            "title": body["name"],
            "streamType": body["stream_type"],
            "stream": body["stream_name"],
            "sql": body["query_condition"]["sql"],
        })
    return items


def validate_queries(api):
    """Run every SQL query across both dashboards and all 5 alerts over a 24h window."""
    end_us = time.time_ns() // 1000
    start_us = end_us - 24 * 3600 * 1_000_000
    results = []

    for item in collect_all_showcase_queries():
        stype = item["streamType"]
        sql = item["sql"]
        record = {
            "source": item["source"],
            "dashboardKey": item["dashboardKey"],
            "tabId": item["tabId"],
            "id": item["id"],
            "title": item["title"],
            "streamType": stype,
            "stream": item["stream"],
            "sql": sql,
            "status": 0,
            "sqlAccepted": False,
            "state": "SQL-rejected",
            "returnedRows": 0,
        }
        try:
            response = api.call(
                "POST",
                p.BASE + "/_search?type=" + quote(stype, safe=""),
                {
                    "query": {
                        "sql": sql,
                        "start_time": start_us,
                        "end_time": end_us,
                        "from": 0,
                        "size": 5,
                        "sql_mode": "full",
                    }
                },
            )
            if isinstance(response, dict) and not response.get("function_error"):
                record["status"] = 200
                record["sqlAccepted"] = True
                record["state"] = "SQL-accepted"
                record["returnedRows"] = len(response.get("hits", []))
        except p.SafeError:
            record["status"] = 400
            record["sqlAccepted"] = False
            record["state"] = "SQL-rejected"
        results.append(record)

    accepted = sum(1 for r in results if r["sqlAccepted"])
    rejected = len(results) - accepted
    return {
        "backendCommit": p.COMMIT,
        "checkedAtMicros": end_us,
        "windowHours": 24,
        "totalQueries": len(results),
        "queriesAccepted": accepted,
        "queriesRejected": rejected,
        "results": results,
    }



def export_json_artifacts(output_dir):
    """Export sanitized, portable OpenObserve V8 dashboard and alert JSON files."""
    out = Path(output_dir).expanduser().resolve()
    dash_dir = out / "dashboards"
    alert_dir = out / "alerts"
    dash_dir.mkdir(parents=True, exist_ok=True)
    alert_dir.mkdir(parents=True, exist_ok=True)

    exported_dashboards = []
    for d_spec in dashboards_catalog():
        key = d_spec["key"]
        body = copy.deepcopy(d_spec["body"])
        body["dashboardId"] = ""
        body["owner"] = ""
        target = dash_dir / f"{key}.v8.json"
        target.write_text(json.dumps(body, indent=2, sort_keys=False) + "\n", encoding="utf-8")
        exported_dashboards.append(str(target))

    alert_specs = []
    for a_spec in alerts_catalog(folder_id="default", destinations=[], enabled=False):
        body = copy.deepcopy(a_spec["body"])
        body.pop("folderId", None)
        alert_specs.append(body)
    alert_target = alert_dir / "openrig-reliability-alerts.json"
    alert_target.write_text(
        json.dumps(
            {
                "folder": {
                    "name": "OpenRig Reliability",
                    "folderType": "alerts",
                    "description": folders_catalog()[1]["description"],
                },
                "alerts": alert_specs,
            },
            indent=2,
            sort_keys=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "ok": True,
        "dashboards": exported_dashboards,
        "alerts": str(alert_target),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_plan = sub.add_parser("plan")
    p_plan.add_argument("--phase", choices=("folders", "catalog"), default="catalog")
    p_plan.add_argument("--backup-dir", type=Path, required=True)

    p_apply = sub.add_parser("apply")
    p_apply.add_argument("--phase", choices=("folders", "catalog"), default="catalog")
    p_apply.add_argument("--backup-dir", type=Path, required=True)

    p_val = sub.add_parser("validate-queries")
    p_val.add_argument("--output", "--out", dest="output", type=Path, required=True)

    p_rb = sub.add_parser("rollback")
    p_rb.add_argument("--backup-dir", type=Path, required=True)

    p_exp = sub.add_parser("export-json")
    p_exp.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent)

    args = parser.parse_args(argv)
    if args.command == "export-json":
        print(json.dumps(export_json_artifacts(args.output_dir)))
        return
    ensure_env_credentials()
    api = p.Api()

    if args.command == "plan":
        backup_root = args.backup_dir.expanduser().resolve()
        backup_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        inv = inventory(api)
        plan = build_plan(inv, phase=args.phase)
        p.private_write(backup_root / f"plan-{args.phase}.json", plan)
        print(json.dumps({
            "ok": True,
            "phase": plan["phase"],
            "create": plan["create"],
            "update": plan["update"],
            "unchanged": plan["unchanged"],
            "actions": len(plan["actions"]),
        }))
    elif args.command == "apply":
        result = apply_phase(api, args.phase, args.backup_dir)
        print(json.dumps(result))
    elif args.command == "validate-queries":
        report = validate_queries(api)
        p.private_write(args.output.expanduser().resolve(), report)
        if report["queriesRejected"] > 0:
            raise p.SafeError(f"OpenRig SQL validation failed ({report['queriesRejected']} rejected)")
        print(json.dumps({
            "ok": True,
            "totalQueries": report["totalQueries"],
            "queriesAccepted": report["queriesAccepted"],
            "queriesRejected": report["queriesRejected"],
        }))
    elif args.command == "rollback":
        print(json.dumps(rollback_backup_dir(api, args.backup_dir)))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            str(exc) if isinstance(exc, p.SafeError) else "OpenRig showcase provisioning failed",
            file=sys.stderr,
        )
        sys.exit(1)
