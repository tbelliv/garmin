"""Read-only query layer over the archive, used by the MCP server.

Rules this module follows everywhere:
  * Missing values are returned as null with a reason, never as zero.
  * Garmin-reported metrics and values calculated here are labelled separately
    ("garmin_*" vs "derived_*" / an explicit "method" note).
  * Every result says where its numbers came from (dataset, endpoint, path).

DuckDB runs in memory with file access restricted to the Parquet directory and
its configuration locked; the ledger is opened read-only.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from . import coverage
from .archive import safe_component
from .db import Ledger
from .endpoints import INVESTIGATE_METRICS, all_endpoints
from .settings import Settings

M_PER_MI = 1609.344

DATASETS = {
    "activities": "activities.parquet",
    "fit_records": "fit/records/*/*.parquet",
    "fit_laps": "fit/laps/*/*.parquet",
    "fit_sessions": "fit/sessions/*/*.parquet",
    "fit_messages": "fit/messages/*/*.parquet",
    "fit_summary": "fit/summary/*/*.parquet",
    "json_records": "json/records/*/*.parquet",
    "json_fields": "json/fields/*/*.parquet",
    "field_inventory": "field_inventory.parquet",
}

# Named metrics -> candidate (endpoint, path pattern) pairs, tried in order.
# A pattern without '%' matches a leaf name exactly (any parent path).
# These follow Garmin Connect field names and must be confirmed against real
# data with list_fields after the first sync; results always report the path used.
METRIC_ALIASES: dict[str, list[tuple[str, str]]] = {
    "resting_hr": [("daily_summaries", "restingHeartRate"), ("heart_rate", "restingHeartRate")],
    "hrv_last_night": [("hrv_summaries", "lastNightAvg"), ("hrv", "lastNightAvg")],
    "hrv_weekly_avg": [("hrv_summaries", "weeklyAvg"), ("hrv", "weeklyAvg")],
    "sleep_seconds": [("sleep_summaries", "sleepTimeSeconds"), ("sleep", "sleepTimeSeconds")],
    "sleep_score": [("sleep", "%sleepScores.overall.value"),
                    ("sleep_summaries", "%overall%value")],
    "stress_avg": [("daily_summaries", "averageStressLevel"),
                   ("daily_stress", "overallStressLevel")],
    "body_battery_high": [("daily_summaries", "bodyBatteryHighestValue")],
    "body_battery_low": [("daily_summaries", "bodyBatteryLowestValue")],
    "steps": [("daily_summaries", "totalSteps"), ("daily_steps", "totalSteps")],
    "spo2_avg": [("spo2_acclimation", "averageSpO2"), ("sleep", "averageSpO2Value"),
                 ("daily_summaries", "averageSpo2")],
    "respiration_avg": [("sleep", "averageRespirationValue"),
                        ("daily_summaries", "avgWakingRespirationValue")],
    "weight_g": [("weight_measurements", "weight")],
    "vigorous_intensity_minutes": [("daily_summaries", "vigorousIntensityMinutes")],
    "moderate_intensity_minutes": [("daily_summaries", "moderateIntensityMinutes")],
    "altitude_acclimation": [("spo2_acclimation", "%cclimation%")],
    "vo2max": [("activity", "vO2MaxValue"), ("activity_search", "vO2MaxValue"),
               ("profile_settings", "vo2MaxRunning")],
    "activity_training_load": [("activity", "activityTrainingLoad"),
                               ("activity_search", "activityTrainingLoad")],
}

STREAM_DEFAULT_FIELDS = [
    "timestamp", "distance", "enhanced_speed", "speed", "heart_rate", "enhanced_altitude",
    "altitude", "power", "cadence", "fractional_cadence", "temperature", "position_lat_deg",
    "position_long_deg", "vertical_oscillation", "stance_time", "step_length",
    "vertical_ratio", "stance_time_balance", "grade",
]
STREAM_UNITS = {
    "timestamp": "UTC", "elapsed_s": "s since first record", "distance": "m",
    "enhanced_speed": "m/s", "speed": "m/s", "heart_rate": "bpm", "enhanced_altitude": "m",
    "altitude": "m", "power": "W", "temperature": "deg C (watch sensor; body heat biases it)",
    "cadence": "FIT running cadence is per leg (strides/min); steps/min = 2 x (cadence + "
               "fractional_cadence)",
    "position_lat_deg": "deg", "position_long_deg": "deg", "vertical_oscillation": "mm",
    "stance_time": "ms", "step_length": "mm", "vertical_ratio": "%", "grade": "%",
}
ALTITUDE_BANDS = [(0, 1000, "low (<1000 m)"), (1000, 2000, "moderate (1000-2000 m)"),
                  (2000, 3000, "high (2000-3000 m)"), (3000, 10000, "very high (>=3000 m)")]
ACTIVITY_ENDPOINTS = ("activity", "activity_workouts", "activity_search")


class QueryError(Exception):
    pass


def jsonable(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else round(value, 6)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def altitude_band(meters: float | None) -> str:
    if meters is None:
        return "unknown (no FIT altitude)"
    for low, high, label in ALTITUDE_BANDS:
        if low <= meters < high:
            return label
    return "very high (>=3000 m)" if meters >= 3000 else "low (<1000 m)"


def pace(speed_mps: float | None) -> dict[str, str | None]:
    if not speed_mps or speed_mps <= 0:
        return {"per_km": None, "per_mi": None}

    def fmt(meters: float) -> str:
        total = meters / speed_mps
        return f"{int(total // 60)}:{int(round(total % 60)):02d}"

    return {"per_km": fmt(1000), "per_mi": fmt(M_PER_MI)}


def _d(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return date.fromisoformat(value[:10]).isoformat()


class Archive:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.root = settings.parquet_dir.resolve()
        self.con = duckdb.connect(config={"autoinstall_known_extensions": "false",
                                          "autoload_known_extensions": "false"})
        self.present: set[str] = set()
        for name, pattern in DATASETS.items():
            if list(self.root.glob(pattern)):
                self.con.execute(
                    f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{self.root}/{pattern}', "
                    "union_by_name=true, hive_partitioning=false)")
                self.present.add(name)
        self.con.execute(f"SET allowed_directories=['{self.root}/']")
        self.con.execute("SET enable_external_access=false")
        self.con.execute("SET lock_configuration=true")
        self.ledger: Ledger | None = None
        if settings.ledger_path.exists():
            self.ledger = Ledger(settings.ledger_path, read_only=True)

    # --- helpers -----------------------------------------------------------
    def q(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        cur = self.con.execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, (jsonable(v) for v in row), strict=True))
                for row in cur.fetchall()]

    def require(self, *names: str) -> None:
        missing = [n for n in names if n not in self.present]
        if missing:
            raise QueryError(
                f"dataset(s) {', '.join(missing)} not built yet: no data has been synced and "
                "processed for them. Run data_coverage for details.")

    def columns(self, source: str) -> set[str]:
        return {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}

    def _fit_file(self, kind: str, source: str, key: str) -> Path | None:
        path = self.root / "fit" / kind / f"source={safe_component(source)}" / (
            f"{safe_component(key)}.parquet")
        return path if path.exists() else None

    def _records_source(self, keys: list[tuple[str, str]]) -> str | None:
        files = [str(f) for src, key in keys if (f := self._fit_file("records", src, key))]
        if not files:
            return None
        listed = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
        return f"read_parquet([{listed}], union_by_name=true, hive_partitioning=false)"

    def _activity(self, activity_key: str) -> dict[str, Any]:
        self.require("activities")
        rows = self.q("SELECT * FROM activities WHERE activity_key = ?", [str(activity_key)])
        if not rows:
            raise QueryError(f"activity {activity_key!r} not found in the archive")
        return rows[0]

    @staticmethod
    def _decorate(row: dict[str, Any]) -> dict[str, Any]:
        dist = row.get("distance_m")
        row["distance_km"] = round(dist / 1000, 3) if dist is not None else None
        row["distance_mi"] = round(dist / M_PER_MI, 3) if dist is not None else None
        moving = row.get("moving_duration_s") or row.get("duration_s")
        speed = row.get("avg_speed_mps") or (dist / moving if dist and moving else None)
        row["avg_pace"] = pace(speed)
        row["altitude_band"] = altitude_band(row.get("altitude_median_m"))
        return row

    # --- activities -----------------------------------------------------------
    def list_activities(self, start_date: str | None = None, end_date: str | None = None,
                        activity_type: str | None = None, limit: int = 100) -> dict[str, Any]:
        return self.search_activities(start_date=start_date, end_date=end_date,
                                      activity_type=activity_type, limit=limit)

    def search_activities(self, *, start_date: str | None = None, end_date: str | None = None,
                          activity_type: str | None = None, name_contains: str | None = None,
                          min_distance_km: float | None = None,
                          max_distance_km: float | None = None,
                          min_elevation_gain_m: float | None = None,
                          min_avg_hr: float | None = None, max_avg_hr: float | None = None,
                          min_altitude_m: float | None = None,
                          max_altitude_m: float | None = None, source: str | None = None,
                          limit: int = 100) -> dict[str, Any]:
        self.require("activities")
        where, params = ["1=1"], []
        filters = [
            (start_date, "substr(start_time_local, 1, 10) >= ?", _d),
            (end_date, "substr(start_time_local, 1, 10) <= ?", _d),
            (activity_type, "activity_type ILIKE ?", lambda v: f"%{v}%"),
            (name_contains, "name ILIKE ?", lambda v: f"%{v}%"),
            (min_distance_km, "distance_m >= ?", lambda v: v * 1000),
            (max_distance_km, "distance_m <= ?", lambda v: v * 1000),
            (min_elevation_gain_m, "elevation_gain_m >= ?", float),
            (min_avg_hr, "avg_hr >= ?", float),
            (max_avg_hr, "avg_hr <= ?", float),
            (min_altitude_m, "altitude_median_m >= ?", float),
            (max_altitude_m, "altitude_median_m <= ?", float),
            (source, "source = ?", str),
        ]
        for value, clause, conv in filters:
            if value is not None:
                where.append(clause)
                params.append(conv(value))
        limit = max(1, min(int(limit), 1000))
        rows = self.q(
            "SELECT activity_key, source, start_time_local, activity_type, name, distance_m, "
            "duration_s, moving_duration_s, elevation_gain_m, elevation_loss_m, avg_hr, max_hr, "
            "avg_speed_mps, avg_power_w, avg_cadence, garmin_vo2max, altitude_median_m, "
            "altitude_max_m, temperature_mean_c, has_power, has_running_dynamics, fit_error "
            f"FROM activities WHERE {' AND '.join(where)} "
            "ORDER BY start_time_local DESC NULLS LAST LIMIT ?", [*params, limit])
        return {
            "count": len(rows),
            "units": {"distance_m": "m", "duration_s": "s", "elevation_gain_m": "m",
                      "avg_hr": "bpm", "avg_speed_mps": "m/s", "avg_power_w": "W",
                      "altitude_median_m": "m (median of FIT records; derived)",
                      "temperature_mean_c": "deg C (watch sensor; derived)"},
            "activities": [self._decorate(r) for r in rows],
        }

    def get_activity(self, activity_key: str) -> dict[str, Any]:
        row = self._decorate(self._activity(activity_key))
        out: dict[str, Any] = {"activity": row}
        if self.ledger is not None and row["source"] == "garmin":
            led = self.ledger.conn.execute(
                "SELECT summary_json, zip_path, zip_sha256, fit_files, fit_parsed_version "
                "FROM activities WHERE activity_id = ?", (int(activity_key),)).fetchone()
            if led:
                out["garmin_summary"] = json.loads(led["summary_json"] or "null")
                out["files"] = {"original_zip": led["zip_path"], "zip_sha256": led["zip_sha256"],
                                "fit": json.loads(led["fit_files"] or "[]"),
                                "fit_parser_version": led["fit_parsed_version"]}
        if "json_records" in self.present:
            for endpoint, label in (("activity", "garmin_detail"),
                                    ("activity_workouts", "garmin_workouts")):
                recs = self.q("SELECT payload, retrieved_at, raw_path FROM json_records "
                              "WHERE endpoint = ? AND unit_key = ? AND is_latest",
                              [endpoint, str(activity_key)])
                if recs:
                    out[label] = json.loads(recs[0]["payload"])
                    out[f"{label}_provenance"] = {"retrieved_at": recs[0]["retrieved_at"],
                                                  "raw_path": recs[0]["raw_path"]}
        summary = self._fit_file("summary", row["source"], str(activity_key))
        if summary:
            out["fit_derived_summary"] = self.q(
                f"SELECT * FROM read_parquet('{summary}')")[0]
        return out

    def get_activity_stream(self, activity_key: str, fields: list[str] | None = None,
                            max_points: int = 2000) -> dict[str, Any]:
        act = self._activity(activity_key)
        path = self._fit_file("records", act["source"], str(activity_key))
        if path is None:
            return {"activity_key": activity_key, "status": "no_fit_records",
                    "reason": act.get("fit_error") or "original file not downloaded or parsed "
                    "yet; see data_coverage"}
        src = f"read_parquet('{path}')"
        available = sorted(self.columns(src))
        wanted = fields or [f for f in STREAM_DEFAULT_FIELDS if f in available]
        unknown = [f for f in wanted if f not in available]
        wanted = [f for f in wanted if f in available]
        total = self.con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        max_points = max(10, min(int(max_points), 20000))
        step = max(1, math.ceil(total / max_points))
        select = ", ".join(f'"{f}"' for f in wanted)
        elapsed = ("epoch(timestamp) - epoch(MIN(timestamp) OVER ()) AS elapsed_s, "
                   if "timestamp" in available else "")
        cur = self.con.execute(
            f"SELECT * FROM (SELECT {elapsed}{select}, message_index FROM {src}) "
            "WHERE message_index % ? = 0 ORDER BY message_index", [step])
        cols = [c[0] for c in cur.description if c[0] != "message_index"]
        rows = [jsonable(list(r[:-1])) for r in cur.fetchall()]
        return {
            "activity_key": activity_key, "source": act["source"], "total_records": total,
            "downsample_step": step, "fields": cols,
            "units": {f: STREAM_UNITS.get(f, "FIT SDK units") for f in cols},
            "unknown_fields_requested": unknown, "available_fields": available,
            "rows": rows,
        }

    def get_laps(self, activity_key: str) -> dict[str, Any]:
        act = self._activity(activity_key)
        out: dict[str, Any] = {"activity_key": activity_key}
        path = self._fit_file("laps", act["source"], str(activity_key))
        if path:
            laps = self.q(f"SELECT * FROM read_parquet('{path}') ORDER BY message_index")
            out["fit_laps"] = [{k: v for k, v in lap.items() if v is not None} for lap in laps]
            out["units"] = "FIT SDK units: distance m, time s, speed m/s, heart rate bpm"
        else:
            out["fit_laps"] = None
            out["fit_laps_reason"] = "no parsed FIT laps for this activity"
        if "json_records" in self.present:
            recs = self.q("SELECT payload FROM json_records WHERE endpoint='activity_workouts' "
                          "AND unit_key = ? AND is_latest", [str(activity_key)])
            out["garmin_workouts"] = json.loads(recs[0]["payload"]) if recs else None
        return out

    # --- health -------------------------------------------------------------
    def _day_status(self, endpoint: str, start: date, end: date) -> dict[str, list[str]]:
        if self.ledger is None:
            return {}
        states = coverage.day_states(self.ledger, endpoint)
        out: dict[str, list[str]] = {}
        day = start
        while day <= end:
            out.setdefault(states.get(day, "not_requested"), []).append(day.isoformat())
            day += timedelta(days=1)
        return out

    @staticmethod
    def _strip_series(node: Any, keep: bool) -> Any:
        if keep:
            return node
        if isinstance(node, dict):
            return {k: Archive._strip_series(v, keep) for k, v in node.items()}
        if isinstance(node, list):
            if len(node) > 20:
                return {"_omitted_list_length": len(node),
                        "_hint": "call again with include_timeseries=true"}
            return [Archive._strip_series(v, keep) for v in node]
        return node

    def get_health_data(self, start_date: str, end_date: str,
                        endpoints: list[str] | None = None,
                        include_timeseries: bool = False) -> dict[str, Any]:
        self.require("json_records")
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
        if (end - start).days > 92:
            raise QueryError("limit health queries to 93 days; use get_metrics_history for "
                             "long trends")
        names = endpoints or [ep.name for ep in all_endpoints(
            self.settings.extra_endpoints_file) if ep.kind != "static"]
        recs = self.q(
            "SELECT endpoint, calendar_date, payload, retrieved_at, raw_path FROM json_records "
            "WHERE is_latest AND calendar_date BETWEEN ? AND ? AND list_contains(?, endpoint) "
            "ORDER BY calendar_date, endpoint, record_index",
            [start.isoformat(), end.isoformat(), names])
        days: dict[str, dict[str, Any]] = {}
        for r in recs:
            entry = days.setdefault(r["calendar_date"], {})
            payload = self._strip_series(json.loads(r["payload"]), include_timeseries)
            entry.setdefault(r["endpoint"], []).append(
                {"data": payload, "retrieved_at": r["retrieved_at"], "raw_path": r["raw_path"]})
        return {
            "range": [start.isoformat(), end.isoformat()],
            "endpoints": names,
            "days": days,
            "day_status_by_endpoint": {n: self._day_status(n, start, end) for n in names},
            "notes": "Days absent from 'days' have no record. day_status_by_endpoint says why: "
                     "empty = Garmin returned nothing (not worn/recorded), not_found = HTTP 404, "
                     "failed = request failed (will be retried), not_requested = not synced yet.",
        }

    def _metric_candidates(self, metric: str) -> list[tuple[str, str]]:
        if metric in METRIC_ALIASES:
            return METRIC_ALIASES[metric]
        if ":" in metric:
            endpoint, path = metric.split(":", 1)
            return [(endpoint, path)]
        raise QueryError(f"unknown metric {metric!r}. Use one of {sorted(METRIC_ALIASES)} or "
                         "'endpoint:field_path' from list_fields.")

    @staticmethod
    def _path_clause(pattern: str) -> tuple[str, list[str]]:
        if "%" in pattern:
            return "path LIKE ?", [pattern]
        if "." in pattern or "[" in pattern:
            return "path = ?", [pattern]
        return "(path = ? OR leaf = ?)", [pattern, pattern]

    def get_metrics_history(self, metric: str, start_date: str, end_date: str,
                            period: str = "day") -> dict[str, Any]:
        self.require("json_fields")
        tried = []
        for endpoint, pattern in self._metric_candidates(metric):
            clause, params = self._path_clause(pattern)
            bucket = ("calendar_date" if period == "day" else "date_trunc('week', calendar_date)"
                      if period == "week" else "date_trunc('month', calendar_date)")
            rows = self.q(
                f"SELECT {bucket} AS period_start, AVG(value_num) AS value, "
                "MIN(value_num) AS min, MAX(value_num) AS max, COUNT(*) AS n, "
                "string_agg(DISTINCT path, ', ') AS paths FROM json_fields "
                f"WHERE endpoint = ? AND {clause} AND value_num IS NOT NULL "
                "AND value_type != 'array' AND calendar_date BETWEEN ? AND ? "
                "GROUP BY 1 ORDER BY 1", [endpoint, *params, start_date, end_date])
            tried.append(f"{endpoint}:{pattern}")
            if rows:
                return {
                    "metric": metric, "period": period, "source_endpoint": endpoint,
                    "source_paths": sorted({p for r in rows for p in r["paths"].split(", ")}),
                    "garmin_reported": True,
                    "points": [{k: v for k, v in r.items() if k != "paths"} for r in rows],
                    "periods_with_values": len(rows),
                    "note": "Periods without a value are omitted (no data), never zero. "
                            "Weekly/monthly values are means of daily values.",
                }
        return {"metric": metric, "status": "not_found_in_retrieved_data", "tried": tried,
                "points": [],
                "note": "No values found. The metric may be inaccessible through the Personal "
                        "API, not recorded by the device, or not synced yet; check "
                        "data_coverage and list_fields."}

    # --- training metrics ---------------------------------------------------
    def get_training_metrics(self, start_date: str | None = None,
                             end_date: str | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {}
        fit_cols: set[str] = set()
        if "fit_sessions" in self.present:
            fit_cols = self.columns("fit_sessions")
        for metric, patterns in INVESTIGATE_METRICS.items():
            found: list[dict[str, Any]] = []
            if "field_inventory" in self.present:
                clause = " OR ".join("lower(leaf) LIKE ?" for _ in patterns)
                found = self.q(
                    "SELECT endpoint, path, occurrences, numeric_occurrences, example, "
                    f"first_date, last_date FROM field_inventory WHERE ({clause}) "
                    "ORDER BY occurrences DESC LIMIT 15", [f"%{p}%" for p in patterns])
            latest: list[dict[str, Any]] = []
            for hit in found[:5]:
                if hit["endpoint"] in ("activity_search",):
                    continue
                params: list[Any] = [hit["endpoint"], hit["path"].replace("[]", "[%]")]
                date_clause = ""
                if start_date:
                    date_clause += " AND calendar_date >= ?"
                    params.append(start_date)
                if end_date:
                    date_clause += " AND calendar_date <= ?"
                    params.append(end_date)
                latest += self.q(
                    "SELECT endpoint, path, calendar_date, source_record_id, "
                    "COALESCE(CAST(value_num AS VARCHAR), value_text) AS value FROM json_fields "
                    f"WHERE endpoint = ? AND path LIKE ?{date_clause} "
                    "ORDER BY calendar_date DESC NULLS LAST LIMIT 10", params)
            fit_matches = sorted(c for c in fit_cols if any(p in c.replace("_", "").lower()
                                                             or p in c.lower()
                                                             for p in patterns))
            fit_values: list[dict[str, Any]] = []
            if fit_matches:
                cols = ", ".join(f'"{c}"' for c in fit_matches[:8])
                fit_values = self.q(
                    f"SELECT activity_key, start_time, {cols} FROM fit_sessions "
                    "ORDER BY start_time DESC NULLS LAST LIMIT 10")
            status = "found_in_retrieved_data" if (found or fit_matches) else \
                "not_found_in_retrieved_data"
            result[metric] = {"status": status, "api_fields": found, "recent_api_values": latest,
                              "fit_session_fields": fit_matches,
                              "recent_fit_values": fit_values}
        return {
            "metrics": result,
            "notes": [
                "The official client 1.0.0 has no dedicated endpoints for VO2 max, training "
                "status, training readiness, recovery time or race predictions. This tool "
                "searches every field retrieved so far (API JSON and FIT session messages).",
                "not_found_in_retrieved_data means not seen yet: the metric may be "
                "inaccessible through the Personal API, not recorded, or not yet synced.",
                "Values are Garmin-reported; nothing here is estimated.",
            ],
        }

    def list_fields(self, search: str | None = None, endpoint: str | None = None,
                    limit: int = 200) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if "field_inventory" in self.present:
            where, params = ["1=1"], []
            if search:
                where.append("(lower(path) LIKE ?)")
                params.append(f"%{search.lower()}%")
            if endpoint:
                where.append("endpoint = ?")
                params.append(endpoint)
            out["api_fields"] = self.q(
                "SELECT endpoint, path, value_types, occurrences, example, first_date, last_date "
                f"FROM field_inventory WHERE {' AND '.join(where)} "
                "ORDER BY endpoint, path LIMIT ?", [*params, max(1, min(int(limit), 2000))])
        else:
            out["api_fields"] = None
        for name in ("fit_records", "fit_laps", "fit_sessions"):
            if name in self.present:
                cols = sorted(self.columns(name))
                out[f"{name}_columns"] = [c for c in cols if not search or search.lower()
                                          in c.lower()]
        if "fit_messages" in self.present:
            out["fit_other_message_types"] = self.q(
                "SELECT message_type, COUNT(*) AS n FROM fit_messages GROUP BY 1 ORDER BY 2 DESC")
        return out

    # --- coverage -------------------------------------------------------------
    def data_coverage(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "status_legend": {
                "retrieved": "available and retrieved for every expected day",
                "partial": "some days retrieved; others pending or failed",
                "no_data_recorded": "requests succeeded but Garmin returned no data",
                "not_accessible": "the Personal API refused (403) or only returned 404",
                "not_investigated": "never requested",
            },
            "datasets_built": sorted(self.present),
        }
        if self.ledger is None:
            out["ledger"] = "missing: no sync has run yet"
            return out
        try:
            out["report"] = coverage.build_report(self.ledger, self.settings)
        except Exception as exc:  # e.g. the sync holds a write lock
            out["report_error"] = f"{type(exc).__name__}: {exc}"
        if "field_inventory" in self.present:
            metrics = self.get_training_metrics()["metrics"]
            out["investigated_metrics"] = {k: v["status"] for k, v in metrics.items()}
        else:
            out["investigated_metrics"] = "not investigated: no normalized data yet"
        return out

    # --- analysis -------------------------------------------------------------
    def heart_rate_zones(self) -> dict[str, Any]:
        """Latest Garmin HR zone snapshot, preferring running zones."""
        if "json_records" not in self.present:
            return {"zones": None, "reason": "no heart_rate_zones snapshot yet"}
        recs = self.q("SELECT payload, retrieved_at FROM json_records WHERE "
                      "endpoint='heart_rate_zones' ORDER BY retrieved_at DESC LIMIT 1")
        if not recs:
            return {"zones": None, "reason": "no heart_rate_zones snapshot yet"}
        payload = json.loads(recs[0]["payload"])
        candidates = payload if isinstance(payload, list) else [payload]
        candidates = [c for c in candidates if isinstance(c, dict) and "zone1Floor" in c]
        if not candidates:
            return {"zones": None, "reason": "zone format not recognised; inspect via "
                    "get_health_data(endpoints=['heart_rate_zones'])"}
        chosen = next((c for c in candidates if str(c.get("sport", "")).upper() == "RUNNING"),
                      candidates[0])
        floors = [chosen.get(f"zone{i}Floor") for i in range(1, 6)]
        if any(f is None for f in floors):
            return {"zones": None, "reason": "incomplete zone floors"}
        return {"zones": floors, "max_hr": chosen.get("maxHeartRateUsed"),
                "sport": chosen.get("sport"), "retrieved_at": recs[0]["retrieved_at"],
                "source": "Garmin heart_rate_zones snapshot"}

    def _activities_between(self, start: str, end: str, activity_type: str | None
                            ) -> list[dict[str, Any]]:
        res = self.search_activities(start_date=start, end_date=end,
                                     activity_type=activity_type, limit=1000)
        return res["activities"]

    @staticmethod
    def _totals(acts: list[dict[str, Any]]) -> dict[str, Any]:
        def total(key: str) -> float | None:
            vals = [a[key] for a in acts if a.get(key) is not None]
            return sum(vals) if vals else None

        dist = total("distance_m")
        moving = sum((a.get("moving_duration_s") or a.get("duration_s") or 0) for a in acts)
        hr_pairs = [(a["avg_hr"], a.get("moving_duration_s") or a.get("duration_s") or 0)
                    for a in acts if a.get("avg_hr")]
        hr_weight = sum(w for _, w in hr_pairs)
        bands: dict[str, dict[str, float]] = {}
        for a in acts:
            b = bands.setdefault(a["altitude_band"], {"activities": 0, "distance_km": 0.0})
            b["activities"] += 1
            b["distance_km"] += (a.get("distance_m") or 0) / 1000
        return {
            "activities": len(acts),
            "distance_km": round(dist / 1000, 2) if dist is not None else None,
            "distance_mi": round(dist / M_PER_MI, 2) if dist is not None else None,
            "moving_time_h": round(moving / 3600, 2) if acts else None,
            "elevation_gain_m": total("elevation_gain_m"),
            "avg_hr_time_weighted": round(sum(h * w for h, w in hr_pairs) / hr_weight, 1)
            if hr_weight else None,
            "avg_pace": pace(dist / moving) if dist and moving else pace(None),
            "by_altitude_band": bands,
        }

    def _record_cols(self, src: str) -> tuple[str | None, str | None]:
        cols = self.columns(src)
        speed = ("COALESCE(enhanced_speed, speed)" if {"enhanced_speed", "speed"} <= cols
                 else "enhanced_speed" if "enhanced_speed" in cols
                 else "speed" if "speed" in cols else None)
        hr = "heart_rate" if "heart_rate" in cols else None
        return speed, hr

    def pace_at_hr(self, acts: list[dict[str, Any]], hr_low: float, hr_high: float
                   ) -> dict[str, Any]:
        """Mean moving speed while heart rate is within [hr_low, hr_high], grouped by
        altitude band.  Derived from FIT records; not grade-adjusted."""
        keys = [(a["source"], a["activity_key"]) for a in acts]
        src = self._records_source(keys)
        if src is None:
            return {"status": "no_fit_records"}
        speed, hr = self._record_cols(src)
        if speed is None or hr is None:
            return {"status": "records lack speed or heart rate"}
        bands = {a["activity_key"]: a["altitude_band"] for a in acts}
        rows = self.q(
            f"SELECT activity_key, AVG({speed}) AS speed, COUNT(*) AS n FROM {src} "
            f"WHERE {hr} BETWEEN ? AND ? AND {speed} > 1.0 GROUP BY activity_key",
            [hr_low, hr_high])
        grouped: dict[str, dict[str, float]] = {}
        for r in rows:
            g = grouped.setdefault(bands.get(r["activity_key"], "unknown"),
                                   {"weighted": 0.0, "records": 0})
            g["weighted"] += r["speed"] * r["n"]
            g["records"] += r["n"]
        return {
            "hr_range_bpm": [hr_low, hr_high],
            "method": "derived: mean FIT speed over records with HR in range and speed > 1 m/s;"
                      " ~1 record per second; not grade- or temperature-adjusted",
            "by_altitude_band": {
                band: {"records": int(g["records"]),
                       "mean_speed_mps": round(g["weighted"] / g["records"], 3),
                       "pace": pace(g["weighted"] / g["records"])}
                for band, g in grouped.items() if g["records"]},
        }

    def time_in_zones(self, acts: list[dict[str, Any]], floors: list[float]) -> dict[str, Any]:
        src = self._records_source([(a["source"], a["activity_key"]) for a in acts])
        if src is None:
            return {"status": "no_fit_records"}
        if "heart_rate" not in self.columns(src):
            return {"status": "no heart rate in records"}
        z = floors
        rows = self.q(f"""
            WITH r AS (
                SELECT heart_rate, timestamp,
                       LEAD(timestamp) OVER (PARTITION BY activity_key, fit_file
                                             ORDER BY timestamp) AS nxt
                FROM {src})
            SELECT CASE WHEN heart_rate < ? THEN 'below_zone1' WHEN heart_rate < ? THEN 'zone1'
                        WHEN heart_rate < ? THEN 'zone2' WHEN heart_rate < ? THEN 'zone3'
                        WHEN heart_rate < ? THEN 'zone4' ELSE 'zone5' END AS zone,
                   SUM(LEAST(epoch(nxt) - epoch(timestamp), 10)) AS seconds
            FROM r WHERE nxt IS NOT NULL AND heart_rate IS NOT NULL GROUP BY 1 ORDER BY 1""",
            [z[0], z[1], z[2], z[3], z[4]])
        total = sum(r["seconds"] or 0 for r in rows)
        return {"zone_floors_bpm": z, "total_seconds_with_hr": total,
                "zones": {r["zone"]: {"seconds": r["seconds"],
                                      "share": round(r["seconds"] / total, 3) if total else None}
                          for r in rows},
                "method": "derived from FIT records; gaps between records capped at 10 s"}

    def _health_means(self, start: str, end: str) -> dict[str, Any]:
        out = {}
        for metric in ("resting_hr", "hrv_last_night", "sleep_seconds", "sleep_score",
                       "stress_avg", "body_battery_high", "spo2_avg"):
            if "json_fields" not in self.present:
                out[metric] = None
                continue
            res = self.get_metrics_history(metric, start, end)
            pts = [p["value"] for p in res.get("points", []) if p["value"] is not None]
            out[metric] = ({"mean": round(sum(pts) / len(pts), 2), "days": len(pts),
                            "source": f"{res['source_endpoint']}:{','.join(res['source_paths'])}"}
                           if pts else None)
        return out

    def compare_periods(self, period_a_start: str, period_a_end: str, period_b_start: str,
                        period_b_end: str, activity_type: str | None = "running",
                        hr_low: float | None = None, hr_high: float | None = None
                        ) -> dict[str, Any]:
        zones = self.heart_rate_zones()
        if hr_low is None or hr_high is None:
            if zones.get("zones"):
                hr_low, hr_high = zones["zones"][1], zones["zones"][2] - 1
            else:
                hr_low = hr_high = None
        out: dict[str, Any] = {"activity_type": activity_type, "hr_zones": zones}
        for label, start, end in (("period_a", period_a_start, period_a_end),
                                  ("period_b", period_b_start, period_b_end)):
            acts = self._activities_between(start, end, activity_type)
            out[label] = {
                "range": [start, end],
                "training": self._totals(acts),
                "health_means": self._health_means(start, end),
                "pace_at_hr": self.pace_at_hr(acts, hr_low, hr_high) if hr_low else
                {"status": "no HR range: pass hr_low/hr_high or sync heart_rate_zones"},
            }
        out["notes"] = [
            "Compare pace at HR only within the same altitude band; Boulder (~1650 m) runs "
            "fall in the moderate band.",
            "Health means average daily Garmin values; null means no data, not zero.",
        ]
        return out

    def weekly_report_data(self, week_start: str | None = None,
                           activity_type: str | None = None) -> dict[str, Any]:
        if week_start:
            start = date.fromisoformat(week_start)
        else:
            today = date.today()
            start = today - timedelta(days=today.weekday() + 7)  # last complete week
        start = start - timedelta(days=start.weekday())
        end = start + timedelta(days=6)
        prior_start = start - timedelta(days=28)
        prior_end = start - timedelta(days=1)
        acts = self._activities_between(start.isoformat(), end.isoformat(), activity_type)
        prior = self._activities_between(prior_start.isoformat(), prior_end.isoformat(),
                                         activity_type)
        zones = self.heart_rate_zones()
        this_totals, prior_totals = self._totals(acts), self._totals(prior)
        week_km = this_totals["distance_km"] or 0.0
        prior_weekly_km = (prior_totals["distance_km"] or 0.0) / 4
        runs = [a for a in acts if "run" in (a.get("activity_type") or "")]
        prior_runs = [a for a in prior if "run" in (a.get("activity_type") or "")]
        hr_rng = (zones["zones"][1], zones["zones"][2] - 1) if zones.get("zones") else None
        load = None
        if "json_fields" in self.present:
            load_hist = self.get_metrics_history("activity_training_load", start.isoformat(),
                                                 end.isoformat())
            if load_hist.get("points"):
                load = {"sum": round(sum(p["value"] * p["n"] for p in load_hist["points"]), 1),
                        "source": load_hist["source_endpoint"]}
        return {
            "week": [start.isoformat(), end.isoformat()],
            "activities": [{k: a.get(k) for k in (
                "activity_key", "start_time_local", "activity_type", "name", "distance_km",
                "distance_mi", "moving_duration_s", "elevation_gain_m", "avg_hr", "avg_pace",
                "altitude_band", "altitude_median_m", "temperature_mean_c", "avg_power_w")}
                for a in acts],
            "totals": this_totals,
            "prior_4_weeks_totals": prior_totals,
            "load_indicators": {
                "week_distance_km": round(week_km, 2),
                "prior_4wk_avg_weekly_km": round(prior_weekly_km, 2),
                "distance_ratio_vs_prior_4wk": round(week_km / prior_weekly_km, 2)
                if prior_weekly_km else None,
                "garmin_activity_training_load": load,
                "method": "distance ratio is derived (acute week / mean of prior 4 weeks); "
                          "it is a crude indicator, not Garmin's training status",
            },
            "intensity_distribution": self.time_in_zones(acts, zones["zones"])
            if zones.get("zones") else {"status": zones.get("reason")},
            "pace_at_zone2_hr": {
                "this_week": self.pace_at_hr(runs, *hr_rng) if hr_rng else None,
                "prior_4_weeks": self.pace_at_hr(prior_runs, *hr_rng) if hr_rng else None,
            },
            "recovery": {
                "this_week": self._health_means(start.isoformat(), end.isoformat()),
                "prior_4_weeks": self._health_means(prior_start.isoformat(),
                                                    prior_end.isoformat()),
            },
            "hr_zones": zones,
            "data_gaps": {ep: {k: v for k, v in self._day_status(ep, start, end).items()
                               if k not in ("ok",)}
                          for ep in ("daily_summaries", "sleep", "hrv", "heart_rate")},
        }

    # --- SQL ------------------------------------------------------------------
    def run_sql(self, sql: str, max_rows: int = 500) -> dict[str, Any]:
        try:
            statements = self.con.extract_statements(sql)
        except duckdb.Error as exc:
            raise QueryError(f"SQL parse error: {exc}") from exc
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise QueryError("only a single SELECT (or WITH ... SELECT / DESCRIBE / "
                             "SUMMARIZE) statement is allowed")
        max_rows = max(1, min(int(max_rows), 5000))
        cur = self.con.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchmany(max_rows + 1)
        return {"columns": cols, "rows": [jsonable(list(r)) for r in rows[:max_rows]],
                "truncated": len(rows) > max_rows, "views": sorted(self.present)}
