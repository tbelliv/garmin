"""Normalize archived JSON into Parquet datasets for DuckDB.

Response formats of the Personal API are not assumed: normalization is
generic and lossless, and can always be rebuilt from the raw archive.

  parquet/json/records/endpoint=<name>/data.parquet
      One row per record per retrieval, with the full record as JSON and
      provenance (source, endpoint, record id, calendar date, retrieval time,
      raw file, hash, parser version).  is_latest marks the newest retrieval.
  parquet/json/fields/endpoint=<name>/data.parquet
      Every scalar leaf of the latest version of each record as
      (path, value_num, value_text) - so any metric can be queried by name.
  parquet/field_inventory.parquet
      Which fields exist, how often, and over which dates.
  parquet/activities.parquet
      One row per activity from every source, joined with FIT-derived context
      (altitude, temperature) for altitude-aware analysis.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime
from typing import Any

import duckdb
import pyarrow as pa

from . import PARSER_VERSION
from .db import Ledger
from .fit_import import rows_to_table, write_parquet
from .settings import Settings

log = logging.getLogger("garmin.normalize")

DATE_KEYS = ("calendarDate", "summaryDate", "date", "startDate", "startTimeLocal",
             "startTimestampLocal", "startTimeGMT", "timestampLocal")
ID_KEYS = ("activityId", "id", "summaryId", "userDailySummaryId", "samplePk", "sleepId",
           "dailySleepDTO.id", "date")
SPLIT_ENDPOINTS_KIND = {"range"}
# Endpoints whose unit_key is an activity id: one logical record per activity.
PER_ACTIVITY_ENDPOINTS = {"activity", "activity_workouts"}
MAX_EXPANDED_LIST = 60  # lists of dicts longer than this stay JSON-only (time series)
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _as_date(value: Any) -> str | None:
    if isinstance(value, str) and _ISO_DATE.match(value):
        return value[:10]
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 10**11:
        return datetime.fromtimestamp(value / 1000, UTC).date().isoformat()
    return None


def find_date(record: Any) -> tuple[str | None, str | None]:
    """Return (calendar_date, key it came from), searching one nesting level."""
    if not isinstance(record, dict):
        return None, None
    for key in DATE_KEYS:
        found = _as_date(record.get(key))
        if found:
            return found, key
    for name, child in record.items():
        if isinstance(child, dict):
            for key in DATE_KEYS:
                found = _as_date(child.get(key))
                if found:
                    return found, f"{name}.{key}"
    return None, None


def find_id(record: Any) -> str | None:
    if not isinstance(record, dict):
        return None
    for key in ID_KEYS:
        node: Any = record
        for part in key.split("."):
            node = node.get(part) if isinstance(node, dict) else None
        if isinstance(node, (str, int)) and not isinstance(node, bool):
            return str(node)
    return None


def split_records(payload: Any, kind: str) -> tuple[list[Any], dict[str, Any] | None]:
    """Split a response into records.  Per-day and static responses are one
    record.  Range responses are split when they are a list, or a dict with
    exactly one list of dated objects (other keys are kept as the envelope)."""
    if kind not in SPLIT_ENDPOINTS_KIND:
        return [payload], None
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict):
        candidates = [
            key for key, value in payload.items()
            if isinstance(value, list) and value and all(isinstance(v, dict) for v in value)
            and find_date(value[0])[0]
        ]
        if len(candidates) == 1:
            key = candidates[0]
            envelope = {k: v for k, v in payload.items() if k != key}
            return payload[key], dict(envelope, _split_key=key)
    return [payload], None


def flatten(node: Any, prefix: str = "") -> list[tuple[str, Any]]:
    """Scalar leaves as (dotted.path, value).  Lists of scalars and long lists
    of objects are summarized by the inventory but stay JSON-only."""
    out: list[tuple[str, Any]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            out.extend(flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(node, list):
        if node and len(node) <= MAX_EXPANDED_LIST and all(isinstance(v, dict) for v in node):
            for i, value in enumerate(node):
                out.extend(flatten(value, f"{prefix}[{i}]"))
        else:
            out.append((prefix, ("__array__", len(node))))
    else:
        out.append((prefix, node))
    return out


def _endpoint_kinds(settings: Settings) -> dict[str, str]:
    from .endpoints import all_endpoints

    kinds = {ep.name: ep.kind for ep in all_endpoints(settings.extra_endpoints_file)}
    kinds.update({"activity_search": "range", "activity": "day", "activity_workouts": "day"})
    return kinds


def build_endpoint(settings: Settings, ledger: Ledger, endpoint: str, kind: str) -> list[dict]:
    attempts = ledger.conn.execute(
        "SELECT id, unit_key, range_start, range_end, started_at, raw_path, sha256 "
        "FROM attempts WHERE endpoint=? AND raw_path IS NOT NULL ORDER BY id", (endpoint,)
    ).fetchall()
    records: list[dict[str, Any]] = []
    for att in attempts:
        path = settings.data_dir / att["raw_path"]
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log.warning("cannot read %s: %s", path, exc)
            continue
        items, envelope = split_records(payload, kind)
        for index, item in enumerate(items):
            cal, date_key = find_date(item)
            if cal is None and att["range_start"] and att["range_start"] == att["range_end"]:
                cal, date_key = att["range_start"], "request_date"
            rid = find_id(item)
            if endpoint in PER_ACTIVITY_ENDPOINTS:
                rid = rid or att["unit_key"]
            records.append({
                "source": "garmin",
                "endpoint": endpoint,
                "unit_key": att["unit_key"],
                "record_index": index,
                "source_record_id": rid,
                "calendar_date": date.fromisoformat(cal) if cal else None,
                "date_source": date_key,
                "retrieved_at": datetime.fromisoformat(att["started_at"].replace("Z", "+00:00")),
                "raw_path": att["raw_path"],
                "raw_sha256": att["sha256"],
                "parser_version": PARSER_VERSION,
                "envelope": json.dumps(envelope) if envelope else None,
                "payload": json.dumps(item, ensure_ascii=False),
                "_item": item,
            })
    # Latest version of each logical record.
    latest: dict[tuple, dict[str, Any]] = {}
    for rec in records:
        if endpoint in PER_ACTIVITY_ENDPOINTS:
            identity = ("activity", rec["unit_key"], rec["record_index"])
        elif rec["calendar_date"] is not None or rec["source_record_id"] is not None:
            identity = (rec["calendar_date"], rec["source_record_id"])
        else:
            identity = ("unit", rec["unit_key"], rec["record_index"])
        current = latest.get(identity)
        if current is None or rec["retrieved_at"] >= current["retrieved_at"]:
            latest[identity] = rec
    latest_ids = {id(r) for r in latest.values()}
    fields: list[dict[str, Any]] = []
    for rec in records:
        rec["is_latest"] = id(rec) in latest_ids
        if not rec["is_latest"]:
            continue
        for path, value in flatten(rec["_item"]):
            is_array = isinstance(value, tuple) and value[:1] == ("__array__",)
            num = (float(value) if isinstance(value, (int, float)) and not isinstance(value, bool)
                   else None)
            fields.append({
                "source": "garmin",
                "endpoint": endpoint,
                "calendar_date": rec["calendar_date"],
                "source_record_id": rec["source_record_id"],
                "record_index": rec["record_index"],
                "retrieved_at": rec["retrieved_at"],
                "path": path,
                "leaf": re.sub(r"\[\d+\]", "", path.rsplit(".", 1)[-1]),
                "value_type": "array" if is_array else type(value).__name__,
                "value_num": float(value[1]) if is_array else num,
                "value_text": None if is_array or num is not None or value is None
                else str(value),
                "parser_version": PARSER_VERSION,
            })
    base = settings.parquet_dir / "json"
    rec_rows = [{k: v for k, v in r.items() if k != "_item"} for r in records]
    if rec_rows:
        write_parquet(rows_to_table(rec_rows), base / "records" / f"endpoint={endpoint}" /
                      "data.parquet")
    if fields:
        write_parquet(rows_to_table(fields), base / "fields" / f"endpoint={endpoint}" /
                      "data.parquet")
    return fields


def build_inventory(settings: Settings) -> None:
    fields_glob = settings.parquet_dir / "json" / "fields" / "*" / "*.parquet"
    if not list(settings.parquet_dir.glob("json/fields/*/*.parquet")):
        return
    con = duckdb.connect()
    target = settings.parquet_dir / "field_inventory.parquet"
    tmp = target.with_name(".field_inventory.tmp.parquet")
    con.execute(f"""
        COPY (
            SELECT source, endpoint, regexp_replace(path, '\\[\\d+\\]', '[]', 'g') AS path,
                   any_value(leaf) AS leaf,
                   string_agg(DISTINCT value_type, ',') AS value_types,
                   COUNT(*) AS occurrences,
                   COUNT(value_num) AS numeric_occurrences,
                   any_value(COALESCE(CAST(value_num AS VARCHAR), value_text)) AS example,
                   MIN(calendar_date) AS first_date, MAX(calendar_date) AS last_date,
                   '{PARSER_VERSION}' AS parser_version
            FROM read_parquet('{fields_glob}', union_by_name=true)
            GROUP BY ALL
        ) TO '{tmp}' (FORMAT parquet)
    """)
    tmp.replace(target)


def parquet_columns(con: duckdb.DuckDBPyConnection, source: str) -> set[str]:
    return {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}


def build_activities(settings: Settings, ledger: Ledger) -> None:
    rows = []
    for r in ledger.conn.execute("SELECT * FROM activities"):
        row = dict(r)
        rows.append({
            "activity_key": str(row["activity_id"]),
            "source": row["source"],
            "source_record_id": str(row["activity_id"]),
            "activity_type": row["activity_type"],
            "name": row["name"],
            "start_time_local": row["start_time_local"],
            "start_time_gmt": row["start_time_gmt"],
            "distance_m": row["distance_m"],
            "duration_s": row["duration_s"],
            "moving_duration_s": row["moving_duration_s"],
            "elevation_gain_m": row["elevation_gain_m"],
            "elevation_loss_m": row["elevation_loss_m"],
            "avg_hr": row["avg_hr"],
            "max_hr": row["max_hr"],
            "avg_speed_mps": row["avg_speed_mps"],
            "max_speed_mps": row["max_speed_mps"],
            "avg_power_w": row["avg_power_w"],
            "avg_cadence": row["avg_cadence"],
            "garmin_vo2max": row["vo2max"],
            "retrieved_at": row["last_seen"],
            "zip_path": row["zip_path"],
            "fit_error": row["fit_error"],
            "parser_version": PARSER_VERSION,
        })
    for r in ledger.conn.execute("SELECT * FROM imported_fit"):
        rows.append({"activity_key": r["activity_key"], "source": r["source"],
                     "source_record_id": r["activity_key"], "retrieved_at": r["imported_at"],
                     "zip_path": r["rel_path"], "fit_error": r["fit_error"],
                     "parser_version": PARSER_VERSION})
    if not rows:
        return
    staging = settings.parquet_dir / ".activities_ledger.parquet"
    numeric = ("distance_m", "duration_s", "moving_duration_s", "elevation_gain_m",
               "elevation_loss_m", "avg_hr", "max_hr", "avg_speed_mps", "max_speed_mps",
               "avg_power_w", "avg_cadence", "garmin_vo2max")
    text = ("activity_key", "source", "source_record_id", "activity_type", "name",
            "start_time_local", "start_time_gmt", "retrieved_at", "zip_path", "fit_error",
            "parser_version")
    types = {**{c: pa.float64() for c in numeric}, **{c: pa.string() for c in text}}
    for row in rows:
        for col in numeric:
            if row.get(col) is not None:
                row[col] = float(row[col])
    write_parquet(rows_to_table(rows, types), staging)
    con = duckdb.connect()
    fit = settings.parquet_dir / "fit"
    have_summary = bool(list(fit.glob("summary/*/*.parquet")))
    have_sessions = bool(list(fit.glob("sessions/*/*.parquet")))
    summary = (f"read_parquet('{fit}/summary/*/*.parquet', union_by_name=true)"
               if have_summary else
               "(SELECT NULL::VARCHAR AS activity_key, NULL::VARCHAR AS source LIMIT 0)")
    sessions = "(SELECT NULL::VARCHAR AS activity_key, NULL::VARCHAR AS source LIMIT 0)"
    if have_sessions:
        sessions_src = f"read_parquet('{fit}/sessions/*/*.parquet', union_by_name=true)"
        present = parquet_columns(con, sessions_src)

        def agg(fn: str, col: str, cast: str = "DOUBLE") -> str:
            return f"{fn}({col}) AS {col}" if col in present else f"NULL::{cast} AS {col}"

        sessions = f"""(SELECT activity_key, source,
                {agg('any_value', 'sport', 'VARCHAR')},
                {agg('SUM', 'total_distance')}, {agg('SUM', 'total_timer_time')},
                {agg('SUM', 'total_elapsed_time')}, {agg('SUM', 'total_ascent')},
                {agg('SUM', 'total_descent')}, {agg('AVG', 'avg_heart_rate')},
                {agg('MAX', 'max_heart_rate')}, {agg('MIN', 'start_time', 'TIMESTAMPTZ')}
            FROM {sessions_src} GROUP BY activity_key, source)"""
    target = settings.parquet_dir / "activities.parquet"
    tmp = target.with_name(".activities.tmp.parquet")
    # COALESCE prefers Garmin's own summary values; FIT session values fill in
    # for imported (non-Garmin) activities.
    select_fit = (
        """s.altitude_median_m, s.altitude_min_m, s.altitude_max_m, s.temperature_mean_c,
           s.has_gps, s.has_power, s.has_running_dynamics, s.record_count AS fit_record_count"""
        if have_summary else
        """NULL::DOUBLE AS altitude_median_m, NULL::DOUBLE AS altitude_min_m,
           NULL::DOUBLE AS altitude_max_m, NULL::DOUBLE AS temperature_mean_c,
           NULL::BOOLEAN AS has_gps, NULL::BOOLEAN AS has_power,
           NULL::BOOLEAN AS has_running_dynamics, NULL::BIGINT AS fit_record_count""")
    sess_cols = (
        """COALESCE(a.activity_type, x.sport) AS activity_type,
           COALESCE(a.start_time_local, strftime(x.start_time, '%Y-%m-%d %H:%M:%S'))
               AS start_time_local,
           COALESCE(a.distance_m, x.total_distance) AS distance_m,
           COALESCE(a.duration_s, x.total_elapsed_time) AS duration_s,
           COALESCE(a.moving_duration_s, x.total_timer_time) AS moving_duration_s,
           COALESCE(a.elevation_gain_m, x.total_ascent) AS elevation_gain_m,
           COALESCE(a.elevation_loss_m, x.total_descent) AS elevation_loss_m,
           COALESCE(a.avg_hr, x.avg_heart_rate) AS avg_hr,
           COALESCE(a.max_hr, x.max_heart_rate) AS max_hr,"""
        if have_sessions else
        """a.activity_type, a.start_time_local, a.distance_m, a.duration_s,
           a.moving_duration_s, a.elevation_gain_m, a.elevation_loss_m, a.avg_hr, a.max_hr,""")
    con.execute(f"""
        COPY (
            SELECT a.activity_key, a.source, a.source_record_id, a.name, {sess_cols}
                   a.start_time_gmt, a.avg_speed_mps, a.max_speed_mps, a.avg_power_w,
                   a.avg_cadence, a.garmin_vo2max, {select_fit},
                   a.retrieved_at, a.zip_path, a.fit_error, a.parser_version
            FROM read_parquet('{staging}', union_by_name=true) a
            LEFT JOIN {summary} s ON s.activity_key = a.activity_key AND s.source = a.source
            LEFT JOIN {sessions} x ON x.activity_key = a.activity_key AND x.source = a.source
        ) TO '{tmp}' (FORMAT parquet)
    """)
    tmp.replace(target)
    staging.unlink(missing_ok=True)


def rebuild(settings: Settings, ledger: Ledger, *, force: bool = False) -> None:
    kinds = _endpoint_kinds(settings)
    rebuilt = 0
    for endpoint, kind in kinds.items():
        marker = ledger.conn.execute(
            "SELECT MAX(id) FROM attempts WHERE endpoint=? AND raw_path IS NOT NULL", (endpoint,)
        ).fetchone()[0]
        if marker is None:
            continue
        stamp = f"{PARSER_VERSION}:{marker}"
        if not force and ledger.get_kv(f"normalized:{endpoint}") == stamp:
            continue
        build_endpoint(settings, ledger, endpoint, kind)
        ledger.set_kv(f"normalized:{endpoint}", stamp)
        rebuilt += 1
    if rebuilt or force:
        build_inventory(settings)
    build_activities(settings, ledger)
    log.info("normalized %d endpoint dataset(s)", rebuilt)
