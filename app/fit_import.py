"""Decode original FIT files into Parquet, using Garmin's official FIT SDK.

Original files are never modified or deleted.  For every FIT file this writes:
  parquet/fit/records/source=<src>/<activity_key>.parquet   one row per record message
  parquet/fit/laps/source=<src>/<activity_key>.parquet      lap messages
  parquet/fit/sessions/source=<src>/<activity_key>.parquet  session messages
  parquet/fit/messages/source=<src>/<activity_key>.parquet  every other message, as JSON
  parquet/fit/summary/source=<src>/<activity_key>.parquet   one derived row per activity

Every field the SDK decodes is kept: scalar fields become columns, lists and
developer fields are stored as JSON text.  Positions are kept in semicircles
and also converted to degrees in *_deg columns.

CLI:
  python -m app.fit_import parse            # parse all pending FIT files
  python -m app.fit_import parse --all      # re-parse everything (new parser version)
  python -m app.fit_import import-dir --source coros /import/coros
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from garmin_fit_sdk import Decoder, Stream

from . import PARSER_VERSION
from .archive import safe_component, sha256_file, write_immutable
from .db import Ledger, utcnow
from .settings import Settings, load_settings

log = logging.getLogger("garmin.fit")

SEMICIRCLE_TO_DEG = 180.0 / 2**31
CORE_MESSAGES = {"record_mesgs": "records", "lap_mesgs": "laps", "session_mesgs": "sessions"}
RUNNING_DYNAMICS = ("vertical_oscillation", "stance_time", "step_length", "vertical_ratio",
                    "stance_time_balance", "stance_time_percent")


class FitDecodeError(Exception):
    pass


def decode_fit(path: Path) -> dict[str, list[dict[str, Any]]]:
    stream = Stream.from_file(str(path))
    decoder = Decoder(stream)
    if not decoder.is_fit():
        raise FitDecodeError(f"{path.name} is not a FIT file")
    messages, errors = decoder.read(
        apply_scale_and_offset=True,
        convert_datetimes_to_dates=True,
        convert_types_to_strings=True,
        expand_sub_fields=True,
        expand_components=True,
        merge_heart_rates=True,
    )
    if errors:
        # The SDK returns the messages decoded before an error; keep them but
        # surface the problem.
        log.warning("%s: %d decode error(s): %s", path.name, len(errors), errors[:3])
        if not messages:
            raise FitDecodeError(f"{path.name}: {errors[0]}")
    return {str(k): v for k, v in messages.items()}


# --- value conversion -------------------------------------------------------


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def to_json(value: Any) -> str:
    return json.dumps(value, default=_json_default, separators=(",", ":"))


def _scalar_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, str):
        return "str"
    return "json"


def rows_to_table(rows: list[dict[str, Any]],
                  types: dict[str, pa.DataType] | None = None) -> pa.Table:
    """Build an Arrow table from heterogeneous dict rows without losing fields.

    Each column gets a single type: numeric columns widen int->float, datetime
    columns become UTC timestamps, anything mixed or nested becomes JSON text.
    """
    columns: dict[str, list[Any]] = {}
    for i, row in enumerate(rows):
        for key in row:
            if key not in columns:
                columns[key] = [None] * i
        for key, values in columns.items():
            values.append(row.get(key))
    arrays: dict[str, pa.Array] = {}
    for key, values in columns.items():
        if types and key in types:
            arrays[key] = pa.array(values, types[key])
            continue
        kinds = {_scalar_kind(v) for v in values} - {"null"}
        if not kinds:
            arrays[key] = pa.nulls(len(values), pa.string())
        elif kinds <= {"int"}:
            try:
                arrays[key] = pa.array(values, pa.int64())
            except (OverflowError, pa.ArrowInvalid):
                arrays[key] = pa.array(values, pa.float64())
        elif kinds <= {"int", "float"}:
            arrays[key] = pa.array([None if v is None else float(v) for v in values],
                                   pa.float64())
        elif kinds == {"bool"}:
            arrays[key] = pa.array(values, pa.bool_())
        elif kinds == {"datetime"}:
            arrays[key] = pa.array(
                [None if v is None else (v if v.tzinfo else v.replace(tzinfo=UTC))
                 for v in values], pa.timestamp("us", tz="UTC"))
        elif kinds == {"date"}:
            arrays[key] = pa.array(values, pa.date32())
        elif kinds == {"str"}:
            arrays[key] = pa.array(values, pa.string())
        else:
            arrays[key] = pa.array([None if v is None else to_json(v) for v in values],
                                   pa.string())
    return pa.table(arrays)


def write_parquet(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


# --- per-activity processing ------------------------------------------------


def _provenance(source: str, activity_key: str, source_id: str, fit_rel: str,
                parsed_at: str) -> dict[str, Any]:
    return {
        "source": source,
        "activity_key": activity_key,
        "source_record_id": source_id,
        "fit_file": fit_rel,
        "parser_version": PARSER_VERSION,
        "parsed_at": parsed_at,
    }


def _prepare_rows(messages: list[dict[str, Any]], prov: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for index, msg in enumerate(messages):
        row = dict(prov, message_index=index)
        for key, value in msg.items():
            if key == "developer_fields":
                row["developer_fields_json"] = to_json(value) if value else None
                continue
            row[key if key not in prov else f"fit_{key}"] = value
        for field in ("position_lat", "position_long", "start_position_lat",
                      "start_position_long", "end_position_lat", "end_position_long"):
            value = msg.get(field)
            if isinstance(value, (int, float)):
                row[f"{field}_deg"] = value * SEMICIRCLE_TO_DEG
        rows.append(row)
    return rows


def _num(values: Iterable[Any]) -> list[float]:
    return [float(v) for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]


def summarize(records: list[dict[str, Any]], sessions: list[dict[str, Any]],
              prov: dict[str, Any]) -> dict[str, Any]:
    """Derived per-activity context used for altitude/temperature-aware analysis.
    These are independent calculations from FIT records, not Garmin metrics."""
    alts = _num(r.get("enhanced_altitude", r.get("altitude")) for r in records)
    temps = _num(r.get("temperature") for r in records)
    hrs = _num(r.get("heart_rate") for r in records)
    times = [r["timestamp"] for r in records if isinstance(r.get("timestamp"), datetime)]
    session = sessions[0] if sessions else {}
    keys = set().union(*(r.keys() for r in records)) if records else set()
    return dict(
        prov,
        record_count=len(records),
        first_timestamp=min(times) if times else None,
        last_timestamp=max(times) if times else None,
        sport=session.get("sport"),
        sub_sport=session.get("sub_sport"),
        altitude_median_m=statistics.median(alts) if alts else None,
        altitude_min_m=min(alts) if alts else None,
        altitude_max_m=max(alts) if alts else None,
        temperature_mean_c=statistics.fmean(temps) if temps else None,
        heart_rate_mean=statistics.fmean(hrs) if hrs else None,
        has_gps="position_lat" in keys,
        has_power="power" in keys,
        has_running_dynamics=any(k in keys for k in RUNNING_DYNAMICS),
        record_fields=to_json(sorted(keys)),
        session_count=len(sessions),
    )


def parse_activity(settings: Settings, *, source: str, activity_key: str, source_id: str,
                   fit_paths: list[str]) -> dict[str, Any]:
    parsed_at = utcnow()
    all_records: list[dict[str, Any]] = []
    tables: dict[str, list[dict[str, Any]]] = {"records": [], "laps": [], "sessions": [],
                                                "messages": []}
    sessions_raw: list[dict[str, Any]] = []
    parsed_any = False
    for rel in fit_paths:
        if not rel.lower().endswith(".fit"):
            continue
        messages = decode_fit(settings.data_dir / rel)
        parsed_any = True
        prov = _provenance(source, activity_key, source_id, rel, parsed_at)
        for name, msgs in messages.items():
            if name in CORE_MESSAGES:
                tables[CORE_MESSAGES[name]].extend(_prepare_rows(msgs, prov))
                if name == "record_mesgs":
                    all_records.extend(msgs)
                if name == "session_mesgs":
                    sessions_raw.extend(msgs)
            else:
                for index, msg in enumerate(msgs):
                    tables["messages"].append(dict(prov, message_type=name,
                                                   message_index=index, payload=to_json(msg)))
    if not parsed_any:
        raise FitDecodeError("no .fit file in activity download")
    base = settings.parquet_dir / "fit"
    part = f"source={safe_component(source)}/{safe_component(activity_key)}.parquet"
    for name, rows in tables.items():
        target = base / name / part
        if rows:
            write_parquet(rows_to_table(rows), target)
        else:
            target.unlink(missing_ok=True)
    summary = summarize(all_records, sessions_raw,
                        _provenance(source, activity_key, source_id, ",".join(fit_paths),
                                    parsed_at))
    write_parquet(rows_to_table([summary]), base / "summary" / part)
    return summary


def parse_pending(settings: Settings, ledger: Ledger, *, reparse_all: bool = False) -> int:
    count = 0
    cond = "" if reparse_all else " AND (fit_parsed_version IS NULL OR fit_parsed_version != ?)"
    params: tuple[Any, ...] = () if reparse_all else (PARSER_VERSION,)
    rows = ledger.conn.execute(
        f"SELECT activity_id, fit_files FROM activities WHERE fit_files IS NOT NULL{cond}",
        params).fetchall()
    for row in rows:
        try:
            parse_activity(settings, source="garmin", activity_key=str(row["activity_id"]),
                           source_id=str(row["activity_id"]),
                           fit_paths=json.loads(row["fit_files"]))
            ledger.update_activity(row["activity_id"], fit_parsed_version=PARSER_VERSION,
                                   fit_parsed_at=utcnow(), fit_error=None)
            count += 1
        except Exception as exc:  # keep going; the error is recorded per activity
            log.warning("FIT parse failed for activity %s: %s", row["activity_id"], exc)
            ledger.update_activity(row["activity_id"], fit_parsed_version=PARSER_VERSION,
                                   fit_parsed_at=utcnow(),
                                   fit_error=f"{type(exc).__name__}: {exc}"[:500])
    rows = ledger.conn.execute(
        f"SELECT activity_key, source, rel_path FROM imported_fit WHERE 1=1{cond}",
        params).fetchall()
    for row in rows:
        try:
            parse_activity(settings, source=row["source"], activity_key=row["activity_key"],
                           source_id=row["activity_key"], fit_paths=[row["rel_path"]])
            error = None
            count += 1
        except Exception as exc:
            log.warning("FIT parse failed for %s: %s", row["activity_key"], exc)
            error = f"{type(exc).__name__}: {exc}"[:500]
        with ledger.tx() as c:
            c.execute("UPDATE imported_fit SET fit_parsed_version=?, fit_parsed_at=?, "
                      "fit_error=? WHERE activity_key=?",
                      (PARSER_VERSION, utcnow(), error, row["activity_key"]))
    if count:
        log.info("parsed %d activity file set(s)", count)
    return count


def import_dir(settings: Settings, ledger: Ledger, source: str, directory: Path) -> int:
    """Copy FIT files from another platform into the immutable archive."""
    source = safe_component(source.lower())
    if source == "garmin":
        raise ValueError("use the Garmin sync for Garmin activities")
    imported = 0
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.suffix.lower() != ".fit":
            continue
        digest = sha256_file(path)
        if ledger.conn.execute("SELECT 1 FROM imported_fit WHERE sha256=?", (digest,)
                               ).fetchone():
            continue
        key = f"{source}-{digest[:16]}"
        rel = f"raw/imports/{source}/{digest[:2]}/{digest}.fit"
        stored = write_immutable(settings.data_dir, rel, path.read_bytes())
        ledger.record_file(rel, stored.sha256, stored.size, f"fit_import_{source}")
        with ledger.tx() as c:
            c.execute("INSERT INTO imported_fit(activity_key, source, rel_path, sha256, "
                      "original_name, imported_at) VALUES (?,?,?,?,?,?)",
                      (key, source, rel, digest, path.name, utcnow()))
        imported += 1
    log.info("imported %d new %s FIT file(s) from %s", imported, source, directory)
    return imported


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.fit_import")
    sub = parser.add_subparsers(dest="command", required=True)
    parse = sub.add_parser("parse")
    parse.add_argument("--all", action="store_true", help="re-parse every file")
    imp = sub.add_parser("import-dir")
    imp.add_argument("--source", required=True, help="e.g. coros")
    imp.add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    settings.ensure_dirs()
    ledger = Ledger(settings.ledger_path)
    if args.command == "parse":
        parse_pending(settings, ledger, reparse_all=args.all)
    else:
        import_dir(settings, ledger, args.source, args.directory)
        parse_pending(settings, ledger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
