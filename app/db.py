"""SQLite ledger: sync runs, every request attempt, work-unit coverage, the
activity index and the archive file manifest.

The ledger is the source of truth for *what has been retrieved*.  Backfill
completion is always computed from work units; there is no "backfill done"
flag that could be set merely because a process exited.

The database uses a rollback journal (not WAL) so the MCP container can open
it read-only from a read-only bind mount.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    outcome      TEXT,
    detail       TEXT
);

-- One row per HTTP request made to Garmin, successful or not.
CREATE TABLE IF NOT EXISTS attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       INTEGER,
    endpoint     TEXT NOT NULL,
    unit_key     TEXT NOT NULL,
    range_start  TEXT,
    range_end    TEXT,
    started_at   TEXT NOT NULL,
    finished_at  TEXT NOT NULL,
    status       TEXT NOT NULL,
    http_status  INTEGER,
    error        TEXT,
    raw_path     TEXT,
    sha256       TEXT,
    bytes        INTEGER,
    duration_ms  INTEGER
);
CREATE INDEX IF NOT EXISTS attempts_endpoint ON attempts(endpoint, unit_key);

-- Current state of each unit of work (an endpoint + date range, or an activity).
CREATE TABLE IF NOT EXISTS units (
    endpoint         TEXT NOT NULL,
    unit_key         TEXT NOT NULL,
    range_start      TEXT,
    range_end        TEXT,
    state            TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_attempt_at  TEXT,
    next_retry_at    TEXT,
    last_http_status INTEGER,
    last_error       TEXT,
    raw_path         TEXT,
    sha256           TEXT,
    record_count     INTEGER,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (endpoint, unit_key)
);
CREATE INDEX IF NOT EXISTS units_state ON units(endpoint, state);

CREATE TABLE IF NOT EXISTS endpoint_status (
    endpoint     TEXT PRIMARY KEY,
    availability TEXT NOT NULL,
    http_status  INTEGER,
    detail       TEXT,
    checked_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activities (
    activity_id        INTEGER PRIMARY KEY,
    source             TEXT NOT NULL DEFAULT 'garmin',
    start_time_local   TEXT,
    start_time_gmt     TEXT,
    activity_type      TEXT,
    name               TEXT,
    distance_m         REAL,
    duration_s         REAL,
    moving_duration_s  REAL,
    elevation_gain_m   REAL,
    elevation_loss_m   REAL,
    avg_hr             REAL,
    max_hr             REAL,
    avg_speed_mps      REAL,
    max_speed_mps      REAL,
    avg_power_w        REAL,
    avg_cadence        REAL,
    vo2max             REAL,
    summary_json       TEXT,
    first_seen         TEXT NOT NULL,
    last_seen          TEXT NOT NULL,
    zip_path           TEXT,
    zip_sha256         TEXT,
    fit_files          TEXT,
    fit_parsed_version TEXT,
    fit_parsed_at      TEXT,
    fit_error          TEXT
);
CREATE INDEX IF NOT EXISTS activities_start ON activities(start_time_local);

-- Every file written to the archive, with its hash, for integrity checks and
-- restore verification.
CREATE TABLE IF NOT EXISTS archive_files (
    path        TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL,
    bytes       INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- FIT files imported from other sources (e.g. COROS exports).
CREATE TABLE IF NOT EXISTS imported_fit (
    activity_key       TEXT PRIMARY KEY,
    source             TEXT NOT NULL,
    rel_path           TEXT NOT NULL,
    sha256             TEXT NOT NULL UNIQUE,
    original_name      TEXT,
    imported_at        TEXT NOT NULL,
    fit_parsed_version TEXT,
    fit_parsed_at      TEXT,
    fit_error          TEXT
);

CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

ACTIVITY_COLUMNS = (
    "start_time_local", "start_time_gmt", "activity_type", "name", "distance_m", "duration_s",
    "moving_duration_s", "elevation_gain_m", "elevation_loss_m", "avg_hr", "max_hr",
    "avg_speed_mps", "max_speed_mps", "avg_power_w", "avg_cadence", "vo2max",
)

# Unit states.  RESOLVED states count toward coverage; the rest do not.
RESOLVED_STATES = ("ok", "empty", "not_found")
FAILED_STATES = ("failed", "forbidden", "bad_request", "auth_error")


def utcnow() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Ledger:
    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self.path = path
        self.read_only = read_only
        if read_only:
            uri = f"file:{path}?mode=ro"
            self.conn = sqlite3.connect(uri, uri=True, timeout=30, check_same_thread=False)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(path, timeout=30)
            self.conn.execute("PRAGMA journal_mode=DELETE")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.executescript(SCHEMA)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self.conn:
            yield self.conn

    # --- runs -------------------------------------------------------------
    def start_run(self, kind: str) -> int:
        with self.tx() as c:
            cur = c.execute("INSERT INTO runs(kind, started_at) VALUES (?, ?)", (kind, utcnow()))
            return int(cur.lastrowid or 0)

    def finish_run(self, run_id: int, outcome: str, detail: dict[str, Any] | None = None) -> None:
        with self.tx() as c:
            c.execute(
                "UPDATE runs SET finished_at=?, outcome=?, detail=? WHERE run_id=?",
                (utcnow(), outcome, json.dumps(detail or {}), run_id),
            )

    # --- attempts and units ----------------------------------------------
    def record_attempt(
        self,
        *,
        run_id: int | None,
        endpoint: str,
        unit_key: str,
        range_start: str | None,
        range_end: str | None,
        started_at: str,
        status: str,
        http_status: int | None = None,
        error: str | None = None,
        raw_path: str | None = None,
        sha256: str | None = None,
        size: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                """INSERT INTO attempts(run_id, endpoint, unit_key, range_start, range_end,
                       started_at, finished_at, status, http_status, error, raw_path, sha256,
                       bytes, duration_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, endpoint, unit_key, range_start, range_end, started_at, utcnow(),
                 status, http_status, (error or "")[:2000] or None, raw_path, sha256, size,
                 duration_ms),
            )

    def get_unit(self, endpoint: str, unit_key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM units WHERE endpoint=? AND unit_key=?", (endpoint, unit_key)
        ).fetchone()

    def set_unit(
        self,
        endpoint: str,
        unit_key: str,
        *,
        range_start: str | None,
        range_end: str | None,
        state: str,
        http_status: int | None = None,
        error: str | None = None,
        raw_path: str | None = None,
        sha256: str | None = None,
        record_count: int | None = None,
        next_retry_at: str | None = None,
        count_attempt: bool = True,
    ) -> None:
        now = utcnow()
        with self.tx() as c:
            c.execute(
                """INSERT INTO units(endpoint, unit_key, range_start, range_end, state, attempts,
                       last_attempt_at, next_retry_at, last_http_status, last_error, raw_path,
                       sha256, record_count, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(endpoint, unit_key) DO UPDATE SET
                       state=excluded.state,
                       attempts=units.attempts + excluded.attempts,
                       last_attempt_at=excluded.last_attempt_at,
                       next_retry_at=excluded.next_retry_at,
                       last_http_status=excluded.last_http_status,
                       last_error=excluded.last_error,
                       raw_path=COALESCE(excluded.raw_path, units.raw_path),
                       sha256=COALESCE(excluded.sha256, units.sha256),
                       record_count=COALESCE(excluded.record_count, units.record_count),
                       updated_at=excluded.updated_at""",
                (endpoint, unit_key, range_start, range_end, state, 1 if count_attempt else 0,
                 now, next_retry_at, http_status, error, raw_path, sha256, record_count, now),
            )

    def units_for(self, endpoint: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM units WHERE endpoint=? ORDER BY range_start, unit_key", (endpoint,)
        ).fetchall()

    # --- endpoint availability -------------------------------------------
    def set_endpoint_status(
        self, endpoint: str, availability: str, http_status: int | None, detail: str | None
    ) -> None:
        with self.tx() as c:
            c.execute(
                """INSERT INTO endpoint_status(endpoint, availability, http_status, detail,
                       checked_at) VALUES (?,?,?,?,?)
                   ON CONFLICT(endpoint) DO UPDATE SET availability=excluded.availability,
                       http_status=excluded.http_status, detail=excluded.detail,
                       checked_at=excluded.checked_at""",
                (endpoint, availability, http_status, (detail or "")[:1000] or None, utcnow()),
            )

    def endpoint_statuses(self) -> dict[str, sqlite3.Row]:
        rows = self.conn.execute("SELECT * FROM endpoint_status").fetchall()
        return {row["endpoint"]: row for row in rows}

    # --- archive manifest -------------------------------------------------
    def record_file(self, path: str, sha256: str, size: int, kind: str) -> None:
        with self.tx() as c:
            c.execute(
                """INSERT OR IGNORE INTO archive_files(path, sha256, bytes, kind, created_at)
                   VALUES (?,?,?,?,?)""",
                (path, sha256, size, kind, utcnow()),
            )

    # --- activities -------------------------------------------------------
    def upsert_activity(self, activity_id: int, fields: dict[str, Any], summary: Any) -> bool:
        """Insert or refresh an activity row; returns True if it was new."""
        now = utcnow()
        existing = self.conn.execute(
            "SELECT 1 FROM activities WHERE activity_id=?", (activity_id,)
        ).fetchone()
        values = [fields.get(col) for col in ACTIVITY_COLUMNS]
        with self.tx() as c:
            if existing:
                assignments = ", ".join(f"{col}=?" for col in ACTIVITY_COLUMNS)
                c.execute(
                    f"UPDATE activities SET {assignments}, summary_json=?, last_seen=? "
                    "WHERE activity_id=?",
                    (*values, json.dumps(summary), now, activity_id),
                )
            else:
                cols = ", ".join(ACTIVITY_COLUMNS)
                marks = ", ".join("?" for _ in ACTIVITY_COLUMNS)
                c.execute(
                    f"INSERT INTO activities(activity_id, {cols}, summary_json, first_seen, "
                    f"last_seen) VALUES (?, {marks}, ?, ?, ?)",
                    (activity_id, *values, json.dumps(summary), now, now),
                )
        return existing is None

    def update_activity(self, activity_id: int, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key}=?" for key in fields)
        with self.tx() as c:
            c.execute(
                f"UPDATE activities SET {assignments} WHERE activity_id=?",
                (*fields.values(), activity_id),
            )

    def activity_ids(self) -> set[int]:
        return {row[0] for row in self.conn.execute("SELECT activity_id FROM activities")}

    # --- key/value --------------------------------------------------------
    def get_kv(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return None if row is None else row[0]

    def set_kv(self, key: str, value: str) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT INTO kv(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
