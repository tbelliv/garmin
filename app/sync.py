"""Garmin ingestion: probing, historical backfill, incremental sync, daemon.

Usage (inside the garmin-sync container):
  python -m app.sync check                 # Milestone 1: prove real API access
  python -m app.sync probe [--date D]      # test every endpoint once, record availability
  python -m app.sync backfill [...]        # resumable historical import
  python -m app.sync incremental           # recent window + new activities + snapshots
  python -m app.sync process               # parse FIT files, rebuild Parquet
  python -m app.sync status [--json]       # coverage report from the ledger
  python -m app.sync retry-failed          # make failed units eligible immediately
  python -m app.sync daemon                # loop: incremental, backfill slice, process

Planning is day-based.  For each endpoint the planner computes which calendar
days are not yet covered by a successful ("resolved") request and requests
only those, newest first.  A crash or restart therefore loses at most the
request in flight, and a failed range is retried on a later run.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any

from . import coverage
from .archive import safe_component, sha256_bytes, store_api_response, write_immutable
from .db import RESOLVED_STATES, Ledger, utcnow
from .endpoints import ACTIVITY_ENDPOINTS, Endpoint, all_endpoints
from .garmin_client import CallResult, GarminApi
from .settings import Settings, load_settings

log = logging.getLogger("garmin.sync")


class RateLimited(Exception):
    pass


class AuthFailed(Exception):
    pass


class BudgetExceeded(Exception):
    pass


# ---------------------------------------------------------------------------
# date helpers


def daterange(start: date, end: date) -> list[date]:
    if end < start:
        return []
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def chunk_days_desc(days: list[date], max_span: int) -> list[tuple[date, date]]:
    """Group days (any order) into contiguous chunks of at most max_span days,
    newest chunk first."""
    chunks: list[tuple[date, date]] = []
    ordered = sorted(set(days), reverse=True)
    i = 0
    while i < len(ordered):
        end = ordered[i]
        start = end
        j = i + 1
        while (
            j < len(ordered)
            and ordered[j] == start - timedelta(days=1)
            and (end - ordered[j]).days < max_span
        ):
            start = ordered[j]
            j += 1
        chunks.append((start, end))
        i = j
    return chunks


def record_count(payload: Any) -> int:
    if payload is None:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        lists = [v for v in payload.values() if isinstance(v, list)]
        if len(lists) == 1:
            return len(lists[0])
        return 1
    return 1


# ---------------------------------------------------------------------------
# activity summary extraction (fields are Garmin Connect names; the full JSON
# is always kept, so a wrong guess here loses nothing)

_ACTIVITY_KEYS: dict[str, tuple[str, ...]] = {
    "start_time_local": ("startTimeLocal",),
    "start_time_gmt": ("startTimeGMT",),
    "name": ("activityName",),
    "distance_m": ("distance",),
    "duration_s": ("duration", "elapsedDuration"),
    "moving_duration_s": ("movingDuration",),
    "elevation_gain_m": ("elevationGain",),
    "elevation_loss_m": ("elevationLoss",),
    "avg_hr": ("averageHR", "averageHr", "avgHr"),
    "max_hr": ("maxHR", "maxHr"),
    "avg_speed_mps": ("averageSpeed",),
    "max_speed_mps": ("maxSpeed",),
    "avg_power_w": ("avgPower", "averagePower"),
    "avg_cadence": ("averageRunningCadenceInStepsPerMinute", "averageRunCadence",
                    "averageBikingCadenceInRevPerMinute"),
    "vo2max": ("vO2MaxValue", "vo2MaxValue"),
}


def activity_fields(doc: dict[str, Any]) -> dict[str, Any]:
    scopes = [doc]
    for nested in ("summaryDTO", "summary"):
        if isinstance(doc.get(nested), dict):
            scopes.append(doc[nested])
    out: dict[str, Any] = {}
    for column, keys in _ACTIVITY_KEYS.items():
        for scope in scopes:
            found = next((scope[k] for k in keys if scope.get(k) is not None), None)
            if found is not None:
                out[column] = found
                break
    for type_key in ("activityType", "activityTypeDTO"):
        value = doc.get(type_key)
        if isinstance(value, dict) and value.get("typeKey"):
            out["activity_type"] = value["typeKey"]
            break
        if isinstance(value, str):
            out["activity_type"] = value
            break
    return out


def activity_id_of(doc: dict[str, Any]) -> int | None:
    value = doc.get("activityId")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------


@dataclass
class RunStats:
    requests: int = 0
    stored: int = 0
    unchanged: int = 0
    resolved: int = 0
    failed: int = 0
    new_activities: int = 0
    zips: int = 0
    by_status: dict[str, int] = field(default_factory=dict)

    def note(self, status: str) -> None:
        self.by_status[status] = self.by_status.get(status, 0) + 1


class Syncer:
    def __init__(
        self,
        settings: Settings,
        ledger: Ledger,
        api: GarminApi,
        *,
        endpoints: list[Endpoint] | None = None,
        today: date | None = None,
    ) -> None:
        self.settings = settings
        self.ledger = ledger
        self.api = api
        self.endpoints = endpoints if endpoints is not None else all_endpoints(
            settings.extra_endpoints_file
        )
        self.today = today or date.today()
        self.run_id: int | None = None
        self.stats = RunStats()
        self.deadline: float | None = None
        self._skip_endpoints: set[str] = set()

    # --- core unit execution ---------------------------------------------
    def _check_budget(self) -> None:
        if self.deadline is not None and time.monotonic() > self.deadline:
            raise BudgetExceeded()

    def execute(
        self,
        endpoint: str,
        unit_key: str,
        call: Any,
        *,
        range_start: str | None,
        range_end: str | None,
        year: str,
        archive: bool = True,
    ) -> CallResult:
        """Make one request, archive the response and update the ledger."""
        self._check_budget()
        started = utcnow()
        result = self.api.call(call)
        self.stats.requests += 1
        self.stats.note(result.status)
        prev = self.ledger.get_unit(endpoint, unit_key)
        raw_path = sha = None
        size = None
        if result.status in ("ok", "empty") and archive:
            stored = store_api_response(
                self.settings.data_dir,
                endpoint=endpoint,
                unit_key=unit_key,
                year=year,
                payload=result.payload,
                meta={
                    "source": "garmin",
                    "endpoint": endpoint,
                    "unit_key": unit_key,
                    "range_start": range_start,
                    "range_end": range_end,
                    "retrieved_at": started,
                    "run_id": self.run_id,
                    "client": "garmin-connect-personal-api-python",
                },
                previous_sha256=prev["sha256"] if prev else None,
            )
            sha, size = stored.sha256, stored.size
            if stored.written:
                raw_path = stored.rel_path
                self.ledger.record_file(stored.rel_path, stored.sha256, stored.size, "api_json")
                self.stats.stored += 1
            else:
                self.stats.unchanged += 1
        if result.resolved:
            self.stats.resolved += 1
            state = result.status
            next_retry = None
        else:
            self.stats.failed += 1
            state = "failed" if result.status in ("rate_limited", "failed") else result.status
            attempts = (prev["attempts"] if prev else 0) + 1
            delay = min(timedelta(hours=24), timedelta(minutes=5) * (2 ** min(attempts, 9)))
            next_retry = (datetime.now(UTC) + delay).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.ledger.record_attempt(
            run_id=self.run_id, endpoint=endpoint, unit_key=unit_key, range_start=range_start,
            range_end=range_end, started_at=started, status=result.status,
            http_status=result.http_status, error=result.error, raw_path=raw_path, sha256=sha,
            size=size, duration_ms=result.duration_ms,
        )
        self.ledger.set_unit(
            endpoint, unit_key, range_start=range_start, range_end=range_end, state=state,
            http_status=result.http_status, error=result.error, raw_path=raw_path, sha256=sha,
            record_count=record_count(result.payload) if result.resolved else None,
            next_retry_at=next_retry,
        )
        if result.status == "rate_limited":
            raise RateLimited(result.error or "HTTP 429")
        if result.status == "auth_error":
            raise AuthFailed(result.error or "HTTP 401 after token refresh")
        return result

    # --- date-grid planning ----------------------------------------------
    def day_floor(self) -> date:
        """Earliest date per-day endpoints are requested from.

        With DAY_ENDPOINT_START=auto this is the start of the earliest
        daily_summaries chunk that returned data, or the earliest activity,
        whichever is earlier.  Days before it are reported as skipped, never
        as complete.
        """
        configured = self.settings.day_endpoint_start
        if configured != "auto":
            return max(self.settings.backfill_start, date.fromisoformat(configured))
        evidence: list[date] = []
        row = self.ledger.conn.execute(
            "SELECT MIN(range_start) FROM units WHERE endpoint='daily_summaries' AND state='ok'"
        ).fetchone()
        if row and row[0]:
            evidence.append(date.fromisoformat(row[0]))
        row = self.ledger.conn.execute(
            "SELECT MIN(substr(start_time_local, 1, 10)) FROM activities"
        ).fetchone()
        if row and row[0]:
            evidence.append(date.fromisoformat(row[0]))
        summaries_done = not coverage.unresolved_days(
            self.ledger, "daily_summaries", self.settings.backfill_start, self.today
        )
        if not summaries_done and not evidence:
            return self.settings.backfill_start
        if not evidence:
            return max(self.settings.backfill_start,
                       self.today - timedelta(days=self.settings.overlap_days))
        floor = max(self.settings.backfill_start, min(evidence))
        if not summaries_done:
            # daily_summaries history not finished yet: do not skip anything.
            floor = self.settings.backfill_start
        self.ledger.set_kv("day_floor", floor.isoformat())
        return floor

    def plan(self, ep: Endpoint, *, overlap: bool, history: bool,
             retry_now: bool = False) -> list[tuple[date, date]]:
        """Chunks to request for ep, newest first.

        overlap: always re-request the last OVERLAP_DAYS (late-arriving data).
        history: request every day since BACKFILL_START not yet resolved.
        """
        todo: set[date] = set()
        if overlap:
            todo |= set(daterange(self.today - timedelta(days=self.settings.overlap_days),
                                  self.today))
        if history and ep.backfill:
            start = self.settings.backfill_start
            if ep.kind == "day":
                start = max(start, self.day_floor())
            todo |= coverage.unresolved_days(
                self.ledger, ep.name, start, self.today, include_blocked=retry_now
            )
        return chunk_days_desc(list(todo), self.max_span(ep))

    def max_span(self, ep: Endpoint) -> int:
        if ep.kind != "range":
            return 1
        learned = self.ledger.get_kv(f"max_span:{ep.name}")
        return max(1, min(self.settings.range_chunk_days, int(learned) if learned else 10**6))

    def request_chunk(self, ep: Endpoint, start: date, end: date) -> list[tuple[date, date]]:
        """Request one chunk; return replacement chunks if the range must be split."""
        key = f"{start.isoformat()}_{end.isoformat()}"
        result = self.execute(
            ep.name, key, lambda c: ep.call(c, start, end),
            range_start=start.isoformat(), range_end=end.isoformat(), year=str(start.year),
        )
        if result.status == "bad_request" and ep.kind == "range" and end > start:
            # The range may be too long for this endpoint: learn a smaller span
            # and re-request the same days in halves straight away.
            days = (end - start).days + 1
            span = max(1, days // 2)
            self.ledger.set_kv(f"max_span:{ep.name}", str(span))
            log.warning("%s: HTTP 400 for a %d-day range; retrying with %d-day chunks",
                        ep.name, days, span)
            return chunk_days_desc(daterange(start, end), span)
        if result.status == "forbidden":
            self.ledger.set_endpoint_status(ep.name, "not_accessible", 403, result.error)
            self._skip_endpoints.add(ep.name)
            log.warning("%s: HTTP 403, skipping this endpoint for the rest of the run", ep.name)
        elif result.status in ("ok", "empty"):
            self.ledger.set_endpoint_status(ep.name, "available", None, None)
        return []

    def sync_dated(self, endpoints: list[Endpoint], *, overlap: bool, history: bool,
                   retry_now: bool = False) -> None:
        """Request planned chunks round-robin across endpoints, so a long
        history for one endpoint cannot starve the others."""
        queues = {ep.name: self.plan(ep, overlap=overlap, history=history, retry_now=retry_now)
                  for ep in endpoints if ep.name not in self._skip_endpoints}
        by_name = {ep.name: ep for ep in endpoints}
        for name, queue in queues.items():
            if queue:
                log.info("%s: %d request(s) planned", name, len(queue))
        while any(queues.values()):
            for name, queue in queues.items():
                if not queue:
                    continue
                if name in self._skip_endpoints:
                    queue.clear()
                    continue
                start, end = queue.pop(0)
                queue[:0] = self.request_chunk(by_name[name], start, end)

    def sync_static_endpoint(self, ep: Endpoint) -> None:
        key = self.today.isoformat()
        unit = self.ledger.get_unit(ep.name, key)
        if unit is not None and unit["state"] in RESOLVED_STATES:
            return
        result = self.execute(ep.name, key, lambda c: ep.call(c), range_start=key,
                              range_end=key, year=str(self.today.year))
        if result.status == "forbidden":
            self.ledger.set_endpoint_status(ep.name, "not_accessible", 403, result.error)
        elif result.resolved:
            self.ledger.set_endpoint_status(ep.name, "available", None, None)

    # --- activities ---------------------------------------------------------
    def list_activities(self, *, full: bool) -> list[dict[str, Any]]:
        """Page through activity search results (newest first)."""
        size = self.settings.activity_page_size
        known = self.ledger.activity_ids()
        cutoff = (self.today - timedelta(days=self.settings.overlap_days)).isoformat()
        listed: list[dict[str, Any]] = []
        start = 0
        while True:
            key = f"page_{start:06d}"
            result = self.execute(
                "activity_search", key,
                lambda c, s=start: c.search_activities(start=s, limit=size),
                range_start=None, range_end=None, year=str(self.today.year),
            )
            if not result.resolved:
                raise RuntimeError(f"activity search failed at offset {start}: {result.error}")
            page = result.payload if isinstance(result.payload, list) else []
            listed.extend(item for item in page if isinstance(item, dict))
            if len(page) < size:
                self.ledger.set_kv("activity_list_complete_at", utcnow())
                break
            if not full:
                ids = {activity_id_of(item) for item in page}
                oldest = min(str(item.get("startTimeLocal") or "") for item in page)
                if ids <= known and oldest < cutoff:
                    break
            start += size
        return listed

    def sync_activities(self, *, full: bool) -> None:
        listed = self.list_activities(full=full)
        cutoff = (self.today - timedelta(days=self.settings.overlap_days)).isoformat()
        for summary in listed:
            activity_id = activity_id_of(summary)
            if activity_id is None:
                continue
            if self.ledger.upsert_activity(activity_id, activity_fields(summary), summary):
                self.stats.new_activities += 1
        # Fetch details, workouts and original files for every known activity
        # that is missing them, newest first.
        rows = self.ledger.conn.execute(
            "SELECT activity_id, start_time_local, zip_path FROM activities "
            "ORDER BY start_time_local DESC"
        ).fetchall()
        for row in rows:
            self._check_budget()
            self.sync_one_activity(row["activity_id"], row["start_time_local"] or "",
                                   row["zip_path"], recent=(row["start_time_local"] or "") >= cutoff)

    def _activity_unit_due(self, endpoint: str, activity_id: int, recent: bool) -> bool:
        unit = self.ledger.get_unit(endpoint, str(activity_id))
        if unit is None:
            return True
        if unit["state"] in RESOLVED_STATES:
            return recent and endpoint != "activity_download"
        if unit["next_retry_at"] and unit["next_retry_at"] > utcnow():
            return False
        return True

    def sync_one_activity(self, activity_id: int, start_local: str, zip_path: str | None,
                          *, recent: bool) -> None:
        year = start_local[:4] or "unknown"
        key = str(activity_id)
        if self._activity_unit_due("activity", activity_id, recent):
            result = self.execute("activity", key, lambda c: c.get_activity(activity_id),
                                  range_start=start_local[:10] or None,
                                  range_end=start_local[:10] or None, year=year)
            if result.status == "ok" and isinstance(result.payload, dict):
                detail = activity_fields(result.payload)
                row = self.ledger.conn.execute(
                    "SELECT * FROM activities WHERE activity_id=?", (activity_id,)
                ).fetchone()
                fill = {k: v for k, v in detail.items() if row is not None and row[k] is None}
                self.ledger.update_activity(activity_id, **fill)
        if self._activity_unit_due("activity_workouts", activity_id, recent):
            self.execute("activity_workouts", key, lambda c: c.get_activity_workouts(activity_id),
                         range_start=start_local[:10] or None,
                         range_end=start_local[:10] or None, year=year)
        if zip_path is None and self._activity_unit_due("activity_download", activity_id, False):
            self.download_activity(activity_id, year, start_local)

    def download_activity(self, activity_id: int, year: str, start_local: str) -> None:
        key = str(activity_id)
        result = self.execute(
            "activity_download", key, lambda c: c.download_activity(activity_id),
            range_start=start_local[:10] or None, range_end=start_local[:10] or None,
            year=year, archive=False,
        )
        if result.status != "ok" or not isinstance(result.payload, (bytes, bytearray)):
            return
        data = bytes(result.payload)
        rel_zip = f"raw/activity-zips/{safe_component(year)}/{activity_id}.zip"
        stored = write_immutable(self.settings.data_dir, rel_zip, data)
        self.ledger.record_file(rel_zip, stored.sha256, stored.size, "activity_zip")
        try:
            fit_paths = self.extract_zip(activity_id, year, data)
        except zipfile.BadZipFile:
            # Not a ZIP.  If it is a bare FIT file keep a copy under raw/fit too.
            fit_paths = []
            if data[8:12] == b".FIT":
                rel_fit = f"raw/fit/{safe_component(year)}/{activity_id}/{activity_id}.fit"
                write_immutable(self.settings.data_dir, rel_fit, data)
                self.ledger.record_file(rel_fit, stored.sha256, stored.size, "fit")
                fit_paths = [rel_fit]
            else:
                log.warning("activity %s download is neither ZIP nor FIT; kept as %s",
                            activity_id, rel_zip)
        self.ledger.update_activity(activity_id, zip_path=rel_zip, zip_sha256=stored.sha256,
                                    fit_files=json.dumps(fit_paths))
        self.ledger.set_unit("activity_download", key, range_start=start_local[:10] or None,
                             range_end=start_local[:10] or None, state="ok",
                             raw_path=rel_zip, sha256=stored.sha256,
                             record_count=len(fit_paths), count_attempt=False)
        self.stats.zips += 1

    def extract_zip(self, activity_id: int, year: str, data: bytes) -> list[str]:
        """Copy every member of the downloaded ZIP into raw/fit/ (the ZIP itself
        is also kept).  Member names are reduced to their basename so a hostile
        archive cannot write outside the activity directory."""
        out: list[str] = []
        with zipfile.ZipFile(BytesIO(data)) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                name = safe_component(PurePosixPath(info.filename).name)
                rel = f"raw/fit/{safe_component(year)}/{activity_id}/{name}"
                content = archive.read(info)
                stored = write_immutable(self.settings.data_dir, rel, content)
                kind = "fit" if name.lower().endswith(".fit") else "activity_file"
                self.ledger.record_file(rel, sha256_bytes(content), len(content), kind)
                out.append(stored.rel_path)
        return out

    # --- orchestration ------------------------------------------------------
    def run(self, kind: str, *, backfill: bool, activities: bool = True,
            only: set[str] | None = None, budget_minutes: float | None = None,
            retry_now: bool = False) -> str:
        """One sync run.

        1. Profile/zone snapshots.
        2. Activities: new ones (and the full list until it has been walked once).
        3. The recent overlap window for every dated endpoint.
        4. If backfill: unresolved history, range endpoints first (they are cheap
           and establish the per-day floor), then per-day endpoints.
        """
        self.run_id = self.ledger.start_run(kind)
        self.stats = RunStats()
        if budget_minutes:
            self.deadline = time.monotonic() + budget_minutes * 60
        outcome = "completed"
        detail: dict[str, Any] = {}
        try:
            selected = [e for e in self.endpoints if only is None or e.name in only]
            dated = [e for e in selected if e.kind != "static"]
            for ep in selected:
                if ep.kind == "static":
                    self.sync_static_endpoint(ep)
            if activities and (only is None or "activities" in only):
                full = backfill and self.ledger.get_kv("activity_list_complete_at") is None
                self.sync_activities(full=full)
            self.sync_dated(dated, overlap=True, history=False)
            if backfill:
                self.sync_dated([e for e in dated if e.kind == "range"], overlap=False,
                                history=True, retry_now=retry_now)
                self.sync_dated([e for e in dated if e.kind == "day"], overlap=False,
                                history=True, retry_now=retry_now)
        except BudgetExceeded:
            outcome = "budget_exhausted"
        except RateLimited as exc:
            outcome = "rate_limited"
            detail["error"] = str(exc)
            log.error("Rate limited by Garmin; stopping this run: %s", exc)
        except AuthFailed as exc:
            outcome = "auth_failed"
            detail["error"] = str(exc)
            log.error("Authentication failed: %s", exc)
        except Exception as exc:  # recorded, then re-raised for a non-zero exit
            outcome = "error"
            detail["error"] = f"{type(exc).__name__}: {exc}"
            self.ledger.finish_run(self.run_id, outcome, {**detail, **self.stats.__dict__})
            raise
        if outcome == "completed" and self.stats.failed:
            outcome = "completed_with_failures"
        self.ledger.finish_run(self.run_id, outcome, {**detail, **self.stats.__dict__})
        log.info("%s run %s: %s %s", kind, self.run_id, outcome, json.dumps(self.stats.__dict__))
        return outcome

    def probe(self, probe_date: date) -> list[dict[str, Any]]:
        """Request every endpoint once for probe_date and record availability."""
        self.run_id = self.ledger.start_run("probe")
        report = []
        for ep in self.endpoints:
            if ep.kind == "range":
                start, end = probe_date - timedelta(days=6), probe_date
            else:
                start = end = probe_date
            key = f"probe_{start.isoformat()}_{end.isoformat()}"
            try:
                result = self.execute(
                    ep.name, key,
                    (lambda c: ep.call(c)) if ep.kind == "static"
                    else (lambda c, s=start, e=end: ep.call(c, s, e)),
                    range_start=start.isoformat(), range_end=end.isoformat(),
                    year=str(start.year),
                )
            except RateLimited as exc:
                report.append({"endpoint": ep.name, "status": "rate_limited", "error": str(exc)})
                break
            availability = {
                "ok": "available",
                "empty": "available_empty_on_probe_date",
                "not_found": "not_found_or_no_data",
                "forbidden": "not_accessible",
                "bad_request": "bad_request",
            }.get(result.status, "error")
            self.ledger.set_endpoint_status(ep.name, availability, result.http_status,
                                            result.error)
            report.append({
                "endpoint": ep.name, "kind": ep.kind, "status": result.status,
                "http_status": result.http_status, "availability": availability,
                "records": record_count(result.payload) if result.resolved else None,
                "top_level_keys": sorted(result.payload)[:25]
                if isinstance(result.payload, dict) else None,
                "error": (result.error or "")[:200] or None,
            })
        self.ledger.finish_run(self.run_id, "completed", {"endpoints": len(report)})
        return report


# ---------------------------------------------------------------------------
# CLI


def setup_logging(settings: Settings) -> None:
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(fmt)
    root.addHandler(stderr)
    logfile = logging.FileHandler(settings.log_dir / "sync.log")
    logfile.setFormatter(fmt)
    root.addHandler(logfile)
    # The official client logs each request path at INFO; keep it, it is useful
    # and contains no secrets.


def cmd_check(syncer: Syncer, download: bool) -> int:
    """Milestone 1 acceptance: real token, one activity, one health record."""
    syncer.run_id = syncer.ledger.start_run("check")
    results: dict[str, Any] = {}
    ok = True
    try:
        profile = syncer.execute("profile_settings", syncer.today.isoformat(),
                                 lambda c: c.get_user_profile_settings(),
                                 range_start=syncer.today.isoformat(),
                                 range_end=syncer.today.isoformat(), year=str(syncer.today.year))
        results["auth_and_profile"] = profile.status
        ok &= profile.status == "ok"
        page = syncer.execute("activity_search", "check_page_000000",
                              lambda c: c.search_activities(start=0, limit=1),
                              range_start=None, range_end=None, year=str(syncer.today.year))
        results["activity_search"] = page.status
        items = page.payload if isinstance(page.payload, list) else []
        if items:
            activity_id = activity_id_of(items[0])
            results["latest_activity_id"] = activity_id
            results["latest_activity_fields"] = sorted(items[0])[:40]
            if activity_id is not None:
                detail = syncer.execute("activity", str(activity_id),
                                        lambda c: c.get_activity(activity_id),
                                        range_start=None, range_end=None,
                                        year=str(items[0].get("startTimeLocal", "")[:4]))
                results["activity_detail"] = detail.status
                if download:
                    syncer.ledger.upsert_activity(activity_id, activity_fields(items[0]),
                                                  items[0])
                    syncer.download_activity(activity_id,
                                             str(items[0].get("startTimeLocal", "")[:4]),
                                             str(items[0].get("startTimeLocal", "")))
                    row = syncer.ledger.conn.execute(
                        "SELECT zip_path, fit_files FROM activities WHERE activity_id=?",
                        (activity_id,)).fetchone()
                    results["activity_download"] = dict(row) if row else None
        else:
            results["activity_search_note"] = "no activities on the account yet"
        day = syncer.today - timedelta(days=1)
        health = syncer.execute("daily_summaries", f"{day}_{day}",
                                lambda c: c.get_daily_summaries(day, day),
                                range_start=day.isoformat(), range_end=day.isoformat(),
                                year=str(day.year))
        results["health_record_daily_summaries"] = health.status
        ok &= health.resolved
    except (AuthFailed, RateLimited) as exc:
        results["error"] = str(exc)
        ok = False
    syncer.ledger.finish_run(syncer.run_id, "completed" if ok else "failed", results)
    print(json.dumps(results, indent=2, default=str))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.sync")
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="verify authentication and real data retrieval")
    check.add_argument("--download", action="store_true",
                       help="also download the newest activity's original file")
    probe = sub.add_parser("probe", help="request every endpoint once and record availability")
    probe.add_argument("--date", type=date.fromisoformat,
                       default=date.today() - timedelta(days=1))
    for name in ("backfill", "incremental"):
        p = sub.add_parser(name)
        p.add_argument("--endpoints", help="comma-separated endpoint names "
                       "(use 'activities' for the activity sync)")
        p.add_argument("--no-activities", action="store_true")
        p.add_argument("--budget-minutes", type=float)
        p.add_argument("--retry-now", action="store_true",
                       help="ignore retry back-off for previously failed ranges")
    sub.add_parser("process", help="parse FIT files and rebuild Parquet datasets")
    status = sub.add_parser("status", help="print the coverage report")
    status.add_argument("--json", action="store_true")
    sub.add_parser("retry-failed", help="make all failed units eligible for retry now")
    sub.add_parser("daemon", help="run incremental + backfill slices forever")
    args = parser.parse_args(argv)

    settings = load_settings()
    settings.ensure_dirs()
    setup_logging(settings)
    ledger = Ledger(settings.ledger_path)

    if args.command == "status":
        report = coverage.build_report(ledger, settings)
        print(json.dumps(report, indent=2) if args.json else coverage.format_report(report))
        return 0
    if args.command == "retry-failed":
        with ledger.tx() as c:
            n = c.execute("UPDATE units SET next_retry_at=NULL WHERE state NOT IN "
                          "('ok','empty','not_found')").rowcount
        print(f"{n} unit(s) eligible for retry")
        return 0
    if args.command == "process":
        return process(settings, ledger)

    api = GarminApi(settings)
    syncer = Syncer(settings, ledger, api)
    try:
        if args.command == "check":
            return cmd_check(syncer, args.download)
        if args.command == "probe":
            report = syncer.probe(args.date)
            print(json.dumps(report, indent=2))
            return 0
        if args.command in ("backfill", "incremental"):
            only = set(args.endpoints.split(",")) if args.endpoints else None
            outcome = syncer.run(args.command, backfill=args.command == "backfill",
                                 activities=not args.no_activities, only=only,
                                 budget_minutes=args.budget_minutes, retry_now=args.retry_now)
            process(settings, ledger)
            print(coverage.format_report(coverage.build_report(ledger, settings)))
            return 0 if outcome in ("completed", "budget_exhausted") else 2
        if args.command == "daemon":
            return daemon(settings, ledger, api)
    finally:
        api.close()
    return 0


def process(settings: Settings, ledger: Ledger) -> int:
    from . import fit_import, normalize

    fit_import.parse_pending(settings, ledger)
    normalize.rebuild(settings, ledger)
    return 0


def daemon(settings: Settings, ledger: Ledger, api: GarminApi) -> int:
    interval = settings.daemon_interval_minutes * 60
    while True:
        started = time.monotonic()
        syncer = Syncer(settings, ledger, api)
        try:
            syncer.run("scheduled", backfill=True,
                       budget_minutes=settings.backfill_budget_minutes)
            process(settings, ledger)
        except Exception:
            log.exception("sync cycle failed; will retry next cycle")
        remaining = interval - (time.monotonic() - started)
        log.info("next sync in %.0f minutes", max(0, remaining) / 60)
        time.sleep(max(60, remaining))


if __name__ == "__main__":
    sys.exit(main())
