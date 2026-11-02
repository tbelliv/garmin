"""Coverage accounting, computed only from the ledger.

A calendar day counts as covered for an endpoint when a request whose range
includes that day succeeded (HTTP 200 with data, HTTP 200 empty, or HTTP 404).
Nothing here trusts a process exit code or a stored "done" flag.

Availability labels used throughout the platform:
  retrieved            data retrieved for every expected day
  partial              some days retrieved, others pending or failed
  no_data_recorded     requests succeeded but every response was empty
  not_accessible       the API refused access (HTTP 403) or never returned data (404s only)
  not_investigated     never requested
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

from .db import FAILED_STATES, RESOLVED_STATES, Ledger
from .endpoints import ACTIVITY_ENDPOINTS, all_endpoints
from .settings import Settings


def record_settings(ledger: Ledger, settings: Settings) -> None:
    """Store the settings coverage depends on, so readers (the MCP container)
    report coverage against what the sync was actually configured to do."""
    ledger.set_kv("config:backfill_start", settings.backfill_start.isoformat())
    ledger.set_kv("config:overlap_days", str(settings.overlap_days))


def effective_settings(ledger: Ledger, settings: Settings) -> Settings:
    start = ledger.get_kv("config:backfill_start")
    overlap = ledger.get_kv("config:overlap_days")
    return replace(
        settings,
        backfill_start=date.fromisoformat(start) if start else settings.backfill_start,
        overlap_days=int(overlap) if overlap else settings.overlap_days,
    )


def _days(start: str, end: str) -> list[date]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [s + timedelta(days=i) for i in range((e - s).days + 1)]


def day_index(ledger: Ledger, endpoint: str) -> tuple[dict[date, str], dict[date, str]]:
    """Best state per day across all units (ok > empty > not_found > failed),
    and for unresolved days the time their retry back-off ends."""
    rank = {"ok": 5, "empty": 4, "not_found": 3}
    states: dict[date, str] = {}
    blocked_until: dict[date, str] = {}
    rows = ledger.conn.execute(
        "SELECT range_start, range_end, state, next_retry_at FROM units "
        "WHERE endpoint=? AND range_start IS NOT NULL AND range_end IS NOT NULL",
        (endpoint,),
    ).fetchall()
    for row in rows:
        try:
            days = _days(row["range_start"], row["range_end"])
        except ValueError:
            continue
        state = row["state"]
        for day in days:
            current = states.get(day)
            if current is None or rank.get(state, 0) > rank.get(current, 0):
                states[day] = state
            if state not in RESOLVED_STATES and row["next_retry_at"]:
                blocked_until[day] = max(blocked_until.get(day, ""), row["next_retry_at"])
    return states, blocked_until


def day_states(ledger: Ledger, endpoint: str) -> dict[date, str]:
    return day_index(ledger, endpoint)[0]


def unresolved_days(ledger: Ledger, endpoint: str, start: date, end: date, *,
                    include_blocked: bool = False) -> set[date]:
    """Days in [start, end] with no successful request.  Days whose last
    failure is still in retry back-off are excluded unless include_blocked."""
    states, blocked = day_index(ledger, endpoint)
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = set()
    day = start
    while day <= end:
        unresolved = states.get(day) not in RESOLVED_STATES
        if unresolved and (include_blocked or blocked.get(day, "") <= now):
            out.add(day)
        day += timedelta(days=1)
    return out


def _ranges(days: list[date], limit: int = 25) -> list[list[str]]:
    out: list[list[str]] = []
    for day in sorted(days):
        if out and date.fromisoformat(out[-1][1]) + timedelta(days=1) == day:
            out[-1][1] = day.isoformat()
        else:
            out.append([day.isoformat(), day.isoformat()])
    return out[:limit]


def endpoint_report(ledger: Ledger, settings: Settings, ep: Any, today: date,
                    statuses: dict[str, Any]) -> dict[str, Any]:
    states = day_states(ledger, ep.name)
    floor_kv = ledger.get_kv("day_floor")
    start = (settings.backfill_start if ep.backfill
             else today - timedelta(days=settings.overlap_days))
    floor = start
    if ep.kind == "day" and ep.backfill and floor_kv:
        floor = max(start, date.fromisoformat(floor_kv))
    counts = {"ok": 0, "empty": 0, "not_found": 0, "failed": 0, "pending": 0,
              "skipped_before_floor": 0}
    missing: list[date] = []
    failed_days: list[date] = []
    day = start
    while day <= today:
        state = states.get(day)
        if day < floor and state not in RESOLVED_STATES:
            counts["skipped_before_floor"] += 1
        elif state in RESOLVED_STATES:
            counts[state] += 1
        elif state in FAILED_STATES or state == "failed":
            counts["failed"] += 1
            failed_days.append(day)
        else:
            counts["pending"] += 1
            missing.append(day)
        day += timedelta(days=1)
    expected = (today - start).days + 1 if ep.kind != "static" else 0
    failed_units = [
        {
            "unit": row["unit_key"], "state": row["state"], "http_status": row["last_http_status"],
            "attempts": row["attempts"], "next_retry_at": row["next_retry_at"],
            "error": (row["last_error"] or "")[:200],
        }
        for row in ledger.conn.execute(
            "SELECT * FROM units WHERE endpoint=? AND state NOT IN ('ok','empty','not_found') "
            "ORDER BY range_start DESC LIMIT 20", (ep.name,))
    ]
    total_units = ledger.conn.execute(
        "SELECT COUNT(*) FROM units WHERE endpoint=?", (ep.name,)).fetchone()[0]
    status_row = statuses.get(ep.name)
    resolved = counts["ok"] + counts["empty"] + counts["not_found"]
    if ep.kind == "static":
        snaps = ledger.conn.execute(
            "SELECT COUNT(*), MAX(unit_key) FROM units WHERE endpoint=? AND state='ok'",
            (ep.name,)).fetchone()
        availability = ("retrieved" if snaps[0] else
                        "not_accessible" if status_row and status_row["availability"]
                        == "not_accessible" else
                        "not_investigated" if total_units == 0 else "partial")
        return {"endpoint": ep.name, "kind": ep.kind, "category": ep.category,
                "availability": availability, "snapshots": snaps[0], "latest_snapshot": snaps[1],
                "complete": bool(snaps[0]), "failed_units": failed_units}
    if total_units == 0:
        availability = "not_investigated"
    elif (status_row is not None and status_row["availability"] == "not_accessible") or (
            counts["ok"] == 0 and counts["empty"] == 0 and counts["not_found"] > 0):
        # 403, or nothing but 404s: the API does not serve this data to this account.
        availability = "not_accessible"
    elif counts["ok"] == 0 and counts["empty"] > 0:
        availability = "no_data_recorded"
    elif counts["pending"] or counts["failed"]:
        availability = "partial"
    elif resolved:
        availability = "retrieved"
    else:
        availability = "partial"
    return {
        "endpoint": ep.name,
        "kind": ep.kind,
        "category": ep.category,
        "builtin": ep.builtin,
        "in_backfill": ep.backfill,
        "http_path": ep.http_path,
        "availability": availability,
        "expected_range": [start.isoformat(), today.isoformat()],
        "requested_from": floor.isoformat(),
        "days_expected": expected,
        "days": counts,
        "complete": counts["pending"] == 0 and counts["failed"] == 0,
        "missing_ranges": _ranges(missing),
        "failed_ranges": _ranges(failed_days),
        "failed_units": failed_units,
    }


def activity_report(ledger: Ledger) -> dict[str, Any]:
    c = ledger.conn
    total = c.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
    span = c.execute("SELECT MIN(start_time_local), MAX(start_time_local) FROM activities"
                     ).fetchone()

    def resolved(endpoint: str) -> int:
        return c.execute(
            "SELECT COUNT(*) FROM units u JOIN activities a ON u.unit_key = "
            "CAST(a.activity_id AS TEXT) WHERE u.endpoint=? AND u.state IN "
            "('ok','empty','not_found')", (endpoint,)).fetchone()[0]

    failures = [dict(r) for r in c.execute(
        "SELECT endpoint, unit_key AS activity_id, state, last_http_status AS http_status, "
        "attempts, next_retry_at, substr(last_error, 1, 200) AS error FROM units "
        "WHERE endpoint IN ('activity','activity_workouts','activity_download') "
        "AND state NOT IN ('ok','empty','not_found') LIMIT 50")]
    with_zip = c.execute("SELECT COUNT(*) FROM activities WHERE zip_path IS NOT NULL"
                         ).fetchone()[0]
    parsed = c.execute("SELECT COUNT(*) FROM activities WHERE fit_parsed_version IS NOT NULL "
                       "AND fit_error IS NULL"
                       ).fetchone()[0]
    fit_errors = c.execute("SELECT COUNT(*) FROM activities WHERE fit_error IS NOT NULL"
                           ).fetchone()[0]
    list_complete = ledger.get_kv("activity_list_complete_at")
    details = resolved("activity")
    workouts = resolved("activity_workouts")
    complete = bool(list_complete) and total == details == workouts == with_zip
    return {
        "activities_indexed": total,
        "oldest": span[0], "newest": span[1],
        "full_list_walked_at": list_complete,
        "details_retrieved": details,
        "workouts_retrieved": workouts,
        "original_files_downloaded": with_zip,
        "fit_parsed": parsed,
        "fit_parse_errors": fit_errors,
        "complete": complete,
        "failures": failures,
        "http_paths": ACTIVITY_ENDPOINTS,
    }


def build_report(ledger: Ledger, settings: Settings, today: date | None = None) -> dict[str, Any]:
    today = today or date.today()
    settings = effective_settings(ledger, settings)
    statuses = ledger.endpoint_statuses()
    endpoints = [endpoint_report(ledger, settings, ep, today, statuses)
                 for ep in all_endpoints(settings.extra_endpoints_file)]
    activities = activity_report(ledger)
    blocking = []
    for rep in endpoints:
        if rep.get("in_backfill", True) and not rep["complete"]:
            if rep["availability"] == "not_accessible":
                continue
            blocking.append(f"{rep['endpoint']}: {rep.get('days', {})}")
    if not activities["complete"]:
        blocking.append("activities: not every activity has details, workouts and original file")
    inaccessible = [r["endpoint"] for r in endpoints if r["availability"] == "not_accessible"]
    runs = [dict(r) for r in ledger.conn.execute(
        "SELECT run_id, kind, started_at, finished_at, outcome FROM runs "
        "ORDER BY run_id DESC LIMIT 10")]
    attempts = ledger.conn.execute(
        "SELECT status, COUNT(*) AS n FROM attempts GROUP BY status").fetchall()
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "backfill_start": settings.backfill_start.isoformat(),
        "day_endpoint_floor": ledger.get_kv("day_floor"),
        "backfill_complete": not blocking,
        "blocking": blocking,
        "inaccessible_endpoints": inaccessible,
        "activities": activities,
        "endpoints": endpoints,
        "request_totals": {r["status"]: r["n"] for r in attempts},
        "recent_runs": runs,
        "notes": [
            "A day is covered when a request including it succeeded; an empty response "
            "means Garmin returned no data for that day (not worn, not recorded, or not "
            "supported), never zero.",
            "skipped_before_floor days precede the earliest evidence of account data and "
            "were not requested; they are not counted as complete.",
        ],
    }


def format_report(report: dict[str, Any]) -> str:
    lines = [
        f"Coverage report {report['generated_at']}",
        f"Backfill start {report['backfill_start']}; per-day floor "
        f"{report['day_endpoint_floor'] or 'not yet determined'}",
        f"BACKFILL COMPLETE: {'yes' if report['backfill_complete'] else 'no'}",
        "",
        f"{'endpoint':30} {'availability':18} {'ok':>6} {'empty':>6} {'404':>5} "
        f"{'failed':>6} {'pending':>7} {'skipped':>7}",
    ]
    for rep in report["endpoints"]:
        d = rep.get("days")
        if d is None:
            lines.append(f"{rep['endpoint']:30} {rep['availability']:18} "
                         f"snapshots={rep.get('snapshots')}")
            continue
        lines.append(
            f"{rep['endpoint']:30} {rep['availability']:18} {d['ok']:>6} {d['empty']:>6} "
            f"{d['not_found']:>5} {d['failed']:>6} {d['pending']:>7} "
            f"{d['skipped_before_floor']:>7}"
        )
    a = report["activities"]
    lines += [
        "",
        f"Activities: {a['activities_indexed']} indexed ({a['oldest']} .. {a['newest']}); "
        f"details {a['details_retrieved']}, workouts {a['workouts_retrieved']}, "
        f"original files {a['original_files_downloaded']}, FIT parsed {a['fit_parsed']}, "
        f"parse errors {a['fit_parse_errors']}; full list walked: "
        f"{a['full_list_walked_at'] or 'no'}",
    ]
    if report["blocking"]:
        lines += ["", "Not complete because:"] + [f"  - {b}" for b in report["blocking"]]
    if report["inaccessible_endpoints"]:
        lines += ["", "Not accessible through the API: "
                  + ", ".join(report["inaccessible_endpoints"])]
    return "\n".join(lines)
