"""Read-only MCP server over the Garmin archive (stdio transport).

Run inside the garmin-mcp container, which mounts the archive read-only, has
no network and no Garmin credentials:
    python -m app.mcp_server

All logging goes to stderr; stdout carries only MCP protocol messages.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from functools import wraps
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from .analytics import METRIC_ALIASES, Archive, QueryError
from .settings import load_settings

INSTRUCTIONS = """\
Personal Garmin archive (Forerunner and any imported COROS history), served read-only.

Ground rules for using these tools:
- Never treat a missing value as zero. Tools return null plus a status explaining why
  (no data recorded, not accessible via the API, failed request, not synced yet).
- Distinguish Garmin-reported metrics from values derived here; derived results carry a
  'method' note. Do not apply generic altitude corrections to VO2 max.
- The athlete trains around Boulder, Colorado (~1650 m). Compare pace/HR only within the
  same altitude band and note temperature and elevation gain.
- Start with data_coverage when unsure whether something exists. Use list_fields to find
  exact field paths, and run_sql for anything the other tools do not cover.
- Cite the dates, activity keys and values behind every recommendation and state uncertainty.
"""

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True,
                            open_world_hint=False)

log = logging.getLogger("garmin.mcp")


def build_server(archive: Archive | None = None) -> MCPServer:
    archive = archive or Archive(load_settings())
    server = MCPServer("garmin", instructions=INSTRUCTIONS)

    def tool(fn: Callable[..., dict[str, Any]] | None = None, *, extra_doc: str = ""
             ) -> Any:
        if fn is None:
            return lambda f: tool(f, extra_doc=extra_doc)

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
            try:
                return fn(*args, **kwargs)
            except QueryError as exc:
                return {"error": str(exc)}
            except Exception as exc:
                log.exception("tool %s failed", fn.__name__)
                return {"error": f"{type(exc).__name__}: {exc}"}

        description = " ".join((fn.__doc__ or "").split()) + extra_doc
        return server.tool(annotations=READ_ONLY, description=description)(wrapper)

    @tool
    def list_activities(start_date: str | None = None, end_date: str | None = None,
                        activity_type: str | None = None, limit: int = 100) -> dict[str, Any]:
        """List activities, newest first. Dates are YYYY-MM-DD (local start date).
        activity_type matches partially, e.g. 'running' also matches trail_running."""
        return archive.list_activities(start_date, end_date, activity_type, limit)

    @tool
    def search_activities(start_date: str | None = None, end_date: str | None = None,
                          activity_type: str | None = None, name_contains: str | None = None,
                          min_distance_km: float | None = None,
                          max_distance_km: float | None = None,
                          min_elevation_gain_m: float | None = None,
                          min_avg_hr: float | None = None, max_avg_hr: float | None = None,
                          min_altitude_m: float | None = None,
                          max_altitude_m: float | None = None, source: str | None = None,
                          limit: int = 100) -> dict[str, Any]:
        """Find activities by criteria. Altitude filters use the median altitude of the
        activity's FIT records; source is 'garmin' or an import source such as 'coros'."""
        return archive.search_activities(
            start_date=start_date, end_date=end_date, activity_type=activity_type,
            name_contains=name_contains, min_distance_km=min_distance_km,
            max_distance_km=max_distance_km, min_elevation_gain_m=min_elevation_gain_m,
            min_avg_hr=min_avg_hr, max_avg_hr=max_avg_hr, min_altitude_m=min_altitude_m,
            max_altitude_m=max_altitude_m, source=source, limit=limit)

    @tool
    def get_activity(activity_key: str) -> dict[str, Any]:
        """Full metadata for one activity: index row, Garmin summary and detail JSON,
        workouts, archived file paths and FIT-derived context (altitude, temperature)."""
        return archive.get_activity(activity_key)

    @tool
    def get_activity_stream(activity_key: str, fields: list[str] | None = None,
                            max_points: int = 2000) -> dict[str, Any]:
        """Parsed FIT time series (about one record per second), downsampled evenly to at
        most max_points rows. Returns available_fields so you can request others (e.g.
        running dynamics or developer fields)."""
        return archive.get_activity_stream(activity_key, fields, max_points)

    @tool
    def get_laps(activity_key: str) -> dict[str, Any]:
        """Lap/interval messages from the FIT file plus Garmin's workout data."""
        return archive.get_laps(activity_key)

    @tool
    def get_health_data(start_date: str, end_date: str, endpoints: list[str] | None = None,
                        include_timeseries: bool = False) -> dict[str, Any]:
        """Daily health and recovery records (sleep, HRV, resting HR, stress, Body Battery,
        steps, respiration, SpO2/acclimation, weight...) for up to 93 days, with a per-day
        status explaining any gaps. Long intraday arrays are omitted unless
        include_timeseries is true."""
        return archive.get_health_data(start_date, end_date, endpoints, include_timeseries)

    @tool
    def get_training_metrics(start_date: str | None = None,
                             end_date: str | None = None) -> dict[str, Any]:
        """Search everything retrieved for VO2 max, training status/load/readiness, recovery
        time, acclimation, race predictions, training effect, lactate threshold and more,
        reporting where each was found and recent values, or that it was not found."""
        return archive.get_training_metrics(start_date, end_date)

    @tool(extra_doc=" Named metrics: " + ", ".join(sorted(METRIC_ALIASES)) + ".")
    def get_metrics_history(metric: str, start_date: str, end_date: str,
                            period: str = "day") -> dict[str, Any]:
        """Trend of one metric. metric is a named metric or 'endpoint:field_path' from
        list_fields. period is day, week or month."""
        return archive.get_metrics_history(metric, start_date, end_date, period)

    @tool
    def compare_periods(period_a_start: str, period_a_end: str, period_b_start: str,
                        period_b_end: str, activity_type: str | None = "running",
                        hr_low: float | None = None, hr_high: float | None = None
                        ) -> dict[str, Any]:
        """Compare two training blocks: volume, elevation, pace, HR, altitude bands, pace at a
        heart-rate range (default: Garmin zone 2) by altitude band, and recovery means."""
        return archive.compare_periods(period_a_start, period_a_end, period_b_start,
                                       period_b_end, activity_type, hr_low, hr_high)

    @tool
    def weekly_report_data(week_start: str | None = None,
                           activity_type: str | None = None) -> dict[str, Any]:
        """Numbers for a weekly training report (defaults to the last complete Monday-Sunday
        week): activities, totals vs prior 4 weeks, intensity distribution, pace at zone 2 by
        altitude band, recovery trends, load indicators and data gaps."""
        return archive.weekly_report_data(week_start, activity_type)

    @tool
    def data_coverage() -> dict[str, Any]:
        """What has been retrieved, what is missing and why, per endpoint and for activities;
        whether the historical backfill is complete; which training metrics were found."""
        return archive.data_coverage()

    @tool
    def list_fields(search: str | None = None, endpoint: str | None = None,
                    limit: int = 200) -> dict[str, Any]:
        """Field inventory: every JSON field path seen per endpoint (with counts, example and
        date span) and every FIT column. Use it to find exact names for other tools."""
        return archive.list_fields(search, endpoint, limit)

    @tool
    def run_sql(sql: str, max_rows: int = 500) -> dict[str, Any]:
        """Run one read-only DuckDB SELECT. Views: activities, fit_records, fit_laps,
        fit_sessions, fit_messages, fit_summary, json_records (payload JSON per record, use
        is_latest), json_fields (path/value_num/value_text per scalar), field_inventory.
        Prefer filtering fit_records by activity_key; it spans every activity."""
        return archive.run_sql(sql, max_rows)

    return server


def main() -> int:
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    build_server().run("stdio")
    return 0


if __name__ == "__main__":
    sys.exit(main())
