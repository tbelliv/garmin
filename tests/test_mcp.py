from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

from app.analytics import Archive, QueryError
from app.mcp_server import build_server
from app.sync import process
from tests.conftest import FakeClient, Harness

EXPECTED_TOOLS = {
    "list_activities", "get_activity", "get_activity_stream", "get_laps", "get_health_data",
    "get_training_metrics", "get_metrics_history", "compare_periods", "search_activities",
    "data_coverage", "list_fields", "run_sql", "weekly_report_data",
}


@pytest.fixture(scope="module")
def synced(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("mcp")
    mp = pytest.MonkeyPatch()
    for key, value in {
        "DATA_DIR": str(tmp / "data"), "GARMIN_API_KEY_FILE": str(tmp / "key"),
        "GARMIN_TOKEN_CACHE_FILE": str(tmp / "token.json"),
        "EXTRA_ENDPOINTS_FILE": str(tmp / "extra.json"), "BACKFILL_START": "2026-09-01",
        "MIN_REQUEST_INTERVAL_SECONDS": "0", "MAX_RETRIES": "1", "OVERLAP_DAYS": "3",
        "ACTIVITY_PAGE_SIZE": "2",
    }.items():
        mp.setenv(key, value)
    from app.settings import load_settings

    settings = load_settings()
    settings.ensure_dirs()
    harness = Harness(settings, FakeClient(activities=5, first_day=__import__("datetime").date(
        2026, 9, 1)))
    harness.syncer().run("backfill", backfill=True)
    process(settings, harness.ledger)
    yield settings
    mp.undo()


@pytest.fixture
def archive(synced):
    return Archive(synced)


def call(server, name, args=None):
    result = asyncio.run(server.call_tool(name, args or {}))
    return json.loads(result.content[0].text)


def test_tools_registered_read_only(archive):
    server = build_server(archive)
    tools = asyncio.run(server.list_tools())
    assert {t.name for t in tools} == EXPECTED_TOOLS
    for t in tools:
        assert t.annotations.read_only_hint is True
        assert t.description
    history = next(t for t in tools if t.name == "get_metrics_history")
    assert "resting_hr" in history.description


def test_activity_tools(archive):
    server = build_server(archive)
    listed = call(server, "list_activities", {"activity_type": "running"})
    assert listed["count"] == 5
    first = listed["activities"][0]
    assert first["altitude_band"].startswith("moderate")
    assert first["avg_pace"]["per_km"]
    key = first["activity_key"]
    detail = call(server, "get_activity", {"activity_key": key})
    assert detail["garmin_detail"]["summaryDTO"]["trainingEffect"] == 3.1
    assert detail["files"]["original_zip"].endswith(f"{key}.zip")
    stream = call(server, "get_activity_stream", {"activity_key": key, "max_points": 20})
    assert stream["total_records"] == 60 and len(stream["rows"]) <= 20
    assert "heart_rate" in stream["fields"] and stream["units"]["heart_rate"] == "bpm"
    laps = call(server, "get_laps", {"activity_key": key})
    assert laps["fit_laps"][0]["total_distance"] == pytest.approx(192.0)
    found = call(server, "search_activities", {"min_altitude_m": 1600, "max_altitude_m": 1700})
    assert found["count"] == 5
    assert call(server, "get_activity", {"activity_key": "nope"})["error"]


def test_health_and_metric_tools(archive):
    server = build_server(archive)
    health = call(server, "get_health_data", {"start_date": "2026-10-28",
                                              "end_date": "2026-10-30"})
    day = health["days"]["2026-10-29"]
    assert day["sleep"][0]["data"]["dailySleepDTO"]["sleepTimeSeconds"] == 27000
    assert health["day_status_by_endpoint"]["daily_hydration"] == {
        "empty": ["2026-10-28", "2026-10-29", "2026-10-30"]}
    hist = call(server, "get_metrics_history", {"metric": "resting_hr",
                                                "start_date": "2026-10-01",
                                                "end_date": "2026-10-31"})
    assert hist["source_endpoint"] == "daily_summaries" and len(hist["points"]) == 31
    assert all(p["value"] > 0 for p in hist["points"])
    weekly = call(server, "get_metrics_history", {"metric": "sleep_score", "period": "week",
                                                  "start_date": "2026-10-01",
                                                  "end_date": "2026-10-31"})
    assert weekly["points"][0]["value"] == 82
    missing = call(server, "get_metrics_history", {"metric": "weight_g",
                                                   "start_date": "2026-10-01",
                                                   "end_date": "2026-10-31"})
    assert missing["status"] == "not_found_in_retrieved_data" and missing["points"] == []
    metrics = call(server, "get_training_metrics")["metrics"]
    assert metrics["vo2_max"]["status"] == "found_in_retrieved_data"
    assert metrics["training_effect"]["status"] == "found_in_retrieved_data"
    assert metrics["race_predictions"]["status"] == "not_found_in_retrieved_data"


def test_coverage_compare_weekly_and_fields(archive):
    server = build_server(archive)
    cov = call(server, "data_coverage")
    assert cov["report"]["backfill_complete"] is True
    assert cov["investigated_metrics"]["vo2_max"] == "found_in_retrieved_data"
    cmp_ = call(server, "compare_periods", {
        "period_a_start": "2026-10-01", "period_a_end": "2026-10-15",
        "period_b_start": "2026-10-16", "period_b_end": "2026-10-31"})
    assert cmp_["hr_zones"]["zones"] == [100, 120, 140, 160, 175]
    b = cmp_["period_b"]
    assert b["training"]["activities"] >= 1
    assert b["pace_at_hr"]["hr_range_bpm"] == [120, 139]
    assert b["health_means"]["resting_hr"]["mean"] > 0
    report = call(server, "weekly_report_data", {"week_start": "2026-10-26"})
    assert report["week"] == ["2026-10-26", "2026-11-01"]
    zones = report["intensity_distribution"]["zones"]
    assert zones["zone3"]["seconds"] > 0  # synthetic HR is 140-149 bpm
    assert report["load_indicators"]["garmin_activity_training_load"]["sum"] > 0
    fields = call(server, "list_fields", {"search": "rest"})
    assert any(f["path"] == "restingHeartRate" for f in fields["api_fields"])


def test_run_sql_is_read_only(archive):
    server = build_server(archive)
    ok = call(server, "run_sql", {"sql": "SELECT COUNT(*) AS n FROM activities"})
    assert ok["rows"] == [[5]]
    for bad in ["COPY (SELECT 1) TO 'x.parquet'", "SELECT 1; SELECT 2",
                "SET enable_external_access=true", "INSTALL httpfs",
                "SELECT * FROM read_csv('/etc/passwd')", "ATTACH '/tmp/x.db'"]:
        res = call(server, "run_sql", {"sql": bad})
        assert "error" in res, bad
    with pytest.raises(QueryError):
        archive.run_sql("DELETE FROM activities")


def test_stdio_server_end_to_end(synced):
    """Spawn the real server process and talk MCP over stdio, as Claude Code will."""
    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    env = dict(os.environ, DATA_DIR=str(synced.data_dir),
               EXTRA_ENDPOINTS_FILE=str(synced.extra_endpoints_file))
    params = StdioServerParameters(command=sys.executable, args=["-m", "app.mcp_server"],
                                   env=env, cwd=str(Path(__file__).resolve().parents[1]))

    async def run():
        async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            result = await session.call_tool("list_activities", {"limit": 2})
            return {t.name for t in tools.tools}, json.loads(result.content[0].text)

    names, listed = asyncio.run(run())
    assert names == EXPECTED_TOOLS
    assert listed["count"] == 2
