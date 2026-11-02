from __future__ import annotations

import json
from datetime import date, timedelta

import pyarrow.parquet as pq

from app import coverage
from app.sync import chunk_days_desc, process
from tests.conftest import TODAY, FakeClient, Harness, http_error


def all_files(settings):
    return sorted(p for p in settings.data_dir.rglob("*") if p.is_file())


def test_chunking_newest_first():
    days = [date(2026, 1, d) for d in range(1, 11)] + [date(2026, 1, 20)]
    assert chunk_days_desc(days, 4) == [
        (date(2026, 1, 20), date(2026, 1, 20)),
        (date(2026, 1, 7), date(2026, 1, 10)),
        (date(2026, 1, 3), date(2026, 1, 6)),
        (date(2026, 1, 1), date(2026, 1, 2)),
    ]


def test_full_backfill_reaches_complete_coverage(harness: Harness):
    outcome = harness.syncer().run("backfill", backfill=True)
    assert outcome == "completed"
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert report["backfill_complete"], report["blocking"]
    by_name = {r["endpoint"]: r for r in report["endpoints"]}
    assert by_name["daily_summaries"]["availability"] == "retrieved"
    assert by_name["daily_hydration"]["availability"] == "no_data_recorded"
    # Menstrual endpoints are not backfilled and return 404 for the recent window.
    assert by_name["menstrual_cycle_summary"]["availability"] == "not_accessible"
    # Per-day endpoints start at the earliest daily_summaries chunk with data.
    assert report["day_endpoint_floor"] is not None
    assert by_name["sleep"]["days"]["skipped_before_floor"] >= 0
    acts = report["activities"]
    assert acts["activities_indexed"] == 3 and acts["complete"]
    assert acts["original_files_downloaded"] == 3


def test_second_run_only_refetches_overlap_window(harness: Harness):
    harness.syncer().run("backfill", backfill=True)
    before = len(harness.client.calls)
    harness.syncer().run("backfill", backfill=True)
    second = harness.client.calls[before:]
    day_calls = [c for c in second if c[0] == "get_daily_sleep"]
    # Overlap is OVERLAP_DAYS=3 -> today and 3 previous days.
    assert sorted(c[1] for c in day_calls) == [
        (TODAY - timedelta(days=i)).isoformat() for i in range(3, -1, -1)]
    assert not [c for c in second if c[0] == "download_activity"]


def test_crash_mid_backfill_resumes_without_rerequesting(harness: Harness):
    # Simulate the process dying part-way through the per-day history pass.
    harness.client.failures[("get_daily_sleep", "2026-09-15")] = [RuntimeError("killed")]
    try:
        harness.syncer().run("backfill", backfill=True)
    except RuntimeError:
        pass
    run = harness.ledger.conn.execute("SELECT outcome FROM runs ORDER BY run_id DESC").fetchone()
    assert run["outcome"] == "error"
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert not report["backfill_complete"]
    done_summaries = {c for c in harness.client.calls if c[0] == "get_daily_summaries"}
    sleep_before = {c for c in harness.client.calls if c[0] == "get_daily_sleep"}
    harness.client.calls.clear()
    harness.syncer().run("backfill", backfill=True)
    again = {c for c in harness.client.calls if c[0] == "get_daily_summaries"}
    # Only the overlap window is re-requested for an already-complete endpoint.
    assert len(again) <= 1 and len(done_summaries) > 1
    sleep_after = {c for c in harness.client.calls if c[0] == "get_daily_sleep"}
    overlap = {("get_daily_sleep", (TODAY - timedelta(days=i)).isoformat()) for i in range(4)}
    assert not (sleep_after - overlap) & (sleep_before - {("get_daily_sleep", "2026-09-15")})
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert report["backfill_complete"], report["blocking"]


def test_range_too_long_is_split_and_learned(harness: Harness):
    harness.client.max_range_days = 7
    harness.syncer().run("backfill", backfill=True)
    assert harness.ledger.get_kv("max_span:daily_summaries") is not None
    span = int(harness.ledger.get_kv("max_span:daily_summaries"))
    assert span <= 7
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert report["backfill_complete"], report["blocking"]


def test_forbidden_endpoint_is_skipped_and_reported(harness: Harness):
    harness.client.always_fail["get_daily_hrv"] = http_error(403, "not permitted")
    harness.syncer().run("backfill", backfill=True)
    hrv_calls = [c for c in harness.client.calls if c[0] == "get_daily_hrv"]
    assert len(hrv_calls) == 1  # skipped after the first 403
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    by_name = {r["endpoint"]: r for r in report["endpoints"]}
    assert by_name["hrv"]["availability"] == "not_accessible"
    assert "hrv" in report["inaccessible_endpoints"]
    # An inaccessible endpoint is listed separately; it does not hide other gaps.
    assert report["backfill_complete"]


def test_rate_limit_stops_run_and_failures_stay_visible(harness: Harness):
    harness.client.fail_if["get_daily_steps"] = (
        lambda key: http_error(429, "slow down") if key < "2026-10-01" else None)
    outcome = harness.syncer().run("backfill", backfill=True)
    assert outcome == "rate_limited"
    steps = [c for c in harness.client.calls if c[0] == "get_daily_steps"]
    failing = [c for c in steps if c[1] < "2026-10-01"]
    # The first rate-limited range is retried MAX_RETRIES times, then the run stops.
    assert len(failing) == 1 + harness.settings.max_retries
    assert len(set(failing)) == 1
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert not report["backfill_complete"]
    rep = {r["endpoint"]: r for r in report["endpoints"]}["daily_steps"]
    assert rep["days"]["failed"] > 0 and rep["failed_units"]
    # Within the back-off window the failed range is not retried...
    del harness.client.fail_if["get_daily_steps"]
    harness.client.calls.clear()
    harness.syncer().run("backfill", backfill=True)
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert not report["backfill_complete"]
    # ...unless explicitly requested.
    harness.syncer().run("backfill", backfill=True, retry_now=True)
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    assert report["backfill_complete"], report["blocking"]


def test_transient_errors_are_retried(harness: Harness):
    key = (TODAY - timedelta(days=1)).isoformat()
    harness.client.failures[("get_daily_sleep", key)] = [http_error(503), http_error(502)]
    harness.syncer().run("incremental", backfill=False)
    unit = harness.ledger.get_unit("sleep", f"{key}_{key}")
    assert unit["state"] == "ok" and unit["attempts"] == 1
    statuses = [r["status"] for r in harness.ledger.conn.execute(
        "SELECT status FROM attempts WHERE endpoint='sleep' AND unit_key=?", (f"{key}_{key}",))]
    assert statuses == ["ok"]


def test_expired_token_is_refreshed_once(harness: Harness):
    harness.client.failures[("get_user_profile_settings", None)] = [http_error(401)]
    harness.syncer().run("incremental", backfill=False, activities=False)
    assert harness.tokens == [False, True]
    assert harness.client.token == "token-2"


def test_raw_archive_is_immutable_and_versioned(harness: Harness):
    harness.syncer().run("incremental", backfill=False, activities=False)
    files_before = set(all_files(harness.settings))
    harness.syncer().run("incremental", backfill=False, activities=False)
    # Identical responses are not written again.
    new = set(all_files(harness.settings)) - files_before
    assert not [p for p in new if "/raw/" in str(p)]
    harness.client.version = 2  # Garmin revised the daily summaries
    harness.syncer().run("incremental", backfill=False, activities=False)
    new = set(all_files(harness.settings)) - files_before
    revised = [p for p in new if "/raw/api/daily_summaries/" in str(p)]
    assert revised and all(p.exists() for p in files_before)
    for path in all_files(harness.settings):
        if "/raw/" in str(path):
            assert (path.stat().st_mode & 0o222) == 0, path


def test_activity_files_are_preserved_and_parsed(harness: Harness):
    harness.syncer().run("backfill", backfill=True)
    process(harness.settings, harness.ledger)
    row = harness.ledger.conn.execute(
        "SELECT * FROM activities WHERE activity_id=1000").fetchone()
    assert row["zip_path"].startswith("raw/activity-zips/2026/")
    fit_files = json.loads(row["fit_files"])
    assert fit_files and (harness.settings.data_dir / fit_files[0]).exists()
    assert row["fit_error"] is None and row["fit_parsed_version"]
    assert row["avg_power_w"] == 260  # filled in from activity details
    records = pq.read_table(harness.settings.parquet_dir / "fit" / "records" /
                            "source=garmin" / "1000.parquet").to_pylist()
    assert len(records) == 60
    first = records[0]
    assert first["heart_rate"] == 140 and first["power"] == 250
    assert abs(first["position_lat_deg"] - 477560000 * 180 / 2**31) < 1e-9
    assert first["source"] == "garmin" and first["parser_version"]
    acts = pq.read_table(harness.settings.parquet_dir / "activities.parquet").to_pylist()
    a = next(x for x in acts if x["activity_key"] == "1000")
    assert 1650 <= a["altitude_median_m"] <= 1700


def test_zip_member_names_cannot_escape(harness: Harness):
    from tests.conftest import zip_bytes

    syncer = harness.syncer()
    paths = syncer.extract_zip(5, "2026", zip_bytes({"../../evil.fit": b"x",
                                                     "/abs/also.fit": b"y"}))
    for rel in paths:
        resolved = (harness.settings.data_dir / rel).resolve()
        assert str(resolved).startswith(str(harness.settings.fit_dir.resolve()))


def test_day_floor_skips_days_before_account_data(env):
    harness = Harness(env, FakeClient(first_day=date(2026, 10, 1)))
    harness.syncer().run("backfill", backfill=True)
    sleep_days = {c[1] for c in harness.client.calls if c[0] == "get_daily_sleep"}
    assert min(sleep_days) >= "2026-09-01"  # floor is the start of a 28-day chunk
    report = coverage.build_report(harness.ledger, harness.settings, today=TODAY)
    rep = {r["endpoint"]: r for r in report["endpoints"]}["sleep"]
    assert rep["days"]["skipped_before_floor"] > 0
    assert report["backfill_complete"]


def test_extra_endpoints_are_probed(env):
    (env.extra_endpoints_file).write_text(json.dumps([
        {"name": "training_status_candidate", "kind": "day",
         "path": "/metrics-service/trainingstatus/{date}"}]))
    harness = Harness(env, FakeClient())
    report = harness.syncer().probe(TODAY - timedelta(days=1))
    entry = next(r for r in report if r["endpoint"] == "training_status_candidate")
    assert entry["availability"] == "available"
    assert ("get", "/metrics-service/trainingstatus/2026-11-01") in harness.client.calls
