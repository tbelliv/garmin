"""End-to-end through Garmin's official client against the local fake API."""

from __future__ import annotations

import json
import os
import stat
from datetime import date, timedelta

import pytest

from app import coverage
from app.db import Ledger
from app.settings import load_settings
from app.sync import main as sync_main
from tests.fake_garmin_server import FakeGarmin
from tests.fakes import FakeClient


@pytest.fixture
def live(env, monkeypatch, tmp_path):
    today = date.today()
    fake = FakeGarmin("test-key", FakeClient(activities=3, first_day=today - timedelta(days=40)),
                      rate_limit_every=37)
    server = fake.serve()
    monkeypatch.setenv("GARMIN_PERSONAL_API_BASE_URL",
                       f"http://127.0.0.1:{server.server_address[1]}")
    monkeypatch.setenv("BACKFILL_START", (today - timedelta(days=60)).isoformat())
    key = tmp_path / "key"
    key.write_text("test-key\n")
    key.chmod(0o600)
    yield fake, load_settings()
    server.shutdown()


def test_check_backfill_and_token_cache(live, capsys):
    fake, settings = live
    assert sync_main(["check", "--download"]) == 0
    check = json.loads(capsys.readouterr().out)
    assert check["auth_and_profile"] == "ok"
    assert check["activity_download"]["zip_path"].endswith(".zip")
    assert fake.token_requests == 1
    # The bearer token is cached with private permissions and reused next run.
    assert stat.S_IMODE(os.stat(settings.token_cache_file).st_mode) == 0o600
    assert sync_main(["backfill"]) == 0
    assert fake.token_requests == 1
    ledger = Ledger(settings.ledger_path)
    report = coverage.build_report(ledger, settings)
    assert report["backfill_complete"], report["blocking"]
    # Injected 429s were retried after Retry-After and are visible in the ledger.
    assert report["request_totals"].get("ok", 0) > 50
    assert fake.requests > report["request_totals"]["ok"]
    assert ledger.conn.execute("SELECT COUNT(*) FROM activities WHERE fit_parsed_version "
                               "IS NOT NULL AND fit_error IS NULL").fetchone()[0] == 3


def test_revoked_token_is_replaced(live):
    fake, settings = live
    assert sync_main(["incremental", "--no-activities"]) == 0
    fake.tokens.clear()  # Garmin revoked every token
    assert sync_main(["incremental", "--no-activities"]) == 0
    assert fake.token_requests == 2


def test_wrong_api_key_fails_cleanly(live, tmp_path):
    (tmp_path / "key").write_text("wrong")
    assert sync_main(["check"]) == 1
