from __future__ import annotations

from datetime import date

import pytest

from app.db import Ledger
from app.garmin_client import GarminApi
from app.settings import load_settings
from app.sync import Syncer
from tests.fakes import TODAY, FakeClient, http_error, make_fit, zip_bytes

__all__ = ["TODAY", "FakeClient", "Harness", "http_error", "make_fit", "zip_bytes"]


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GARMIN_API_KEY_FILE", str(tmp_path / "key"))
    monkeypatch.setenv("GARMIN_TOKEN_CACHE_FILE", str(tmp_path / "token.json"))
    monkeypatch.setenv("EXTRA_ENDPOINTS_FILE", str(tmp_path / "extra.json"))
    monkeypatch.setenv("BACKFILL_START", "2026-05-01")
    monkeypatch.setenv("MIN_REQUEST_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("MAX_RETRIES", "2")
    monkeypatch.setenv("OVERLAP_DAYS", "3")
    monkeypatch.setenv("RANGE_CHUNK_DAYS", "28")
    monkeypatch.setenv("ACTIVITY_PAGE_SIZE", "2")
    settings = load_settings()
    settings.ensure_dirs()
    return settings


class Harness:
    def __init__(self, settings, client: FakeClient) -> None:
        self.settings = settings
        self.client = client
        self.ledger = Ledger(settings.ledger_path)
        self.ledger.conn.execute("PRAGMA synchronous=OFF")  # tests only
        self.tokens: list[bool] = []
        self.api = GarminApi(settings, client_factory=lambda token: client,
                             token_provider=self._token, sleep=lambda s: None)

    def _token(self, force: bool) -> str:
        self.tokens.append(force)
        return f"token-{len(self.tokens)}"

    def syncer(self, today: date = TODAY) -> Syncer:
        return Syncer(self.settings, self.ledger, self.api, today=today)


@pytest.fixture
def harness(env):
    return Harness(env, FakeClient())
