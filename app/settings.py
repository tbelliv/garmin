"""Runtime configuration, read from environment variables.

Every path has a container default matching compose.yaml, so the same code runs
in Docker and in tests (tests point DATA_DIR at a temporary directory).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value.strip()


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    api_key_file: Path
    token_cache_file: Path
    extra_endpoints_file: Path
    backfill_start: date
    day_endpoint_start: str  # "auto" or YYYY-MM-DD
    overlap_days: int
    range_chunk_days: int
    activity_page_size: int
    min_request_interval: float
    max_retries: int
    request_timeout: float
    daemon_interval_minutes: int
    backfill_budget_minutes: int
    timezone: str

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def api_dir(self) -> Path:
        return self.raw_dir / "api"

    @property
    def zip_dir(self) -> Path:
        return self.raw_dir / "activity-zips"

    @property
    def fit_dir(self) -> Path:
        return self.raw_dir / "fit"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    @property
    def db_dir(self) -> Path:
        return self.data_dir / "db"

    @property
    def ledger_path(self) -> Path:
        return self.db_dir / "ledger.sqlite"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    def ensure_dirs(self) -> None:
        for path in (
            self.api_dir,
            self.zip_dir,
            self.fit_dir,
            self.parquet_dir,
            self.db_dir,
            self.log_dir,
            self.data_dir / "state",
        ):
            path.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(_env("DATA_DIR", "/data")),
        api_key_file=Path(_env("GARMIN_API_KEY_FILE", "/run/secrets/garmin_api_key")),
        token_cache_file=Path(_env("GARMIN_TOKEN_CACHE_FILE", "/state/bearer-token.json")),
        extra_endpoints_file=Path(_env("EXTRA_ENDPOINTS_FILE", "/config/extra-endpoints.json")),
        backfill_start=date.fromisoformat(_env("BACKFILL_START", "2015-01-01")),
        day_endpoint_start=_env("DAY_ENDPOINT_START", "auto"),
        overlap_days=int(_env("OVERLAP_DAYS", "7")),
        range_chunk_days=int(_env("RANGE_CHUNK_DAYS", "28")),
        activity_page_size=int(_env("ACTIVITY_PAGE_SIZE", "50")),
        min_request_interval=float(_env("MIN_REQUEST_INTERVAL_SECONDS", "1.0")),
        max_retries=int(_env("MAX_RETRIES", "5")),
        request_timeout=float(_env("REQUEST_TIMEOUT_SECONDS", "60")),
        daemon_interval_minutes=int(_env("SYNC_INTERVAL_MINUTES", "180")),
        backfill_budget_minutes=int(_env("BACKFILL_BUDGET_MINUTES", "120")),
        timezone=_env("TZ", "America/Denver"),
    )
