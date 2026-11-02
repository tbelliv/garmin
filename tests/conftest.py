from __future__ import annotations

import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from garmin_fit_sdk import Encoder, Profile
from garmin_personal_api import HttpError

from app.db import Ledger
from app.garmin_client import GarminApi
from app.settings import load_settings
from app.sync import Syncer

TODAY = date(2026, 11, 2)


def make_fit(start: datetime, *, seconds: int = 60, altitude: float = 1650.0) -> bytes:
    enc = Encoder()
    mesg = Profile["mesg_num"]
    enc.on_mesg(mesg["FILE_ID"], {"type": "activity", "manufacturer": "garmin", "product": 1,
                                  "time_created": start, "serial_number": 1})
    for i in range(seconds):
        enc.on_mesg(mesg["RECORD"], {
            "timestamp": start + timedelta(seconds=i), "heart_rate": 140 + i % 10,
            "enhanced_altitude": altitude + i * 0.5, "enhanced_speed": 3.2,
            "position_lat": 477560000 + i, "position_long": -1258000000, "cadence": 88,
            "power": 250, "temperature": 12, "vertical_oscillation": 85.0,
        })
    enc.on_mesg(mesg["LAP"], {"timestamp": start + timedelta(seconds=seconds),
                              "start_time": start, "total_elapsed_time": float(seconds),
                              "total_distance": 3.2 * seconds})
    enc.on_mesg(mesg["SESSION"], {"timestamp": start + timedelta(seconds=seconds),
                                  "start_time": start, "sport": "running",
                                  "total_elapsed_time": float(seconds),
                                  "total_timer_time": float(seconds),
                                  "total_distance": 3.2 * seconds, "total_ascent": 30,
                                  "avg_heart_rate": 145, "max_heart_rate": 149})
    return enc.close()


def zip_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


def http_error(code: int, text: str = "") -> HttpError:
    return HttpError(status_code=code, method="GET", url="https://example/x", response_text=text)


class FakeClient:
    """Mimics GarminPersonalApiClient wrappers with synthetic data.

    failures: {(method, key): [exc, exc, ...]} raised in order before succeeding.
    key is the first date argument as ISO string, an activity id, or None.
    """

    def __init__(self, *, activities: int = 3, first_day: date = date(2026, 6, 1)) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.failures: dict[tuple[str, Any], list[Exception]] = {}
        self.always_fail: dict[str, Exception] = {}
        self.fail_if: dict[str, Any] = {}  # method -> callable(key) -> Exception | None
        self.max_range_days: int | None = None
        self.first_day = first_day
        self.token = None
        self.version = 1
        self.activities = [
            {"activityId": 1000 + i, "activityName": f"Run {i}",
             "startTimeLocal": (datetime(2026, 10, 30, 7) - timedelta(days=3 * i)).strftime(
                 "%Y-%m-%d %H:%M:%S"),
             "activityType": {"typeKey": "running"}, "distance": 10000.0 + i,
             "duration": 3000.0, "averageHR": 150, "maxHR": 170, "elevationGain": 120,
             "vO2MaxValue": 55}
            for i in range(activities)
        ]

    def update_bearer_token(self, token: str) -> None:
        self.token = token

    def close(self) -> None:
        pass

    def _hit(self, method: str, key: Any) -> None:
        self.calls.append((method, key))
        if method in self.always_fail:
            raise self.always_fail[method]
        if method in self.fail_if:
            exc = self.fail_if[method](key)
            if exc is not None:
                raise exc
        queue = self.failures.get((method, key))
        if queue:
            raise queue.pop(0)

    def _range(self, method: str, start: date, end: date, builder) -> Any:
        self._hit(method, start.isoformat())
        if self.max_range_days and (end - start).days + 1 > self.max_range_days:
            raise http_error(400, "range too long")
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        return [builder(d) for d in days if d >= self.first_day]

    # range wrappers
    def get_daily_summaries(self, s, e):
        items = self._range("get_daily_summaries", s, e, lambda d: {
            "calendarDate": d.isoformat(), "restingHeartRate": 48 + d.day % 3,
            "totalSteps": 10000 + self.version, "averageStressLevel": 25})
        return {"data": items}

    def get_daily_stress(self, s, e):
        return self._range("get_daily_stress", s, e, lambda d: {"calendarDate": d.isoformat(),
                                                                 "values": {"x": 1}})

    def get_daily_steps(self, s, e):
        return self._range("get_daily_steps", s, e, lambda d: {"calendarDate": d.isoformat(),
                                                                "totalSteps": 9000})

    def get_daily_hydration(self, s, e):
        return self._range("get_daily_hydration", s, e, lambda d: {})[:0]

    def get_daily_sleep_summaries(self, s, e):
        return self._range("get_daily_sleep_summaries", s, e,
                           lambda d: {"calendarDate": d.isoformat(), "sleepTimeSeconds": 27000})

    def get_daily_body_battery(self, s, e, *, chunks=None):
        return self._range("get_daily_body_battery", s, e,
                           lambda d: {"date": d.isoformat(), "charged": 60})

    def get_daily_hrv_summaries(self, s, e):
        items = self._range("get_daily_hrv_summaries", s, e, lambda d: {
            "calendarDate": d.isoformat(), "lastNightAvg": 70, "status": "BALANCED"})
        return {"hrvSummaries": items, "userProfilePk": 1}

    def get_weight_measurements(self, s, e):
        return self._range("get_weight_measurements", s, e, lambda d: {})[:0]

    def get_effective_weight_goals(self, s, e):
        return self._range("get_effective_weight_goals", s, e, lambda d: {})[:0]

    def get_wellness_activity_summaries(self, s, e):
        return self._range("get_wellness_activity_summaries", s, e, lambda d: {})[:0]

    # day wrappers
    def _day(self, method: str, d: date, payload: dict) -> Any:
        self._hit(method, d.isoformat())
        return payload if d >= self.first_day else None

    def get_daily_sleep(self, d):
        return self._day("get_daily_sleep", d, {"dailySleepDTO": {
            "calendarDate": d.isoformat(), "sleepTimeSeconds": 27000,
            "sleepScores": {"overall": {"value": 82}}}, "sleepMovement": [[1, 2], [3, 4]]})

    def get_daily_heart_rate(self, d):
        return self._day("get_daily_heart_rate", d, {
            "calendarDate": d.isoformat(), "restingHeartRate": 49,
            "heartRateValues": [[1, 60], [2, 61]]})

    def get_daily_hrv(self, d):
        return self._day("get_daily_hrv", d, {"hrvSummary": {"calendarDate": d.isoformat(),
                                                             "lastNightAvg": 70}})

    def get_daily_spo2(self, d):
        return self._day("get_daily_spo2", d, {"calendarDate": d.isoformat(),
                                               "averageSpO2": 94, "altitudeAcclimation": 40})

    def get_fitness_age(self, d):
        return self._day("get_fitness_age", d, {"fitnessAge": 30})

    def get_daily_food_logs(self, d):
        return self._day("get_daily_food_logs", d, {})

    def get_menstrual_cycle_summary(self, d):
        self._hit("get_menstrual_cycle_summary", d.isoformat())
        raise http_error(404)

    def get_menstrual_cycle_day_view(self, d, today):
        self._hit("get_menstrual_cycle_day_view", d.isoformat())
        raise http_error(404)

    # static
    def get_user_profile_settings(self):
        self._hit("get_user_profile_settings", None)
        return {"userData": {"vo2MaxRunning": 55}}

    def get_heart_rate_zones(self):
        self._hit("get_heart_rate_zones", None)
        return [{"sport": "DEFAULT", "zone1Floor": 100, "zone2Floor": 120, "zone3Floor": 140,
                 "zone4Floor": 160, "zone5Floor": 175, "maxHeartRateUsed": 190}]

    # activities
    def search_activities(self, *, start=0, limit=20, **kwargs):
        self._hit("search_activities", start)
        return self.activities[start:start + limit]

    def get_activity(self, activity_id):
        self._hit("get_activity", activity_id)
        base = next(a for a in self.activities if a["activityId"] == activity_id)
        return {"activityId": activity_id, "activityName": base["activityName"],
                "summaryDTO": {"avgPower": 260, "trainingEffect": 3.1,
                               "activityTrainingLoad": 120}}

    def get_activity_workouts(self, activity_id):
        self._hit("get_activity_workouts", activity_id)
        return []

    def download_activity(self, activity_id):
        self._hit("download_activity", activity_id)
        base = next(a for a in self.activities if a["activityId"] == activity_id)
        start = datetime.strptime(base["startTimeLocal"], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=UTC)
        return zip_bytes({f"{activity_id}_ACTIVITY.fit": make_fit(start)})

    def get(self, path, params=None):
        self._hit("get", path)
        return {"path": path, "params": params}


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
