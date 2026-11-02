"""Catalog of Garmin Personal API data sets this platform archives.

Every built-in entry calls a wrapper on the official ``GarminPersonalApiClient``
(garmin-connect-personal-api-python 1.0.0), so request paths come from Garmin's
own client rather than from this project.  The HTTP path is listed for
reference and for the coverage report only.

Endpoint kinds:
  range   - takes (start_date, end_date); requested in chunks.
  day     - takes one calendar date; requested once per day.
  static  - takes no date; snapshotted on every sync.

Additional endpoints from Garmin's Swagger documentation can be added without
code changes through config/extra-endpoints.json (see ExtraEndpoint).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

Kind = Literal["range", "day", "static"]


@dataclass(frozen=True)
class Endpoint:
    name: str
    kind: Kind
    category: str
    http_path: str
    wrapper: str | None = None  # method name on GarminPersonalApiClient
    path_template: str | None = None  # for extra endpoints called through client.get()
    params: dict[str, str] = field(default_factory=dict)
    description: str = ""
    backfill: bool = True  # include in historical backfill
    builtin: bool = True

    def call(self, client: Any, start: date | None = None, end: date | None = None) -> Any:
        if self.wrapper is not None:
            method = getattr(client, self.wrapper)
            if self.kind == "range":
                return method(start, end)
            if self.kind == "day":
                if self.name == "menstrual_cycle_day_view":
                    return method(start, date.today())
                return method(start)
            return method()
        assert self.path_template is not None
        values = {
            "date": start.isoformat() if start else "",
            "start_date": start.isoformat() if start else "",
            "end_date": end.isoformat() if end else "",
        }
        path = self.path_template.format(**values)
        params = {key: value.format(**values) for key, value in self.params.items()}
        return client.get(path, params=params or None)


BUILTIN_ENDPOINTS: tuple[Endpoint, ...] = (
    # --- range endpoints -------------------------------------------------
    Endpoint("daily_summaries", "range", "wellness",
             "/usersummary-service/v2/dailysummaries", "get_daily_summaries",
             description="Daily user summary: steps, calories, resting HR, stress, "
                         "Body Battery, intensity minutes and more."),
    Endpoint("daily_stress", "range", "wellness",
             "/usersummary-service/stats/stress/daily/{start}/{end}", "get_daily_stress"),
    Endpoint("daily_steps", "range", "wellness",
             "/usersummary-service/stats/steps/daily/{start}/{end}", "get_daily_steps"),
    Endpoint("daily_hydration", "range", "wellness",
             "/usersummary-service/stats/hydration/daily/{start}/{end}", "get_daily_hydration"),
    Endpoint("sleep_summaries", "range", "sleep",
             "/sleep-service/sleep/dailySleepSummariesByDate", "get_daily_sleep_summaries"),
    Endpoint("body_battery", "range", "recovery",
             "/wellness-service/wellness/bodyBattery/reports/daily", "get_daily_body_battery"),
    Endpoint("hrv_summaries", "range", "recovery",
             "/hrv-service/hrv/daily/{start}/{end}", "get_daily_hrv_summaries"),
    Endpoint("weight_measurements", "range", "body",
             "/weight-service/weight/range/{start}/{end}?includeAll=true",
             "get_weight_measurements"),
    Endpoint("effective_weight_goals", "range", "body",
             "/goal-service/goal/user/effective/weightgoal/{start}/{end}",
             "get_effective_weight_goals"),
    Endpoint("wellness_activity_summaries", "range", "wellness",
             "/wellnessactivity-service/activity/summary/range/{start}/{end}",
             "get_wellness_activity_summaries"),
    # --- per-day endpoints ------------------------------------------------
    Endpoint("sleep", "day", "sleep",
             "/sleep-service/sleep/dailySleep?date={date}", "get_daily_sleep",
             description="Detailed nightly sleep: stages, scores, movement, SpO2, respiration."),
    Endpoint("heart_rate", "day", "wellness",
             "/wellness-service/wellness/dailyHeartRate?date={date}", "get_daily_heart_rate",
             description="All-day heart rate time series and resting HR."),
    Endpoint("hrv", "day", "recovery",
             "/hrv-service/hrv/{date}", "get_daily_hrv",
             description="Overnight HRV summary and individual readings."),
    Endpoint("spo2_acclimation", "day", "recovery",
             "/wellness-service/wellness/daily/spo2acclimation/{date}", "get_daily_spo2",
             description="Pulse Ox and altitude acclimation data."),
    Endpoint("fitness_age", "day", "performance",
             "/fitnessage-service/fitnessage/v2/{date}", "get_fitness_age"),
    Endpoint("food_logs", "day", "nutrition",
             "/nutrition-service/food/logs/{date}", "get_daily_food_logs"),
    Endpoint("menstrual_cycle_summary", "day", "health",
             "/periodichealth-service/menstrualcycle/summary/{date}",
             "get_menstrual_cycle_summary", backfill=False),
    Endpoint("menstrual_cycle_day_view", "day", "health",
             "/periodichealth-service/menstrualcycle/dayview/{date}?todayDate={today}",
             "get_menstrual_cycle_day_view", backfill=False),
    # --- snapshots --------------------------------------------------------
    Endpoint("profile_settings", "static", "profile",
             "/userprofile-service/userprofile/settings", "get_user_profile_settings"),
    Endpoint("heart_rate_zones", "static", "profile",
             "/biometric-service/heartRateZones/v2", "get_heart_rate_zones"),
)

# Activity endpoints are handled by the activity sync rather than the date grid.
ACTIVITY_ENDPOINTS = {
    "activity_search": "/activitylist-service/activities/search/activities",
    "activity": "/activity-service/activity/{activity_id}",
    "activity_workouts": "/activity-service/activity/{activity_id}/workouts",
    "activity_download": "/download-service/files/activity/{activity_id}",
}

# Metrics the handoff spec asks us to investigate.  None of these has a
# dedicated wrapper in the official client 1.0.0.  They may appear as fields
# inside daily summaries, activity details or FIT files; the field inventory
# reports where (if anywhere) each one was found.
INVESTIGATE_METRICS = {
    "vo2_max": ["vo2max", "vo2maxvalue", "vo2_max", "maxmet"],
    "training_status": ["trainingstatus", "training_status"],
    "training_load": ["trainingload", "activitytrainingload", "training_load",
                      "acuteload", "chronicload"],
    "training_readiness": ["trainingreadiness", "readiness"],
    "recovery_time": ["recoverytime", "recovery_time", "timetorecovery"],
    "acclimation": ["acclimation", "heatacclimation", "altitudeacclimation"],
    "race_predictions": ["racepredict", "racetime", "predictedtime"],
    "training_effect": ["trainingeffect", "aerobictrainingeffect", "anaerobictrainingeffect",
                        "total_training_effect"],
    "lactate_threshold": ["lactatethreshold", "lactate_threshold", "thresholdheartrate"],
    "running_power": ["power", "avgpower", "normpower"],
    "endurance_score": ["endurancescore"],
    "hill_score": ["hillscore"],
}


def load_extra_endpoints(path: Path) -> list[Endpoint]:
    """Load user-defined endpoints copied from Garmin's Swagger documentation.

    File format (JSON list):
      [{"name": "my_endpoint", "kind": "day", "category": "performance",
        "path": "/some-service/thing/{date}", "params": {"x": "{date}"},
        "backfill": false}]

    Placeholders: {date}, {start_date}, {end_date}.  Entries default to
    backfill=false so a new endpoint is probed before it is bulk-requested.
    """
    if not path.exists():
        return []
    entries = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise ValueError(f"{path} must contain a JSON list")
    builtin_names = {e.name for e in BUILTIN_ENDPOINTS}
    result = []
    for entry in entries:
        name = entry["name"]
        if name in builtin_names:
            raise ValueError(f"extra endpoint {name!r} collides with a built-in endpoint")
        kind = entry.get("kind", "day")
        if kind not in ("range", "day", "static"):
            raise ValueError(f"extra endpoint {name!r} has invalid kind {kind!r}")
        result.append(
            Endpoint(
                name=name,
                kind=kind,
                category=entry.get("category", "extra"),
                http_path=entry["path"],
                path_template=entry["path"],
                params=dict(entry.get("params", {})),
                description=entry.get("description", ""),
                backfill=bool(entry.get("backfill", False)),
                builtin=False,
            )
        )
    return result


def all_endpoints(extra_file: Path | None = None) -> list[Endpoint]:
    endpoints = list(BUILTIN_ENDPOINTS)
    if extra_file is not None:
        endpoints.extend(load_extra_endpoints(extra_file))
    return endpoints
