"""A local stand-in for the Garmin Personal API, for end-to-end tests only.

It serves the same paths the official client calls (see garmin_personal_api
client.py) with synthetic data from tests.fakes.FakeClient, issues JWT bearer
tokens with an exp claim, enforces bearer auth, and can inject HTTP 429 with
Retry-After.  Point the official client at it with
GARMIN_PERSONAL_API_BASE_URL=http://host:port.

    python -m tests.fake_garmin_server --port 8765 --api-key test-key
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import threading
import time
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from garmin_personal_api import HttpError

from tests.fakes import FakeClient

D = r"(\d{4}-\d{2}-\d{2})"
ROUTES: list[tuple[re.Pattern[str], Any]] = [(re.compile(p + "$"), f) for p, f in [
    (r"/usersummary-service/v2/dailysummaries",
     lambda c, m, q: c.get_daily_summaries(_d(q["startDate"]), _d(q["endDate"]))),
    (rf"/usersummary-service/stats/stress/daily/{D}/{D}",
     lambda c, m, q: c.get_daily_stress(_d(m[1]), _d(m[2]))),
    (rf"/usersummary-service/stats/steps/daily/{D}/{D}",
     lambda c, m, q: c.get_daily_steps(_d(m[1]), _d(m[2]))),
    (rf"/usersummary-service/stats/hydration/daily/{D}/{D}",
     lambda c, m, q: c.get_daily_hydration(_d(m[1]), _d(m[2]))),
    (r"/sleep-service/sleep/dailySleepSummariesByDate",
     lambda c, m, q: c.get_daily_sleep_summaries(_d(q["startDate"]), _d(q["endDate"]))),
    (r"/wellness-service/wellness/bodyBattery/reports/daily",
     lambda c, m, q: c.get_daily_body_battery(_d(q["startDate"]), _d(q["endDate"]))),
    (rf"/hrv-service/hrv/daily/{D}/{D}",
     lambda c, m, q: c.get_daily_hrv_summaries(_d(m[1]), _d(m[2]))),
    (rf"/weight-service/weight/range/{D}/{D}",
     lambda c, m, q: c.get_weight_measurements(_d(m[1]), _d(m[2]))),
    (rf"/goal-service/goal/user/effective/weightgoal/{D}/{D}",
     lambda c, m, q: c.get_effective_weight_goals(_d(m[1]), _d(m[2]))),
    (rf"/wellnessactivity-service/activity/summary/range/{D}/{D}",
     lambda c, m, q: c.get_wellness_activity_summaries(_d(m[1]), _d(m[2]))),
    (r"/sleep-service/sleep/dailySleep", lambda c, m, q: c.get_daily_sleep(_d(q["date"]))),
    (r"/wellness-service/wellness/dailyHeartRate",
     lambda c, m, q: c.get_daily_heart_rate(_d(q["date"]))),
    (rf"/hrv-service/hrv/{D}", lambda c, m, q: c.get_daily_hrv(_d(m[1]))),
    (rf"/wellness-service/wellness/daily/spo2acclimation/{D}",
     lambda c, m, q: c.get_daily_spo2(_d(m[1]))),
    (rf"/fitnessage-service/fitnessage/v2/{D}", lambda c, m, q: c.get_fitness_age(_d(m[1]))),
    (rf"/nutrition-service/food/logs/{D}", lambda c, m, q: c.get_daily_food_logs(_d(m[1]))),
    (rf"/periodichealth-service/menstrualcycle/summary/{D}",
     lambda c, m, q: c.get_menstrual_cycle_summary(_d(m[1]))),
    (rf"/periodichealth-service/menstrualcycle/dayview/{D}",
     lambda c, m, q: c.get_menstrual_cycle_day_view(_d(m[1]), None)),
    (r"/userprofile-service/userprofile/settings",
     lambda c, m, q: c.get_user_profile_settings()),
    (r"/biometric-service/heartRateZones/v2", lambda c, m, q: c.get_heart_rate_zones()),
    (r"/activitylist-service/activities/search/activities",
     lambda c, m, q: c.search_activities(start=int(q.get("start", 0)),
                                         limit=int(q.get("limit", 20)))),
    (r"/activity-service/activity/(\d+)/workouts",
     lambda c, m, q: c.get_activity_workouts(int(m[1]))),
    (r"/activity-service/activity/(\d+)", lambda c, m, q: c.get_activity(int(m[1]))),
    (r"/download-service/files/activity/(\d+)",
     lambda c, m, q: c.download_activity(int(m[1]))),
]]
TOKEN_PATH = "/personalapitoken-service/personal-api/personal-api-key/token"


def _d(value: str) -> date:
    return date.fromisoformat(value)


def make_jwt(exp: float) -> str:
    def b64(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{b64({'alg': 'none'})}.{b64({'exp': int(exp), 'n': time.time_ns()})}.sig"


class FakeGarmin:
    def __init__(self, api_key: str = "test-key", client: FakeClient | None = None,
                 rate_limit_every: int = 0) -> None:
        self.api_key = api_key
        self.client = client or FakeClient()
        self.tokens: set[str] = set()
        self.rate_limit_every = rate_limit_every
        self.requests = 0
        self.token_requests = 0
        self.lock = threading.Lock()

    def handler(self) -> type[BaseHTTPRequestHandler]:
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def _send(self, code: int, body: bytes, ctype: str = "application/json",
                      headers: dict[str, str] | None = None) -> None:
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                if urlparse(self.path).path != TOKEN_PATH:
                    return self._send(404, b"")
                if self.headers.get("Authorization") != f"ApiKey {fake.api_key}":
                    return self._send(401, b'{"error":"bad api key"}')
                token = make_jwt(time.time() + 24 * 3600)
                with fake.lock:
                    fake.tokens.add(token)
                    fake.token_requests += 1
                self._send(200, token.encode(), "text/plain")

            def do_GET(self) -> None:
                url = urlparse(self.path)
                auth = self.headers.get("Authorization", "")
                if not auth.startswith("Bearer ") or auth[7:] not in fake.tokens:
                    return self._send(401, b'{"error":"invalid token"}')
                with fake.lock:
                    fake.requests += 1
                    limited = fake.rate_limit_every and fake.requests % fake.rate_limit_every == 0
                if limited:
                    return self._send(429, b'{"error":"slow down"}',
                                      headers={"Retry-After": "0"})
                query = {k: v[0] for k, v in parse_qs(url.query).items()}
                for pattern, fn in ROUTES:
                    match = pattern.match(url.path)
                    if match:
                        try:
                            with fake.lock:
                                result = fn(fake.client, match, query)
                        except HttpError as exc:
                            return self._send(exc.status_code, exc.response_text.encode())
                        if isinstance(result, bytes):
                            return self._send(200, result, "application/zip")
                        if result is None:
                            return self._send(200, b"")
                        return self._send(200, json.dumps(result).encode())
                self._send(404, b'{"error":"no such endpoint"}')

        return Handler

    def serve(self, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
        server = ThreadingHTTPServer((host, port), self.handler())
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--api-key", default="test-key")
    parser.add_argument("--activities", type=int, default=4)
    args = parser.parse_args()
    fake = FakeGarmin(args.api_key, FakeClient(activities=args.activities))
    server = ThreadingHTTPServer((args.host, args.port), fake.handler())
    print(f"fake Garmin API on {args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
