"""Thin layer over Garmin's official Personal API client.

Adds what unattended ingestion needs and the official client deliberately
leaves out: request pacing, retry with exponential backoff, honoring
Retry-After on HTTP 429, one automatic token refresh on HTTP 401, and a
classification of every failure so the ledger can tell "no data" apart from
"endpoint not accessible" and "temporary error".

The API key is read only here, from a file mounted solely into the sync
container.  It is never logged.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests
from garmin_personal_api import (
    GarminPersonalApiClient,
    GarminPersonalApiError,
    HttpError,
    PersonalApiAuthenticator,
    resolve_api_key,
)

from .settings import Settings

log = logging.getLogger(__name__)

RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}


@dataclass
class CallResult:
    # ok | empty | not_found | forbidden | bad_request | auth_error | rate_limited | failed
    status: str
    payload: Any = None
    http_status: int | None = None
    error: str | None = None
    retry_after: float | None = None
    duration_ms: int = 0

    @property
    def resolved(self) -> bool:
        return self.status in ("ok", "empty", "not_found")


def is_empty_payload(payload: Any) -> bool:
    return payload is None or (isinstance(payload, (list, dict, str)) and len(payload) == 0)


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, min(3600.0, float(value)))
    except ValueError:
        return None


class GarminApi:
    def __init__(
        self,
        settings: Settings,
        *,
        client_factory: Callable[[str], Any] | None = None,
        token_provider: Callable[[bool], str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self._sleep = sleep
        self._clock = clock
        self._last_request = 0.0
        self._token_provider = token_provider or self._default_token_provider
        self._client_factory = client_factory or self._default_client_factory
        self._client: Any = None
        self.requests_made = 0
        # The official HttpError does not expose headers, so a response hook on
        # the client's session records Retry-After for 429/503 responses.
        self.last_retry_after: float | None = None

    def _capture_headers(self, response: requests.Response, *args: Any, **kwargs: Any) -> None:
        self.last_retry_after = parse_retry_after(response.headers.get("Retry-After"))

    def _default_client_factory(self, token: str) -> Any:
        session = requests.Session()
        session.hooks["response"].append(self._capture_headers)
        return GarminPersonalApiClient(
            token, timeout=self.settings.request_timeout, session=session
        )

    # --- auth -------------------------------------------------------------
    def _default_token_provider(self, force_refresh: bool) -> str:
        api_key = resolve_api_key(key_file=self.settings.api_key_file)
        with PersonalApiAuthenticator(
            timeout=self.settings.request_timeout,
            bearer_token_cache_file=self.settings.token_cache_file,
        ) as authenticator:
            return authenticator.obtain_token(api_key, force_refresh=force_refresh).access_token

    def client(self, *, force_refresh: bool = False) -> Any:
        if self._client is None or force_refresh:
            token = self._token_provider(force_refresh)
            if self._client is None:
                self._client = self._client_factory(token)
            else:
                self._client.update_bearer_token(token)
        return self._client

    def close(self) -> None:
        if self._client is not None and hasattr(self._client, "close"):
            self._client.close()

    # --- pacing -----------------------------------------------------------
    def _pace(self) -> None:
        wait = self.settings.min_request_interval - (self._clock() - self._last_request)
        if wait > 0:
            self._sleep(wait)
        self._last_request = self._clock()

    def _backoff(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return retry_after
        base = min(300.0, 2.0 * (2 ** attempt))
        return base + random.uniform(0, base / 4)

    # --- calls ------------------------------------------------------------
    def call(self, fn: Callable[[Any], Any]) -> CallResult:
        """Run fn(client) with pacing, retries and failure classification."""
        refreshed = False
        attempt = 0
        while True:
            self._pace()
            started = time.monotonic()
            self.requests_made += 1
            self.last_retry_after = None
            try:
                payload = fn(self.client())
                elapsed = int((time.monotonic() - started) * 1000)
                status = "empty" if is_empty_payload(payload) else "ok"
                return CallResult(status, payload=payload, duration_ms=elapsed)
            except HttpError as exc:
                elapsed = int((time.monotonic() - started) * 1000)
                code = exc.status_code
                text = (exc.response_text or "").strip()[:500]
                if code == 401 and not refreshed:
                    log.warning("HTTP 401; obtaining a fresh bearer token and retrying once")
                    self.client(force_refresh=True)
                    refreshed = True
                    continue
                if code in RETRYABLE_HTTP and attempt < self.settings.max_retries:
                    delay = self._backoff(attempt, self.last_retry_after)
                    log.warning("HTTP %s; retry %d/%d in %.0fs", code, attempt + 1,
                                self.settings.max_retries, delay)
                    self._sleep(delay)
                    attempt += 1
                    continue
                status = {
                    401: "auth_error",
                    403: "forbidden",
                    404: "not_found",
                    400: "bad_request",
                    422: "bad_request",
                }.get(code, "failed")
                if code == 429:
                    status = "rate_limited"
                return CallResult(status, http_status=code, error=text or str(exc),
                                  retry_after=self.last_retry_after, duration_ms=elapsed)
            except (requests.RequestException, ValueError) as exc:
                # Network failures and invalid JSON (requests raises ValueError subclasses).
                elapsed = int((time.monotonic() - started) * 1000)
                if attempt < self.settings.max_retries:
                    delay = self._backoff(attempt, None)
                    log.warning("%s; retry %d/%d in %.0fs", type(exc).__name__, attempt + 1,
                                self.settings.max_retries, delay)
                    self._sleep(delay)
                    attempt += 1
                    continue
                return CallResult("failed", error=f"{type(exc).__name__}: {exc}",
                                  duration_ms=elapsed)
            except GarminPersonalApiError as exc:
                elapsed = int((time.monotonic() - started) * 1000)
                return CallResult("failed", error=str(exc), duration_ms=elapsed)
