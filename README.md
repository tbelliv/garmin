# Garmin personal data platform (work in progress)

Status: **unfinished, not deployed, not tested against the real Garmin API.**
Do not install this on a server yet.

Built on Garmin's official client (`garmin-connect-personal-api-python` 1.0.0)
and Garmin's official FIT SDK (`garmin-fit-sdk`).

Written so far (in `app/`):

- `endpoints.py` lists the data sets, using the official client's wrappers.
- `garmin_client.py` adds pacing, retries, Retry-After handling, token refresh and error classification.
- `db.py` is the SQLite ledger of runs, request attempts, per-range coverage, activities and file hashes.
- `archive.py` writes raw responses immutably and keeps every changed version.
- `sync.py` handles resumable day-based backfill, the recent-overlap window, activities, original file download and probing.
- `coverage.py` reports coverage from the ledger only.
- `fit_import.py` turns FIT files into Parquet and imports COROS FIT files.
- `normalize.py` builds lossless JSON-to-Parquet datasets and a field inventory.

Not written yet: the MCP server, Docker Compose and Dockerfile, the backup and
restore scripts, and the coaching skill.

Known failing tests, out of 14:

- `test_rate_limit_stops_run_and_failures_stay_visible`
- `test_activity_files_are_preserved_and_parsed`
- `test_day_floor_skips_days_before_account_data`

The tests are also slow because the ledger commits each write separately.
