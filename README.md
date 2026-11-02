# Garmin personal data platform

A self-hosted archive of everything the **Garmin Connect Personal API** gives you,
built on Garmin's official tools, with a read-only MCP server so Claude Code can
analyse your training securely from your laptop.

Status: **implemented and tested locally against a fake Garmin API. Not yet run
against the real Garmin API and not deployed.** Start with
[docs/SETUP.md](docs/SETUP.md) when the Forerunner 970 arrives.

## What it does

- **Archives all accessible history.** It requests every day since `BACKFILL_START` for each data
  set, plus every activity's details, workouts and original FIT/ZIP download.
- **Never loses or rewrites source data.** Raw responses and files are written once and made
  read-only. A revised response is stored as a new version next to the old one.
- **Is resumable, honest backfill.** A ledger records every request. Coverage is computed per
  endpoint and per day, never from a "done" flag. Failures stay visible and are retried with
  back-off.
- **Is analytics-ready.** FIT files are decoded with Garmin's official FIT SDK into Parquet
  time series. All JSON is normalized losslessly into DuckDB-queryable Parquet with provenance.
- **Gives Claude read-only access.** 13 MCP tools run in a container with no network, no
  credentials and a read-only archive, reached over SSH through Tailscale.
- **Supports altitude-aware coaching.** Activities carry FIT-derived altitude and temperature
  context. A Claude Code skill writes evidence-based weekly reports.
- **Keeps COROS history.** COROS FIT exports can be imported with their source recorded.

## Layout

```
app/
  endpoints.py      data sets, via the official client's wrappers
  garmin_client.py  pacing, retries, Retry-After, token refresh, error classes
  sync.py           check / probe / backfill / incremental / daemon CLI
  db.py             SQLite ledger (runs, attempts, coverage units, activities, file hashes)
  archive.py        immutable raw writer
  coverage.py       coverage report computed from the ledger
  fit_import.py     FIT -> Parquet (official FIT SDK), COROS import
  normalize.py      JSON -> Parquet records, field values and inventory
  analytics.py      read-only query layer
  mcp_server.py     MCP tools (stdio)
  maintenance.py    ledger snapshots, integrity verification
scripts/            host setup, backup, verify-restore, restore
claude/skills/      garmin-coach skill for Claude Code
tests/              unit tests, fake Garmin API, Docker end-to-end test
docs/               SETUP.md (deployment), DATA.md (data model and metric availability)
```

## Testing locally

```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt
pytest
```

The Docker end-to-end test builds and runs the real containers against the fake API:

```bash
docker build -t fitness-data:local .
PYTHON=.venv/bin/python tests/docker/local-e2e.sh
```
