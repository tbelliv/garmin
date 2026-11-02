# Data model and what Garmin makes available

## Sources and access

All Garmin data comes through the **Garmin Connect Personal API**. It uses your Personal API
key, exchanged for a bearer token by Garmin's official client
(`garmin-connect-personal-api-python` 1.0.0). Original activity files are decoded with
Garmin's official FIT SDK (`garmin-fit-sdk`).

"All Garmin data" means everything those endpoints return for your account. Some screens in
Garmin Connect have no Personal API equivalent.

### Endpoints archived

All of these use the official client's wrappers, listed in `app/endpoints.py`:

- **Daily ranges:** daily summaries, stress, steps, hydration, sleep summaries, Body Battery,
  HRV summaries, weight, weight goals and wellness activity summaries.
- **Per day:** detailed sleep, all-day heart rate, HRV readings, SpO2 and altitude
  acclimation, fitness age and food logs. Menstrual cycle data is fetched for recent days
  only.
- **Snapshots:** profile settings and heart rate zones.
- **Activities:** the full activity list, each activity's details and workouts, and the
  original download as a ZIP with the FIT file inside.

### Endpoints not in the official client

Garmin's Swagger documentation lists the full Personal API. It could not be checked while
this was built. To add an endpoint from it, copy `endpoints/extra-endpoints.example.json` to
`endpoints/extra-endpoints.json` and fill in the path. Run `app.sync probe`, and set
`"backfill": true` once it returns data. No code change is needed.

### Training and performance metrics

The official client has **no dedicated endpoints** for VO2 max, training status, training
readiness, recovery time or race predictions. They may still appear in three places:

- inside activity details or activity summaries, for example `vO2MaxValue`, training effect
  or activity training load;
- inside daily summaries or profile settings;
- in FIT session messages written by the watch, for example total training effect.

The `get_training_metrics` tool and the field inventory search everything retrieved and say
where each metric was found, or that it was not found. Until the first real sync, treat every
metric as **not yet investigated**.

## Storage layout (`/srv/fitness-data`)

```
raw/api/<endpoint>/<year>/<unit>/<UTC timestamp>.json      exact API response (read-only)
raw/api/.../<UTC timestamp>.meta.json                      endpoint, range, retrieval time, hash
raw/activity-zips/<year>/<activity id>.zip                 original download (read-only)
raw/fit/<year>/<activity id>/<file>.fit                    files extracted from the ZIP
raw/imports/coros/<sha256>.fit                             imported COROS files
db/ledger.sqlite                                           runs, attempts, coverage, activities, hashes
db/snapshots/ledger-<stamp>.sqlite                         consistent copies for backup
parquet/activities.parquet                                 one row per activity, all sources
parquet/fit/{records,laps,sessions,messages,summary}/source=<src>/<key>.parquet
parquet/json/records/endpoint=<name>/data.parquet          every record, every retrieval, full JSON
parquet/json/fields/endpoint=<name>/data.parquet           every scalar field of the latest records
parquet/field_inventory.parquet                            which fields exist, counts, date span
logs/sync.log
```

The `raw/` directory is the source of truth. Everything under `parquet/` can be rebuilt from
it at any time with `app.fit_import parse --all` and `app.sync process`.

## Provenance

Every normalized row carries:

- `source`, which is `garmin` or `coros`;
- the original record identifier, which is the activity id, record id or calendar date;
- the recorded timestamp or calendar date;
- `retrieved_at`;
- `raw_path` and `raw_sha256`, pointing at the exact archived file;
- `parser_version`.

When Garmin revises a day, for example a late sleep sync, the new response is stored next to
the old one. `is_latest` marks the newest version. Nothing is overwritten.

## Coverage statuses

| Status | Meaning |
| --- | --- |
| `retrieved` | Every expected day was requested successfully |
| `partial` | Some days are retrieved; others are pending or failed |
| `no_data_recorded` | Requests succeeded, but Garmin returned nothing |
| `not_accessible` | The API refused access (HTTP 403) or returned only 404s |
| `not_investigated` | Never requested |

Per day, a successful empty response means no data was recorded that day. That is different
from a failed request, which is retried, and from a day not yet requested. Days before the
first evidence of account data are reported as `skipped_before_floor`, never as complete.
Tools never turn missing values into zeros.

## Derived values

Values computed here, rather than reported by Garmin, are labelled with their method:

- **Altitude band:** from the median FIT altitude of the activity. Bands are below 1000 m,
  1000-2000 m, 2000-3000 m and 3000 m or higher.
- **Mean temperature:** from the watch sensor, which reads high because of body heat.
- **Pace at heart rate:** mean FIT speed while HR is in a range and speed is above 1 m/s.
  It is not grade-adjusted.
- **Time in HR zones:** uses Garmin's zone floors. Gaps between records are capped at 10 s.
- **Weekly distance ratio:** the week's distance against the mean of the prior 4 weeks. It is
  a crude load indicator and is not Garmin's training status.

No altitude correction is applied to VO2 max or pace.
