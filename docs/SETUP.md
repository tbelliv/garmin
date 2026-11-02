# Deployment runbook

Nothing here has been run on your server yet. Follow it in order. Each milestone
ends with an acceptance check. Commands run on the **server** unless marked
**laptop**.

## 0. Prerequisites

- A Linux server with Docker Engine and the Compose plugin (`docker compose version`).
- `rsync` on the server.
- Tailscale on the server and the laptop, both logged in to the same tailnet.
- The Forerunner 970 paired with Garmin Connect and synced at least once.

Home Assistant is untouched. This is a separate Compose project named `fitness-data`.

## Milestone 1: authentication

1. Clone the repository on the server and prepare the host:

   ```bash
   git clone https://github.com/tbelliv/garmin.git ~/garmin && cd ~/garmin
   ```

   ```bash
   scripts/setup-host.sh /opt/fitness-data /srv/fitness-data
   ```

   This creates `/opt/fitness-data` (code, config, state) and `/srv/fitness-data` (archive),
   owned by you, and writes `.env` with your user id.

2. Create a Personal API key at
   <https://connect.garmin.com/app/settings/personalApiToken>. Store it with nothing else in
   the file:

   ```bash
   umask 077 && nano /opt/fitness-data/config/garmin-api-key
   ```

   ```bash
   chmod 700 /opt/fitness-data/config && chmod 600 /opt/fitness-data/config/garmin-api-key
   ```

   The key is mounted only into the sync container, as a Compose secret. It is never in
   `.env`, never logged and never visible to the MCP container or Claude.

3. Build and check the official client:

   ```bash
   cd /opt/fitness-data && docker compose build
   ```

   ```bash
   docker compose run --rm --entrypoint gc-personal garmin-sync --help
   ```

   The command list should match the `wrapper` entries in `app/endpoints.py`.

4. Prove real access. This retrieves your profile, the newest activity's details and original
   file, and yesterday's daily summary:

   ```bash
   docker compose run --rm garmin-sync app.sync check --download
   ```

   **Acceptance:** `auth_and_profile` and `health_record_daily_summaries` are `ok`. If the watch
   has recorded an activity, `activity_download` shows a ZIP path.

5. Probe every endpoint once and record what your account can access:

   ```bash
   docker compose run --rm garmin-sync app.sync probe
   ```

   Look at `availability` and `top_level_keys` for each endpoint. `not_accessible` (HTTP 403)
   means the Personal API refuses that data for your account.

## Milestone 2: historical archive

1. Review `.env`. Defaults: history from `2015-01-01`, one request per second, and the last 7
   days re-fetched on every run. With `DAY_ENDPOINT_START=auto`, per-day endpoints start at the
   first month in which Garmin has any daily summary for you. Earlier days are reported as
   skipped, not complete, and are not requested one by one.

2. Run a first backfill in the foreground and watch it:

   ```bash
   docker compose run --rm garmin-sync app.sync backfill --budget-minutes 60
   ```

   It stops cleanly at the budget, on a rate limit (HTTP 429) or on an authentication error.
   Rerunning continues where it left off.

3. Start the daemon. It runs a sync cycle every 3 hours: snapshots, new activities, the recent
   window, then up to 2 hours of remaining history. It then parses FIT files and rebuilds
   Parquet.

   ```bash
   docker compose up -d garmin-sync
   ```

4. Check coverage at any time:

   ```bash
   docker compose run --rm garmin-sync app.sync status
   ```

   **Acceptance:** the report says `BACKFILL COMPLETE: yes`, or lists exactly which endpoints
   and date ranges are missing or failing and why. Failed ranges are retried automatically
   with back-off. To retry immediately, run `app.sync retry-failed`.

## Milestone 3: FIT parsing and analytics

The daemon parses new FIT files and rebuilds Parquet after each cycle. To run that by hand:

```bash
docker compose run --rm garmin-sync app.sync process
```

**Acceptance:** `data_coverage` in Claude shows FIT parsed equal to original files, and
`get_activity_stream` returns per-second data. After upgrading the parser, re-parse everything
with `app.fit_import parse --all`. Original files are never modified.

### Optional: COROS history

Export your COROS activities as FIT files, copy them to `/srv/fitness-imports/coros/`, then:

```bash
docker compose run --rm garmin-sync app.fit_import import-dir --source coros /import/coros
```

Each file is archived under `raw/imports/coros/` with source `coros`. Re-running skips files
already imported, matched by content hash.

## Milestone 4: MCP and remote access

1. **Server:** check that the MCP container starts and speaks MCP on stdio. It has no network,
   no credentials and a read-only archive.

   ```bash
   cd /opt/fitness-data && docker compose --profile tools run --rm -T --no-deps garmin-mcp </dev/null
   ```

   It should exit quietly when stdin closes.

2. **Laptop:** create a dedicated SSH key for Claude:

   ```bash
   ssh-keygen -t ed25519 -f ~/.ssh/garmin_mcp -C claude-garmin-mcp -N ''
   ```

3. **Server:** authorize that key for the MCP command only. Add one line to
   `~/.ssh/authorized_keys`, replacing `AAAA...` with the contents of `garmin_mcp.pub`:

   ```
   command="cd /opt/fitness-data && docker compose --profile tools run --rm -T --no-deps garmin-mcp",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty ssh-ed25519 AAAA... claude-garmin-mcp
   ```

   With the forced command, this key can do nothing except start the read-only MCP container.
   Your account is in the `docker` group, which is root-equivalent. The forced command is what
   keeps this key from being one.

4. **Server:** accept SSH only over Tailscale. For example, with ufw:

   ```bash
   sudo ufw allow in on tailscale0 to any port 22 proto tcp
   ```

   Don't add router port forwarding, a public MCP endpoint or Tailscale Funnel.

5. **Laptop:** register the server with Claude Code. Replace the user and the Tailscale host
   name:

   ```bash
   claude mcp add --transport stdio --scope user garmin -- ssh -T -i ~/.ssh/garmin_mcp -o BatchMode=yes YOUR_LINUX_USER@YOUR_SERVER
   ```

   ```bash
   claude mcp list
   ```

   In Claude Code, run `/mcp` and confirm the `garmin` server lists 13 tools.

   **Acceptance:** asking Claude "what does data_coverage say?" returns your real coverage
   report.

## Milestone 5: coaching

Install the skill on the laptop:

```bash
mkdir -p ~/.claude/skills && cp -r claude/skills/garmin-coach ~/.claude/skills/
```

Then ask Claude Code for a weekly report, for example "weekly training report for last week".
The skill uses `weekly_report_data`, `compare_periods` and the other tools. Every
recommendation cites the dates and numbers behind it.

## Backups

The archive on the server's disk is the primary copy, not the only one. Mount a NAS or
external disk, then schedule nightly backups and a monthly restore test with `crontab -e`:

```
17 3 * * * /opt/fitness-data/scripts/backup.sh /mnt/nas/fitness-backups >> /srv/fitness-data/logs/backup.log 2>&1
43 4 1 * * /opt/fitness-data/scripts/verify-restore.sh /mnt/nas/fitness-backups >> /srv/fitness-data/logs/backup.log 2>&1
```

- The backup refuses a destination inside the archive or app directory, or on the same disk.
- Each backup is a dated snapshot. Unchanged files are hard-linked, so they take no extra
  space.
- The SQLite ledger is copied from a consistent snapshot taken while the sync keeps running.
- The API key and bearer token are never backed up. Re-create the key in Garmin Connect after
  a restore.
- `verify-restore.sh` restores to a scratch directory, re-hashes every archived file against
  the ledger, opens every Parquet file and runs the MCP query layer on the copy.
- To restore onto a new server, run `scripts/restore.sh`.

If the backup target is untrusted, for example cloud storage, put it on an encrypted volume
or use an encrypting tool such as restic instead of rsync.

## Upgrading

```bash
cd ~/garmin && git pull && scripts/setup-host.sh /opt/fitness-data /srv/fitness-data
```

```bash
cd /opt/fitness-data && docker compose build && docker compose up -d garmin-sync
```

`setup-host.sh` never overwrites `.env`, `config/` or `state/`.
