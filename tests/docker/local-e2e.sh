#!/usr/bin/env bash
# Local end-to-end test of the Docker deployment against a fake Garmin API.
# Touches nothing outside a temporary directory and local Docker.
#
#   tests/docker/local-e2e.sh            (needs fitness-data:local built and a
#                                         Python with the `mcp` package as $PYTHON)
set -euo pipefail

REPO_DIR=$(cd "$(dirname "$0")/../.." && pwd)
PYTHON=${PYTHON:-python3}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/fitness-e2e.XXXXXX")
export REPO_DIR PUID=1000 PGID=1000
DC=(docker compose --project-directory "$WORK" -f "$REPO_DIR/compose.yaml"
    -f "$REPO_DIR/tests/docker/compose.test.yaml")
BACKUP=$(mktemp -d "${TMPDIR:-/tmp}/fitness-e2e-backup.XXXXXX")
cleanup() {
    "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$WORK" "$BACKUP"
}
trap cleanup EXIT
step() { printf '\n=== %s\n' "$*"; }

mkdir -p "$WORK"/{data,state,config,endpoints,imports/coros}
printf 'test-key\n' > "$WORK/config/garmin-api-key"
chmod 700 "$WORK/config" "$WORK/state"
chmod 600 "$WORK/config/garmin-api-key"
cat > "$WORK/.env" <<ENV
PUID=$PUID
PGID=$PGID
DATA_ROOT=$WORK/data
IMPORT_ROOT=$WORK/imports
ENV
chown -R "$PUID:$PGID" "$WORK"

step "compose config is valid"
"${DC[@]}" config --quiet
"${DC[@]}" --profile tools config --services

step "start fake Garmin API"
"${DC[@]}" up -d --wait fake-garmin

step "Milestone 1: check (auth, profile, activity, download, health record)"
"${DC[@]}" run --rm -T garmin-sync app.sync check --download

step "official CLI inside the sync container"
"${DC[@]}" run --rm -T --entrypoint gc-personal garmin-sync \
    --api-key-file /run/secrets/garmin_api_key \
    --bearer-token-cache-file /state/bearer-token.json --quiet heart-rate-zones | head -c 200; echo

step "historical backfill"
"${DC[@]}" run --rm -T garmin-sync app.sync backfill | tail -n 30

step "token cache persisted across containers with private permissions"
stat -c '%a %U' "$WORK/state/bearer-token.json"
[ "$(stat -c %a "$WORK/state/bearer-token.json")" = 600 ]

step "import a COROS FIT export"
docker run --rm -u "$PUID:$PGID" -v "$WORK/imports/coros:/out" \
    -v "$REPO_DIR/tests:/opt/app/tests:ro" --entrypoint python fitness-data:local -c \
    "from datetime import datetime, UTC; from tests.fakes import make_fit; \
open('/out/coros-run.fit','wb').write(make_fit(datetime(2025,3,1,15,tzinfo=UTC), altitude=1700))"
"${DC[@]}" run --rm -T garmin-sync app.fit_import import-dir --source coros /import/coros

step "archive files are immutable and owned by PUID"
find "$WORK/data/raw" -type f -perm /222 | head -n 3 | grep . && { echo "writable raw file"; exit 1; }
stat -c '%u' "$(find "$WORK/data/raw/activity-zips" -type f | head -n 1)" | grep -qx "$PUID"

step "MCP container: no network, no credentials, read-only archive"
"${DC[@]}" --profile tools run --rm -T --no-deps --entrypoint sh garmin-mcp -c '
    if touch /data/probe 2>/dev/null; then echo "archive writable"; exit 1; fi
    if [ -e /run/secrets/garmin_api_key ] || [ -d /config ]; then echo "credentials visible"; exit 1; fi
    python -c "import socket; socket.create_connection((\"1.1.1.1\", 53), 2)" 2>/dev/null \
        && { echo "network reachable"; exit 1; }
    echo "isolation ok"'

step "MCP protocol over 'docker compose run' stdio (what SSH will execute)"
"$PYTHON" "$REPO_DIR/tests/docker/mcp_client_check.py" -- "${DC[@]}" --profile tools run --rm -T \
    --no-deps garmin-mcp

step "live archive integrity"
"${DC[@]}" run --rm -T garmin-sync app.maintenance verify --smoke | tail -n 12

step "backup refuses a destination inside the data directory"
mkdir -p "$WORK/data/inside"
if COMPOSE="${DC[*]}" APP_DIR="$WORK" "$REPO_DIR/scripts/backup.sh" "$WORK/data/inside" 2>&1; then
    echo "backup into the data directory was allowed"; exit 1
fi
rmdir "$WORK/data/inside"

step "backup twice (second run hard-links unchanged files), verify restore, restore"
export COMPOSE="${DC[*]}" APP_DIR="$WORK" ALLOW_SAME_DEVICE=1
"$REPO_DIR/scripts/backup.sh" "$BACKUP"
sleep 1
"$REPO_DIR/scripts/backup.sh" "$BACKUP"
first=$(ls -1 "$BACKUP/snapshots" | head -n 1)
zipfile=$(cd "$BACKUP/snapshots/$first/data" && find raw/activity-zips -type f | head -n 1)
[ "$(stat -c %i "$BACKUP/snapshots/$first/data/$zipfile")" = \
  "$(stat -c %i "$BACKUP/latest/data/$zipfile")" ] && echo "hard-linked: $zipfile"
[ ! -e "$BACKUP/latest/deploy/config/garmin-api-key" ] && echo "API key not in backup"
"$REPO_DIR/scripts/verify-restore.sh" "$BACKUP" | tail -n 8
"$REPO_DIR/scripts/restore.sh" "$BACKUP" latest "$WORK/restored-data" "$WORK/restored-app"
docker run --rm --network none -u "$PUID:$PGID" -v "$WORK/restored-data:/data:ro" \
    -e DATA_DIR=/data fitness-data:local app.maintenance verify | grep '"ok": true'

step "ALL LOCAL E2E CHECKS PASSED"
