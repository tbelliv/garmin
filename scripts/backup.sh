#!/usr/bin/env bash
# Back up the archive to another device (NAS mount, USB disk).
#
#   scripts/backup.sh /mnt/nas/fitness-backups
#
# Each run creates snapshots/<UTC stamp>/ using rsync hard links against the
# previous snapshot, so unchanged files cost no extra space. The live SQLite
# ledger is not copied; a consistent snapshot of it (db/snapshots/) is.
# Credentials are excluded: the API key can be re-created in Garmin Connect
# and the bearer token is re-issued automatically.
set -euo pipefail

DEST=${1:?usage: backup.sh DEST_DIR}
APP_DIR=${APP_DIR:-/opt/fitness-data}
set -a; [ -f "$APP_DIR/.env" ] && . "$APP_DIR/.env"; set +a
DATA_ROOT=${DATA_ROOT:-/srv/fitness-data}
COMPOSE=${COMPOSE:-docker compose}

if [ ! -d "$DEST" ]; then
    echo "backup destination $DEST does not exist (is the NAS mounted?)" >&2
    exit 1
fi
DEST_REAL=$(cd "$DEST" && pwd -P)
for inside in "$DATA_ROOT" "$APP_DIR"; do
    INSIDE_REAL=$(cd "$inside" && pwd -P)
    case "$DEST_REAL/" in
        "$INSIDE_REAL"/*) echo "refusing: $DEST is inside $inside" >&2; exit 1 ;;
    esac
done
if [ "$(stat -c %d "$DEST")" = "$(stat -c %d "$DATA_ROOT")" ] && [ -z "${ALLOW_SAME_DEVICE:-}" ]; then
    echo "refusing: $DEST is on the same filesystem as $DATA_ROOT (set ALLOW_SAME_DEVICE=1 to override)" >&2
    exit 1
fi

(cd "$APP_DIR" && $COMPOSE run --rm --no-deps -T garmin-sync app.maintenance snapshot)

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
TARGET="$DEST/snapshots/$STAMP"
mkdir -p "$TARGET"
LINK=()
if [ -d "$DEST/latest/data" ]; then
    LINK=(--link-dest="$(cd "$DEST/latest" && pwd -P)/data")
fi

rsync -a "${LINK[@]}" --exclude 'db/ledger.sqlite' --exclude 'db/ledger.sqlite-journal' \
    "$DATA_ROOT/" "$TARGET/data/"
# Deployment files only (an explicit list, so nothing else under APP_DIR is swept in).
mkdir -p "$TARGET/deploy"
(cd "$APP_DIR" && for item in compose.yaml Dockerfile requirements.txt .dockerignore .env \
        .env.example app scripts endpoints claude docs README.md; do
    if [ -e "$item" ]; then rsync -a --relative "./$item" "$TARGET/deploy/"; fi
done)

ln -sfn "snapshots/$STAMP" "$DEST/latest.tmp" && mv -T "$DEST/latest.tmp" "$DEST/latest"
echo "backup written to $TARGET"
