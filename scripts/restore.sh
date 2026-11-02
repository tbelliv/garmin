#!/usr/bin/env bash
# Restore a backup snapshot onto a (new) server. Run verify-restore.sh first.
#
#   scripts/restore.sh /mnt/nas/fitness-backups [SNAPSHOT] [/srv/fitness-data] [/opt/fitness-data]
#
# Refuses to overwrite a non-empty data directory. Afterwards re-create
# config/garmin-api-key (credentials are never backed up).
set -euo pipefail

DEST=${1:?usage: restore.sh BACKUP_DIR [SNAPSHOT] [DATA_ROOT] [APP_DIR]}
SNAP=${2:-latest}
DATA_ROOT=${3:-/srv/fitness-data}
APP_DIR=${4:-/opt/fitness-data}
SRC="$DEST/$SNAP"
[ "$SNAP" != latest ] && SRC="$DEST/snapshots/$SNAP"

if [ -d "$DATA_ROOT" ] && [ -n "$(ls -A "$DATA_ROOT")" ]; then
    echo "refusing: $DATA_ROOT is not empty" >&2
    exit 1
fi
mkdir -p "$DATA_ROOT" "$APP_DIR"
rsync -a "$SRC/data/" "$DATA_ROOT/"
LEDGER=$(ls -1 "$DATA_ROOT/db/snapshots/"ledger-*.sqlite | tail -n 1)
cp "$LEDGER" "$DATA_ROOT/db/ledger.sqlite"
rsync -a --ignore-existing "$SRC/deploy/" "$APP_DIR/"
mkdir -p "$APP_DIR/config" "$APP_DIR/state"
chmod 700 "$APP_DIR/config" "$APP_DIR/state"
echo "restored $SRC to $DATA_ROOT and $APP_DIR; now recreate $APP_DIR/config/garmin-api-key"
