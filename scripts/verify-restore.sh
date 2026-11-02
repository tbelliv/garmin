#!/usr/bin/env bash
# Prove a backup can be restored: copy a snapshot to a scratch directory, put
# its newest ledger snapshot in place, then re-hash every file against the
# ledger, open every Parquet file and run the MCP query layer on the copy.
#
#   scripts/verify-restore.sh /mnt/nas/fitness-backups [SNAPSHOT_STAMP]
set -euo pipefail

DEST=${1:?usage: verify-restore.sh BACKUP_DIR [SNAPSHOT]}
SNAP=${2:-latest}
IMAGE=${IMAGE:-fitness-data:local}
SRC="$DEST/$SNAP"
[ "$SNAP" != latest ] && SRC="$DEST/snapshots/$SNAP"
[ -d "$SRC/data" ] || { echo "no snapshot data at $SRC/data" >&2; exit 1; }

SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/fitness-restore.XXXXXX")
trap 'rm -rf "$SCRATCH"' EXIT
rsync -a "$SRC/data/" "$SCRATCH/data/"
LEDGER=$(ls -1 "$SCRATCH/data/db/snapshots/"ledger-*.sqlite 2>/dev/null | tail -n 1)
[ -n "$LEDGER" ] || { echo "backup has no ledger snapshot" >&2; exit 1; }
cp "$LEDGER" "$SCRATCH/data/db/ledger.sqlite"

docker run --rm --network none --read-only --tmpfs /tmp -e DATA_DIR=/data \
    -u "$(id -u):$(id -g)" -v "$SCRATCH/data:/data:ro" \
    "$IMAGE" app.maintenance verify --smoke
echo "restore verified from $SRC"
