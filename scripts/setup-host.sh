#!/usr/bin/env bash
# One-time host setup for the Garmin data platform. Run as your normal user
# (with sudo available) from the checked-out repository on the Linux server.
#
#   scripts/setup-host.sh [/opt/fitness-data] [/srv/fitness-data]
#
# Creates directories with private permissions, copies the deployment files,
# and prepares .env. It does not start anything and never asks for the API key.
set -euo pipefail

APP_DIR=${1:-/opt/fitness-data}
DATA_ROOT=${2:-/srv/fitness-data}
IMPORT_ROOT=${IMPORT_ROOT:-/srv/fitness-imports}
REPO_DIR=$(cd "$(dirname "$0")/.." && pwd)
PUID=$(id -u)
PGID=$(id -g)

sudo mkdir -p "$APP_DIR" "$DATA_ROOT" "$IMPORT_ROOT"
sudo chown "$PUID:$PGID" "$APP_DIR" "$DATA_ROOT" "$IMPORT_ROOT"
chmod 750 "$APP_DIR" "$DATA_ROOT"

rsync -a --exclude .git --exclude tests --exclude '.env' --exclude 'config/' \
    --exclude 'state/' "$REPO_DIR/" "$APP_DIR/"
mkdir -p "$APP_DIR/config" "$APP_DIR/state" "$APP_DIR/endpoints"
chmod 700 "$APP_DIR/config" "$APP_DIR/state"

if [ ! -f "$APP_DIR/.env" ]; then
    sed -e "s/^PUID=.*/PUID=$PUID/" -e "s/^PGID=.*/PGID=$PGID/" \
        -e "s|^DATA_ROOT=.*|DATA_ROOT=$DATA_ROOT|" -e "s|^IMPORT_ROOT=.*|IMPORT_ROOT=$IMPORT_ROOT|" \
        "$APP_DIR/.env.example" > "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
fi

cat <<MSG
Host directories are ready.

Next steps:
  1. Create a Personal API key at https://connect.garmin.com/app/settings/personalApiToken
  2. Save it (no other content) and lock it down:
       umask 077; nano $APP_DIR/config/garmin-api-key; chmod 600 $APP_DIR/config/garmin-api-key
  3. cd $APP_DIR && docker compose build
  4. docker compose run --rm garmin-sync app.sync check --download
MSG
