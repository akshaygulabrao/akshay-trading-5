#!/usr/bin/env bash
set -euo pipefail

for var in FORECAST_DB_PATH WEATHER_DB_PATH ORDERBOOK_DB_PATH; do
    [[ -z "${!var:-}" ]] && { echo "Missing env var $var"; exit 1; }
    [[ ! -f "${!var}" ]] && { echo "File not found: ${!var}"; exit 1; }
done

# ---------- 1. Environment-variable checks ----------
required_vars=(EC2_HOST EC2_KEY)
missing=()
for var in "${required_vars[@]}"; do
  [[ -z "${!var:-}" ]] && missing+=("$var")
done
if (( ${#missing[@]} )); then
  echo "Error: missing environment variables:"
  printf ' - %s\n' "${missing[@]}"
  exit 1
fi

# ---------- 2. Local constants ----------
USER=ec2-user
HOST=$EC2_HOST
KEY=$EC2_KEY
REMOTE_DIR=/opt/trading_server
LOCAL_DIR=./db_backup

# ---------- 3. Pre-flight checks ----------
[[ -f "$KEY" ]] || { echo "Key file $KEY not found"; exit 1; }

# ---------- 4. Pull *.db* files from remote ----------
echo "Creating local backup directory: $LOCAL_DIR"
mkdir -p "$LOCAL_DIR"

echo "Copying *.db* files from $HOST:$REMOTE_DIR ..."
rsync -avz --include='*.db*' --exclude='*' \
  -e "ssh -i $KEY" \
  "$USER@$HOST:$REMOTE_DIR/" \
  "$LOCAL_DIR/"

echo "Remote db files copied into $LOCAL_DIR"

sqlite3 forecast.db "SELECT COUNT() from forecast;"
./script_merge_forecast_db.py $FORECAST_DB_PATH ./db_backup/forecast.db
sqlite3 forecast.db "SELECT COUNT() from forecast;"

sqlite3 weather.db "SELECT COUNT() from weather;"
./script_merge_sensor_db.py $WEATHER_DB_PATH ./db_backup/weather.db
sqlite3 weather.db "SELECT COUNT() from weather;"
