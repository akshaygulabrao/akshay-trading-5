#!/bin/bash
set -euo pipefail

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
./script_merge_forecast_db.py /opt/data/forecast.db ./db_backup/forecast.db
sqlite3 forecast.db "SELECT COUNT() from forecast;"

sqlite3 weather.db "SELECT COUNT() from weather;"
./script_merge_sensor_db.py /opt/data/weather.db ./db_backup/weather.db
sqlite3 weather.db "SELECT COUNT() from weather;"
