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
DEST_DIR=/opt/trading_server

# ---------- 3. File list (edit as needed) ----------
FILES=(
  "/Users/ox/workspace/akshay-trading-5/kalshi_ref.py"
  "/Users/ox/workspace/akshay-trading-5/pyproject.toml"
  "/Users/ox/workspace/akshay-trading-5/orderbook_update.py"
  "/Users/ox/workspace/akshay-trading-5/weather_sensor_reading.py"
  "/Users/ox/workspace/akshay-trading-5/stream_orderbook2.py"
  "/Users/ox/workspace/akshay-trading-5/orderbook.py"
  "/Users/ox/workspace/akshay-trading-5/weather_extract_forecast.py"
  "/Users/ox/workspace/akshay-trading-5/orderbook_trader.py"
  "/opt/data/orders.db"
)

# ---------- 4. Pre-flight checks ----------
[[ -f "$KEY" ]]   || { echo "Key file $KEY not found"; exit 1; }
[[ ${#FILES[@]} -gt 0 ]] || { echo "No files listed in FILES array"; exit 1; }

missing_files=()
for f in "${FILES[@]}"; do
  [[ -f "$f" ]] || missing_files+=("$f")
done
if (( ${#missing_files[@]} )); then
  echo "Error: the following local files do NOT exist:"
  printf ' - %s\n' "${missing_files[@]}"
  exit 1
fi

# ---------- 5. rsync via sudo ----------
# We first rsync to a tmp dir under $USER, then sudo-mv into place.
TMP_DIR="/tmp/rsync_${USER}_$(date +%s)"

echo "Staging files on remote host under $TMP_DIR ..."
ssh -i "$KEY" "$USER@$HOST" "mkdir -p '$TMP_DIR'"

rsync -avz \
  -e "ssh -i $KEY" \
  "${FILES[@]}" "$USER@$HOST:$TMP_DIR/"

echo "Moving staged files into $DEST_DIR with sudo ..."
ssh -t -i "$KEY" "$USER@$HOST" \
  "sudo mkdir -p '$DEST_DIR' && \
  sudo mv $TMP_DIR/* '$DEST_DIR/' && \
  sudo chown $USER:$USER '$DEST_DIR'/* && \
  rm -rf '$TMP_DIR'"

echo "Verifying remote checksums ..."
ssh -i "$KEY" "$USER@$HOST" \
  "sudo sha256sum $DEST_DIR/*"

echo "All files copied successfully."
