#!/usr/bin/env bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "🚀 Starting F1 Data Warehouse..."

if [ ! -f "$PROJECT_DIR/.env" ]; then
  echo "⚠️  .env file not found. Creating from .env.example..."
  cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
fi

docker compose \
  --env-file "$PROJECT_DIR/.env" \
  -f "$PROJECT_DIR/docker/docker-compose.yml" \
  up --build

echo "⏳ Waiting for Metabase to become healthy..."

until curl -s http://localhost:3000/api/health | grep -q "ok"; do
  sleep 2
done

echo ""
echo "========================================"
echo "🚀 F1 DATA WAREHOUSE IS READY 🚀"
echo "Opening: http://localhost:3000"
echo "========================================"
echo ""

URL="http://localhost:3000"

if [[ "$OSTYPE" == "darwin"* ]]; then
  open "$URL"                # macOS
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
  xdg-open "$URL" >/dev/null 2>&1 || true  # Linux
elif [[ "$OSTYPE" == "msys"* ]] || [[ "$OSTYPE" == "cygwin"* ]]; then
  start "$URL"               # Git Bash / Windows
else
  echo "⚠️  Could not detect OS. Please open $URL manually."
fi