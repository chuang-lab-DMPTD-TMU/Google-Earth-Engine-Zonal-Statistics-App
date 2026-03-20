#!/usr/bin/env bash
# Stop.command — macOS stopper for GEE Web App (React + FastAPI)
cd "$(dirname "${BASH_SOURCE[0]}")"

echo ""
echo " Stopping GEE Web App (React + FastAPI)..."
echo ""

docker compose --profile prod down

echo ""
echo " All services stopped."
echo ""
read -rp "Press Enter to close..."
