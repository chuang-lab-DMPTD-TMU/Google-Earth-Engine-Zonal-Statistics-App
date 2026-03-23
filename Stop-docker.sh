#!/usr/bin/env bash
# stop.sh — Linux stopper for GEE Web App (React + FastAPI, Docker)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo ""
echo " Stopping GEE Web App (React + FastAPI)..."
echo ""

if docker compose --profile prod down; then
    echo " All services stopped."
else
    echo " WARNING: Some containers may still be running."
    echo " Run: docker compose --profile prod ps"
fi

echo ""
