#!/usr/bin/env bash
# Stop.command — macOS stopper for GEE Web App (React + FastAPI)
cd "$(dirname "${BASH_SOURCE[0]}")"

echo ""
echo " Stopping GEE Web App (React + FastAPI)..."
echo ""

if docker compose --profile prod down; then
    echo ""
    echo " All services stopped."
else
    echo ""
    echo " WARNING: Some containers may still be running."
    echo " Run: docker compose --profile prod ps"
fi

echo ""
read -rp "Press Enter to close..."
