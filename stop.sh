#!/usr/bin/env bash
# stop.sh — Linux stopper for GEE Web App (React + FastAPI)
#
# Usage:
#   ./stop.sh          # stops React stack (backend + frontend, prod profile)
#   ./stop.sh --all    # stops everything including legacy Streamlit container
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo ""
echo " Stopping GEE Web App (React + FastAPI)..."
echo ""

if [[ "${1:-}" == "--all" ]]; then
    if docker compose --profile prod --profile dev --profile legacy down; then
        echo " All profiles stopped."
    else
        echo " WARNING: Some containers may still be running."
        echo " Run: docker compose --profile prod --profile dev --profile legacy ps"
    fi
else
    if docker compose --profile prod down; then
        echo " React stack stopped."
        echo " (Pass --all to also stop legacy Streamlit / dev containers.)"
    else
        echo " WARNING: Some containers may still be running."
        echo " Run: docker compose --profile prod ps"
    fi
fi

echo ""
