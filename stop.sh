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
    docker compose --profile prod --profile dev --profile legacy down
    echo " All profiles stopped."
else
    docker compose --profile prod down
    echo " React stack stopped."
    echo " (Pass --all to also stop legacy Streamlit / dev containers.)"
fi

echo ""
