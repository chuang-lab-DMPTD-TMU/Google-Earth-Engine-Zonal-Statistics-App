#!/usr/bin/env bash
# stop-pixi.sh — Linux stopper for GEE Web App (Pixi)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo ""
echo " Stopping GEE Web App (Pixi)..."
echo ""

if [[ -f .pixi.pid ]]; then
    PID="$(cat .pixi.pid)"
    if kill "$PID" 2>/dev/null; then
        echo " Stopped (PID $PID)."
    else
        echo " Process $PID was already stopped."
    fi
    rm -f .pixi.pid .pixi.port
else
    # Fallback: kill by saved port, or default 8000
    PORT="$(cat .pixi.port 2>/dev/null || echo 8000)"
    PID="$(lsof -ti ":${PORT}" 2>/dev/null || true)"
    if [[ -n "$PID" ]]; then
        kill "$PID"
        echo " Stopped process on port $PORT."
        rm -f .pixi.port
    else
        echo " No running app found."
    fi
fi

echo ""
