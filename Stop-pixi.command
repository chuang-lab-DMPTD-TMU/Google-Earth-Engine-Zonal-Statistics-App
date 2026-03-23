#!/usr/bin/env bash
# Stop-pixi.command — macOS stopper for GEE Web App (Pixi)
# Double-click in Finder or run from Terminal.
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
read -rp "Press Enter to close..."
