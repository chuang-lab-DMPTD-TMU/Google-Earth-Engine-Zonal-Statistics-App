#!/usr/bin/env bash
# Start-pixi.command — macOS launcher for GEE Web App (Pixi, no Docker required)
# Double-click in Finder or run from Terminal.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo " GEE Web App - Pixi (no Docker)"
echo " ================================"
echo ""

# --- Check pixi ---
if ! command -v pixi &>/dev/null; then
    echo "-----------------------------------------------------"
    echo " Pixi not found. Install it with:"
    echo "   curl -fsSL https://pixi.sh/install.sh | sh"
    echo " Then open a new terminal and try again."
    echo "-----------------------------------------------------"
    read -rp "Press Enter to close..."
    exit 1
fi

# --- Find a free port (8000-8003) ---
PORT=""
for p in 8000 8001 8002 8003; do
    if ! nc -z localhost "$p" 2>/dev/null; then
        PORT="$p"; break
    fi
done
if [[ -z "$PORT" ]]; then
    echo " No free port (tried 8000-8003). Free a port and try again."
    read -rp "Press Enter to close..."
    exit 1
fi

# --- Warn if GEE key missing ---
if [[ ! -f "config/gee-key.json" ]]; then
    echo " WARNING: config/gee-key.json not found."
    echo " The app will start but GEE operations will fail until a key is uploaded."
    echo ""
fi

echo " App port : $PORT"
echo ""

# --- Build frontend ---
echo " Building frontend..."
pixi run build-frontend || { echo " Frontend build failed."; read -rp "Press Enter..."; exit 1; }

# --- Start backend in background, save PID and port ---
echo " Starting backend..."
GOOGLE_APPLICATION_CREDENTIALS=config/gee-key.json \
    pixi run uvicorn backend.app:app --host 0.0.0.0 --port "$PORT" > pixi.log 2>&1 &
echo $! > .pixi.pid
echo "$PORT" > .pixi.port

# --- Wait for ready ---
echo -n " Waiting for app"
ready=0
for i in $(seq 1 60); do
    if curl -fsS "http://localhost:${PORT}/api/gee-key" >/dev/null 2>&1; then
        ready=1; break
    fi
    sleep 1; echo -n "."
done
echo ""

if [[ $ready -eq 0 ]]; then
    echo ""
    echo "-----------------------------------------------------"
    echo " ERROR: App did not respond after 60 s."
    echo " Check pixi.log for details."
    echo "-----------------------------------------------------"
    read -rp "Press Enter to close..."
    exit 1
fi

echo ""
echo " =========================================="
echo "  GEE Web App is ready"
echo "  http://localhost:${PORT}"
echo " =========================================="
echo ""

open "http://localhost:${PORT}"
echo " Run Stop-pixi.command when you are done."
echo ""
