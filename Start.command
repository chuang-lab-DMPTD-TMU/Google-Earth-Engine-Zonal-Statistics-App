#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- Docker check ---
if ! docker info >/dev/null 2>&1; then
    echo "-----------------------------------------------------"
    echo " Docker is not running."
    echo " Please start Docker Desktop, then try again."
    echo "-----------------------------------------------------"
    read -rp "Press Enter to close this window..."
    exit 1
fi

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

# --- Find a free port ---
PORTS=(8501 8502 8503 8504 8505)
export APP_PORT=""
for port in "${PORTS[@]}"; do
    if ! nc -z localhost "$port" 2>/dev/null; then
        APP_PORT=$port
        break
    fi
done

if [[ -z "$APP_PORT" ]]; then
    echo "No available port found (tried 8501-8505). Free a port and try again."
    read -rp "Press Enter to close this window..."
    exit 1
fi

# --- Build and start ---
echo "Starting GEE Batch Processor on port $APP_PORT..."
docker compose build app
docker compose up -d app

# --- Wait for UI, then open browser ---
for i in $(seq 1 30); do
    if curl -fsS "http://localhost:${APP_PORT}" >/dev/null 2>&1; then
        echo "Opening http://localhost:${APP_PORT} ..."
        open "http://localhost:${APP_PORT}"
        exit 0
    fi
    sleep 1
done

echo "-----------------------------------------------------"
echo " The app did not respond after 30 seconds."
echo " To see what went wrong, run:"
echo "   docker compose logs -f app"
echo "-----------------------------------------------------"
read -rp "Press Enter to close this window..."
