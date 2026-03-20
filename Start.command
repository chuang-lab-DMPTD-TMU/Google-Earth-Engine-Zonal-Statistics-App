#!/usr/bin/env bash
# Start.command — macOS launcher for GEE Web App (React + FastAPI)
# Double-click in Finder or run from Terminal.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo " GEE Web App - React + FastAPI"
echo " ================================"
echo ""

# --- Docker check ---
if ! docker info >/dev/null 2>&1; then
    echo "-----------------------------------------------------"
    echo " Docker is not running."
    echo " Please start Docker Desktop, then try again."
    echo "-----------------------------------------------------"
    read -rp "Press Enter to close..."
    exit 1
fi

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

# --- Find a free backend port (8000-8003) ---
BACKEND_PORT=""
for port in 8000 8001 8002 8003; do
    if ! nc -z localhost "$port" 2>/dev/null; then
        BACKEND_PORT="$port"
        break
    fi
done
if [[ -z "$BACKEND_PORT" ]]; then
    echo " No free port for backend (tried 8000-8003). Free a port and try again."
    read -rp "Press Enter to close..."
    exit 1
fi

# --- Find a free frontend port (3000-3003) ---
FRONTEND_PORT=""
for port in 3000 3001 3002 3003; do
    if ! nc -z localhost "$port" 2>/dev/null; then
        FRONTEND_PORT="$port"
        break
    fi
done
if [[ -z "$FRONTEND_PORT" ]]; then
    echo " No free port for frontend (tried 3000-3003). Free a port and try again."
    read -rp "Press Enter to close..."
    exit 1
fi

echo " Backend  port : $BACKEND_PORT"
echo " Frontend port : $FRONTEND_PORT"
echo ""

# --- Update .env (preserve existing lines, update/add our keys) ---
touch .env
for key in BACKEND_PORT APP_PORT HOST_UID HOST_GID; do
    sed -i.bak "/^${key}=/d" .env
done
rm -f .env.bak
{
    echo "BACKEND_PORT=${BACKEND_PORT}"
    echo "APP_PORT=${FRONTEND_PORT}"
    echo "HOST_UID=${HOST_UID}"
    echo "HOST_GID=${HOST_GID}"
} >> .env

# --- Build images ---
echo " Building backend image..."
docker compose build backend || { echo " Backend build failed."; read -rp "Press Enter..."; exit 1; }

echo " Building frontend image..."
docker compose build frontend || { echo " Frontend build failed."; read -rp "Press Enter..."; exit 1; }

# --- Start services ---
echo " Starting services..."
docker compose --profile prod up -d --force-recreate backend frontend \
    || { echo " Failed to start containers."; read -rp "Press Enter..."; exit 1; }

# --- Wait for backend ---
echo -n " Waiting for backend"
backend_ready=0
for i in $(seq 1 40); do
    if curl -fsS "http://localhost:${BACKEND_PORT}/api/gee-key" >/dev/null 2>&1; then
        backend_ready=1
        break
    fi
    sleep 1
    echo -n "."
done
echo ""

if [[ $backend_ready -eq 0 ]]; then
    echo ""
    echo " WARNING: Backend did not respond after 40 s."
    echo " Check: docker compose logs -f backend"
    echo ""
fi

# --- Wait for frontend ---
echo -n " Waiting for frontend"
frontend_ready=0
for i in $(seq 1 60); do
    if curl -fsS "http://localhost:${FRONTEND_PORT}" >/dev/null 2>&1; then
        frontend_ready=1
        break
    fi
    sleep 1
    echo -n "."
done
echo ""

if [[ $frontend_ready -eq 1 ]]; then
    echo ""
    echo " =========================================="
    echo "  GEE Web App is ready"
    echo "  Frontend : http://localhost:${FRONTEND_PORT}"
    echo "  Backend  : http://localhost:${BACKEND_PORT}"
    echo " =========================================="
    echo ""
    open "http://localhost:${FRONTEND_PORT}"
else
    echo ""
    echo "-----------------------------------------------------"
    echo " Frontend did not respond after 60 s."
    echo " Check: docker compose logs -f frontend"
    echo "-----------------------------------------------------"
    read -rp "Press Enter to close..."
    exit 1
fi

echo " Run Stop.command when you are done."
echo ""
