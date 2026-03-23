#!/usr/bin/env bash
# quickstart-react.sh — start the React + FastAPI stack
#
# Usage:
#   ./quickstart-react.sh          # production build (nginx + FastAPI)
#   ./quickstart-react.sh --dev    # Vite dev server with HMR + FastAPI
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MODE="prod"
if [[ "${1:-}" == "--dev" ]]; then
  MODE="dev"
fi

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

# ── Port selection ────────────────────────────────────────────────────────────

BACKEND_PORTS=(8000 8001 8002 8003)
FRONTEND_PORTS_PROD=(3000 3001 3002 3003)
FRONTEND_PORTS_DEV=(5173 5174 5175 5176)

pick_port() {
  local -n arr=$1
  for port in "${arr[@]}"; do
    if ! ss -ltn 2>/dev/null | grep -q ":${port} "; then
      echo "$port"
      return 0
    fi
  done
  echo ""
}

BACKEND_PORT="$(pick_port BACKEND_PORTS)"
if [[ -z "$BACKEND_PORT" ]]; then
  echo "ERROR: No free port for the backend in ${BACKEND_PORTS[*]}."
  exit 1
fi

if [[ "$MODE" == "dev" ]]; then
  FRONTEND_PORT="$(pick_port FRONTEND_PORTS_DEV)"
else
  FRONTEND_PORT="$(pick_port FRONTEND_PORTS_PROD)"
fi
if [[ -z "$FRONTEND_PORT" ]]; then
  echo "ERROR: No free port for the frontend."
  exit 1
fi

# Persist ports to .env so docker compose picks them up
update_env() {
  local key="$1" val="$2"
  if grep -q "^${key}=" .env 2>/dev/null; then
    sed -i "s|^${key}=.*|${key}=${val}|" .env
  else
    echo "${key}=${val}" >> .env
  fi
}
touch .env
update_env "HOST_UID"    "$HOST_UID"
update_env "HOST_GID"    "$HOST_GID"
update_env "BACKEND_PORT" "$BACKEND_PORT"
update_env "APP_PORT"    "$FRONTEND_PORT"

# ── Docker daemon check ───────────────────────────────────────────────────────

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Start Docker and try again."
  exit 1
fi

# ── Pixi conflict check ───────────────────────────────────────────────────────

if [ -f ".pixi.pid" ]; then
    PIXI_PID=$(cat .pixi.pid)
    if kill -0 "$PIXI_PID" 2>/dev/null; then
        PIXI_PORT=$(cat .pixi.port 2>/dev/null || echo "unknown")
        echo "ERROR: A pixi-managed backend is already running (PID $PIXI_PID, port $PIXI_PORT)."
        echo "Stop it first with: ./stop-pixi.sh"
        exit 1
    else
        rm -f .pixi.pid .pixi.port
    fi
fi

# ── Build & launch ────────────────────────────────────────────────────────────

echo "Building backend image…"
docker compose build backend

if [[ "$MODE" == "dev" ]]; then
  echo "Starting backend + Vite dev server (--profile dev)…"
  docker compose --profile dev up -d --force-recreate backend frontend-dev
  FRONTEND_URL="http://localhost:${FRONTEND_PORT}"
  WAIT_PATH="/"
  CONTAINER="gee_frontend_dev"
else
  echo "Building React production image…"
  docker compose build frontend
  echo "Starting backend + nginx frontend (--profile prod)…"
  docker compose --profile prod up -d --force-recreate backend frontend
  FRONTEND_URL="http://localhost:${FRONTEND_PORT}"
  WAIT_PATH="/"
  CONTAINER="gee_frontend"
fi

# ── Wait for services to be ready ─────────────────────────────────────────────

echo -n "Waiting for backend (http://localhost:${BACKEND_PORT}/api/gee-key)…"
backend_ready=0
for i in $(seq 1 40); do
  if curl -fsS "http://localhost:${BACKEND_PORT}/api/gee-key" >/dev/null 2>&1; then
    backend_ready=1
    break
  fi
  sleep 1
  echo -n "."
done
echo

if [[ $backend_ready -eq 0 ]]; then
  echo "WARNING: Backend did not respond after 40 s. Check logs:"
  echo "  docker compose logs -f backend"
fi

echo -n "Waiting for frontend (${FRONTEND_URL})…"
frontend_ready=0
for i in $(seq 1 60); do
  if curl -fsS "${FRONTEND_URL}" >/dev/null 2>&1; then
    frontend_ready=1
    break
  fi
  sleep 1
  echo -n "."
done
echo

# ── Open browser ──────────────────────────────────────────────────────────────

if [[ $frontend_ready -eq 1 ]]; then
  echo
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  GEE Web App (React) is ready"
  echo "  Frontend : ${FRONTEND_URL}"
  echo "  Backend  : http://localhost:${BACKEND_PORT}"
  if [[ "$MODE" == "dev" ]]; then
    echo "  Mode     : dev (Vite HMR — source changes reload instantly)"
  else
    echo "  Mode     : prod (nginx serving built bundle)"
  fi
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo

  # Auto-open browser if possible
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${FRONTEND_URL}" &
  elif command -v open >/dev/null 2>&1; then
    open "${FRONTEND_URL}"
  fi
else
  echo
  echo "ERROR: Frontend did not respond after 60 s."
  echo "  docker compose logs -f ${CONTAINER}"
  exit 1
fi

echo "Run ./stop.sh (or './stop.sh --all' to also stop dev/legacy containers) when you are done."
