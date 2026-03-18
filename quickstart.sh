#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

# Find the first available port from the candidates list
PORTS=(8501 8502 8503 8504 8505)
export APP_PORT=""
for port in "${PORTS[@]}"; do
	if ! ss -ltn | grep -q ":${port} "; then
		APP_PORT=$port
		break
	fi
done

if [[ -z "$APP_PORT" ]]; then
	echo "No available port found in ${PORTS[*]}. Free up a port and try again."
	exit 1
fi

docker compose build app
docker compose up -d app

ready=0
for i in $(seq 1 30); do
	if curl -fsS http://localhost:${APP_PORT} >/dev/null; then
		ready=1
		break
	fi
	sleep 1
done

if [[ $ready -eq 1 ]]; then
	echo
	echo "Streamlit UI is ready at http://localhost:${APP_PORT}"
else
	echo
	echo "Streamlit did not respond after 30 seconds; check:"
	echo "  docker compose logs -f app"
fi

echo "Run ./stop.sh when you are done."