#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")"
docker compose down
echo "App stopped."
