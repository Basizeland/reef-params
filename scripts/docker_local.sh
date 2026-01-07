#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME=${IMAGE_NAME:-reef_metrics:latest}
ENV_FILE=${ENV_FILE:-.env}
PORT=${PORT:-8005}


docker build --no-cache -t "$IMAGE_NAME" .

if [[ -f "$ENV_FILE" ]]; then
  docker run --env-file "$ENV_FILE" -p "${PORT}:8000" "$IMAGE_NAME"
else
  docker run -p "${PORT}:8000" "$IMAGE_NAME"
fi
