#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# stop.sh — tear down the pipeline.
# Usage: ./scripts/stop.sh
# ────────────────────────────────────────────────────────────────────
set -euo pipefail
echo "==> Stopping pipeline..."
docker compose down -v
echo "Done."