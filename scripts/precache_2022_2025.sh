#!/usr/bin/env bash
set -euo pipefail

# Pre-cache and validate prepared replay parquets for all thesis seasons.
# Uses a static official calendar fallback to reduce dependency on schedule endpoints.
#
# Usage:
#   ./scripts/precache_2022_2025.sh
#   ./scripts/precache_2022_2025.sh --skip-existing
#   ./scripts/precache_2022_2025.sh --calendar-source auto
#   ./scripts/precache_2022_2025.sh --continue-on-error

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

.venv/bin/python f1-telemetry-producer/src/precache_seasons.py \
  --years 2022 2023 2024 2025 \
  --session R \
  --calendar-source fallback \
  --post-race-buffer-seconds 120 \
  --retry-attempts 4 \
  --retry-delay-seconds 20 \
  --sleep-between-races 2 \
  "$@"
