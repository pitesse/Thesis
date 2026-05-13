#!/usr/bin/env bash
set -euo pipefail

# Quick health check for F1 live timing endpoints used by FastF1.
# Returns non-zero when both official and mirror are unavailable.
#
# Usage:
#   ./scripts/check_f1_api_health.sh
#   ./scripts/check_f1_api_health.sh --path "/static/2022/2022-03-20_Bahrain_Grand_Prix/2022-03-20_Race/"

API_PATH="/static/2022/2022-03-20_Bahrain_Grand_Prix/2022-03-20_Race/"
TIMEOUT=20

while [[ $# -gt 0 ]]; do
	case "$1" in
	--path)
		API_PATH="$2"
		shift 2
		;;
	--timeout)
		TIMEOUT="$2"
		shift 2
		;;
	*)
		echo "Unknown argument: $1" >&2
		exit 2
		;;
	esac
done

BASE="https://livetiming.formula1.com"
MIRROR="https://livetiming-mirror.fastf1.dev"
UA="BestHTTP"
HEADERS=(-H "User-Agent: ${UA}" -H "Connection: close" -H "TE: identity" -H "Accept-Encoding: gzip, identity")

ENDPOINTS=(
	"SessionInfo.jsonStream"
	"DriverList.jsonStream"
	"TimingDataF1.jsonStream"
)

ok_count=0
echo "FastF1 health check path: ${API_PATH}"
for ep in "${ENDPOINTS[@]}"; do
	base_code=$(curl -sS -L -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" "${HEADERS[@]}" "${BASE}${API_PATH}${ep}" || echo "000")
	mirror_code=$(curl -sS -L -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" "${HEADERS[@]}" "${MIRROR}${API_PATH}${ep}" || echo "000")
	echo "$(printf '%-24s' "$ep") base=${base_code} mirror=${mirror_code}"
	if [[ "$base_code" == "200" || "$mirror_code" == "200" ]]; then
		ok_count=$((ok_count + 1))
	fi
done

if [[ "$ok_count" -eq "${#ENDPOINTS[@]}" ]]; then
	echo "FastF1 health: OK"
	exit 0
fi

echo "FastF1 health: UNAVAILABLE (upstream deny/rate-limit/outage likely)"
exit 1
