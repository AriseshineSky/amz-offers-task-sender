#!/usr/bin/env bash
# Enqueue offer updates at priority 0 (critical) from Amazon URLs / ASINs.
# Does not clear existing queues (unlike amz_offers_update_task_sender.sh).
#
# File format (one per line):
#   https://www.amazon.com/dp/B00WW3LSUO
#   B012345678
#
# Usage:
#   ./scripts/send_urgent_item_offers.sh /tmp/amz_urgent_links.txt
#   LINKS_FILE=/tmp/amz_urgent_links.txt ./scripts/send_urgent_item_offers.sh
#   ./scripts/send_urgent_item_offers.sh /tmp/links.txt -q 10
#
# Env:
#   EM_SPAPI_CELERY_CONFIG  required by em-spapi-celery (e.g. ~/.em_celery/config.ini)
#   BROKER_URL              default: redis://127.0.0.1:6379/0
#   QPS                     default: 20
#   PRIORITY                default: 0
#   LINKS_FILE              used when no file arg is given

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export EM_SPAPI_CELERY_CONFIG="${EM_SPAPI_CELERY_CONFIG:-${HOME}/.em_celery/config.ini}"
: "${BROKER_URL:=redis://127.0.0.1:6379/0}"
export BROKER_URL
QPS="${QPS:-20}"
PRIORITY="${PRIORITY:-0}"

LINKS_FILE="${LINKS_FILE:-}"
EXTRA_ARGS=()
if [[ $# -gt 0 && "$1" != -* ]]; then
  LINKS_FILE="$1"
  shift
  EXTRA_ARGS=("$@")
elif [[ -n "${LINKS_FILE}" ]]; then
  EXTRA_ARGS=("$@")
else
  echo "Usage: $0 <links_file> [extra amz_offers_urgent_task_sender args...]" >&2
  echo "   or: LINKS_FILE=/path/to/links.txt $0" >&2
  exit 1
fi

if [[ ! -f "${LINKS_FILE}" ]]; then
  echo "Links file not found: ${LINKS_FILE}" >&2
  exit 1
fi

cd "${PROJECT_ROOT}"

exec uv run amz_offers_urgent_task_sender \
  -p "${PRIORITY}" \
  -q "${QPS}" \
  "${EXTRA_ARGS[@]}" \
  "${LINKS_FILE}"
