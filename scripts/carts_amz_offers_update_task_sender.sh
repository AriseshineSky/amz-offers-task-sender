#!/usr/bin/env bash
# Send Amazon offer update tasks from cart analytics seeds (em-spapi-celery).
#
# Run with defaults (no args):
#   ./scripts/carts_amz_offers_update_task_sender.sh
#
# Run with explicit CLI args:
#   ./scripts/carts_amz_offers_update_task_sender.sh \
#     -s ~/.em_celery/gcs-sa.json \
#     -b 'redis://127.0.0.1:6379/0' \
#     -q 20 -t 72
#
# Override defaults via env when running with no args:
#   TTL=24 QPS=10 ./scripts/carts_amz_offers_update_task_sender.sh
#
# Direct equivalents:
#   uv run carts_amz_offers_update_task_sender -s ~/.em_celery/gcs-sa.json -b 'redis://...' -q 20 -t 72
#   uv run python -m carts_amz_offers.cli -s ~/.em_celery/gcs-sa.json -b 'redis://...' -q 20 -t 72

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GCS_SA="${GCS_SA:-${HOME}/.em_celery/gcs-sa.json}"
BROKER_URL="${BROKER_URL:-redis://127.0.0.1:6379/0}"
QPS="${QPS:-20}"
TTL="${TTL:-24}"

cd "${PROJECT_ROOT}"

if [[ $# -gt 0 ]]; then
  exec uv run carts_amz_offers_update_task_sender "$@"
fi

exec uv run carts_amz_offers_update_task_sender \
  -s "${GCS_SA}" \
  -b "${BROKER_URL}" \
  -q "${QPS}" \
  -t "${TTL}"
