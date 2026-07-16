#!/usr/bin/env bash
# Send Amazon offer update tasks from cart, ads, and catalog seeds (em-spapi-celery).
#
# Run with defaults (no args):
#   ./scripts/amz_offers_update_task_sender.sh
#
# Run with explicit CLI args:
#   ./scripts/amz_offers_update_task_sender.sh \
#     -s ~/.em_celery/gcs-sa.json \
#     -q 20
#
# Broker URL is passed via BROKER_URL env (not -b) so passwords with
# shell metacharacters ($, `, ", \, !) are not re-interpreted by bash.
# Use single quotes when exporting, or URL-encode special chars in the password.
#
#   export BROKER_URL='redis://:p@ssw0rd@host:6379/0'
#   ./scripts/amz_offers_update_task_sender.sh
#
# TTL is read from [amz.offer.filter.{mp}] in EM_SPAPI_CELERY_CONFIG
# (expire_hour / cart_expire_hour / ads_expire_hour); missing keys abort.
#
# Direct equivalents:
#   uv run amz_offers_update_task_sender -s ~/.em_celery/gcs-sa.json -b 'redis://...' -q 20
#   uv run python -m carts_amz_offers.cli -s ~/.em_celery/gcs-sa.json -b 'redis://...' -q 20
#
# Limit to one or more marketplaces:
#   ./scripts/amz_offers_update_task_sender.sh -m US
#   ./scripts/amz_offers_update_task_sender.sh -m US -m CA
#   ./scripts/amz_offers_update_task_sender.sh -m US,CA,DE
# (default: all 15 marketplaces)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GCS_SA="${GCS_SA:-${HOME}/.em_celery/gcs-sa.json}"
: "${BROKER_URL:=redis://127.0.0.1:6379/0}"
export BROKER_URL
QPS="${QPS:-20}"

cd "${PROJECT_ROOT}"

# Always apply -s/-q defaults; extra CLI args (e.g. -m AE) append after.
# If the caller also passes -s/-q, Click keeps the last value.
exec uv run amz_offers_update_task_sender \
  -s "${GCS_SA}" \
  -q "${QPS}" \
  "$@"
