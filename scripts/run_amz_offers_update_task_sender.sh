#!/usr/bin/env bash
# VPS cron wrapper: one marketplace per invocation, with flock.
# Cron (every 4 hours, offset from repricer): 45 */4 * * *
#
#   /home/Admin/scripts/run_amz_offers_update_task_sender.sh TR \
#     >> /tmp/amz_offers_update_task_sender_tr.log 2>&1
#
# If a previous run for the same marketplace still holds the lock, exit 0.
# Env from: /home/Admin/.em_celery/amz_offers_sender.env

set -euo pipefail

MARKETPLACE="${1:?Usage: $0 <MARKETPLACE>  (e.g. US, CA, DE, TR)}"
ENV_FILE="${AMZ_OFFERS_SENDER_ENV:-${HOME}/.em_celery/amz_offers_sender.env}"
APP_ROOT="${AMZ_OFFERS_APP_ROOT:-${HOME}/amz-offers-task-sender}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prefer cron/stdout logs; avoid duplicating into the rotating file handler.
export EM_SPAPI_CELERY_LOG_TO_FILE="${EM_SPAPI_CELERY_LOG_TO_FILE:-0}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
fi

# Re-read APP_ROOT after sourcing env (env may override).
APP_ROOT="${AMZ_OFFERS_APP_ROOT:-${APP_ROOT}}"

mp_lower="$(echo "${MARKETPLACE}" | tr '[:upper:]' '[:lower:]')"
LOCK_NAME="amz_offers_update_${mp_lower}"

# Nested re-entry after flock acquired.
if [[ "${AMZ_OFFERS_HOLDING_LOCK:-0}" == "1" ]]; then
  cd "${APP_ROOT}"
  exec ./scripts/amz_offers_update_task_sender.sh -m "${MARKETPLACE}"
fi

export AMZ_OFFERS_HOLDING_LOCK=1
exec "${SCRIPT_DIR}/run_with_lock.sh" "${LOCK_NAME}" \
  "${SCRIPT_DIR}/run_amz_offers_update_task_sender.sh" "${MARKETPLACE}"
