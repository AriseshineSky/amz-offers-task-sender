#!/usr/bin/env bash
# VPS cron wrapper: one marketplace per invocation.
# Cron: 15 */4 * * * (every 4 hours). Each run clears that MP's offer queue
# before enqueue (clear_marketplace_offer_queue in CLI).
#
#   /home/Admin/em-scripts/run_amz_offers_update_task_sender.sh DE \
#     >> /home/Admin/.em_celery/logs/amz_offers_update_task_sender_de.log 2>&1
#
# Env from: /home/Admin/.em_celery/amz_offers_sender.env
set -euo pipefail

MARKETPLACE="${1:?Usage: $0 <MARKETPLACE>  (e.g. US, CA, DE)}"
ENV_FILE="${AMZ_OFFERS_SENDER_ENV:-${HOME}/.em_celery/amz_offers_sender.env}"
APP_ROOT="${AMZ_OFFERS_APP_ROOT:-${HOME}/amz-offers-task-sender}"

# Prefer cron/stdout logs; avoid duplicating into the rotating file handler.
export EM_SPAPI_CELERY_LOG_TO_FILE="${EM_SPAPI_CELERY_LOG_TO_FILE:-0}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
fi

cd "${APP_ROOT}"
exec ./scripts/amz_offers_update_task_sender.sh -m "${MARKETPLACE}"
