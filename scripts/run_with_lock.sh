#!/usr/bin/env bash
# Run a command under an exclusive non-blocking flock.
#
#   ./scripts/run_with_lock.sh amz_offers_update_tr ./scripts/run_amz_offers_update_task_sender.sh TR
#
# Lock files live under ~/.em_celery/locks/<name>.lock
# If the lock is busy, exit 0 so cron does not spam failure mail.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <lock_name> <command> [args...]" >&2
  exit 2
fi

LOCK_NAME="$1"
shift

LOCK_DIR="${AMZ_OFFERS_LOCK_DIR:-${HOME}/.em_celery/locks}"
LOG_PREFIX="[amz_offers_lock ${LOCK_NAME}]"
mkdir -p "${LOCK_DIR}"
LOCK_FILE="${LOCK_DIR}/${LOCK_NAME}.lock"
PID_FILE="${LOCK_DIR}/${LOCK_NAME}.pid"

_lock_holders() {
  if command -v fuser >/dev/null 2>&1; then
    fuser "${LOCK_FILE}" 2>/dev/null | tr -s ' ' '\n' | sed '/^$/d' || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -t "${LOCK_FILE}" 2>/dev/null || true
  fi
}

_release_lock() {
  flock -u 9 2>/dev/null || true
  exec 9>&- 2>/dev/null || true
  rm -f "${PID_FILE}" 2>/dev/null || true
}

_on_signal() {
  local sig="$1"
  echo "${LOG_PREFIX} caught ${sig}; releasing lock ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >&2
  _release_lock
  case "${sig}" in
    INT) exit 130 ;;
    TERM) exit 143 ;;
    TSTP) exit 148 ;;
    *) exit 1 ;;
  esac
}

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  holders="$(_lock_holders | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  stale_pid=""
  if [[ -f "${PID_FILE}" ]]; then
    stale_pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  fi
  echo "${LOG_PREFIX} skipped: already running ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
  if [[ -n "${holders}" ]]; then
    echo "${LOG_PREFIX} holders: ${holders}  (kill to release: kill ${holders})" >&2
  elif [[ -n "${stale_pid}" ]]; then
    echo "${LOG_PREFIX} pidfile=${stale_pid} (no open holders; try: rm -f ${PID_FILE})" >&2
  fi
  exit 0
fi

echo "$$" >"${PID_FILE}"
trap '_on_signal INT' INT
trap '_on_signal TERM' TERM
trap '_on_signal TSTP' TSTP
trap '_release_lock' EXIT

echo "${LOG_PREFIX} acquired pid=$$ $(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
"$@"
status=$?
set -e
echo "${LOG_PREFIX} released status=${status} $(date -u +%Y-%m-%dT%H:%M:%SZ)"
exit "${status}"
