#!/usr/bin/env bash
set -euo pipefail

CRON_ENV="/app/cron.env"
CRON_SCHEDULE="${BACKUP_CRON_SCHEDULE:-0 3 * * *}"

# Persist runtime environment for cron jobs (cron spawns with a minimal env).
env | sort > "$CRON_ENV"

if [[ -n "${TZ:-}" && -f "/usr/share/zoneinfo/${TZ}" ]]; then
  ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime
  echo "$TZ" > /etc/timezone
fi

cat >/etc/crontabs/root <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
${CRON_SCHEDULE} /bin/bash -lc '. ${CRON_ENV} && /app/backup.sh >> /proc/1/fd/1 2>&1'
EOF

echo "Loaded cron schedule: ${CRON_SCHEDULE}"
exec crond -f -l 2
