#!/usr/bin/env bash
set -euo pipefail

log() {
  # UTC timestamp to keep logs consistent across hosts
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $*"
}

# Resolve database connection settings.
resolve_db_dump() {
  if [[ -n "${DATABASE_URL:-}" ]]; then
    pg_dump "$DATABASE_URL"
    return
  fi

  local host="${DB_HOST:-db}"
  local port="${DB_PORT:-5432}"
  local name="${DB_NAME:-wattattack}"
  local user="${DB_USER:-wattattack}"
  local password="${DB_PASSWORD:-wattattack}"

  PGPASSWORD="$password" pg_dump -h "$host" -p "$port" -U "$user" "$name"
}

psql_admin_ids() {
  local sql="SELECT tg_id FROM admins WHERE tg_id IS NOT NULL"
  if [[ -n "${DATABASE_URL:-}" ]]; then
    PGPASSWORD='' psql "$DATABASE_URL" -At -c "$sql"
  else
    local host="${DB_HOST:-db}"
    local port="${DB_PORT:-5432}"
    local name="${DB_NAME:-wattattack}"
    local user="${DB_USER:-wattattack}"
    local password="${DB_PASSWORD:-wattattack}"
    PGPASSWORD="$password" psql -h "$host" -p "$port" -U "$user" -d "$name" -At -c "$sql"
  fi
}

gather_chat_ids() {
  local raw_admins
  local chats=()
  if raw_admins="$(psql_admin_ids 2>/dev/null)"; then
    while IFS= read -r line; do
      if [[ "$line" =~ ^[0-9]+$ ]]; then
        chats+=("$line")
      fi
    done <<<"$raw_admins"
  else
    log "warn: failed to load admins from database; will use env fallbacks"
  fi

  local fallback="${BACKUP_TG_CHAT_ID:-}"
  local extra_ids="${BACKUP_TG_CHAT_IDS:-}"
  if [[ -n "$extra_ids" ]]; then
    if [[ -n "$fallback" ]]; then
      fallback="${fallback},${extra_ids}"
    else
      fallback="${extra_ids}"
    fi
  fi

  if [[ -n "$fallback" ]]; then
    IFS=',' read -ra parts <<<"$fallback"
    for part in "${parts[@]}"; do
      part="${part//[[:space:]]/}"
      if [[ -n "$part" ]]; then
        chats+=("$part")
      fi
    done
  fi

  # deduplicate
  local uniq=()
  local seen=""
  for id in "${chats[@]}"; do
    if [[ ",${seen}," != *,"${id}",* ]]; then
      uniq+=("$id")
      seen="${seen}${id},"
    fi
  done
  echo "${uniq[@]:-}"
}

BOT_TOKEN="${BACKUP_TG_BOT_TOKEN:-${KRUTILKAFIT_BOT_TOKEN:-}}"
CHAT_IDS=($(gather_chat_ids))

if [[ -z "$BOT_TOKEN" ]]; then
  log "Telegram bot token is required (BACKUP_TG_BOT_TOKEN or KRUTILKAFIT_BOT_TOKEN)"
  exit 1
fi

if [[ ${#CHAT_IDS[@]} -eq 0 ]]; then
  log "No chat ids found. Add admins via bot or set BACKUP_TG_CHAT_ID/BACKUP_TG_CHAT_IDS."
  exit 1
fi

RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
CAPTION_TEMPLATE="${BACKUP_TG_CAPTION:-DB backup}"

timestamp="$(date -u +'%Y%m%d-%H%M%S')"
outfile="/backups/db-${timestamp}.sql.gz"

log "starting backup to ${outfile}"
mkdir -p /backups
umask 077

if ! resolve_db_dump | gzip > "$outfile"; then
  log "backup failed during pg_dump"
  exit 1
fi

caption="${CAPTION_TEMPLATE} ${timestamp} UTC"
for chat in "${CHAT_IDS[@]}"; do
  log "sending backup to Telegram chat ${chat}"
  if ! curl --fail -sS -X POST \
    -F "chat_id=${chat}" \
    -F "caption=${caption}" \
    -F "document=@${outfile}" \
    "https://api.telegram.org/bot${BOT_TOKEN}/sendDocument" >/dev/null; then
    log "failed to send backup to chat ${chat}"
  fi
done

log "sent backup, applying retention ${RETENTION_DAYS}d"
find /backups -type f -name 'db-*.sql.gz' -mtime +"${RETENTION_DAYS}" -delete || true
log "backup finished"
