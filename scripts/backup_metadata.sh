#!/usr/bin/env bash
# Back up PostgreSQL metadata/control state without placing credentials in the artifact.

set -euo pipefail
umask 077

POSTGRES_HOST=${POSTGRES_HOST:-${PGHOST:-127.0.0.1}}
POSTGRES_PORT=${POSTGRES_PORT:-${PGPORT:-5432}}
POSTGRES_DB=${POSTGRES_DB:-${PGDATABASE:-vnclimate}}
POSTGRES_USER=${POSTGRES_USER:-${PGUSER:-vnclimate}}
BACKUP_DIR=${BACKUP_DIR:-backups/postgres}
PG_DUMP_BIN=${PG_DUMP_BIN:-pg_dump}
PG_RESTORE_BIN=${PG_RESTORE_BIN:-pg_restore}

if [[ -n ${POSTGRES_PASSWORD_FILE:-} ]]; then
  [[ -r $POSTGRES_PASSWORD_FILE ]] || {
    echo "Không đọc được POSTGRES_PASSWORD_FILE." >&2
    exit 2
  }
  PGPASSWORD=$(<"$POSTGRES_PASSWORD_FILE")
  export PGPASSWORD
elif [[ -n ${POSTGRES_PASSWORD+x} ]]; then
  export PGPASSWORD=$POSTGRES_PASSWORD
fi
export PGHOST=$POSTGRES_HOST PGPORT=$POSTGRES_PORT PGDATABASE=$POSTGRES_DB PGUSER=$POSTGRES_USER
export PGAPPNAME=${PGAPPNAME:-vn-climate-metadata-backup}

command -v "$PG_DUMP_BIN" >/dev/null 2>&1 || {
  echo "Không tìm thấy $PG_DUMP_BIN; hãy cài PostgreSQL client hoặc đặt PG_DUMP_BIN." >&2
  exit 127
}
command -v "$PG_RESTORE_BIN" >/dev/null 2>&1 || {
  echo "Không tìm thấy $PG_RESTORE_BIN; hãy cài PostgreSQL client hoặc đặt PG_RESTORE_BIN." >&2
  exit 127
}
command -v sha256sum >/dev/null 2>&1 || {
  echo "Không tìm thấy sha256sum." >&2
  exit 127
}
[[ $POSTGRES_PORT =~ ^[0-9]+$ ]] || {
  echo "POSTGRES_PORT phải là số nguyên." >&2
  exit 2
}

mkdir -p "$BACKUP_DIR"
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
backup_file=${BACKUP_FILE:-$BACKUP_DIR/${POSTGRES_DB}_metadata_${timestamp}.dump}
backup_dir=$(dirname "$backup_file")
backup_name=$(basename "$backup_file")
checksum_file=${backup_file}.sha256
mkdir -p "$backup_dir"

if [[ -L $backup_file || -L $checksum_file ]]; then
  echo "Từ chối ghi backup/checksum qua symlink." >&2
  exit 2
fi
if [[ (-e $backup_file && ! -f $backup_file) || (-e $checksum_file && ! -f $checksum_file) ]]; then
  echo "Đường dẫn backup/checksum hiện có phải là file thường." >&2
  exit 2
fi
if [[ -e $backup_file || -e $checksum_file ]]; then
  if [[ ${BACKUP_OVERWRITE:-0} != 1 ]]; then
    echo "Backup đã tồn tại; đặt BACKUP_OVERWRITE=1 để thay thế: $backup_file" >&2
    exit 2
  fi
fi

tmp_backup=$(mktemp "$backup_dir/.${backup_name}.tmp.XXXXXX")
tmp_checksum=$(mktemp "$backup_dir/.${backup_name}.sha256.tmp.XXXXXX")
cleanup() {
  rm -f -- "$tmp_backup" "$tmp_checksum"
}
trap cleanup EXIT HUP INT TERM

schema_args=()
if [[ -n ${POSTGRES_SCHEMAS:-} ]]; then
  read -r -a schema_names <<< "$POSTGRES_SCHEMAS"
  for schema in "${schema_names[@]}"; do
    schema_args+=(--schema="$schema")
  done
fi

"$PG_DUMP_BIN" \
  --format=custom \
  --compress=6 \
  --no-owner \
  --no-privileges \
  --file="$tmp_backup" \
  "${schema_args[@]}"

# Publish only after the archive and checksum are valid.
"$PG_RESTORE_BIN" --list "$tmp_backup" >/dev/null
digest=$(sha256sum "$tmp_backup")
printf '%s  %s\n' "${digest%% *}" "$backup_name" >"$tmp_checksum"
mv -f -- "$tmp_backup" "$backup_file"
mv -f -- "$tmp_checksum" "$checksum_file"
trap - EXIT HUP INT TERM

printf 'Backup PostgreSQL hoàn tất:\n  %s\n  %s\n' "$backup_file" "$checksum_file"
