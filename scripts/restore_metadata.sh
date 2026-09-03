#!/usr/bin/env bash
# Restore a validated PostgreSQL metadata backup. Credentials stay in the environment.

set -euo pipefail
umask 077

POSTGRES_HOST=${POSTGRES_HOST:-${PGHOST:-127.0.0.1}}
POSTGRES_PORT=${POSTGRES_PORT:-${PGPORT:-5432}}
POSTGRES_DB=${POSTGRES_DB:-${PGDATABASE:-vnclimate}}
POSTGRES_USER=${POSTGRES_USER:-${PGUSER:-vnclimate}}
PG_RESTORE_BIN=${PG_RESTORE_BIN:-pg_restore}
: "${BACKUP_FILE:?Đặt BACKUP_FILE tới file .dump cần phục hồi}"

if [[ -n ${POSTGRES_PASSWORD+x} ]]; then
  export PGPASSWORD=$POSTGRES_PASSWORD
fi
export PGHOST=$POSTGRES_HOST PGPORT=$POSTGRES_PORT PGDATABASE=$POSTGRES_DB PGUSER=$POSTGRES_USER
export PGAPPNAME=${PGAPPNAME:-vn-climate-metadata-restore}

command -v "$PG_RESTORE_BIN" >/dev/null 2>&1 || {
  echo "Không tìm thấy $PG_RESTORE_BIN; hãy cài PostgreSQL client hoặc đặt PG_RESTORE_BIN." >&2
  exit 127
}
[[ $POSTGRES_PORT =~ ^[0-9]+$ ]] || {
  echo "POSTGRES_PORT phải là số nguyên." >&2
  exit 2
}
[[ -f $BACKUP_FILE && ! -L $BACKUP_FILE ]] || {
  echo "BACKUP_FILE phải là file thường, không phải symlink: $BACKUP_FILE" >&2
  exit 2
}
[[ ${RESTORE_CONFIRM:-} == "$POSTGRES_DB" ]] || {
  echo "Restore sẽ thay thế object hiện có trong database '$POSTGRES_DB'." >&2
  echo "Đặt RESTORE_CONFIRM=$POSTGRES_DB để xác nhận." >&2
  exit 2
}

checksum_file=${BACKUP_FILE}.sha256
case ${RESTORE_VERIFY_CHECKSUM:-1} in
  1)
    command -v sha256sum >/dev/null 2>&1 || {
      echo "Không tìm thấy sha256sum." >&2
      exit 127
    }
    [[ -f $checksum_file && ! -L $checksum_file ]] || {
      echo "Thiếu checksum an toàn: $checksum_file" >&2
      echo "Chỉ đặt RESTORE_VERIFY_CHECKSUM=0 nếu artifact đến từ nguồn tin cậy khác." >&2
      exit 2
    }
    (
      cd "$(dirname "$BACKUP_FILE")"
      sha256sum --check "$(basename "$checksum_file")"
    )
    ;;
  0) ;;
  *)
    echo "RESTORE_VERIFY_CHECKSUM chỉ nhận 0 hoặc 1." >&2
    exit 2
    ;;
esac

# Validate the custom archive before opening a transaction against PostgreSQL.
"$PG_RESTORE_BIN" --list "$BACKUP_FILE" >/dev/null

schema_args=()
if [[ -n ${POSTGRES_SCHEMAS:-} ]]; then
  read -r -a schema_names <<< "$POSTGRES_SCHEMAS"
  for schema in "${schema_names[@]}"; do
    schema_args+=(--schema="$schema")
  done
fi

"$PG_RESTORE_BIN" \
  --clean \
  --if-exists \
  --exit-on-error \
  --single-transaction \
  --no-owner \
  --no-privileges \
  --dbname="$POSTGRES_DB" \
  "${schema_args[@]}" \
  "$BACKUP_FILE"

printf "Restore PostgreSQL vào database '%s' hoàn tất từ %s\n" "$POSTGRES_DB" "$BACKUP_FILE"
