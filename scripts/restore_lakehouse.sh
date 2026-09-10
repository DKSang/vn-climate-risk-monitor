#!/usr/bin/env bash
# Restore một backup lakehouse đã quiesce. Thao tác thay thế object ở bucket đích.

set -euo pipefail
umask 077

: "${LAKEHOUSE_BACKUP_DIR:?Đặt LAKEHOUSE_BACKUP_DIR tới thư mục lakehouse_<timestamp>}"
MINIO_BUCKET=${MINIO_BUCKET:-vn-climate}
MC_TARGET_ALIAS=${MC_TARGET_ALIAS:-lakehouse}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

# Verify toàn bộ artifact trước khi kiểm tra xác nhận destructive restore.
LAKEHOUSE_BACKUP_DIR=$LAKEHOUSE_BACKUP_DIR \
  "$script_dir/verify_lakehouse_backup.sh"

[[ ${RESTORE_QUIESCED:-} == 1 ]] || {
  echo "Dừng mọi writer rồi đặt RESTORE_QUIESCED=1." >&2
  exit 2
}
[[ ${RESTORE_LAKEHOUSE_CONFIRM:-} == "$MINIO_BUCKET" ]] || {
  echo "Đặt RESTORE_LAKEHOUSE_CONFIRM=$MINIO_BUCKET để xác nhận thay bucket đích." >&2
  exit 2
}
[[ -d $LAKEHOUSE_BACKUP_DIR && ! -L $LAKEHOUSE_BACKUP_DIR ]] || {
  echo "LAKEHOUSE_BACKUP_DIR phải là thư mục thường." >&2
  exit 2
}
[[ -d $LAKEHOUSE_BACKUP_DIR/minio/$MINIO_BUCKET ]] || {
  echo "Thiếu MinIO backup cho bucket $MINIO_BUCKET." >&2
  exit 2
}

command -v mc >/dev/null 2>&1 || {
  echo "Không tìm thấy mc; hãy cài MinIO Client và cấu hình alias '$MC_TARGET_ALIAS'." >&2
  exit 127
}

mapfile -t dumps < <(find "$LAKEHOUSE_BACKUP_DIR/postgres" -maxdepth 1 -type f -name '*.dump')
[[ ${#dumps[@]} -eq 1 ]] || {
  echo "Backup phải chứa đúng một PostgreSQL .dump; tìm thấy ${#dumps[@]}." >&2
  exit 2
}

mc mirror --overwrite --remove \
  "$LAKEHOUSE_BACKUP_DIR/minio/$MINIO_BUCKET" "$MC_TARGET_ALIAS/$MINIO_BUCKET"
BACKUP_FILE=${dumps[0]} RESTORE_CONFIRM=${POSTGRES_DB:-vnclimate} \
  "$script_dir/restore_metadata.sh"

printf 'Restore lakehouse hoàn tất từ %s\n' "$LAKEHOUSE_BACKUP_DIR"
