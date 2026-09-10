#!/usr/bin/env bash
# Xác minh cấu trúc và checksum của backup mà không thay đổi hệ thống đích.

set -euo pipefail

: "${LAKEHOUSE_BACKUP_DIR:?Đặt LAKEHOUSE_BACKUP_DIR tới thư mục lakehouse_<timestamp>}"
PG_RESTORE_BIN=${PG_RESTORE_BIN:-pg_restore}

[[ -d $LAKEHOUSE_BACKUP_DIR && ! -L $LAKEHOUSE_BACKUP_DIR ]] || {
  echo "LAKEHOUSE_BACKUP_DIR phải là thư mục thường." >&2
  exit 2
}
[[ -f $LAKEHOUSE_BACKUP_DIR/backup.env && ! -L $LAKEHOUSE_BACKUP_DIR/backup.env ]] || {
  echo "Thiếu backup.env hợp lệ." >&2
  exit 2
}

bucket=$(sed -n 's/^minio_bucket=//p' "$LAKEHOUSE_BACKUP_DIR/backup.env")
[[ $bucket =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
  echo "minio_bucket trong backup.env không hợp lệ." >&2
  exit 2
}
[[ -d $LAKEHOUSE_BACKUP_DIR/minio/$bucket ]] || {
  echo "Thiếu MinIO backup cho bucket $bucket." >&2
  exit 2
}
[[ -s $LAKEHOUSE_BACKUP_DIR/minio-manifest.jsonl ]] || {
  echo "Thiếu MinIO manifest hoặc manifest rỗng." >&2
  exit 2
}
[[ -s $LAKEHOUSE_BACKUP_DIR/minio-checksums.sha256 ]] || {
  echo "Thiếu checksum cho MinIO objects." >&2
  exit 2
}

command -v sha256sum >/dev/null 2>&1 || {
  echo "Không tìm thấy sha256sum." >&2
  exit 127
}
command -v "$PG_RESTORE_BIN" >/dev/null 2>&1 || {
  echo "Không tìm thấy $PG_RESTORE_BIN; hãy cài PostgreSQL client hoặc đặt PG_RESTORE_BIN." >&2
  exit 127
}

mapfile -t dumps < <(find "$LAKEHOUSE_BACKUP_DIR/postgres" -maxdepth 1 -type f -name '*.dump')
[[ ${#dumps[@]} -eq 1 ]] || {
  echo "Backup phải chứa đúng một PostgreSQL .dump; tìm thấy ${#dumps[@]}." >&2
  exit 2
}
dump=${dumps[0]}
[[ -f ${dump}.sha256 && ! -L ${dump}.sha256 ]] || {
  echo "Thiếu checksum PostgreSQL an toàn: ${dump}.sha256" >&2
  exit 2
}

(
  cd "$LAKEHOUSE_BACKUP_DIR"
  sha256sum --check minio-checksums.sha256
)
(
  cd "$(dirname "$dump")"
  sha256sum --check "$(basename "${dump}.sha256")"
)
"$PG_RESTORE_BIN" --list "$dump" >/dev/null

printf 'Backup lakehouse hợp lệ: %s (bucket %s)\n' "$LAKEHOUSE_BACKUP_DIR" "$bucket"
