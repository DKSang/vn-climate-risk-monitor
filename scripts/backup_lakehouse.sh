#!/usr/bin/env bash
# Backup nhất quán PostgreSQL catalog/control plane và toàn bộ MinIO bucket.

set -euo pipefail
umask 077

: "${LAKEHOUSE_BACKUP_ROOT:?Đặt LAKEHOUSE_BACKUP_ROOT tới filesystem backup độc lập}"
[[ ${BACKUP_QUIESCED:-} == 1 ]] || {
  echo "Dừng mọi writer rồi đặt BACKUP_QUIESCED=1." >&2
  exit 2
}

MC_SOURCE_ALIAS=${MC_SOURCE_ALIAS:-lakehouse}
MINIO_BUCKET=${MINIO_BUCKET:-vn-climate}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
target=$LAKEHOUSE_BACKUP_ROOT/lakehouse_$timestamp
temporary=$LAKEHOUSE_BACKUP_ROOT/.lakehouse_$timestamp.tmp

command -v mc >/dev/null 2>&1 || {
  echo "Không tìm thấy mc; hãy cài MinIO Client và cấu hình alias '$MC_SOURCE_ALIAS'." >&2
  exit 127
}
[[ $LAKEHOUSE_BACKUP_ROOT = /* && $LAKEHOUSE_BACKUP_ROOT != / ]] || {
  echo "LAKEHOUSE_BACKUP_ROOT phải là đường dẫn tuyệt đối khác /." >&2
  exit 2
}
[[ ! -e $target && ! -e $temporary ]] || {
  echo "Backup target đã tồn tại: $target" >&2
  exit 2
}

mkdir -p "$temporary/minio/$MINIO_BUCKET" "$temporary/postgres"
cleanup() { rm -rf -- "$temporary"; }
trap cleanup EXIT HUP INT TERM

mc mirror --preserve "$MC_SOURCE_ALIAS/$MINIO_BUCKET" "$temporary/minio/$MINIO_BUCKET"
mc ls --recursive --json "$MC_SOURCE_ALIAS/$MINIO_BUCKET" >"$temporary/minio-manifest.jsonl"
BACKUP_DIR="$temporary/postgres" "$script_dir/backup_metadata.sh"

# Checksum mirrored MinIO objects before publishing the backup.
(
  cd "$temporary"
  find minio -type f -print0 | sort -z | xargs -0 -r sha256sum \
    >minio-checksums.sha256
)
[[ -s $temporary/minio-checksums.sha256 ]] || {
  echo "MinIO backup rỗng; từ chối publish artifact." >&2
  exit 2
}

printf 'created_at_utc=%s\nminio_bucket=%s\n' "$timestamp" "$MINIO_BUCKET" \
  >"$temporary/backup.env"
mv -- "$temporary" "$target"
trap - EXIT HUP INT TERM
printf 'Backup lakehouse hoàn tất: %s\n' "$target"
