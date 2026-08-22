#!/usr/bin/env bash
# Backfill Open-Meteo Archive theo từng năm, mặc định 2001 → nay.
#
# Resumable: fetch tự bỏ qua TỪNG FILE đã có, nên interrupt rồi chạy lại chỉ
# đi tiếp phần thiếu — năm đã xong chỉ tốn listing MinIO, không tốn request.
#
# Dùng chung lock /tmp/vn-climate-open-meteo.lock với cron forecast và tail
# (pacer chỉ bảo vệ trong một tiến trình — các job phải loại trừ nhau bằng lock).
#
# Dùng:
#   make backfill-archive              # 2001 → nay
#   make backfill-archive FROM=2003    # từ 2003 tới nay
#   scripts/backfill_archive.sh [FROM_YEAR] [TO_YEAR]
#
# Xem kế hoạch KHÔNG tốn quota: make fetch-archive START=2001-01-01 END=2001-12-01

set -euo pipefail
cd "$(dirname "$0")/.."

FROM=${1:-2001}
TO=${2:-$(date +%Y)}
[ "$TO" -gt "$(date +%Y)" ] && TO=$(date +%Y)

# Năm hiện tại chỉ lấy tới THÁNG TRƯỚC: 2 tháng gần nhất do cron tail bồi
# hằng ngày, và tháng hiện tại chưa có dữ liệu ERA5 (trễ ~5 ngày).
END_CUR=$(date -d "last month" +%Y-%m-01)

exec 9>/tmp/vn-climate-open-meteo.lock
flock -n 9 || { echo "Đã có job Open-Meteo khác đang giữ lock — thử lại sau."; exit 1; }

for year in $(seq "$FROM" "$TO"); do
  if [ "$year" = "$(date +%Y)" ]; then end=$END_CUR; else end="$year-12-01"; fi
  echo
  echo "════ Năm $year (tháng 01 → ${end#*-}) ════"
  make fetch-archive EXEC=1 START="$year-01-01" END="$end"
  make load SOURCE=open_meteo_archive
done
