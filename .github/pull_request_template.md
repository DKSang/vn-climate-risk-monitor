## Mục tiêu

Mô tả vấn đề và outcome; không chỉ liệt kê file đã sửa.

## Data impact

- Change class: `implementation-only | compatible | semantic | breaking | emergency`
- Sources/models/consumers bị ảnh hưởng:
- Grain/key/unit/timezone có đổi không:
- Backfill/full-refresh cần thiết và `REASON`:
- License, attribution, privacy hoặc retention impact:

## Validation evidence

- [ ] Unit tests và Ruff
- [ ] `dbt parse` và dbt tests theo phạm vi
- [ ] Source freshness và dbt quality tests nếu đổi ingestion hoặc transform
- [ ] Health `--require-gold` và consumer smoke test nếu đổi serving
- [ ] Tài liệu/contract/runbook/risk register đã cập nhật
- [ ] Không có secret, credential hoặc dữ liệu nhạy cảm trong diff/log

Ghi lệnh và kết quả quan trọng:

## Rollout và rollback

- Rollout/published snapshot:
- Backward compatibility hoặc migration window:
- Rollback/reprocess procedure:
- Risk còn chấp nhận, owner và trigger xem lại:
