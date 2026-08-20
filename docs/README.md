# Hanoi Flood & Climate Risk Monitor — Tài liệu dự án

Theo dõi rủi ro **ngập úng / lũ / hạn hán** cho Hà Nội, chi tiết đến **126 phường-xã** và
**tuyến đường**.

## Quy trình 9 bước

| # | Bước | File | Trạng thái |
|---|---|---|---|
| 1 | Bắt đầu từ bài toán nghiệp vụ | [01-business-problem.md](01-business-problem.md) | ✅ Xong (chờ duyệt) |
| 2 | Xác định & đánh giá nguồn dữ liệu | [02-data-sources.md](02-data-sources.md) | ✅ Xong (chờ duyệt) |
| 3 | Thiết kế kiến trúc | `03-architecture.md` | ⬜ Chưa bắt đầu |
| 4 | Ingest dữ liệu | `04-ingestion.md` | ⬜ |
| 5 | Clean & Transform | `05-transformation.md` | ⬜ |
| 6 | Lưu trữ — single source of truth | `06-storage-modeling.md` | ⬜ |
| 7 | Data Quality & Observability | `07-data-quality.md` | ⬜ |
| 8 | Make it accessible | `08-serving-bi.md` | ⬜ |
| 9 | Governance & Continuous Improvement | `09-governance.md` | ⬜ |

## Nguyên tắc làm việc

- **Không sang bước sau khi bước trước chưa được duyệt.**
- Mọi khẳng định về nguồn dữ liệu phải được **kiểm chứng bằng cách gọi thật**, không đọc doc rồi tin.
- Mọi giả định ghi rõ dạng `A1`, `A2`… để trace ngược.
- Mọi rủi ro ghi rõ dạng `R1`, `R2`… kèm cách xử lý.
