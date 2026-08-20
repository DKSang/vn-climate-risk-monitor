# Bước 2 — Xác định & Đánh giá nguồn dữ liệu

**Hanoi Flood & Climate Risk Monitor** · v0.3 (đơn giản hóa) · 2026-08-20 · *Trạng thái: CHỜ DUYỆT*

> **Thay đổi so với v0.2:** đã thử mở rộng sang radar, camera HSDC, GloFAS, elevation, METAR —
> quá phức tạp cho quy mô dự án này. **Chốt lại: chỉ dùng Open-Meteo (Archive + Forecast) làm
> nguồn thời tiết duy nhất**, cộng với dữ liệu tham chiếu tĩnh (ranh giới hành chính, ngưỡng
> ngập chính thức). Toàn bộ kết quả khảo sát mở rộng lưu ở cuối file để không mất công nếu
> cần dùng lại sau.

---

## 1. Nguồn dùng thật (giai đoạn 1)

| # | Nguồn | Vai trò | Update | Access |
|---|---|---|---|---|
| **S1** | **Open-Meteo Forecast API** | Mưa dự báo theo giờ, 1–16 ngày tới | Giờ | API mở, no key |
| **S2** | **Open-Meteo Archive API (ERA5)** | Mưa lịch sử 1981–nay, dùng làm chuẩn khí hậu | Ngày (trễ ~5 ngày) | API mở, no key |
| **S5** | **QĐ 2280/QĐ-UBND** — ngưỡng mưa gây ngập | Chuyển mm/h → cấp độ rủi ro | Năm, tĩnh | Nhập tay từ văn bản |
| **S13** | **vietnamese-provinces-database** — polygon 126 phường-xã | Ranh giới + mã hành chính, gán điểm lưới → phường | Theo nghị quyết, tĩnh | GitHub raw, mở |

**Bốn nguồn này là đủ để trả lời toàn bộ câu hỏi phân tích ở Bước 1 §6** — vì bản chất bài toán
chỉ là: *lấy mưa (Open-Meteo) → so ngưỡng (QĐ 2280) → gắn vào địa bàn (S13)*.

## 2. Cách dùng cụ thể

### S1 — Open-Meteo Forecast
- `https://api.open-meteo.com/v1/forecast?latitude=...&longitude=...&hourly=precipitation&models=best_match`
- Gọi **multi-point 1 request** cho toàn bộ điểm lưới cần thiết (đã xác minh hoạt động).
- Dùng `models=best_match` — đã đo, cho độ phân giải hiệu dụng cao nhất trong các model sẵn có.

### S2 — Open-Meteo Archive
- `https://archive-api.open-meteo.com/v1/archive?start_date=1981-01-01...&daily=precipitation_sum`
- Dùng để tính **chuẩn khí hậu 1991–2020**: trung bình, phân vị theo ngày-trong-năm cho mỗi điểm lưới.
- So sánh mưa hiện tại với chuẩn này → ra được "bất thường bao nhiêu %" (trả lời Q2, Q8 ở Bước 1).

### Độ phân giải — chấp nhận là ràng buộc, không cố giải quyết
Đã đo thực tế (xem phụ lục): **126 phường-xã Hà Nội → 49 ô lưới mưa phân biệt được** với
`best_match`. Đây là giới hạn của mọi nguồn mô hình dự báo số trị, không riêng Open-Meteo.

→ **Quyết định đơn giản hóa:** không cố vá bằng nguồn thứ hai (radar/camera). Chấp nhận
độ phân giải ô lưới, thiết kế đúng theo nó:
- Tính mưa theo **ô lưới** (không theo từng phường riêng).
- Gán mỗi phường-xã vào ô lưới chứa centroid của nó (dùng S13).
- Rủi ro phường = rủi ro của ô lưới nó thuộc về. Phường cùng ô thì cùng mức rủi ro — **đúng bản
  chất dữ liệu, không giả vờ chính xác hơn thực tế.**

### S5 — Ngưỡng mưa → rủi ro ngập (từ QĐ 2280/QĐ-UBND)

| Mưa (mm/h) | Số điểm ngập toàn TP | Cấp rủi ro đề xuất |
|---|---|---|
| < 50 | ~0 (vài vị trí cục bộ) | 1 — Thấp |
| 50–70 | 11 | 2 — Trung bình |
| 70–100 | 71 | 3 — Cao |
| > 100 (kéo dài) | 220 | 4 — Rất cao |

Đây là bộ quy tắc **if/else đơn giản** trên giá trị `precipitation` giờ lớn nhất trong cửa sổ
đang xét — không cần mô hình, không cần ML.

### S13 — Ranh giới + mã 126 phường-xã
- `https://raw.githubusercontent.com/ThangLeQuoc/vietnamese-provinces-database/master/json/geojson/01_ha_noi/wards/*.geojson`
- Đã tải và xác minh: đúng 126 file, tổng diện tích 3.360 km² khớp số liệu chính thức.
- Dùng centroid từng phường làm điểm gọi Open-Meteo → khỏi cần tự vẽ lưới, tự chọn điểm lấy mẫu.
- **Ghim commit SHA cụ thể khi ingest**, không lấy `master` trôi nổi (repo có thể cập nhật theo
  nghị quyết hành chính mới).

## 3. Việc thủ công cần làm

1. Lấy nội dung ngưỡng mưa từ QĐ 2280 (đã có đủ 4 mức ở bảng trên từ báo chí — đủ dùng cho GĐ1,
   không bắt buộc phải có toàn văn phụ lục 220 điểm ngay).
2. Tải 126 file GeoJSON từ S13, tính centroid, ghim version.
3. Viết hàm gán phường-xã → ô lưới Open-Meteo gần nhất (point trùng ô nào thì nhận ô đó).

## 4. Đã loại khỏi phạm vi (giữ lại ghi chú để không điều tra lại)

| Nguồn | Lý do loại |
|---|---|
| GloFAS / Open-Meteo Flood API | Ô lưới không khớp lòng sông chính (đã kiểm chứng: Hà Nội trả 1,3–11,8 m³/s, thực tế sông Hồng mùa lũ hàng nghìn m³/s). Cần hiệu chỉnh thủ công tốn công, không đáng cho GĐ1 |
| Open-Meteo Elevation / DEM | Chỉ trả số nguyên mét, không đủ mịn để tự tìm điểm trũng |
| Radar RainViewer | Có phủ VN thật, nhưng không có lịch sử (chỉ ~2h) và không phải dự báo — không hợp với nhịp cảnh báo trước 3h+ |
| Camera/trạm đo HSDC | Dữ liệu tốt nhất về lý thuyết nhưng không có API công khai |
| METAR sân bay | Trường lượng mưa rỗng ở mọi trạm VN đã thử |
| NCHMF, mực nước sông | Chưa khảo sát, không cần cho GĐ1 vì QĐ 2280 đã cho ngưỡng sẵn |
| OSM ranh giới phường-xã | Thay bằng S13 — có mã hành chính chính thức, OSM thì không |

## 5. Giới hạn đã biết (nói rõ cho người dùng, không giấu)

- Rủi ro tính theo **ô lưới ~11km**, không phải theo từng phường. Phường trong cùng ô lưới sẽ
  hiển thị cùng một mức rủi ro dự báo.
- "Ngập" ở đây là **rủi ro dựa trên ngưỡng mưa của thành phố**, không phải mô phỏng thủy lực.
  Không tính đến tình trạng cống rãnh thực tế tại thời điểm dự báo.
- Không có cách nào trong nguồn hiện tại để **xác nhận** dự báo có đúng hay không (không có
  quan trắc ngập thực tế) — chấp nhận là giới hạn giai đoạn 1.

---

## Phụ lục — Kết quả khảo sát mở rộng (đã làm, không dùng ở GĐ1, giữ để tham khảo sau)

<details>
<summary>Bấm để xem — radar, camera HSDC, GloFAS, METAR, độ phân giải theo model</summary>

**Đo độ phân giải theo model (2026-08-20), dùng centroid thật của 126 phường:**

| Model | Số ô lưới | Ô đông nhất gom |
|---|---|---|
| `best_match` | 49 | 15 phường |
| `ukmo_seamless` | 28 | 23 phường |
| `icon_seamless` | 25 | 28 phường |
| ECMWF/GFS/JMA/GEM/ARPEGE | không tách nổi 2 điểm cách 5km | |

**Radar RainViewer:** có phủ Bắc Bộ (đã giải mã tile, so đối chứng vùng biển/sa mạc trả tile
rỗng). Chỉ ~13 frame quá khứ (~2h), không có nowcast dài, không có lịch sử — không hợp cho
cảnh báo trước 3h+ hay backfill.

**Camera/trạm đo HSDC Maps:** Công ty Thoát nước Hà Nội có 31 camera giám sát mưa/ngập thời gian
thực, hiển thị qua app HSDC Maps và iHanoi. Không tìm thấy API công khai.

**GloFAS (Open-Meteo Flood API):** endpoint `flood-api.open-meteo.com/v1/flood`, biến
`river_discharge`. Test tại tọa độ trung tâm Hà Nội trả về lưu lượng phi thực tế thấp — ô lưới
không trùng lòng sông chính, cần chọn tay điểm kiểm soát mới dùng được.

**METAR:** `aviationweather.gov/api/data/metar?ids=VVNB,VVCI,VVTS` — trường `precip1h` rỗng ở
mọi trạm Việt Nam đã thử.

Nếu sau này muốn nâng cấp độ chính xác (GĐ2+), thứ tự ưu tiên nên là: (1) tiếp cận HSDC nếu có
đường chính thức, (2) radar cho lớp "hiện trạng" (không thay dự báo), (3) GloFAS đã hiệu chỉnh
điểm kiểm soát cho lũ sông lớn.

</details>
