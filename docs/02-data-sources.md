# Bước 2 — Xác định & Đánh giá nguồn dữ liệu

**Hanoi Flood & Climate Risk Monitor** · v0.4 (đơn giản hóa) · 2026-08-21 · *Trạng thái: CHỜ DUYỆT*

> **Thay đổi so với v0.2:** đã thử mở rộng sang radar, camera HSDC, GloFAS, elevation, METAR —
> quá phức tạp cho quy mô dự án này. **Chốt lại: chỉ dùng Open-Meteo (Archive + Forecast) làm
> nguồn thời tiết duy nhất**, cộng với dữ liệu tham chiếu tĩnh (ranh giới hành chính, kịch bản
> mưa/úng ngập chính thức). Toàn bộ kết quả khảo sát mở rộng lưu ở cuối file để không mất công nếu
> cần dùng lại sau.

---

## 1. Nguồn dùng thật (giai đoạn 1)

| # | Nguồn | Vai trò | Update | Access |
|---|---|---|---|---|
| **S1** | **Open-Meteo Forecast API** | Mưa dự báo theo giờ, 1–16 ngày tới | Giờ | Free API, no key, non-commercial |
| **S2** | **Open-Meteo Historical Weather API** | Reanalysis lịch sử, dùng xây baseline khí hậu | Ngày (trễ khoảng 5 ngày) | Free API, no key, non-commercial |
| **S5** | **QĐ 2280/QĐ-UBND** — kịch bản mưa/úng ngập | Chuyển mưa một giờ → scenario vận hành | Năm, tĩnh | Văn bản chính thức |
| **S13** | **vietnamese-provinces-database** — polygon 126 phường-xã | Ranh giới + mã hành chính, gán điểm lưới → phường | Theo nghị quyết, tĩnh | GitHub raw, mở |

Bốn nguồn này đủ cho **rainfall hazard/pressure MVP**: *lấy mưa → tính rolling/peak → so
kịch bản vận hành → projection sang địa bàn*. Chúng không đủ để dự báo xác suất, độ sâu hoặc
thời gian rút nước khi ngập.

Open-Meteo Free API không có uptime guarantee và yêu cầu attribution theo CC BY
4.0. Đây là ràng buộc nguồn, không chỉ là chi tiết triển khai; xem cost/request
guardrail tại [04-ingestion.md](04-ingestion.md).

## 2. Cách dùng cụ thể

### S1 — Open-Meteo Forecast
- `https://api.open-meteo.com/v1/forecast?latitude=...&longitude=...&hourly=precipitation&models=best_match`
- Gọi **multi-point 1 request** cho toàn bộ điểm lưới cần thiết (đã xác minh hoạt động).
- MVP hiện đề xuất `models=best_match`, nhưng đây là chuỗi ghép model và có thể thay đổi theo
  thời gian. Phải lưu retrieval vintage, model request và tọa độ grid thực nhận. Việc pin một
  model cụ thể để tăng khả năng tái lập còn là quyết định K6 ở Bước 5.

### S2 — Open-Meteo Archive
- `https://archive-api.open-meteo.com/v1/archive?start_date=...&end_date=...&hourly=precipitation`
- **Hai model theo thời kỳ** (`open_meteo.py::model_for_month`), không phải một
  biến môi trường. IFS không có dữ liệu trước 2017 (probe 2026-08-28: 2016 mọi
  quý NULL) — bỏ ERA5 là mất toàn bộ 2000–2016.

  | thời kỳ | `models=` | độ phân giải | ô Hà Nội | bảng bronze |
  |---|---|---|---|---|
  | trước 2017 | `era5` | 0,25° (~25 km) | 12 | `open_meteo_archive` |
  | từ 2017-01 | `ecmwf_ifs` | ~9 km | 48 | `open_meteo_ifs` |

- Canary H3 xác nhận ERA5 có đủ rain/precipitation và soil moisture. `era5_land`
  0,1° đã bị loại vì ba trường mưa/weather code trả toàn `null` trong canary
  01/2000. `era5_seamless` bị loại vì toạ độ mịn nhưng mưa vẫn là ERA5 0,25° dán
  lại — trùng lặp bị giấu, hỏng dedup theo ô.
- Backfill từ `2000-01-01`, fetch theo ô lưới (không theo 126 phường). Không dùng
  Best Match trôi nổi cho baseline dài hạn.
- `ecmwf_ifs` là chuỗi phân tích nghiệp vụ, không phải reanalysis. Không so
  trực tiếp trung bình trước/sau mốc 2017; `weather_model` nằm trong mọi khoá
  join. Phân vị khí hậu tính **trong từng model**, không gộp hai phân phối.

### Độ phân giải — chấp nhận là ràng buộc, không cố giải quyết
Đã đo thực tế (xem phụ lục): **126 phường-xã Hà Nội → 49 ô lưới mưa phân biệt được** với
forecast `best_match`. Archive: 12 ô ERA5 (trước 2017) và 48 ô IFS (từ 2017).
Đây là giới hạn của mọi nguồn mô hình dự báo số trị, không riêng Open-Meteo.

→ **Quyết định đơn giản hóa:** không cố vá bằng nguồn thứ hai (radar/camera). Chấp nhận
độ phân giải ô lưới, thiết kế đúng theo nó:
- Tính mưa theo **ô lưới** (không theo từng phường riêng).
- Gán mỗi phường-xã vào ô lưới chứa centroid của nó (dùng S13).
- Áp lực mưa của phường = forcing của ô lưới nó thuộc về. Phường cùng ô thì cùng scenario — **đúng bản
  chất dữ liệu, không giả vờ chính xác hơn thực tế.**

### S5 — Mưa một giờ → kịch bản vận hành (từ QĐ 2280/QĐ-UBND)

| Mưa một giờ | Số điểm úng ngập trong kịch bản toàn TP | Scenario code |
|---|---|---|
| `< 50 mm` | Cơ bản không ngập; vẫn có thể ứ đọng cục bộ | `below_50` |
| `50–<70 mm` | 11 | `from_50_to_under_70` |
| `70–100 mm` | 71 | `from_70_to_100` |
| `>100 mm`; văn bản yêu cầu kéo dài nhiều giờ | 220 | `over_100` |

Đây là bộ quy tắc **if/else minh bạch** trên tổng mưa một giờ/peak một giờ. Không gọi các
scenario này là cấp rủi ro pháp lý hoặc xác suất ngập. Điều kiện “kéo dài nhiều giờ” phải được
định nghĩa riêng trước khi coi kịch bản cuối đã thỏa đầy đủ.

### S13 — Ranh giới + mã 126 phường-xã
- `https://raw.githubusercontent.com/ThangLeQuoc/vietnamese-provinces-database/master/json/geojson/01_ha_noi/wards/*.geojson`
- Đã tải và xác minh: đúng 126 file, tổng diện tích 3.360 km² khớp số liệu chính thức.
- Dùng centroid từng phường làm điểm gọi Open-Meteo → khỏi cần tự vẽ lưới, tự chọn điểm lấy mẫu.
- **Ghim commit SHA cụ thể khi ingest**, không lấy `master` trôi nổi (repo có thể cập nhật theo
  nghị quyết hành chính mới).

## 3. Việc thủ công cần làm

1. Ghim bản chính thức QĐ 2280 và checksum/version của văn bản; bốn dải mưa đã được đối chiếu
   trực tiếp trong quyết định. Phụ lục điểm úng ngập cần pipeline nhập và review riêng nếu dùng.
2. Tải 126 file GeoJSON từ S13, tính centroid, ghim version.
3. Viết hàm gán phường-xã → ô lưới Open-Meteo gần nhất (point trùng ô nào thì nhận ô đó).

> **Trạng thái tư liệu (2026-08-22):** centroid đã vào pipeline qua dbt seed
> `transform/seeds/ward_coordinates_seed.csv`. Còn `reference/s13_wards/*.geojson`
> và `reference/hanoi_ward_centroids.csv` là **tư liệu gốc chưa được model/script
> nào tiêu thụ** — giữ làm bằng chứng nguồn, đừng hiểu nhầm là đang chạy.

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

- Áp lực mưa tính theo **ô lưới model**, không phải theo từng phường. Phường trong cùng ô lưới
  hiển thị cùng forcing/scenario dự báo.
- "Ngập" ở đây chỉ là **áp lực thời tiết và scenario vận hành dựa trên mưa**, không phải mô
  phỏng thủy lực hay xác suất ngập đã hiệu chỉnh.
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
