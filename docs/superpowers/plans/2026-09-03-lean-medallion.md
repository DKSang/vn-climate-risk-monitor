# Medallion gọn — thiết kế lại

**Ngày:** 2026-09-03 · **Thay thế:** 27 model / 15 bảng gold của thiết kế 2026-08-31

---

## 1. Vì sao bản cũ phình

| Triệu chứng | Số liệu |
|---|---|
| Hai bảng Bronze schema y hệt nhau | `open_meteo_archive` + `open_meteo_ifs`, hai file SQL lệch **một dòng** |
| Ba dạng ánh xạ phường→ô lưới | `ward_grid_map`, `bridge_hanoi_ward_era5_grid`, `bridge_hanoi_ward_forecast_grid` |
| Hai nguồn cho cùng danh mục phường | `pg_source.public.wards` **và** `ward_coordinates_seed.csv` |
| Gold cho tính năng chưa ai hỏi | climatology, anomaly, event, drought, replay = 8/15 bảng |
| Cột lặp theo cửa sổ | `rain_Nh_mm` + `_coverage_ratio` + `_is_complete` × 7 cửa sổ = 21 cột |

Không cái nào sai về kỹ thuật. Chúng chỉ trả lời câu hỏi chưa được hỏi, và mỗi
cái là một chỗ phải sửa khi schema đổi.

## 2. Nguyên tắc

1. **Một bảng Bronze cho một hợp đồng dữ liệu**, không phải cho một endpoint.
   Model phân biệt bằng cột, không bằng bảng.
2. **Một nguồn cho một thực thể.** Phường chỉ đến từ seed. Bỏ `pg_source`.
3. **Chiếu về phường ở grain NGÀY, không phải giờ.** 126 phường chỉ rơi vào 12
   (era5) hoặc 48 (ifs) ô — chiếu theo giờ nhân bản 26 triệu dòng giống nhau.
   Câu hỏi theo giờ ở cấp phường join bridge lúc query; bridge có 252 dòng.
4. **NULL đã mang nghĩa "cửa sổ thiếu dữ liệu".** Không cần cột `_is_complete`
   song song.
5. **Chỉ xây thứ đang có câu hỏi.** Climatology/drought/event thêm sau, từng cái
   một, không phải cùng lúc.

## 3. Kiến trúc

```
BRONZE  (autoloader, KHÔNG dùng dbt)
  open_meteo_hourly     era5 2000–2016 (12 ô) + ecmwf_ifs 2017–nay (48 ô)   19,9M dòng
  open_meteo_forecast   forecast run                                          20k dòng

SEED    (input artifact, version-control)
  ward_coordinates_seed  3.321 phường/xã VN, có centroid
  ward_grid_map_seed     phường → ô lưới, lấy từ CHÍNH phép snap của Open-Meteo
                         (nearest-neighbour tự tính LỆCH so với cái API trả về)

SILVER  (view — dedup + conform, KHÔNG business logic)
  weather_hourly   dedup theo (model, ô, giờ), canonical lat/lon 6 số, + grid_cell_id
  ward             126 phường Hà Nội, lọc từ 3.321
  ward_grid        phường × model → grid_cell_id

GOLD    (table)
  dim_grid              ô lưới                                    60 dòng
  dim_ward              phường                                   126 dòng
  bridge_ward_grid      phường × model → ô                       252 dòng
  fct_rain_hourly       (grid_cell_id, valid_time_utc)  incremental
  fct_rain_daily        (grid_cell_id, rain_date)
  fct_ward_rain_daily   (ward_code, rain_date)
```

**3 silver view + 6 gold table = 9 model** (cũ: 27).

## 4. Grain và khoá

| Bảng | Grain | Khoá |
|---|---|---|
| `fct_rain_hourly` | ô lưới × giờ | `rain_hourly_key = MD5(grid_cell_id \| valid_time_utc)` |
| `fct_rain_daily` | ô lưới × ngày | `rain_daily_key = MD5(grid_cell_id \| rain_date)` |
| `fct_ward_rain_daily` | phường × ngày | `ward_rain_daily_key = MD5(ward_code \| model \| rain_date)` |

`weather_model` nằm trong `grid_cell_id`, nên era5 và ecmwf_ifs không bao giờ
đụng khoá nhau dù hai thời kỳ liền nhau (2016→2017).

## 5. Cửa sổ trượt — khai báo một chỗ

Macro `rolling_rain(windows)` sinh cột `rain_{N}h_mm` cho mỗi N. Thêm cửa sổ 72h
sau này = sửa một danh sách, không phải sửa 3 model.

`rain_{N}h_mm` **NULL khi cửa sổ thiếu giờ** — đó là toàn bộ ngữ nghĩa
completeness, không có cột phụ.

Cửa sổ phase này: **1, 3, 6, 12, 24**. Bỏ 48/72 (chỉ drought cần, ngoài phạm vi)
— nhờ đó lookback incremental giảm từ 71h xuống **23h**.

## 6. Ngưỡng nghiệp vụ

Macro `rain_band(...)` giữ ngưỡng QĐ 2280 / quy chuẩn VN ở MỘT chỗ:

| Band | Cửa sổ | Ngưỡng |
|---|---|---|
| `hanoi_rain_scenario_band` | 1h | <50 / 50–<70 / 70–100 / >100 |
| `vn_rain_band_12h` | 12h | <50 / 50–100 / >100 |
| `vn_rain_band_24h` | 24h | <100 / 100–200 / >200–400 / >400 |

## 7. Incremental

Chỉ `fct_rain_hourly` incremental (bảng lớn nhất). Hai fact còn lại full refresh
— chúng nhỏ và phụ thuộc rolling window đã tính, nên rebuild rẻ hơn là quản lý
thêm hai checkpoint.

Dùng đúng framework đã có: `processing.processing_state` + macro
`incremental_input_scope` / `incremental_output_scope`, lookback `23 hours`.

## 8. Ngoài phạm vi (thêm sau, từng cái)

- **Forecast Gold.** Bronze đã có. Grain khác (`as_of` vintage) nên là fact
  riêng, dùng lại macro `rolling_rain` + `rain_band`. Không cần đụng lõi.
- Climatology 1991–2020, anomaly, drought, event segmentation, replay window.
- Điểm ngập QĐ 2280 (Q1), OSM đường (Q3), mực nước sông (Q9).
- SCD2 ranh giới phường.

## 9. Đã bỏ hẳn

| Bỏ | Lý do |
|---|---|
| `models/bronze/*` (5 model) | Bronze là việc của autoloader. gso_* chỉ là bản sao của `pg_source` |
| `pg_source` trong profiles + sources | Nguồn thứ hai cho cùng danh mục phường |
| `fct_*_forecast_*` (4) | Ngoài phạm vi phase này, Bronze vẫn giữ |
| `fct_rainfall_climatology/anomaly/drought/event/replay` (6) | Chưa có câu hỏi |
| `_coverage_ratio`, `_is_complete` (14 cột) | NULL đã đủ nghĩa |
