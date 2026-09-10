# Bước 2 — Nguồn dữ liệu hiện hành

**Hanoi Flood & Climate Risk Monitor** · v2.2 · 2026-09-08

## 1. Source inventory

| Nguồn | Vai trò | Đích đầu tiên |
|---|---|---|
| Open-Meteo Forecast API | Forecast mưa theo giờ | `silver.stg_weather_forecast` |
| Open-Meteo Historical Weather API | Archive ERA5 | `silver.stg_weather_archive_hourly` |
| Open-Meteo Historical Forecast API | Archive ECMWF IFS | `silver.stg_weather_archive_hourly` |
| `ward_coordinates_seed.csv` | 126 phường/xã và centroid | dbt seed |
| `ward_grid_map_seed.csv` | Mapping ward → archive grid | dbt seed |
| S13 GeoJSON | Polygon hành chính cho dashboard/geocode | reference file |
| `hanoi_flood_points_seed.csv` | 15 điểm úng có nguồn/tọa độ | dbt seed |
| Flood observation seeds | Ghi nhận báo chí + kết quả người soát | dbt seed |

## 2. Forecast

Forecast production yêu cầu đủ 126 location và 72 giờ trước khi publish. Mỗi
lần retrieval có `forecast_run_id` riêng; các run không ghi đè nhau để pressure
mart tính persistence và revision.

Open-Meteo có thể trả nhiều requested location về cùng grid. Silver canonicalize
tọa độ 6 chữ số và dedup trong từng run. Snapshot hiện hành có 48 forecast grid
cho 126 phường/xã.

## 3. Archive

- Trước 2017 dùng ERA5, 12 grid cho Hà Nội.
- Từ 2017 dùng ECMWF IFS archive, 48 grid.
- Hai model không được ghép thành một phân phối đồng nhất.
- Fetch theo grid thay vì lặp request cho từng phường.
- Raw JSON giữ bất biến trong `bronze/files/` để có thể replay parser.

Archive dùng cho trang phát lại mưa quá khứ; không được diễn giải là quan trắc
trạm tại từng phường.

## 4. Geography và flood reference

Ward code giữ dạng `VARCHAR` có zero-padding. Polygon S13 phải phủ đúng tập 126
code trong seed trước khi thay file dashboard.

Danh mục điểm úng chỉ publish các dòng có nguồn và tọa độ. Nhãn số lượng trong
văn bản QĐ 2280 không được dùng để tự tạo thêm point không có vị trí.

Flood observation và geocode được tách thành hai seed: file quan sát có thể được
tạo lại từ nguồn, còn file geocode chứa công sức review thủ công. Chỉ người soát
mới đặt `geocode_verified = TRUE`; model Gold tiếp tục yêu cầu nguồn, giờ và ward
trước khi đặt `is_replay_eligible = TRUE`.

## 5. Giới hạn và giấy phép

- Open-Meteo Free API là best-effort, không có uptime guarantee.
- Dữ liệu công bố phải có attribution theo điều khoản nguồn.
- Forecast/reanalysis grid không có độ phân giải đường phố.
- Không có radar, camera, mạng cống, DEM chi tiết hoặc mực nước sông trong
  serving contract.
- Quan sát báo chí không phải sampling frame đầy đủ cho calibration xác suất.

Request budget, retry, object naming và recovery được mô tả trong
[04b-ingestion-runbook.md](04b-ingestion-runbook.md).
