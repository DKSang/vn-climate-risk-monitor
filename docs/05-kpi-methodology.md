# Bước 5 — Transform và KPI hiện hành

**Hanoi Flood & Climate Risk Monitor** · v2.2 · 2026-09-08

Tài liệu này chỉ mô tả contract đang chạy và có consumer rõ ràng.

## 1. Nguyên tắc sản phẩm

- Chỉ công bố lượng mưa và tín hiệu **áp lực mưa** giải thích được.
- Không gọi bất kỳ output nào là xác suất ngập, độ sâu ngập hoặc cảnh báo KTTV.
- Không thay `NULL` bằng 0 khi một cửa sổ thiếu giờ.
- Forecast history giữ riêng từng `forecast_run_id`; không trộn vintage.
- ERA5 và ECMWF IFS archive luôn giữ riêng theo `weather_model`.

## 2. Grain và thời gian

| Model | Grain | Vai trò |
|---|---|---|
| `silver.int_weather_archive_hourly` | grid × giờ | archive đã dedup |
| `silver.int_weather_forecast_hourly` | run × grid × giờ | forecast history đã dedup |
| `gold.fct_rain_archive_hourly` | grid × giờ | replay mưa quá khứ |
| `gold.fct_rain_forecast_hourly` | run × grid × giờ | forecast history và rainfall windows |
| `gold.fct_rain_forecast_current_hourly` | run mới nhất × grid × giờ còn hạn | serving view |
| `gold.fct_rain_pressure_alert` | run × phường × giờ | tín hiệu vận hành |

Timestamp lưu bằng UTC. Dashboard đổi sang `Asia/Ho_Chi_Minh` ở ranh giới hiển
thị. Biên archive replay dùng khoảng nửa mở `[start, end)`.

## 3. Rainfall windows

Danh sách cửa sổ hiện hành là `1/3/6/12/24h`, khai báo một lần trong
`transform/macros/rainfall.sql`.

Trailing window mô tả lượng mưa đã rơi đến thời điểm `t`:

```text
rain_Hh_mm(t) = sum precipitation trong (t-H, t]
```

Forward window mô tả lượng mưa forecast sau `t`, không trộn giờ hiện tại hoặc
quá khứ:

```text
forecast_next_Hh_mm(t) = sum precipitation trong (t, t+H]
```

Mỗi tổng chỉ có giá trị khi đủ đúng `H` quan sát theo giờ. Window SQL dùng
`RANGE ... INTERVAL`, vì vậy gap thời gian không bị che giấu bằng cách kéo thêm
row cũ.

## 4. Pressure signal

Ngưỡng được version-control trong `transform/dbt_project.yml`:

| Level | Điều kiện chính |
|---|---|
| `HIGH` | next 1h ≥ 70 mm, next 3h ≥ 50 mm, hoặc next 6h ≥ 100 mm |
| `ELEVATED` | next 6h ≥ 50 mm, next 24h ≥ 100 mm, hoặc next 6h vượt ngưỡng WATCH trong 3 run |
| `WATCH` | next 6h ≥ 30 mm, next 24h ≥ 50 mm, next 6h vượt ngưỡng WATCH trong 2 run, hoặc forecast 24h tăng ≥ 10 mm |
| `UNKNOWN` | không có trigger đã biết nhưng thiếu ít nhất một input 1/3/6/24h |
| `NORMAL` | đủ input 1/3/6/24h và không thỏa điều kiện cảnh báo |

`pressure_score` là giá trị 0–100 lấy theo tỷ lệ lớn nhất của các cửa sổ hiện có so với
ngưỡng cao tương ứng. Đây là điểm ưu tiên vận hành, không phải phần trăm xác
suất. Score là `NULL` khi không còn cửa sổ nào đủ giờ. `coverage_status` cho biết
input là `COMPLETE`, `PARTIAL` hay `NONE`; thiếu dữ liệu không bao giờ được hiểu
thành 0 mm hoặc `NORMAL`.

`persistence_runs` đếm số run có next 6h ≥ ngưỡng WATCH trong ba forecast
run gần nhất tại cùng grid và valid time. `revision_24h_mm` so tổng 24 giờ
của run mới nhất với run trước; `revision_direction` là `UNKNOWN`, `NEW`,
`RISING`, `FALLING` hoặc `STABLE`.
`trigger_reasons` công bố lý do để người dùng không phải suy ngược từ score.

## 5. Archive replay và dữ liệu ngập

Archive dashboard đọc trực tiếp `gold.fct_rain_archive_hourly` rồi chiếu grid
sang phường bằng `gold.bridge_ward_grid`. Không materialize fact ngày hay fact
phường × ngày khi chưa có consumer.

`gold.dim_flood_point` là danh mục điểm úng từ QĐ 2280.
`gold.fct_flood_event_observation` chỉ chứa ghi nhận báo chí đã geocode và được
người soát; các dòng đủ nguồn, thời gian và phường có `is_replay_eligible =
TRUE`. Chúng dùng để đối chiếu trực quan, không dùng để huấn luyện hoặc chấm
điểm pressure.

## 6. Quality contract

- Unique/not-null theo grain của từng relation.
- Forecast run phải đủ 72 giờ và đủ 126 location trước khi publish.
- Giờ đầu mỗi run/grid phải có đủ forward coverage 24 giờ.
- Rolling windows không giảm khi mở rộng cửa sổ.
- Pressure level thuộc `UNKNOWN/NORMAL/WATCH/ELEVATED/HIGH`; `NORMAL` bắt buộc
  có coverage hoàn chỉnh và score chỉ NULL khi coverage là `NONE`.
- `bridge_ward_grid` phải phủ đúng một mapping cho mỗi phường và weather model.

Các heuristic pressure là ngưỡng vận hành của portfolio. Khi có dữ liệu nhãn
đại diện gồm cả trường hợp ngập và không ngập, calibration/backtest phải được
thiết kế thành một phase riêng thay vì để model nghiên cứu không có consumer
trong production DAG.
