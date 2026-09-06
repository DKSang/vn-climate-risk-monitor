# Phương pháp KPI mưa lớn và áp lực ngập đô thị

> **Đã thay thế một phần (2026-09-03).** Kiến trúc chốt hiện tại ở
> [plan lean medallion](superpowers/plans/2026-09-03-lean-medallion.md) và
> [plan Silver Layer Flow](superpowers/plans/2026-09-03-silver-layer-flow.md):
> MỘT catalog DuckLake (`catalog1`), Bronze chỉ còn là landing zone raw file,
> bảng append-only của autoloader nay là `silver.stg_*`.
> Cấu trúc dbt chuẩn hoá 3 lớp: `staging/` -> `intermediate/` -> `marts/`.
> Fact/dim gọn lại thành `dim_grid`, `dim_ward`, `bridge_ward_grid`, `fct_rain_archive_hourly`,
> `fct_rain_archive_daily`, `fct_ward_rain_archive_daily`.

**Hanoi Flood & Climate Risk Monitor** · MVP v1.0 · 2026-08-22

**Trạng thái:** Gold forecast và Gold lịch sử đã được triển khai và kiểm thử
(`dbt build` 2026-08-31: PASS=212). Baseline là empirical theo từng
`weather_model` + tháng lịch trên archive sẵn có, không phải WMO 1991–2020.
Replay bốn cửa sổ §9 là scenario case, không phải nhãn ngập. Sản phẩm vẫn
chỉ công bố feature áp lực mưa, không công bố xác suất ngập.

Phạm vi đã triển khai:

- Silver chọn đúng một forecast run hoàn chỉnh mới nhất;
- bridge 126 phường sang returned forecast grid;
- rolling 1/3/6/12/24/48/72 giờ (`rain_*h_mm` NULL khi incomplete);
- tổng mưa/peak forecast horizon 3/6/12/24/48 giờ;
- scenario band 50/70/100 bằng CASE trên `rain_1h_mm`;
- dải mưa QĐ 18/2021 12h/24h (`vn_rain_band_*`, không suy ra cấp độ pháp lý);
- projection KPI grid sang 126 phường và fixture test các boundary quan trọng;
- historical rolling/daily, climatology theo model+tháng, anomaly p95/p99;
- drought input 30/60/90 ngày và event rainfall `wet_gt_0_1mm_six_dry_hours_v1`;
- replay bốn cửa sổ lịch sử chiếu xuống 126 phường.

## 1. Mục tiêu

Tài liệu này định nghĩa logic, công thức, đơn vị và giới hạn diễn giải cho các
KPI được sinh từ Open-Meteo. Nó là contract giữa:

- Bronze: dữ liệu nguồn theo giờ, giữ nguyên forecast vintage;
- Silver: dữ liệu đã chuẩn hóa semantics, thời gian, model và grid;
- Gold: KPI mưa, kịch bản vận hành và feature phục vụ nghiên cứu nguy cơ ngập.

Mục tiêu giai đoạn 1 là trả lời:

> Trong 6–24 giờ tới, ô lưới nào tại Hà Nội chịu áp lực mưa lớn; các phường và
> điểm úng ngập chính thức nào nằm trong vùng đó; forecast đang thuộc kịch bản
> mưa vận hành nào của Thành phố?

Hệ thống hiện đo **meteorological forcing** — tác động đầu vào từ thời tiết. Khi
chưa có DEM đủ chi tiết, mạng cống, hồ điều hòa, trạng thái trạm bơm, mực nước và
nhãn ngập thực tế, kết quả không được gọi là xác suất ngập hoặc dự báo độ sâu
ngập.

## 2. Ngôn ngữ sản phẩm

### 2.1 Được phép công bố

```text
rainfall_hazard_features
hanoi_rain_scenario_band
pluvial_weather_pressure_features
threshold_exceedance_probability  # chỉ khi tính từ ensemble members
```

### 2.2 Chưa được phép công bố

```text
flood_probability
flood_depth
flooded_area
expected_flooded_points
official_disaster_risk_level
```

`risk_score` của `gold.fct_flood_risk_score` là baseline nội bộ theo §10.1 —
được tính, không được phát hành, và không phải `flood_probability`.

Ngập do mưa đô thị (*pluvial flooding*) khác với lũ sông (*fluvial flooding*).
Nhánh hiện tại chỉ nghiên cứu áp lực gây ngập do mưa. Mưa tích lũy nhiều ngày
không thay thế cho lưu lượng hay mực nước sông.

## 3. Semantics dữ liệu đầu vào

### 3.1 Lượng mưa theo giờ

Gọi \(P_t\) là `precipitation` tại timestamp \(t\), đơn vị mm. Theo Open-Meteo,
đây là tổng giáng thủy của **giờ liền trước**, gồm rain, showers và snow water
equivalent. Vì vậy:

```text
interval_start_utc = valid_time_utc - interval '1 hour'
interval_end_utc   = valid_time_utc
precipitation_mm   = precipitation
```

Không tính:

```text
precipitation + rain + showers
```

`rain` và `showers` chỉ được giữ để phân tích thành phần và kiểm tra chất lượng.
Semantics của `rain` có thể khác giữa Forecast API và Historical Weather API;
mọi phép so sánh phải giữ `source_endpoint`, model và contract version.

Giá trị số của \(P_t\) cũng bằng cường độ trung bình tương đương trong một giờ:

\[
I_{1h}(t)=\frac{P_t}{1\ \text{hour}}
\]

Đây không phải cường độ tức thời. Mưa dồn trong 10 phút vẫn bị làm trơn trong
ô dữ liệu hourly.

### 3.2 Xác suất mưa

`precipitation_probability` là xác suất tổng mưa của giờ trước vượt 0,1 mm. Nó
không phải:

- xác suất ngập;
- xác suất vượt một ngưỡng tích lũy 3/6/24 giờ;
- hệ số dùng để nhân với lượng mưa deterministic.

Không cộng xác suất giữa các giờ và không dùng
\(1-\prod_t(1-p_t)\), vì các giờ và ensemble members không độc lập.

### 3.3 Độ ẩm đất

Forecast và Historical Weather có thể dùng các lớp đất khác nhau. Silver phải
giữ rõ `layer_top_cm`, `layer_bottom_cm`, `api_variable_name`, model và endpoint;
không gộp mơ hồ thành một cột `soil_moisture_surface`.

Ví dụ profile Forecast tổng quát:

```text
0–1 cm, 1–3 cm, 3–9 cm, 9–27 cm, 27–81 cm
```

Ví dụ profile ERA5/ERA5-Land:

```text
0–7 cm, 7–28 cm, 28–100 cm, 100–255 cm
```

### 3.4 Spatial support và forecast vintage

- `requested_latitude/longitude` là tọa độ yêu cầu.
- `grid_latitude/longitude` là tâm ô lưới thực sự được API sử dụng.
- Nhiều phường có thể dùng chung một grid; đó không phải các forecast độc lập.
- Logical slot trong `_source_file` là thời điểm hệ thống lấy snapshot, không phải
  model run time do nhà cung cấp công bố.
- Rolling KPI forecast không được trộn record từ các logical slot khác nhau.
- Live Best Match không bảo đảm một model cố định qua thời gian. MVP chỉ phục vụ
  snapshot hiện hành; khi K6 hoặc một use case backtest vintage được chốt mới mở
  rộng ingestion contract cho model, endpoint và retrieval metadata.

## 4. Bộ KPI MVP

### 4.1 Mưa tích lũy theo cửa sổ

Với \(H\in\{1,3,6,12,24,48,72\}\):

\[
R_H(t)=\sum_{i=0}^{H-1}P_{t-i}
\]

| KPI | Đơn vị | Vai trò |
|---|---:|---|
| `rain_1h_mm` | mm | Áp lực mưa một giờ |
| `rain_3h_mm` | mm | Mưa đối lưu/ngập đô thị rất ngắn hạn |
| `rain_6h_mm` | mm | Mưa ngắn hạn |
| `rain_12h_mm` | mm | Cửa sổ cảnh báo quốc gia |
| `rain_24h_mm` | mm | Mưa ngày/cửa sổ cảnh báo quốc gia |
| `rain_48h_mm` | mm | Tham chiếu thiết kế thoát nước Hà Nội |
| `rain_72h_mm` | mm | Điều kiện mưa kéo dài/tiền kỳ |

Cường độ trung bình trong cửa sổ là \(R_H/H\); consumer tự chia khi cần, không
lưu thành cột riêng.

Quy ước cửa sổ là `(t-H, t]`. KPI chỉ hợp lệ khi có đủ đúng \(H\) khoảng giờ
liên tục. Khi thiếu dữ liệu:

```text
rain_Hh_mm       = NULL
coverage_ratio   = available_intervals / expected_intervals
is_complete      = false
```

Không thay `NULL` bằng 0.

### 4.2 Tổng mưa trong forecast horizon

Tại thời điểm ra quyết định \(t_0\), tổng mưa dự báo trong \(H\) giờ tới là:

\[
F_H(t_0)=\sum_{t_0<t\le t_0+H}P_t
\]

MVP công bố:

```text
forecast_rain_next_3h_mm
forecast_rain_next_6h_mm
forecast_rain_next_12h_mm
forecast_rain_next_24h_mm
forecast_rain_next_48h_mm
```

`as_of_utc` phải được làm tròn theo boundary giờ đã định nghĩa; không đưa một
phần giờ chưa hoàn tất vào tổng mà không có cờ riêng.

### 4.3 Peak trong forecast horizon

Với horizon tương lai \(F\):

\[
Peak_{1h,F}(t_0)=\max_{t_0<t\le t_0+F}P_t
\]

\[
Peak_{3h,F}(t_0)=\max_{t_0<t\le t_0+F}R_3(t)
\]

MVP công bố:

```text
max_rain_1h_next_6h_mm
max_rain_1h_next_24h_mm
max_rain_3h_next_6h_mm
max_rain_3h_next_24h_mm
time_of_max_rain_1h_utc
```

Mỗi phép tính phải dùng cùng `forecast_vintage` và ghi `as_of_utc`.

### 4.4 Kịch bản mưa vận hành Hà Nội

Quyết định 2280/QĐ-UBND ngày 29/04/2026 mô tả các kịch bản ứng với lượng mưa:

| `rain_1h_mm` | `hanoi_rain_scenario_band` | Mô tả trong kế hoạch |
|---:|---|---|
| `< 50` | `below_50` | Cơ bản không ngập; vẫn có thể ứ đọng tại điểm trũng hoặc khi hệ thống gặp sự cố |
| `50–<70` | `from_50_to_under_70` | 11 điểm úng ngập tại 9 phường và 1 xã |
| `70–100` | `from_70_to_100` | 71 điểm úng ngập cục bộ tại 30 phường/xã |
| `> 100` | `over_100` | Văn bản mô tả trên 100 mm/h kéo dài nhiều giờ, với 220 điểm tại 54 phường/xã |

Quy tắc triển khai:

```text
hanoi_rain_scenario_band =
  below_50        if R1 < 50
  from_50_to_under_70 if 50 <= R1 < 70
  from_70_to_100  if 70 <= R1 <= 100
  over_100        if R1 > 100
```

Band cuối ("trên 100 mm/h kéo dài nhiều giờ") cần thêm điều kiện thời lượng,
nhưng "nhiều giờ" chưa được nghiệp vụ định lượng (mục K1). Chờ K1 chốt mới
triển khai cột thời lượng tương ứng; MVP chỉ gắn band `over_100` theo R1.
Band này là **tham chiếu vận hành toàn thành phố**, không phải bằng
chứng rằng mọi phường hoặc mọi điểm trong danh mục sẽ ngập.

Với bảng KPI theo từng `valid_time`, scenario lấy từ `rain_1h_mm` tại giờ đó.
Với bảng summary 6/24 giờ, scenario lấy từ `max_rain_1h_next_Hh_mm` và phải ghi
rõ horizon; không dùng tổng 6/24 giờ để so trực tiếp với ngưỡng một giờ.

### 4.5 Tham chiếu 310 mm/2 ngày

Kế hoạch Hà Nội đặt mục tiêu bảo đảm thoát nước nhanh với trận mưa 310 mm/2 ngày
tại khu vực đã được cải tạo theo dự án thoát nước Hà Nội:

\[
design\_reference\_ratio_{48h}=\frac{R_{48}}{310}
\]

Tên KPI:

```text
hanoi_310mm_48h_reference_ratio
hanoi_310mm_48h_exceeded
```

Chưa triển khai: ratio là phép chia R48/310, bổ sung cột khi có consumer thật.

Không gọi đây là `drainage_capacity_utilization`: năng lực thực tế phụ thuộc lưu
vực, mực nước đệm, cống, hồ và vận hành trạm bơm.

### 4.6 Dải mưa lớn theo quy định quốc gia

Điều 44 Quyết định 18/2021/QĐ-TTg sử dụng các dải mưa theo 12/24 giờ, đồng thời
xét số ngày kéo dài, địa hình, phạm vi huyện/xã và số tỉnh bị ảnh hưởng. MVP chỉ
được tính các feature lượng mưa:

| Điều kiện lượng mưa | Feature magnitude |
|---|---|
| `50 <= R12 <= 100` | `qd18_rain_12h_50_to_100` |
| `100 <= R24 <= 200` | `qd18_rain_24h_100_to_200` |
| `200 < R24 <= 400` | `qd18_rain_24h_over_200_to_400` |
| `R24 > 400` | `qd18_rain_24h_over_400` |

```text
vn_rain_band_12h
vn_rain_band_24h
vn_rain_threshold_exceeded
```

Đã triển khai trên `fct_rain_archive_hourly` (kế thừa từ `fct_rainfall_forecast_hourly` và
`fct_rainfall_historical_hourly` cũ). Không suy ra `official_disaster_risk_level`
từ một ô lưới hoặc centroid phường.
Cấp độ pháp lý chỉ được bổ sung khi toàn bộ điều kiện của văn bản được mô hình
hóa và kiểm chứng.

## 5. Feature tiền điều kiện

Các feature trong mục này hỗ trợ nghiên cứu nhưng không tự tạo cảnh báo ngập.

### 5.1 Antecedent Precipitation Index

\[
API_t=P_t+kAPI_{t-1}
\]

Biểu diễn hệ số suy giảm bằng half-life \(H\) giờ:

\[
k=2^{-1/H}
\]

Tạo song song:

```text
api_half_life_24h_mm
api_half_life_48h_mm
api_half_life_72h_mm
```

API cần warm-up trước khoảng phân tích. Không gọi một giá trị \(k\) là chuẩn cho
Hà Nội trước khi hiệu chỉnh với độ ẩm đất hoặc sự kiện ngập.

### 5.2 Soil-wetness proxy

Trung bình theo độ dày cho profile Forecast 0–27 cm:

\[
\theta_{0:27}=\frac{
1\theta_{0:1}+2\theta_{1:3}+6\theta_{3:9}+18\theta_{9:27}
}{27}
\]

Chuẩn hóa theo cùng grid, model, profile và tháng:

\[
SMI_t=clip\left(\frac{\theta_t-Q_{10}}{Q_{90}-Q_{10}},0,1\right)
\]

`SMI` là độ ẩm đất của land-surface model, không đại diện cho mặt đường bê tông
hay dung tích hệ thống thoát nước.

### 5.3 Water-balance proxy tùy chọn

Nếu ingest được `evapotranspiration` với cùng model/contract:

\[
WB_H=\sum_H P-\sum_H ET
\]

Tên trường phải là `weather_water_balance_proxy_Hh_mm`, không phải runoff hay độ
sâu nước đọng. `et0_fao_evapotranspiration` chỉ mô tả nhu cầu bay hơi tham chiếu,
không thay thế ET thực tế.

## 6. Baseline khí hậu và anomaly

Baseline đề xuất dùng 2000–2016 trên ERA5 (0,25°) và 2017→nay trên ECMWF IFS
(~9 km), **tách riêng theo `weather_model`**. IFS không có dữ liệu trước 2017,
nên không có một historical product duy nhất cho cả cửa sổ 1991–2020.
Không trộn phân phối ERA5 với IFS qua mốc 2017, và không trộn reanalysis với
Forecast Best Match, nếu chưa đánh giá bias.

Phân vị vận hành cho rolling rainfall:

\[
Percentile_H(t)=100\frac{
\#\{R_H^{base}\le R_H(t)\}+0.5
}{N+1}
\]

Baseline phải cùng location/grid và cùng mùa hoặc tháng. Công bố trên Gold:

```text
rain_*h_exceeds_p95
rain_*h_exceeds_p99
rain_*h_anomaly_from_monthly_median_mm
```

Không lưu `rolling_Hh_percentile` từng giờ: cờ vượt p95/p99 đủ cho vận hành.
Mưa có nhiều giá trị 0 và phân phối lệch phải; không mặc định dùng z-score. Nếu
cần anomaly bền vững cho nghiên cứu:

\[
Z_H^{robust}=\frac{
\ln(1+R_H)-median[\ln(1+R_H^{base})]
}{1.4826\,MAD[\ln(1+R_H^{base})]}
\]

Đây là heuristic của dự án, không phải chỉ số WMO.

Các chỉ số ETCCDI như `Rx1day`, `Rx5day`, `R95p`, `R99p` và `SDII` là chỉ số
khí hậu theo ngày. Chúng thuộc báo cáo lịch sử, không thay thế KPI cảnh báo theo
giờ.

## 7. Event rainfall

Quy tắc khởi tạo, cần version và hiệu chỉnh:

```text
wet_hour = precipitation_mm > 0.1
new_event = wet_hour after at least 6 consecutive dry hours
```

KPI event:

```text
event_total_mm
event_duration_hours
event_peak_1h_mm
event_peak_3h_mm
time_to_peak_hours
convective_fraction
event_definition_version
```

\[
convective\_fraction=
\frac{\sum showers}{\sum precipitation}
\]

Chỉ tính `convective_fraction` khi tổng precipitation dương và `showers` có cùng
semantics, endpoint và model.

## 8. Ensemble và bất định — phase 2

Nếu ingest từng ensemble member, xác suất vượt ngưỡng tích lũy \(T\) phải được
tính trên từng member:

\[
R_H^{(m)}=\sum_H P_t^{(m)}
\]

\[
Pr(R_H\ge T)=\frac{
\sum_{m=1}^{M}1[R_H^{(m)}\ge T]
}{M_{available}}
\]

Đây là xác suất **forecast vượt ngưỡng mưa**, không phải xác suất ngập. Gold phải
giữ `member_count_available`, spread và quantile cùng threshold version.

## 9. IDF và chu kỳ lặp — phase nghiên cứu

Nghiên cứu tại trạm Hà Đông đưa ra dạng phương trình IDF:

\[
q(T,d)=\frac{
0.36\times2320(1+0.655\log_{10}T)
}{(d+9)^{0.633}}
\]

Trong đó \(q\) là mm/h, \(T\) là chu kỳ lặp theo năm và \(d\) là duration theo
phút. Feature nghiên cứu:

\[
idf\_ratio_{H,T}=\frac{R_H/H}{q(T,60H)}
\]

`idf_ratio >= 1` chỉ có nghĩa forecast đạt/vượt mức mưa thiết kế tương ứng tại
trạm nghiên cứu. Nó không có nghĩa chắc chắn ngập; không đại diện đồng nhất cho
toàn Hà Nội; chu kỳ lặp của mưa không phải chu kỳ lặp của ngập.

IDF chưa thuộc MVP. Trước khi triển khai phải kiểm tra lại miền hiệu lực, version
công thức, station coverage và sai lệch giữa grid model với trạm.

## 10. Composite score

**Không phát hành** composite score. Một score kết hợp rolling rain, API, soil
moisture hoặc runoff bằng trọng số thủ công sẽ che mất giả định và tạo cảm giác
chính xác giả.

### 10.1 Ngoại lệ: baseline nội bộ để backtest (2026-09-06)

Cấm *phát hành* không đồng nghĩa cấm *tính*. Không có một đường cơ sở nào thì
cũng không có gì để đo mô hình sau này tốt hơn hay tệ hơn cái gì.

Được phép tồn tại trong Gold, **không** được lên dashboard và **không** được
gọi là xác suất:

```text
gold.fct_flood_risk_score       # hazard_index, vulnerability_index, risk_score
gold.fct_flood_backtest_metric  # POD/FAR/CSI theo từng luật, tách theo trận
```

Điều kiện bắt buộc kèm theo:

- trọng số khai báo tập trung ở `macros/flood_risk.sql`, có version
  (`flood_risk_version()`), không rải trong model;
- `risk_score` trên thang 0–100 để **so sánh giữa các phường trong cùng một
  giờ**, không đọc như phần trăm khả năng ngập;
- `hazard_index` (đo được từ Open-Meteo) và `vulnerability_index` (suy từ lịch
  sử quan sát) phải là hai cột riêng, để lớp phục vụ hiển thị được áp lực mưa
  mà không buộc phải hiển thị điểm tổng hợp;
- version `heuristic_baseline_v1_uncalibrated` **chưa hiệu chỉnh với bất kỳ sự
  kiện ngập nào**. Trọng số do người đặt, không do dữ liệu học.

Chỉ được gỡ trạng thái nội bộ khi POD/FAR/CSI đã đo trên nhãn thật, holdout
theo trọn một trận mưa, và thắng được luật đang chạy (`hanoi_1h_band`).

### 10.2 Chỉ số đánh giá

Khi có nhãn ngập theo `ward/event_window`, có thể huấn luyện mô hình đã hiệu
chỉnh và công bố xác suất. Train/test phải tách theo toàn bộ trận mưa hoặc theo
năm, không random từng dòng giờ. Metric tối thiểu:

\[
POD=\frac{H}{H+M}
\qquad
FAR=\frac{F}{H+F}
\qquad
CSI=\frac{H}{H+M+F}
\]

Trong đó `H`, `M`, `F` lần lượt là hit, miss và false alarm. Cần đánh giá riêng
theo lead-time `0–6h`, `6–24h`, `24–48h` và theo mùa.

## 11. Data model triển khai

### 11.1 Silver forecast hourly

MVP không tạo `silver.forecast_hourly_vintage`. Bronze vẫn giữ `_source_file` và
`_ingested_at`; Silver chọn logical slot mới nhất có đủ đúng 6 batch production,
dedup retry theo source object rồi gom các requested point trùng returned grid.

Grain hiện tại:

```text
forecast_snapshot_id × grid_cell_id × valid_time_utc
```

Cột hiện tại:

```text
forecast_snapshot_id
as_of_utc
grid_cell_id
grid_latitude
grid_longitude
valid_time_utc
precipitation_mm
rain_mm
showers_mm
precipitation_probability_pct nullable
weather_code
source_object_keys
_ingested_at
```

Requested coordinate không cần lặp trên từng dòng: mapping forecast dùng
returned grid gần nhất trong chính snapshot hiện hành. Contract vintage đầy
đủ chỉ bổ sung khi có consumer cần đánh giá lịch sử forecast.

### 11.2 Gold rainfall KPI

Grain forecast KPI:

```text
forecast_snapshot_id × grid_cell_id × as_of_utc × valid_time_utc
```

Phường là projection từ grid:

```text
ward_key → grid_cell_id → rainfall KPI
```

Không average 126 dòng phường để tạo KPI toàn Hà Nội vì grid có nhiều phường sẽ
bị tăng trọng số. KPI toàn thành phố phải dùng unique grid cell hoặc area-weighted
mapping khi có polygon-grid intersection.

Metadata KPI trên Gold:

```text
forecast_snapshot_id
hanoi_rain_scenario_band
source_object_keys   # snapshot grain — fct_rainfall_forecast_summary
```

`rain_*h_mm IS NULL` nghĩa là cửa sổ incomplete; không lưu thêm
`coverage_ratio` / `is_complete`. Lineage source object nằm ở summary, không
lặp trên từng grid-hour.

## 12. Quality rules

- `precipitation_mm >= 0` hoặc `NULL`.
- `0 <= precipitation_probability_pct <= 100` hoặc `NULL`.
- Mọi hourly array phải khớp `hourly.time`; mismatch được rescue và cảnh báo.
- `valid_time_utc` tăng đều theo bước một giờ trong cửa sổ hourly.
- Rolling không được đi qua ranh giới forecast vintage.
- Unit phải lấy từ `hourly_units`, không giả định ngầm.
- Với vùng không tuyết, `precipitation ≈ rain + showers` chỉ là soft check có
  tolerance, không phải công thức tái tạo tổng.
- KPI phường là projection từ model grid; không suy ra độ phân giải phường.

## 13. Definition of Done cho KPI

- Có Silver hourly giữ một snapshot hoàn chỉnh, returned grid và source-object
  lineage; không trộn logical slot.
- Tính đúng `R1/R3/R6/R12/R24/R48/R72` trên fixture có kết quả biết trước.
- Thiếu một giờ làm rolling incomplete, không biến thành mưa 0.
- Không cộng trùng precipitation/rain/showers.
- `hanoi_rain_scenario_band` khớp boundary test tại 50, 70, 100 mm và ngay sau
  100 mm; đúng 100 vẫn thuộc band `from_70_to_100`.
- Band `over_100` không tự tuyên bố điều kiện “kéo dài nhiều giờ” đã thỏa.
- `vn_rain_band_12h` / `vn_rain_band_24h` khớp boundary QĐ 18; không suy ra
  `official_disaster_risk_level`.
- Không sinh `official_disaster_risk_level`, `flood_probability` hoặc
  `flood_depth`.
- Ward cùng grid nhận cùng forcing và UI hiển thị giới hạn độ phân giải.
- 126 phường đều map được vào returned forecast grid và archive grid từng model.
- Gold summary có lineage về các Bronze source object.
- Historical rolling/daily/climatology/anomaly/event tách theo `weather_model`;
  không trộn ERA5 với IFS.
- Bốn cửa sổ replay §9 có hourly rows sau khi archive phủ thời kỳ tương ứng.
- Climatology công bố `observation_count`, khoảng thời gian và
  `climatology_window_id=all_available_by_model_v1`.

Ngoài scope bước 5:

- bảng Silver cho lịch sử forecast vintage và backtest theo model run;
- source checksum/model/endpoint ở row-level ingestion contract;
- chuẩn WMO 1991–2020 (IFS không có trước 2017);
- API/SMI/IDF, ensemble, composite score;
- hiệu chỉnh POD/FAR/CSI với nhãn ngập (K5).

## 14. Quyết định và điểm còn mở

Đã chốt:

- `precipitation` là biến tổng mưa duy nhất cho phép cộng theo thời gian;
- rolling 1/3/6/12/24/48/72 giờ là KPI lõi;
- tổng mưa và peak trong forecast horizon là KPI riêng với rolling kết thúc tại
  một valid time;
- ngưỡng 50/70/100 mm/h là kịch bản vận hành Hà Nội, không phải cấp độ pháp lý;
- chưa có composite score hoặc xác suất ngập trong MVP;
- KPI tính theo grid rồi mới projection sang phường;
- mọi công thức và threshold đều có version.
- historical product: `era5` trước 2017, `ecmwf_ifs` từ 2017 (IFS không có
  dữ liệu trước 2017). H3 xác nhận ERA5 đủ precipitation/rain/weather code
  và soil moisture, trong khi ERA5-Land trả toàn null cho ba biến mưa/weather.
  Không gộp hai model thành một chuỗi percentile.

Còn mở:

- **K1:** định nghĩa định lượng cho “trên 100 mm/h kéo dài nhiều giờ”;
- **K3:** quy tắc event 6 giờ khô đã gắn version `wet_gt_0_1mm_six_dry_hours_v1`,
  chưa hiệu chỉnh với trận mưa Hà Nội;
- **K4:** archive đã có soil moisture ERA5/IFS; ET chưa thành KPI. SMI đã có
  trong `fct_flood_training_feature` (chuẩn hoá p10–p90 theo ô lưới × tháng từ
  `gold.fct_rain_climatology`). Giá trị THÔ không dùng được: trong trận
  07/10/2025 nó nằm phẳng ở 0,430–0,439 và đã gần bão hoà trước khi mưa bắt
  đầu; cùng số đó sau chuẩn hoá cho SMI ≈ 0,95;
- **K5:** đường dẫn nhãn đã dựng —
  `seeds/flood_observation_seed.csv` → `gold.fct_flood_event_observation` →
  `gold.fct_flood_training_feature` → `gold.fct_flood_backtest_metric`, với
  thang nguồn A–D và cờ `is_training_eligible`. **Seed hiện còn RỖNG**, nên
  POD/FAR/CSI chưa có số; toàn bộ chuỗi đã kiểm chứng bằng fixture, chưa kiểm
  chứng bằng sự kiện thật;
- **K6:** có pin forecast model để tăng tính tái lập hay tiếp tục Best Match và
  quản lý thay đổi bằng vintage/metadata.

## 15. Nguồn tham khảo

- [Open-Meteo Weather Forecast API](https://open-meteo.com/en/docs)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [Open-Meteo Ensemble API](https://open-meteo.com/en/docs/ensemble-api)
- [Open-Meteo Single Runs API](https://open-meteo.com/en/docs/single-runs-api)
- [Quyết định 2280/QĐ-UBND ngày 29/04/2026 của UBND Hà Nội](https://datafiles.hanoi.gov.vn/gov-hni/6244/VanBan/2026/4/29/QD-2280-2026.pdf)
- [Quyết định 18/2021/QĐ-TTg](https://datafiles.chinhphu.vn/cpp/files/vbpq/2021/04/18.signed.pdf)
- [WMO Climatological Normals](https://wmo.int/wmo-climatological-normals)
- [WMO/Climpact climate indices](https://etrp.wmo.int/pluginfile.php/47030/mod_resource/content/6/climpact_indices_table_updated18Aug2023.pdf)
- [Nghiên cứu IDF trạm Hà Đông](https://doi.org/10.31814/stce.huce2025-19(1)-05)
- [WMO Manual on Flood Forecasting and Warning](https://old.wmo.int/extranet/pages/prog/hwrp/publications/flood_forecasting_warning/WMO%201072_en.pdf)
