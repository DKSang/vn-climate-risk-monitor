# Bước 1 — Bài toán nghiệp vụ hiện hành

**Hanoi Flood & Climate Risk Monitor** · v2.2 · 2026-09-08

## 1. Mục tiêu

Hệ thống giúp người vận hành xác định phường/xã nào cần được kiểm tra trước khi
mưa lớn, dựa trên forecast theo giờ và sự thay đổi giữa các forecast run.

Output chính là **áp lực mưa**, không phải dự báo xác suất ngập. Hệ thống không
có dữ liệu mạng cống, mực nước, địa hình chi tiết hoặc tập nhãn âm đại diện để
đưa ra kết luận ngập/không ngập.

## 2. Người dùng

- Người vận hành cần bản đồ ưu tiên theo giờ.
- Người phân tích cần xem lượng mưa 1/3/6/12/24 giờ tới và lý do trigger.
- Người kiểm chứng cần phát lại archive quanh một sự kiện quá khứ.

## 3. Câu hỏi sản phẩm

1. Tại một giờ forecast, phường nào có `pressure_score` cao nhất?
2. Level hiện tại là `UNKNOWN`, `NORMAL`, `WATCH`, `ELEVATED` hay `HIGH`,
   và trigger nào tạo ra level đó?
3. Tín hiệu có bền qua ba forecast run gần nhất không?
4. Tổng mưa forecast 24 giờ tăng hay giảm so với run trước?
5. Một phường có chuỗi lượng mưa dự kiến thế nào trong horizon hiện hành?
6. Archive cho thấy lượng mưa quanh một sự kiện đã ghi nhận như thế nào?

## 4. Phạm vi dữ liệu

| Thực thể | Contract hiện hành |
|---|---|
| Địa bàn | 126 phường/xã Hà Nội theo bộ ranh giới S13 |
| Forecast | Open-Meteo ECMWF IFS, 72 giờ, 48 ô lưới hiện hành |
| Archive | ERA5 trước 2017 và ECMWF IFS từ 2017, giữ tách theo model |
| Điểm úng | 15 điểm QĐ 2280 có đủ nguồn và tọa độ trong seed |
| Quan sát sự kiện | Ghi nhận báo chí đã geocode và được người soát |

Nhiều phường có thể dùng chung một ô lưới và nhận cùng rainfall forcing. Bản đồ
phải công bố giới hạn này, không giả vờ có độ phân giải đến từng đường phố.

## 5. Output

- Forecast map theo lượng mưa tương lai và pressure level.
- Ward drilldown theo horizon hiện hành.
- Pressure score 0–100 khi có input; `NULL` khi coverage `NONE`, kèm trigger
  reasons, persistence và revision.
- Archive replay theo ngày/giờ, kèm điểm úng và quan sát đã xác minh.
- Operational health cho freshness, completeness, grain và Gold readiness.

## 6. Ngoài phạm vi

- Xác suất ngập hoặc độ sâu ngập.
- Cảnh báo KTTV hoặc quyết định đóng đường/sơ tán.
- Mô hình thủy lực, lưu lượng/mực nước sông và camera thời gian thực.
- Climatology, drought index, composite vulnerability score và training mart
  khi chưa có consumer/dataset đủ điều kiện.
- Nội suy lượng mưa ở độ phân giải nhỏ hơn grid nguồn.

## 7. Success criteria

| Chỉ số | Mục tiêu |
|---|---|
| Forecast run trước khi publish | đủ 72 giờ và 126 location |
| Coverage địa bàn | 126/126 phường/xã |
| Giải thích pressure | 100% signal có level/coverage; `NORMAL` chỉ khi coverage đủ |
| Grain | không trùng khóa Silver/Gold |
| Freshness forecast | health threshold hiện hành ≤ 24 giờ |
| Diễn giải sai thành xác suất/cảnh báo chính thức | 0 |

Các ngưỡng pressure là heuristic vận hành của portfolio và phải được version
control. Thay đổi ngưỡng là thay đổi business rule, cần full-refresh Gold và ghi
lý do trong processing audit.
