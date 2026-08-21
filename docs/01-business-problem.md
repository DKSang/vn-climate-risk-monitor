# Bước 1 — Bắt đầu từ bài toán nghiệp vụ

**Hanoi Flood & Climate Risk Monitor** · v0.4 · 2026-08-21 · *Trạng thái: ĐÃ CHỐT PHẠM VI MVP*

Khung câu hỏi theo ảnh quy trình: *What decision are we enabling? Who is the stakeholder?
How fresh should the data be?* → Output: problem statement + success metric.

---

## 1. Phát biểu bài toán

> Mỗi mùa mưa, Hà Nội ngập cục bộ ở hàng chục đến hàng trăm điểm. Quyết định
> 2280/QĐ-UBND ngày 29/4/2026 công bố các **kịch bản úng ngập toàn thành phố theo dải mưa**
> và danh mục điểm cần ứng phó, nhưng thông tin nằm trong văn bản tĩnh, chưa gắn với các
> forecast vintage và ô lưới mưa đang cập nhật.
>
> Dự án biến tri thức đó thành **hệ thống theo dõi áp lực mưa động**: ghép kịch bản vận hành
> với dự báo mưa theo giờ, để trả lời câu hỏi *"trong 6–24 giờ tới, ô lưới nào chịu áp lực mưa
> lớn và những phường/điểm úng ngập chính thức nào nằm trong vùng đó?"*

## 2. Chúng ta đang hỗ trợ quyết định gì?

| Quyết định | Ai ra quyết định | Nhịp |
|---|---|---|
| Hôm nay đi làm nên tránh tuyến nào, đi giờ nào | Người dân, shipper, tài xế | Theo giờ |
| Bố trí lực lượng/máy bơm ứng trực ở điểm nào | Cty Thoát nước Hà Nội (HSDC) | Trước mưa 6–12h |
| Có phát cảnh báo cho phường không, mức nào | UBND phường/xã | Trước mưa 3–6h |
| Vùng nào cần đầu tư cải tạo thoát nước | Sở Xây dựng / quy hoạch | Theo mùa/năm |
| Vùng ngoại thành nào cần điều tiết nước tưới | Xí nghiệp thủy lợi, HTX nông nghiệp | Theo tuần/tháng |

**Quyết định lõi của giai đoạn 1** (chỉ chọn 1 để không dàn trải):
→ *"Trong 6–24 giờ tới, khu vực nào cần tăng mức theo dõi và chuẩn bị ứng phó do forecast
mưa vượt kịch bản vận hành nào?"*

Kết quả giai đoạn 1 là `rainfall hazard/pressure`, không phải xác suất ngập hoặc dự báo độ sâu.

## 3. Stakeholder

| Persona | Nhu cầu | Độ chi tiết cần |
|---|---|---|
| **Người dân / người đi lại** | Tuyến đường tôi đi có ngập không | **Tuyến đường** |
| **HSDC — vận hành thoát nước** | Điểm nào cần ứng trực, bơm ở đâu | **Điểm ngập cụ thể** |
| **UBND phường-xã** | Địa bàn tôi rủi ro mức nào | **Phường-xã** |
| **Nhà quy hoạch / phân tích** | Điểm nào ngập lặp lại nhiều năm | **Điểm + chuỗi thời gian** |
| **HTX nông nghiệp ngoại thành** | Hạn/úng ruộng vụ này | **Xã** (Ba Vì, Sóc Sơn, Mỹ Đức, Ứng Hòa) |

## 4. Độ tươi dữ liệu (data freshness)

Đây là ràng buộc khắt khe nhất và nó **quyết định kiến trúc ở Bước 3**:

| Lớp dữ liệu | Yêu cầu tươi | Ghi chú |
|---|---|---|
| Dự báo mưa theo giờ | **≤ 1 giờ** | Mưa rào Hà Nội hình thành trong 1–2h; trễ 3h là vô dụng |
| Quan trắc mưa thực tế | ≤ 1 giờ | Để hiệu chỉnh và ghi nhận sự kiện |
| Trạng thái ngập thực tế | ≤ 30 phút (nếu có nguồn) | Hiện chưa có nguồn mở — xem Bước 2 |
| Registry điểm ngập (QĐ 2280) | Theo năm | Tĩnh, cập nhật khi có quyết định mới |
| Ranh giới phường-xã | Theo năm | Tĩnh, SCD2 |
| Mạng lưới đường OSM | Theo tháng | Tĩnh tương đối |
| Lịch sử khí hậu (chuẩn 1991–2020) | Theo tháng | Backfill 1 lần rồi bồi hằng ngày |

> **Hệ quả:** hệ thống có **hai nhịp khác hẳn nhau** — một luồng near-real-time theo giờ, và
> một luồng batch theo ngày cho lịch sử/chuẩn khí hậu. Không nên ép chung một pipeline.

## 5. Grain (độ chi tiết) — đã chốt

| Thực thể | Grain | Số bản ghi ước tính |
|---|---|---|
| Phường-xã Hà Nội | 126 đơn vị (51 phường + 75 xã, NQ 1656/NQ-UBTVQH15) — **đã có polygon + mã chính thức** | 126 |
| Điểm ngập chính thức | ~220 điểm tại 54 phường-xã (QĐ 2280/QĐ-UBND) | ~220 |
| Tuyến đường có tên | OSM, toàn Hà Nội | ~50.000–100.000 way |
| Ô lưới mưa | **Đã đo thực tế** — xem **R1** | **49 ô** |
| Lưu vực thoát nước | Tô Lịch, Nhuệ, Cầu Bây/Long Biên, Đông Mỹ… | ~5–8 |

**Fact thời tiết chính:** `(ô_lưới, forecast_vintage, valid_hour)`.
**Fact projection:** `(phường_xã, forecast_vintage, valid_hour)` và
`(điểm_úng_ngập, forecast_vintage, valid_hour)`; nhiều phường/điểm có thể dùng chung forcing
của một ô lưới.

## 6. Câu hỏi phân tích (đầu vào cho Bước 6 — mô hình dữ liệu)

- **Q1** — 6 giờ tới, những điểm úng ngập chính thức nào nằm trong ô lưới có mưa vượt ngưỡng?
- **Q2** — Phường-xã X hiện thuộc kịch bản mưa vận hành nào của Hà Nội?
- **Q3** — Tuyến đường Y có nằm trong bán kính ảnh hưởng của điểm ngập đang cảnh báo không?
- **Q4** — Xếp hạng các ô lưới/phường theo áp lực mưa dự báo hôm nay, không nhân bản trọng số
  khi nhiều phường cùng grid.
- **Q5** — Lượng mưa dự báo 1h/3h/6h/24h lớn nhất cho từng phường-xã.
- **Q6** — Điểm ngập nào bị kích hoạt nhiều lần nhất trong 12 tháng qua? (đầu vào cho quy hoạch)
- **Q7** — Số giờ/ngày có peak một giờ vượt 50/70/100 mm tại grid của phường-xã X.
- **Q8** — *(ngoại thành)* Lượng mưa tích lũy 30/60/90 ngày của xã X lệch bao nhiêu % so với chuẩn 1991–2020?
- **Q9** — Mực nước/lưu lượng sông Hồng, sông Nhuệ, sông Đáy đang ở mức nào so với báo động?

## 7. Success metric

| Nhóm | Chỉ số | Ngưỡng giai đoạn 1 |
|---|---|---|
| **Nghiệp vụ MVP** | Tỷ lệ forecast vượt ngưỡng được giải thích đúng nguồn/version | 100% |
| **Nghiệp vụ MVP** | Không công bố xác suất/độ sâu ngập khi chưa hiệu chỉnh | 100% |
| **Kiểm định tương lai** | POD/recall, FAR và CSI theo trận mưa | Chưa đặt ngưỡng trước khi có nhãn |
| **Kiểm định tương lai** | Lead time hữu ích | Đánh giá riêng 0–6h, 6–24h, 24–48h |
| **Kỹ thuật** | Độ trễ từ lúc nguồn có dữ liệu tới lúc lên dashboard | ≤ 20 phút khi upstream sẵn sàng |
| **Kỹ thuật** | Tỷ lệ scheduled run thành công, loại trừ upstream outage | ≥ 99% |
| **Kỹ thuật** | Test chất lượng dữ liệu pass | 100%, fail thì chặn publish |
| **Chi phí** | Chi phí bắt buộc cho phần mềm/API/hạ tầng | **0 đồng/tháng** |

## 8. Phạm vi

**Trong phạm vi (giai đoạn 1) — sau đơn giản hóa 20/08/2026**
- Địa bàn: toàn TP Hà Nội, 126 phường-xã, chấm điểm theo **ô lưới Open-Meteo** (49 ô)
- Nguồn dữ liệu: **chỉ Open-Meteo Forecast + Archive API**, không dùng GloFAS/radar/camera/DEM
- Hiểm họa, đều suy trực tiếp từ mưa:
  - **Ngập úng đô thị** — feature áp lực mưa và kịch bản vận hành QĐ 2280 (50/70/100 mm/h),
    không phải xác suất ngập
  - **Lũ** — mưa tích lũy nhiều ngày theo lưu vực, chỉ là proxy khí tượng, KHÔNG dùng lưu
    lượng/mực nước sông thực
  - **Hạn hán** — mưa tích lũy 30/60/90 ngày so với chuẩn khí hậu 1991–2020 (xã ngoại thành)
- Dự báo mưa theo giờ tới 48h + lịch sử 1981–nay
- Ranh giới + mã 126 phường-xã (S13), kịch bản mưa chính thức (QĐ 2280)

**Ngoài phạm vi**
- Mô hình thủy lực (SWMM/MIKE URBAN) — không có dữ liệu mạng cống
- Lưu lượng sông thực tế (GloFAS) — ô lưới không khớp lòng sông, cần hiệu chỉnh tốn công, hoãn lại
- Radar, camera HSDC, DEM — điều tra rồi nhưng không hợp với quy mô GĐ1 (xem Bước 2 §4)
- Bão, xâm nhập mặn, sạt lở
- Dự báo ML — giai đoạn 1 dùng ngưỡng minh bạch, giải thích được
- Điều hướng/định tuyến tránh ngập, hiển thị chi tiết theo từng tuyến đường riêng lẻ

## 9. Bộ test kiểm chứng — hệ thống phải bắt được

| Sự kiện | Thời gian | Ghi nhận | Kỳ vọng |
|---|---|---|---|
| Mưa lớn cuối T8/2025 | 08/2025 | Hai Bà Trưng 315mm, Yên Sở 310mm, Tây Mỗ 271mm, Hoàng Liệt 235mm | Rolling/peak phản ánh đúng lượng mưa theo dữ liệu nguồn |
| Ngập diện rộng | 08/10/2025 | Có danh sách điểm ngập công bố | Replay scenario và ghi nhận hit/miss; chưa đặt ngưỡng POD khi nhãn chưa đầy đủ |
| Lũ sau bão Yagi | 09/2024 | Mưa lớn nhiều ngày, ngập Long Biên/Gia Lâm | Mưa tích lũy nhiều ngày tăng cao; vẫn ghi rõ đây không phải dự báo lũ sông |
| Mưa lịch sử | 30/10/2008 | Ngập lụt lịch sử Hà Nội, ~600mm/3 ngày | Rolling 24/48/72h vượt các feature magnitude tương ứng |
| **Ngày khô ráo** (test âm) | chọn ngẫu nhiên mùa khô | Không ngập | Không vượt ngưỡng mưa và không tạo scenario sai |

> Test âm quan trọng ngang test dương. Hệ thống báo động liên tục thì cũng vô dụng như không báo.

## 10. Giả định & quyết định sản phẩm

**Giả định**
- **A2** — Kịch bản mưa vận hành lấy theo chính thành phố công bố: <50 mm/h cơ bản không ngập;
  50–70 mm/h → ~11 điểm; 70–100 mm/h → ~71 điểm; >100 mm/h kéo dài → ~220 điểm tại 54 phường-xã.
  Đây là scenario toàn thành phố, không phải quan hệ nhân quả chắc chắn cho từng phường.
- **A3** — QĐ 18/2021/QĐ-TTg chỉ được dùng để tạo feature dải mưa 12/24 giờ. Không suy ra
  cấp độ rủi ro thiên tai khi chưa mô hình hóa đủ phạm vi, địa hình và thời gian kéo dài.
- **A4** — Chuẩn khí hậu dùng thời kỳ 1991–2020 (WMO).
- **A5** — Chấp nhận áp lực mưa hiển thị theo **ô lưới (49 ô)**, không theo từng phường riêng lẻ.
  Phường cùng ô lưới hiển thị cùng forcing/scenario mưa.

**Đã chốt ngày 21/08/2026**

- **B1 — Mục đích:** đây là dự án portfolio nhưng phải vận hành end-to-end như một hệ thống
  production nhỏ: có lịch chạy, idempotency, recovery, quality gate, monitoring và tài liệu
  vận hành. Không dùng dữ liệu giả để thay thế đường chạy thực.
- **B2 — Ngân sách:** chi phí bắt buộc là 0 đồng/tháng. Ưu tiên phần mềm open-source, Free API
  cho non-commercial use và máy sở hữu sẵn hoặc compute free-tier.
- **B3 — Mức dịch vụ:** best-effort single-node. Không tuyên bố high availability hoặc SLA
  nguồn dữ liệu vì Free API không bảo đảm uptime. Upstream outage phải tạo trạng thái
  `DEGRADED`, không bị tính nhầm thành lỗi transformation.

## 11. Rủi ro

| # | Rủi ro | Mức | Xử lý |
|---|---|---|---|
| **R1** | **Dữ liệu mưa KHÔNG phân biệt được giữa các phường nội thành.** Đã **đo chính xác** ngày 20/08/2026 bằng centroid thật của cả 126 phường-xã: **126 phường → chỉ 49 ô lưới mưa phân biệt được**. Một ô gom **15 phường** (Đống Đa, Thanh Xuân, Khương Đình, Hoàng Liệt, Hà Đông, Tây Mỗ…) dùng chung **một chuỗi mưa duy nhất** — đúng những nơi hay ngập nhất. | **Rất cao** | Chấp nhận và thiết kế đúng bản chất: **mưa là biến động theo ô lưới (49 ô), tính dễ tổn thương là biến tĩnh theo điểm/đường**. MVP projection forcing của grid sang điểm/phường, chưa gọi kết quả là xác suất rủi ro. Tuyệt đối không giả vờ có mưa phân giải mét. **Không khắc phục được bằng tọa độ phường chính xác hơn.** |
| **R2** | Chưa có nguồn quan trắc ngập thực tế theo thời gian thực → không thể hiệu chỉnh xác suất ngập hoặc đo POD/FAR liên tục. | Cao | Giai đoạn 1 chỉ công bố pressure feature/kịch bản mưa; §9 là bộ case replay, không phải ground-truth đầy đủ. |
| **R3** | Phụ lục QĐ 2280 có thể chỉ có bản PDF/giấy, phải nhập tay ~220 điểm và geocode. | Cao | Coi là công việc thật, có kế hoạch riêng: nhập tay + geocode qua Nominatim + review. |
| **R4** | DEM miễn phí trả độ cao **số nguyên mét** (đã kiểm chứng: 10, 9, 14 m). Chênh lệch dưới 1m không thấy được, trong khi ngập cục bộ nhạy ở mức decimet. | Cao | Không dùng DEM để tự phát hiện điểm trũng. Dùng DEM làm biến phụ; điểm ngập lấy từ registry chính thức. |
| ~~R5~~ | ~~Ranh giới 126 phường-xã có thể sai/thiếu.~~ **ĐÃ ĐÓNG 20/08/2026** | — | Đã có polygon + **mã hành chính chính thức** đủ 126 đơn vị, tổng diện tích 3.360 km² khớp số liệu chính thức. Xem S13 ở Bước 2. |
| **R6** | Single-node/free-tier có thể sleep, restart, hết disk hoặc bị thu hồi; Free API không có uptime guarantee. | Cao | Container restart policy, checkpoint/idempotency, health check, disk budget, backup metadata và cảnh báo freshness; công bố trạng thái `DEGRADED` khi nguồn hoặc host gián đoạn. |

---

## Phụ lục — Thay đổi so với v0.1

v0.1 lấy phạm vi 34 tỉnh toàn quốc, grain = tỉnh + điểm lưới. v0.2 thu hẹp về Hà Nội, grain =
phường-xã + điểm ngập + tuyến đường. Thay đổi này **làm bài toán khó hơn chứ không dễ hơn**:
độ phân giải dữ liệu mưa trở thành nút thắt (R1), và giá trị của dự án dịch chuyển từ
"gọi API rồi tính chỉ số" sang "ghép tri thức địa phương chính thức với dự báo".
