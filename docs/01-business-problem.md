# Bước 1 — Bắt đầu từ bài toán nghiệp vụ

**Hanoi Flood & Climate Risk Monitor** · v0.2 · 2026-08-20 · *Trạng thái: CHỜ DUYỆT*

Khung câu hỏi theo ảnh quy trình: *What decision are we enabling? Who is the stakeholder?
How fresh should the data be?* → Output: problem statement + success metric.

---

## 1. Phát biểu bài toán

> Mỗi mùa mưa, Hà Nội ngập cục bộ ở hàng chục đến hàng trăm điểm. Thành phố **đã biết trước**
> điểm nào sẽ ngập ở mức mưa nào — thông tin đó nằm trong Quyết định 2280/QĐ-UBND (29/4/2026) —
> nhưng nó ở dạng văn bản tĩnh, không gắn với dự báo mưa thời gian thực, và người dân không
> tra cứu được theo phường-xã hay tuyến đường mình sắp đi qua.
>
> Dự án biến tri thức tĩnh đó thành **hệ thống cảnh báo động**: ghép ngưỡng mưa gây ngập đã
> được thành phố công bố với dự báo mưa theo giờ, để trả lời câu hỏi *"trong 6 giờ tới, tuyến
> đường/phường nào có nguy cơ ngập?"*

## 2. Chúng ta đang hỗ trợ quyết định gì?

| Quyết định | Ai ra quyết định | Nhịp |
|---|---|---|
| Hôm nay đi làm nên tránh tuyến nào, đi giờ nào | Người dân, shipper, tài xế | Theo giờ |
| Bố trí lực lượng/máy bơm ứng trực ở điểm nào | Cty Thoát nước Hà Nội (HSDC) | Trước mưa 6–12h |
| Có phát cảnh báo cho phường không, mức nào | UBND phường/xã | Trước mưa 3–6h |
| Vùng nào cần đầu tư cải tạo thoát nước | Sở Xây dựng / quy hoạch | Theo mùa/năm |
| Vùng ngoại thành nào cần điều tiết nước tưới | Xí nghiệp thủy lợi, HTX nông nghiệp | Theo tuần/tháng |

**Quyết định lõi của giai đoạn 1** (chỉ chọn 1 để không dàn trải):
→ *"Trong 6–24 giờ tới, phường-xã nào và tuyến đường nào có nguy cơ ngập, ở cấp độ nào?"*

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

**Fact chính:** `(điểm_ngập, giờ)` — mỗi điểm ngập, mỗi giờ, một điểm rủi ro.
**Fact tổng hợp:** `(phường_xã, giờ)` và `(phường_xã, ngày)`.

## 6. Câu hỏi phân tích (đầu vào cho Bước 6 — mô hình dữ liệu)

- **Q1** — 6 giờ tới, những điểm ngập nào chuyển sang mức "cảnh báo"? Thuộc phường-xã nào, đường nào?
- **Q2** — Phường-xã X hiện ở cấp độ rủi ro ngập nào (1–4)?
- **Q3** — Tuyến đường Y có nằm trong bán kính ảnh hưởng của điểm ngập đang cảnh báo không?
- **Q4** — Xếp hạng 20 phường-xã rủi ro cao nhất hôm nay.
- **Q5** — Lượng mưa dự báo 1h/3h/6h/24h lớn nhất cho từng phường-xã.
- **Q6** — Điểm ngập nào bị kích hoạt nhiều lần nhất trong 12 tháng qua? (đầu vào cho quy hoạch)
- **Q7** — Số ngày vượt ngưỡng 50/70/100 mm/h tại phường-xã X, so với trung bình nhiều năm.
- **Q8** — *(ngoại thành)* Lượng mưa tích lũy 30/60/90 ngày của xã X lệch bao nhiêu % so với chuẩn 1991–2020?
- **Q9** — Mực nước/lưu lượng sông Hồng, sông Nhuệ, sông Đáy đang ở mức nào so với báo động?

## 7. Success metric

| Nhóm | Chỉ số | Ngưỡng giai đoạn 1 |
|---|---|---|
| **Nghiệp vụ** | Tỷ lệ bắt đúng (recall) các trận ngập đã biết trong §9 | ≥ 80% |
| **Nghiệp vụ** | Tỷ lệ báo động giả (false positive) | ≤ 30% |
| **Nghiệp vụ** | Thời gian cảnh báo trước (lead time) | ≥ 3 giờ |
| **Kỹ thuật** | Độ trễ từ lúc nguồn có dữ liệu tới lúc lên dashboard | ≤ 20 phút |
| **Kỹ thuật** | Uptime pipeline theo giờ trong mùa mưa | ≥ 99% |
| **Kỹ thuật** | Test chất lượng dữ liệu pass | 100%, fail thì chặn publish |
| **Chi phí** | Hạ tầng/tháng | *(chờ chốt — xem §10)* |

## 8. Phạm vi

**Trong phạm vi (giai đoạn 1) — sau đơn giản hóa 20/08/2026**
- Địa bàn: toàn TP Hà Nội, 126 phường-xã, chấm điểm theo **ô lưới Open-Meteo** (49 ô)
- Nguồn dữ liệu: **chỉ Open-Meteo Forecast + Archive API**, không dùng GloFAS/radar/camera/DEM
- Hiểm họa, đều suy trực tiếp từ mưa:
  - **Ngập úng đô thị** — mưa giờ lớn nhất so ngưỡng QĐ 2280 (50/70/100 mm/h)
  - **Lũ** — mưa tích lũy nhiều ngày theo lưu vực (proxy bằng mưa, KHÔNG dùng lưu lượng sông thực)
  - **Hạn hán** — mưa tích lũy 30/60/90 ngày so với chuẩn khí hậu 1991–2020 (xã ngoại thành)
- Dự báo mưa theo giờ tới 48h + lịch sử 1981–nay
- Ranh giới + mã 126 phường-xã (S13), ngưỡng mưa chính thức (QĐ 2280)

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
| Mưa lớn cuối T8/2025 | 08/2025 | Hai Bà Trưng 315mm, Yên Sở 310mm, Tây Mỗ 271mm, Hoàng Liệt 235mm | Rủi ro "rất cao" toàn thành phố |
| Ngập diện rộng | 08/10/2025 | Có danh sách điểm ngập công bố | Bắt được ≥80% điểm trong danh sách |
| Lũ sau bão Yagi | 09/2024 | Mưa lớn nhiều ngày, ngập Long Biên/Gia Lâm | Điểm lũ (mưa tích lũy) "rất cao" — *chấp nhận đây là proxy, không đo lưu lượng sông thực* |
| Mưa lịch sử | 30/10/2008 | Ngập lụt lịch sử Hà Nội, ~600mm/3 ngày | Vượt mọi ngưỡng |
| **Ngày khô ráo** (test âm) | chọn ngẫu nhiên mùa khô | Không ngập | **Không** phát cảnh báo |

> Test âm quan trọng ngang test dương. Hệ thống báo động liên tục thì cũng vô dụng như không báo.

## 10. Giả định & câu hỏi mở

**Giả định**
- **A2** — Ngưỡng mưa gây ngập lấy theo chính thành phố công bố: <50 mm/h cơ bản không ngập;
  50–70 mm/h → ~11 điểm; 70–100 mm/h → ~71 điểm; >100 mm/h kéo dài → ~220 điểm tại 54 phường-xã.
  *(Chỉ cần 4 mức này, không cần toàn văn phụ lục 220 điểm cho GĐ1.)*
- **A3** — Cấp độ rủi ro bám QĐ 18/2021/QĐ-TTg để dùng chung ngôn ngữ với cơ quan nhà nước.
- **A4** — Chuẩn khí hậu dùng thời kỳ 1991–2020 (WMO).
- **A5** — Chấp nhận rủi ro hiển thị theo **ô lưới (49 ô)**, không theo từng phường riêng lẻ.
  Phường cùng ô lưới hiển thị cùng mức rủi ro dự báo.

**Câu hỏi mở — cần chốt trước khi sang Bước 3**
1. **Mục đích thật:** portfolio hay sản phẩm có người dùng? Ảnh hưởng tới đầu tư vào SLA theo giờ.
2. **Ngân sách hạ tầng:** 0đ hay chấp nhận chi phí? Pipeline theo giờ tốn hơn batch ngày đáng kể.

## 11. Rủi ro

| # | Rủi ro | Mức | Xử lý |
|---|---|---|---|
| **R1** | **Dữ liệu mưa KHÔNG phân biệt được giữa các phường nội thành.** Đã **đo chính xác** ngày 20/08/2026 bằng centroid thật của cả 126 phường-xã: **126 phường → chỉ 49 ô lưới mưa phân biệt được**. Một ô gom **15 phường** (Đống Đa, Thanh Xuân, Khương Đình, Hoàng Liệt, Hà Đông, Tây Mỗ…) dùng chung **một chuỗi mưa duy nhất** — đúng những nơi hay ngập nhất. | **Rất cao** | Chấp nhận và thiết kế đúng bản chất: **mưa là biến động theo ô lưới (49 ô), tính dễ tổn thương là biến tĩnh theo điểm/đường**. Rủi ro điểm = f(mưa của ô lưới chứa điểm, độ tổn thương của điểm). Tuyệt đối không giả vờ có mưa phân giải mét. **Không khắc phục được bằng tọa độ phường chính xác hơn.** |
| **R2** | Chưa có nguồn quan trắc ngập thực tế theo thời gian thực → không đo được độ chính xác liên tục. | Cao | Giai đoạn 1 kiểm chứng thủ công theo §9. Khảo sát nguồn ở Bước 2. |
| **R3** | Phụ lục QĐ 2280 có thể chỉ có bản PDF/giấy, phải nhập tay ~220 điểm và geocode. | Cao | Coi là công việc thật, có kế hoạch riêng: nhập tay + geocode qua Nominatim + review. |
| **R4** | DEM miễn phí trả độ cao **số nguyên mét** (đã kiểm chứng: 10, 9, 14 m). Chênh lệch dưới 1m không thấy được, trong khi ngập cục bộ nhạy ở mức decimet. | Cao | Không dùng DEM để tự phát hiện điểm trũng. Dùng DEM làm biến phụ; điểm ngập lấy từ registry chính thức. |
| ~~R5~~ | ~~Ranh giới 126 phường-xã có thể sai/thiếu.~~ **ĐÃ ĐÓNG 20/08/2026** | — | Đã có polygon + **mã hành chính chính thức** đủ 126 đơn vị, tổng diện tích 3.360 km² khớp số liệu chính thức. Xem S13 ở Bước 2. |
| **R6** | Pipeline theo giờ chạy 24/7 → chi phí và độ phức tạp vận hành cao hơn hẳn batch ngày. | Trung bình | Cân nhắc chỉ chạy nhịp giờ trong mùa mưa (T5–T10), ngoài mùa hạ nhịp. |

---

## Phụ lục — Thay đổi so với v0.1

v0.1 lấy phạm vi 34 tỉnh toàn quốc, grain = tỉnh + điểm lưới. v0.2 thu hẹp về Hà Nội, grain =
phường-xã + điểm ngập + tuyến đường. Thay đổi này **làm bài toán khó hơn chứ không dễ hơn**:
độ phân giải dữ liệu mưa trở thành nút thắt (R1), và giá trị của dự án dịch chuyển từ
"gọi API rồi tính chỉ số" sang "ghép tri thức địa phương chính thức với dự báo".
