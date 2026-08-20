# Bước 3 — Thiết kế kiến trúc

**Hanoi Flood & Climate Risk Monitor** · v0.3 · 2026-08-20 · *Trạng thái: CHỜ DUYỆT*

> Đã chốt từ Bước 1 §10.2: **mục đích = portfolio/học tập**, **ngân sách = 0đ, chạy local**.
> v0.2: chuyển mô hình lưu trữ sang **Medallion Architecture** (Bronze → Silver → Gold) và trình
> bày **lựa chọn stack theo từng phase của data lifecycle**.
> v0.3: **đánh giá stack thay thế** dlt + MinIO/DuckLake + DuckDB + dbt + Airflow Lite/Postgres (§6.2);
> quyết định cuối chờ duyệt ở §11.

---

## 1. Ràng buộc đầu vào (từ Bước 1, 2)

| Ràng buộc | Giá trị | Hệ quả thiết kế |
|---|---|---|
| Nguồn | Chỉ S1 Forecast, S2 Archive, S5 QĐ 2280, S13 wards | Không tích hợp thêm; 2 API ngoài, 2 dữ liệu tĩnh |
| Độ phân giải mưa | **49 ô lưới** (R1) | Mưa tính theo ô, phường gán vào ô — không giả vờ chính xác hơn |
| Nhịp | Hourly (forecast) + Daily (archive) | **2 pipeline riêng biệt**, không chung |
| Freshness | Forecast ≤ 1h · Archive ~5 ngày trễ | Lịch giờ, có retry; hiển thị "as-of" trung thực |
| Latency | Nguồn → dashboard ≤ 20 phút | Budget ở §8: thực tế ~2 phút, dư địa lớn |
| DQ | 100% pass, fail → **chặn publish** | Gate trong orchestration, chỉ gold được serve |
| Chi phí | 0đ, local | 100% open-source, một máy |

## 2. Nguyên tắc thiết kế

1. **Simplicity over scale** — 1 thành phố, 49 ô, 2 API. Không cần Spark/Kafka/container phức tạp.
   Distributed ở đây là over-engineering, không phải điểm cộng portfolio.
2. **Chạy được trên 1 máy** — toàn bộ stack gọn trong một tiến trình/local, khởi động lại dễ.
3. **Medallion Architecture** — dữ liệu đi qua đúng 3 lớp Bronze → Silver → Gold, mỗi lớp một vai
   trò, dễ trace dữ liệu hỏng nằm ở đâu.
4. **Trung thực với dữ liệu** — kết quả mang nhãn "dự báo theo ô lưới ~11km", không bao giờ tô
   vẽ mưa theo phường.
5. **Idempotent + backfill được** — mọi bước có thể chạy lại không tạo trùng; lịch sử archive
   backfill theo chunk.
6. **Minh bạch** — luật rủi ro là SQL/if-else đọc được (không mô hình đen hộp), DQ là checks
   đọc được.
7. **DQ chặn publish, không chỉ cảnh báo** — chỉ dữ liệu ở lớp **Gold** được API/dashboard đọc.

## 3. Sơ đồ kiến trúc tổng thể (Medallion)

```
                        ┌───────────────────────────────────────────────┐
                        │        ORCHESTRATION — Dagster (local)        │
                        │  Hourly:  ingest→bronze → silver → gold → DQ  │
                        │           → publish_gate                       │
                        │  Daily:   archive→bronze → silver → gold → DQ  │
                        │           → publish_gate                       │
                        │  On-demand: ref_ingest (S13, S5) → reference   │
                        └──────┬──────────────────────────┬─────────────┘
                               │ fetch                     │ fetch
                   ┌───────────▼──────────┐   ┌────────────▼───────────┐
                   │  S1 Open-Meteo        │   │  S2 Open-Meteo         │
                   │  Forecast API         │   │  Archive API (ERA5)    │
                   │  49 ô · hourly · 48h  │   │  1981–nay · daily      │
                   └───────────┬──────────┘   └────────────┬───────────┘
                               │                            │
        ┌──────────────────────▼────────────────────────────▼───────────┐
        │              STORAGE — DuckDB (single file, SOT)              │
        │                                                                │
        │  ┌─────────────┐   ┌──────────────┐   ┌────────────────────┐  │
        │  │  BRONZE     │   │  SILVER      │   │  GOLD              │  │
        │  │ raw as-is   │→  │ clean/mapped │→  │ risk / flood /     │  │
        │  │ + metadata  │   │ typed, DQ'd  │   │ drought marts      │  │
        │  └─────────────┘   └──────────────┘   └─────────┬──────────┘  │
        │                    reference.* (S13, S5, ward↔grid)           │
        │                    audit.run_log                               │
        └──────────────────────┬────────────────────────────┬───────────┘
                               │ đọc (chỉ GOLD, đã qua gate) │ đọc
                   ┌───────────▼───────────┐   ┌─────────────▼──────────┐
                   │  SERVING — FastAPI    │   │  DASHBOARD — Streamlit │
                   │  read-only /v1/*      │   │  Q1–Q9 + as-of stamp   │
                   └───────────────────────┘   └────────────────────────┘
```

**Một máy, một DuckDB file, một scheduler.** Pipeline hai nhịp đều đi qua đủ 3 lớp medallion;
chỉ Gold được serve.

## 4. Các lớp Medallion

| Lớp | Vai trò | Quy tắc | Ví dụ bảng (DuckDB) |
|---|---|---|---|
| **Bronze** | Dữ liệu **nguyên vẹn như nguồn trả về**, kèm metadata (giờ fetch, source, response). | Bất biến, ghi thêm (append), không sửa. Tái chạy = đè theo partition. | `bronze.forecast`, `bronze.archive` |
| **Silver** | Dữ liệu **đã chuẩn hóa, gán kiểu, dedup, mapping** (ô lưới → phường), áp luật ngưỡng. | Có thể sửa (SCD), mỗi dòng có `updated_at`. | `silver.forecast_hourly`, `silver.daily_archive` |
| **Gold** | **Mart nghiệp vụ đã qua DQ gate** — là thứ duy nhất được serve. | Chỉ ghi khi DQ pass 100%. | `gold.risk_hourly`, `gold.flood_proxy`, `gold.drought_daily` |
| **Reference** (bổ trợ) | Dữ liệu tĩnh version hóa, cấp cho Silver/Gold. | Ghim version (commit SHA / văn bản). | `reference.wards`, `reference.grid_cells`, `reference.ward_to_grid`, `reference.thresholds_qd2280` |

## 5. Luồng dữ liệu chi tiết

### 5.1 Luồng tĩnh — reference (chạy khi cần)

1. **S13**: tải 126 GeoJSON (ghim commit SHA) → `reference.wards`.
2. **Ánh xạ ô lưới**: gọi Forecast cho 126 centroid, đọc tọa độ ô lưới model trả về → dedup →
   **49 ô** → `reference.grid_cells` + `reference.ward_to_grid` (126 bản ghi).
   → Bản cài đặt đúng **R1**: phường cùng ô = cùng chuỗi mưa.
3. **S5**: nhập tay 4 ngưỡng QĐ 2280 → `reference.thresholds_qd2280` (có version + nguồn).

### 5.2 Luồng theo giờ — cảnh báo (S1)

- Chạy **phút :05 mỗi giờ** (freshness ≤ 1h).
- Fetch 49 ô: `hourly=precipitation`, horizon 48h → **Bronze** `bronze.forecast` (partition giờ).
- **Silver**: gán kiểu, dedup, map ô→phường (`ward_to_grid`), áp luật ngưỡng S5 (mm/h → cấp 1–4),
  thêm mưa tích lũy 6h/24h (proxy lũ).
- **DQ gate** → **Gold** `gold.risk_hourly` → API/dashboard.
- Trả lời Q1–Q6.

### 5.3 Luồng theo ngày — lịch sử & chuẩn khí hậu (S2)

- Chạy **1 lần/ngày**, fetch ngày `T-5` (lag ERA5) → **Bronze** `bronze.archive` (idempotent).
- **Silver**: chuẩn hóa; tính chuẩn khí hậu 1991–2020 (A4) → `silver.climate_normal`.
- **DQ gate** → **Gold**: `gold.climate_normal_daily`, `gold.drought_daily` (30/60/90 ngày vs chuẩn,
  Q8), `gold.flood_proxy_daily` (mưa tích lũy nhiều ngày, proxy lũ).
- Backfill 1981–nay chạy 1 lần theo chunk năm, idempotent.

## 6. Lựa chọn stack theo từng phase của data lifecycle

> Đi từng phase trong vòng đời dữ liệu: **Ingest → Bronze storage → Transform/Validate (Silver)
> → Aggregate (Gold) → Serve → Observe/Govern**. Chọn stack phù hợp thang bài toán (0đ, local, 1 máy)
> chứ không chọn stack "nghe cho sang".

### Phase 1 — Ingest (fetch nguồn → Bronze)

| Mục | Chọn | Lý do |
|---|---|---|
| HTTP client | `httpx` (async) | 1 request multi-point cho 49 ô; retry/timeout linh hoạt |
| Lưu raw | DuckDB `bronze.*` + mirror parquet | Ghi nguyên vẹn response + metadata (source, fetched_at); parquet là dạng trung lập, dễ đọc lại |
| Cách khác (không dùng) | Kafka/Spark Streaming, Airbyte/Fivetran | Over-engineering cho 2 HTTP API theo giờ; không có CDC, không có sink phức tạp |

### Phase 2 — Bronze storage (SOT)

| Mục | Chọn | Lý do |
|---|---|---|
| Storage | **DuckDB** (1 file) | 0 ops, SQL, đọc parquet trực tiếp, chạy local, analytics nhanh; đủ SOT giai đoạn 1 |
| Partition | Theo ngày/giờ | Delete-and-reload idempotent, backfill dễ |
| Cách khác (không dùng) | Postgres, MinIO+parquet, BigQuery | Thêm hạ tầng/chi phí không cần thiết (A8: đổi Postgres khi thành sản phẩm, giữ nguyên medallion) |

### Phase 3 — Transform & Validate (Bronze → Silver)

| Mục | Chọn | Lý do |
|---|---|---|
| Transform | **SQL trong DuckDB** + script Python mapping | Luật ngưỡng/sum/join là SQL đọc được, minh bạch (nguyên tắc 6); Python chỉ lo phần mapping địa lý centroid→ô |
| Validate (DQ) | **Dagster asset checks** viết SQL + **Soda Core** | Checks khai báo đọc được; 100% pass mới cho sang Gold; Soda free |
| Cách khác (không dùng) | dbt + Airflow, Spark jobs | dbt mạnh nhưng thêm khái niệm + profile; với 2 nguồn thì SQL thẳng trong DuckDB là đủ |

### Phase 4 — Aggregate (Silver → Gold)

| Mục | Chọn | Lý do |
|---|---|---|
| Aggregation | **SQL view / marts trong DuckDB** | Mart là view/tables tái chạy được, join reference; không cần engine riêng |
| Grain | Gold mart theo `(phường, giờ)`, `(phường, ngày)`, `(ô, ngày)` | Khớp fact ở Bước 1 §5 |
| Cách khác (không dùng) | ClickHouse, dbt incremental | ClickHouse dư sức mạnh cho vài trăm nghìn dòng/năm |

### Phase 5 — Serve (Gold → consumer)

| Mục | Chọn | Lý do |
|---|---|---|
| API | **FastAPI** read-only (`/v1/*`) | Nhẹ, chuẩn OpenAPI, chỉ đọc Gold (bắt buộc theo thiết kế) |
| Dashboard | **Streamlit** | Làm nhanh, đẹp cho demo; hiển thị as-of + nhãn độ phân giải ô lưới |
| Cách khác (không dùng) | Metabase/Superset, dbt Semantic Layer | Metabase nặng hơn cần; Semantic Layer dư cho 1 dashboard |

### Phase 6 — Observe & Govern (xuyên suốt)

| Mục | Chọn | Lý do |
|---|---|---|
| Orchestration | **Dagster** (community) | Schedule hourly/daily, retries, catch-up, lineage UI, DQ-as-asset-checks — đồng bộ với nguyên tắc gate |
| Freshness/alert | Dagster + **ntfy** webhook | Báo fail/stale miễn phí tới điện thoại |
| Lineage & audit | Dagster asset graph + `audit.run_log` | Biết chính xác bản ghi nào từ nguồn nào, qua lớp nào, DQ ra sao |
| Cách khác (không dùng) | Airflow, cron+script rải rác | Airflow nặng cho 1 máy; cron rải rác thì mất lineage & gate |

**Tóm tắt stack** — toàn bộ chạy local, 0đ:

```
Python 3.13 + uv
├── DuckDB          → Bronze / Silver / Gold / Reference (1 file SOT)
├── Dagster         → orchestration, retry, catch-up, DQ gates, lineage
├── Soda Core       → DQ checks khai báo (thay thế: SQL checks thuần)
├── httpx           → fetch Open-Meteo (S1, S2)
├── FastAPI         → serve Gold (read-only)
└── Streamlit       → dashboard (Q1–Q9)
```

### 6.2 Đánh giá stack thay thế đề xuất (v0.3)

Stack được đề xuất để cân nhắc thay thế: **ingest = dlt · lakehouse = MinIO + DuckLake ·
process = DuckDB · transform = dbt · orchestration = Airflow Lite + Postgres**.

**Đánh giá từng phase (so với v0.2):**

| Phase | v0.2 (hiện tại) | Đề xuất mới | Đánh giá | Verdict |
|---|---|---|---|---|
| Ingest | `httpx` gọi API + ghi DuckDB | **dlt** (filesystem/destination Parquet) | dlt khai báo pipeline (`rest_api` source), tự incremental + retry + schema evolution; lưu Bronze dạng file giữ "as-is". Học giá trị cao, chuẩn công cụ ELT hiện đại | ✅ Nên nâng cấp |
| Bronze storage (SOT) | DuckDB 1 file | **MinIO** (Parquet) + **DuckLake** catalog | DuckLake (DuckDB team, **stable từ DuckDB 1.5.2 04/2026**): metadata trong SQL DB, data là Parquet, có ACID/time-travel/schema evolution/multi-writer. Dữ liệu dự án rất nhỏ → lợi ích chủ yếu là *học đúng lakehouse* + forward-compatible, không phải vì volume | ⚠️ Hợp lý nếu muốn học lakehouse |
| Process | DuckDB | **DuckDB** (qua DuckLake extension) | Giữ nguyên engine; DuckDB đọc/ghi Parquet trên MinIO thay vì file cục bộ | ✅ Không đổi |
| Transform & Validate | SQL thủ công + Dagster checks | **dbt** (dbt-duckdb) + `dbt test` | dbt là chuẩn ngành: SQL-first minh bạch (đúng nguyên tắc 6), model = medallion, test = DQ gate, docs + lineage đẹp cho portfolio. `dbt-duckdb` nạp extension (cần POC DuckLake, xem R13) | ✅ Nên nâng cấp |
| Orchestration | Dagster | **Airflow Lite + Postgres** | Airflow = orchestrator phổ biến nhất (giá trị portfolio cao), 3.x đã nhẹ hơn nhưng footprint vẫn ~1GB+ (Dagster ~200MB, APScheduler ~50MB). Bù: Postgres dùng **kép** cho Airflow metadata + DuckLake catalog → giảm một DB | ⚠️ Chấp nhận nếu chịu footprint |
| Serve | FastAPI + Streamlit | **giữ nguyên** | Không phụ thuộc lựa chọn trên | ✅ Không đổi |

**Vì sao stack này nhất quán nội tại (không chỉ "trendy"):**
- **DuckLake giải quyết điểm yếu thật của DuckDB file**: DuckDB đơn file là *single-writer* (một process
  ghi tại một thời điểm). Dưới Airflow, scheduler/worker chạy nhiều process ghi cùng dataset → DuckLake
  (multi-writer, ACID qua catalog DB) là giải pháp đúng vấn đề, không phải thêm cho sang.
- **Postgres dùng kép**: Airflow bắt buộc có metadata DB; DuckLake cần catalog DB. Dùng **một** Postgres
  cho cả hai → hợp nhất hạ tầng.
- **dlt + dbt + DuckDB là bộ ba "ELT hiện đại"**: dlt lo EL (load raw → Bronze), dbt lo T (Bronze→Silver→Gold),
  DuckDB lo engine — đúng tinh thần medallion.

**Thành phần đề xuất cụ thể (nếu chốt):**

```
Docker Compose (1 máy, 0đ)
├── MinIO     → Bronze/Silver/Gold Parquet (lake)
├── Postgres  → Airflow metadata DB + DuckLake catalog (1 DB, 2 vai trò)
├── Airflow   → orchestration, retry, catch-up, trigger dbt
├── dbt + DuckDB (DuckLake ext) → transform + DQ gate (dbt test)
├── dlt       → ingest S1/S2 → Bronze Parquet trên MinIO
├── FastAPI   → serve Gold (read-only)
└── Streamlit → dashboard (Q1–Q9)
```

**Điểm yếu trung thực (không giấu):**
- **Ops weight**: MinIO + Postgres + Airflow = 3 container + footprint Airflow (~1GB RAM idle) —
  nặng hơn hẳn v0.2 (1 process). Máy local cần ~4–8GB RAM; phải Docker Compose + limit resource.
- **dbt-duckdb + DuckLake**: tích hợp còn mới, cần POC ở Bước 4 (R13).
- **dlt normalization**: dlt biến đổi JSON lồng nhau thành bảng; muốn Bronze "as-is" phải dùng
  destination file (parquet/jsonl), không ghi thẳng bảng (A13).
- Với đúng khối lượng dữ liệu này, stack mới **thừa sức** — chấp nhận vì giá trị học + forward-compatible,
  không phải vì dữ liệu đòi hỏi.

## 7. Mô hình lưu trữ (chi tiết Bước 6)

- DuckDB **1 file**, 4 namespace theo medallion + reference + audit.
- Mọi bảng có partition (ngày/giờ) để idempotent; `audit.run_log` ghi mọi run để trace.
- Chỉ Gold được đọc bởi FastAPI/dashboard — áp ở tầng query, không chỉ ở tầng quy ước.
- Nếu chốt stack v0.3 (MinIO + DuckLake): đổi "1 file" thành "Parquet trên MinIO + catalog Postgres",
  **giữ nguyên** namespace medallion và quy tắc trên — chỉ đổi tầng vật lý, không đổi mô hình logic.

## 8. Budget độ trễ (mục tiêu ≤ 20 phút)

| Bước | Ước lượng |
|---|---|
| Fetch 49 ô (1 request multi-point) | ~5–30 s |
| Bronze → Silver → Gold (SQL) | < 30 s |
| DQ checks | < 10 s |
| Publish (ghi Gold, API đọc trực tiếp) | < 5 s |
| **Tổng** | **~1–2 phút** — dư địa lớn so với 20 phút, chấp nhận retry 2–3 lần |

## 9. Xử lý lỗi & retry

- **Network/5xx**: retry exponential backoff (tối đa 3), có jitter.
- **Thiếu giờ forecast** (máy tắt, mất mạng): Dagster catch-up khi máy bật lại → tự chạy các giờ
  lỡ; đối chứng quá khứ lấy từ Archive (S2) khi về (~5 ngày sau).
- **Trùng lặp**: mọi bước idempotent theo partition; chạy lại = ghi đè không trùng.
- **DQ fail**: chặn Gold; run fail + alert qua ntfy; Gold cũ vẫn phục vụ (không mất sản phẩm đột
  ngột), gắn nhãn "stale".

## 10. Observability & DQ

- **Dagster UI**: lịch sử run, success/fail, lineage Bronze→Silver→Gold.
- **Freshness check**: Gold hourly phải mới hơn 1h; vi phạm → alert.
- **DQ checks** (mỗi mart Gold): non-null, phạm vi hợp lý (mưa ≥ 0, cấp 1–4), số ô/giờ đủ, ổn định
  giữa 2 giờ liên tiếp, khớp `reference.ward_to_grid`.
- **Alert**: ntfy khi fail/stale.
- **Trung thực**: dashboard hiển thị **as-of timestamp** (dữ liệu đến giờ nào, từ nguồn nào).

## 11. Giả định & câu hỏi mở

**Giả định mới (tiếp theo A6–A9)**
- **A10** — Soda Core là tùy chọn: nếu thấy thừa dep thì dùng thẳng Dagster asset checks viết SQL
  (cùng kết quả gating).
- **A11** — DuckDB là SOT giai đoạn 1; khi chuyển Postgres (A8) thì cấu trúc medallion giữ nguyên,
  chỉ đổi engine — đây là lý do giữ bronze/silver/gold tách lớp rõ.
- **A12** — Nếu chốt stack mới: DuckLake dùng **catalog Postgres** (dùng chung với Airflow metadata),
  không thêm DB riêng — hợp nhất hạ tầng.
- **A13** — Nếu chốt stack mới: Bronze qua dlt dùng **filesystem destination** (Parquet/JSONL trên
  MinIO) để giữ đúng "as-is" theo nguyên tắc medallion, thay vì để dlt normalize thành bảng.
- **A14** — Nếu chốt stack mới: Airflow chạy **local profile** (LocalExecutor, không Celery/Redis),
  chỉ cài các provider cần thiết (DuckDB/HTTP); Postgres là metadata DB.

**Câu hỏi mở — chờ duyệt**
1. **Chốt stack nào?** (a) giữ v0.2: Dagster + DuckDB file · (b) stack mới: dlt + MinIO/DuckLake +
   dbt + Airflow Lite/Postgres · (c) hybrid: dlt + dbt + Airflow nhưng giữ DuckDB file, bỏ MinIO/DuckLake.
2. Dagster có chấp nhận được với máy local không, hay hạ cấp APScheduler (A9)? — *chỉ khi chọn (a)*.
3. Dashboard Streamlit đọc trực tiếp Gold, hay qua FastAPI (khuyến nghị qua FastAPI để tách serving)?
4. Chấp nhận thêm Soda Core (A10) hay dùng thuần SQL checks? — *chỉ khi chọn (a)*.

## 12. Rủi ro mới (tiếp theo R7–R11)

| # | Rủi ro | Mức | Xử lý |
|---|---|---|---|
| **R10** | Medallion với DuckDB có thể bị hiểu lầm là "thêm lớp vô nghĩa" cho dữ liệu nhỏ. | Thấp | Giữ 3 lớp đúng vai trò (as-is / conformed / business), mỗi lớp có mục đích + DQ riêng; ghi rõ trace ở `audit.run_log`. |
| **R11** | Silver lưu cả chuỗi lịch sử dự báo (forecast 48h × 49 ô × 8760 giờ/năm) — kích thước vẫn nhỏ nhưng cần nén/hợp nhất đúng. | Thấp | Partition theo ngày; nén định kỳ; chỉ giữ gold đã gộp cho serve. |
| **R12** | Stack mới nặng ops: 3 container (MinIO, Postgres, Airflow) + footprint Airflow ~1GB → quá tải máy local. | Trung bình | Docker Compose + limit resource; Airflow chạy LocalExecutor, chỉ bật nhịp giờ trong mùa mưa (R6); hạ cấp APScheduler nếu quá tải (A9). |
| **R13** | `dbt-duckdb` + DuckLake extension chưa chín → transform không chạy trên lakehouse. | Trung bình | **POC ở Bước 4** trước khi cam kết; fallback: dbt chạy trên DuckDB file rồi `COPY` Parquet lên MinIO — vẫn là lakehouse, chỉ bỏ lớp DuckLake catalog. |
| **R14** | dlt normalize làm mất dạng "as-is" của Bronze. | Thấp | Dùng filesystem destination (A13) giữ Parquet/JSONL gốc; bảng normalized coi là Silver. |

## 13. Chưa làm trong bước này (dành cho bước sau)

- Chi tiết schema medallion (bảng/cột/kiểu) — **Bước 6**.
- Cài đặt ingest thật — **Bước 4**; clean/transform — **Bước 5**.
- DQ/observability cài đặt — **Bước 7**; API/dashboard — **Bước 8**.
- Governance (version hóa, tài liệu, chuẩn hóa) — **Bước 9**.