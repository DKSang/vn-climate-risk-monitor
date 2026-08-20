# Bước 3a — Cấu trúc repository

**Hanoi Flood & Climate Risk Monitor** · v0.1 · 2026-08-20 · *Trạng thái: CHỜ DUYỆT*

> Ánh xạ kiến trúc v0.3 (03-architecture.md §3–§6.2) sang cấu trúc thư mục. Mỗi thư mục ứng với
> một phase trong data lifecycle / một layer medallion.

## 1. Sơ đồ cây

```
vn-climate-risk-monitor/
├── pyproject.toml              # workspace root (uv), khai báo packages + tool config
├── Makefile                    # lệnh ngắn: make ingest / make dbt / make up ...
├── .env.example                # mẫu biến môi trường (copy → .env, KHÔNG commit .env)
├── docker-compose.yml          # MinIO + Postgres + Airflow + API + Dashboard
│
├── docs/                       # Tài liệu 9 bước (business → governance)
│
├── ingest/                     # ── PHASE 1: INGEST (dlt)
│   └── dlt_pipelines/
│       ├── sources/            #   nguồn dữ liệu: open_meteo_forecast.py, open_meteo_archive.py
│       └── pipelines/          #   pipeline: forecast_pipeline.py, archive_pipeline.py
│
├── reference/                  # ── DỮ LIỆU TĨNH (S5, S13) — version hóa
│   ├── s13_wards/              #   GeoJSON 126 phường-xã (ghim commit SHA)
│   ├── qd2280/                 #   ngưỡng mưa → cấp rủi ro (nhập tay, có nguồn)
│   └── scripts/                #   build_ward_grid_mapping.py (centroid → 49 ô lưới)
│
├── lake/                       # ── PHASE 2: LAKEHOUSE (MinIO/DuckLake) — GITIGNORED
│   ├── bronze/                 #   raw as-is (dlt → Parquet)
│   ├── silver/                 #   clean/mapped/typed
│   └── gold/                   #   mart nghiệp vụ, qua DQ gate — chỉ Gold được serve
│
├── catalog/                    #   DuckLake catalog DB (metadata, gitignored)
│
├── transform/                  # ── PHASE 3 & 4: TRANSFORM (dbt + DuckDB)
│   └── dbt/
│       ├── dbt_project.yml     #   cấu hình model → mapping medallion
│       ├── profiles.yml        #   profile dbt-duckdb (DuckLake ext) → lake/
│       ├── models/
│       │   ├── bronze/         #   staging: đọc Parquet Bronze
│       │   ├── silver/         #   clean + mapping + luật ngưỡng QĐ 2280
│       │   └── gold/           #   mart: risk_hourly, flood_proxy, drought_daily
│       ├── tests/              #   DQ checks (dbt test) = cổng chặn publish
│       ├── macros/             #   luật tái dùng (mm/h → cấp 1–4)
│       ├── seeds/              #   CSV tĩnh: ngưỡng S5, ánh xạ ô lưới
│       └── analyses/           #   phân tích ad-hoc
│
├── orchestration/              # ── PHASE 6: ORCHESTRATION (Airflow Lite)
│   ├── dags/                   #   hourly_forecast_pipeline.py, daily_archive_pipeline.py,
│   │                           #   reference_load_pipeline.py, backfill_archive.py
│   ├── plugins/                #   hook/helper dùng chung
│   └── Dockerfile              #   Airflow 3.x, LocalExecutor, providers cần thiết
│
├── serving/                    # ── PHASE 5: SERVE
│   ├── api/                    #   FastAPI read-only — chỉ đọc Gold
│   │   └── app/
│   │       ├── main.py         #   /v1/risk, /v1/wards, /v1/flood, /v1/drought, /health
│   │       ├── routers/        #   endpoint theo nghiệp vụ (Q1–Q9)
│   │       └── schemas/        #   Pydantic models (response)
│   └── dashboard/              #   Streamlit
│       ├── app.py              #   bản đồ rủi ro, bảng Q1–Q9, as-of timestamp
│       └── pages/              #   trang: ngập đô thị / lũ / hạn hán
│
├── scripts/                    # ── OPS (ad-hoc)
│   ├── bootstrap.py            #   init: tạo bucket MinIO, chạy reference, tạo catalog
│   └── backfill_archive.py     #   backfill ERA5 1981–nay theo chunk năm
│
├── tests/                      # ── KIỂM THỬ
│   ├── unit/                   #   luật ngưỡng, mapping ô lưới
│   ├── integration/            #   pipeline end-to-end trên fixture nhỏ
│   └── fixtures/               #   response mẫu Open-Meteo, GeoJSON 1–2 phường
│
└── notebooks/                  # ── KHÁM PHÁ (không thuộc pipeline)
```

## 2. Ánh xạ thư mục → kiến trúc

| Kiến trúc (03-architecture.md) | Thư mục |
|---|---|
| Phase 1 Ingest (dlt) | `ingest/dlt_pipelines/` |
| Bronze storage (MinIO/DuckLake) | `lake/bronze/` + `catalog/` |
| Phase 2 SOT (lakehouse) | `lake/` + `catalog/` |
| Phase 3–4 Transform (dbt + DuckDB) | `transform/dbt/` |
| Phase 5 Serve (FastAPI + Streamlit) | `serving/api/`, `serving/dashboard/` |
| Phase 6 Orchestrate (Airflow) | `orchestration/` |
| Reference tĩnh (S5, S13) | `reference/` |
| Observability/audit | `lake/gold/` (dq pass) + `scripts/` + orchestration DAG logs |
| Ops/backfill | `scripts/` |

## 3. Quy ước

- **Bronze không sửa, chỉ append/đè theo partition** — chống lại việc "sửa tay" dữ liệu gốc.
- **Chỉ Gold được serving** — API/dashboard không bao giờ đọc bronze/silver trực tiếp.
- **Mọi thứ tái tạo được**: `lake/`, `catalog/`, `.env` đều gitignored; chạy `make bootstrap`
  để dựng lại từ reference + dữ liệu nguồn.
- **Version hóa dữ liệu tĩnh**: s13 ghim commit SHA, qd2280 có ngày/văn bản nguồn.
- **Không đặt logic nghiệp vụ trong DAG** — DAG chỉ điều phối; logic nằm ở dlt/dbt/scripts.