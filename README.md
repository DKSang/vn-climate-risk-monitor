# VN Climate Risk Monitor

### Nền tảng giám sát áp lực mưa và phát lại dữ liệu thời tiết lịch sử tại Hà Nội

![Python](https://img.shields.io/badge/Language-Python_3.13-3776AB)
![Lakehouse](https://img.shields.io/badge/Lakehouse-DuckLake-F9C74F)
![Storage](https://img.shields.io/badge/Object_Storage-MinIO-C72E49)
![Orchestration](https://img.shields.io/badge/Orchestration-Apache_Airflow-017CEE)
![Dashboard](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B)
![Deployment](https://img.shields.io/badge/Deployment-Docker_Compose-2496ED)

## Bài toán

Mưa lớn và ngập lụt đô thị có thể gây gián đoạn giao thông, ảnh hưởng sinh hoạt
và tạo áp lực lên hạ tầng của Hà Nội. Để hỗ trợ việc theo dõi rủi ro, dữ liệu dự
báo cần được cập nhật thường xuyên, dữ liệu lịch sử cần có khả năng phát lại, và
mọi kết quả công bố phải truy vết được về nguồn.

VN Climate Risk Monitor xây dựng một data platform tự động cho **126 phường/xã
của Hà Nội**. Hệ thống thu thập dữ liệu thời tiết theo giờ từ Open-Meteo, lưu
nguyên bản phản hồi nguồn, xử lý dữ liệu theo kiến trúc medallion và hiển thị:

- dự báo mưa trong 72 giờ;
- chỉ số áp lực mưa có thể giải thích theo từng phường/xã;
- dữ liệu mưa lịch sử theo ngày, giờ và nguồn archive;
- trạng thái pipeline, checkpoint, lineage và lịch sử publication.

> [!IMPORTANT]
> Đây là dự án portfolio chạy trên một máy cục bộ. Hệ thống không phải cảnh báo
> thời tiết chính thức, không dự đoán xác suất ngập và không được thiết kế như
> một dịch vụ high availability.

## Kiến trúc hệ thống

![Kiến trúc VN Climate Risk Monitor](docs/vn-climate-risk-monitor-architecture.png)

| Lớp | Thành phần | Trách nhiệm |
|---|---|---|
| Source | Open-Meteo Forecast & Archive APIs, PostgreSQL ward data và CSV reference data | Cung cấp dữ liệu thời tiết, địa giới và dữ liệu tham chiếu |
| Bronze | MinIO | Lưu nguyên byte phản hồi HTTP bằng object key bất biến |
| Control plane | PostgreSQL | Lưu DuckLake catalog, lease, checkpoint, retry và audit state |
| Silver | Auto Loader + DuckLake | Parse file nguồn, nạp staging và giữ `_source_file` lineage |
| Transform | dbt + DuckDB | Chuẩn hóa, loại trùng, kiểm thử và xây dựng Gold marts |
| Orchestration | Apache Airflow | Lập lịch, quản lý dependency, retry và single-writer pool |
| Serving | Streamlit | Đọc snapshot đã kiểm thử và hiển thị dashboard |

Airflow điều phối toàn bộ luồng: Python thu thập dữ liệu nguồn vào Bronze,
Auto Loader đưa dữ liệu sang Silver, dbt và DuckDB xây dựng Gold rồi Streamlit
đọc snapshot đã công bố. MinIO lưu file dữ liệu, còn PostgreSQL giữ DuckLake
catalog cùng trạng thái ingestion, checkpoint và audit.

### dbt lineage

![dbt lineage của pipeline](docs/dbt-lineage.png)

Lineage được đọc từ trái sang phải; mỗi mũi tên là một dependency `source()` hoặc
`ref()` để dbt tự xác định thứ tự build. Hai nhánh thời tiết archive và forecast
được giữ riêng vì khác model, grain và mục đích sử dụng, nhưng cùng dùng các
dimension địa lý khi xuất bản sang Gold.

| Nhóm model | Grain và biến đổi chính | Vai trò trong pipeline |
|---|---|---|
| `silver_staging.stg_weather_archive_hourly` | Change log append-only của model × tọa độ grid × giờ; giữ `_source_file` và `_ingested_at` | Nguồn archive do Auto Loader nạp từ file Bronze |
| `int_weather_archive_hourly` | Một dòng hiện hành cho model × `grid_cell_id` × `valid_time_utc`; chuẩn hóa tọa độ và loại trùng deterministic | Curated Silver cho lịch sử ERA5 và ECMWF IFS |
| `silver_staging.stg_weather_forecast` | Change log append-only của từng lần lấy forecast | Nguồn forecast do Auto Loader nạp từ Bronze |
| `int_weather_forecast_hourly` | Forecast run × grid × giờ; chỉ nhận run đủ 126 locations × 72 giờ rồi loại trùng | Giữ lịch sử các forecast vintage mà không trộn các run |
| `stg_seed__ward` và `stg_seed__ward_grid` | 126 phường/xã Hà Nội và ánh xạ phường → grid theo weather model | Chuẩn hóa geography seed trước khi tạo dimension |
| `dim_ward`, `dim_grid` | Một dòng cho mỗi phường và mỗi ô lưới thời tiết | Dimension dùng chung cho archive, forecast và dashboard |
| `bridge_ward_grid` | Phường × weather model → grid; forecast dùng grid gần nhất của mesh hiện hành | Nối dữ liệu theo ô lưới về địa bàn hành chính mà không nhúng grid vào `dim_ward` |
| `fct_rain_archive_hourly` | Grid × giờ với cửa sổ mưa 1/3/6/12/24 giờ và các dải kịch bản | Phục vụ phát lại, đối chiếu và phân tích mưa lịch sử |
| `fct_rain_forecast_hourly` | Forecast run × grid × giờ với cả rolling history và `forecast_next_*h_mm` | Giữ đầy đủ lịch sử dự báo để so sánh các lần phát hành |
| `fct_rain_forecast_current_hourly` | View của horizon thuộc forecast run mới nhất | Cung cấp lát cắt forecast hiện hành cho truy vấn nhanh |
| `fct_rain_pressure_alert` | Phường × giờ của run mới nhất; kết hợp forecast 1/3/6/24 giờ, revision và persistence | Tạo `pressure_score`, `pressure_level` và lý do kích hoạt cho dashboard; đây không phải xác suất ngập hay cảnh báo chính thức |

Các test schema kiểm tra unique key, `not_null`, relationship, accepted values,
freshness và phạm vi lượng mưa. Chỉ khi graph cùng các test liên quan thành công,
pipeline mới cập nhật `published_snapshot_id` để Streamlit đọc một snapshot nhất
quán thay vì dữ liệu đang được build dở.

Thiết kế sử dụng một writer duy nhất để giữ quá trình phục hồi dễ hiểu và phù
hợp với môi trường local. Dashboard luôn pin vào một `published_snapshot_id` đã
vượt qua toàn bộ dbt tests, tránh việc một trang đọc lẫn dữ liệu từ hai lần công
bố khác nhau.

Chi tiết về ownership, publication và các đánh đổi kỹ thuật nằm trong
[tài liệu kiến trúc](docs/03-architecture.md).

## Tech Stack

### Lưu trữ và truy vấn

![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![DuckLake](https://img.shields.io/badge/DuckLake-Lakehouse-F9C74F?style=for-the-badge)

### Data Engineering

![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Provero](https://img.shields.io/badge/Provero-Data_Quality-00A88F?style=for-the-badge)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![uv](https://img.shields.io/badge/uv-Package_Manager-DE5FE9?style=for-the-badge&logo=uv&logoColor=white)

### Dashboard

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![deck.gl](https://img.shields.io/badge/deck.gl-8A2BE2?style=for-the-badge)
![Altair](https://img.shields.io/badge/Altair-1F77B4?style=for-the-badge)

### Containerization

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)

### Thư viện chính

| Thư viện | Vai trò |
|---|---|
| `duckdb` | Compute engine và kết nối DuckLake |
| `minio` | Đọc/ghi dữ liệu Bronze trên object storage |
| `psycopg` | Truy cập PostgreSQL control plane |
| `dbt-duckdb` | Transformation, data tests và publication |
| `provero` | Quality gate fail-closed cho Raw và Silver intermediate |
| `streamlit` | Xây dựng giao diện dashboard |
| `pydeck` | Hiển thị bản đồ tương tác |
| `pandas` và `altair` | Xử lý và trực quan hóa dữ liệu trên dashboard |

## Cấu trúc dự án

```text
vn-climate-risk-monitor/
│
├── docker/                         # Dockerfile cho Airflow và dashboard
├── docs/                           # Kiến trúc, data contract và runbook
├── orchestration/
│   └── dags/                       # Forecast, archive và maintenance DAGs
├── scripts/                        # Backup, restore và kiểm tra tài liệu
├── serving/
│   └── dashboard/                  # Ứng dụng Streamlit và query modules
├── src/vn_climate_risk_monitor/
│   ├── auto_loader/                # Bronze → Silver staging và parser SQL
│   ├── auto_process/               # Silver → Gold với dbt
│   ├── operations/                 # Bootstrap, maintenance và reset
│   ├── platform/                   # Settings và storage adapters
│   ├── quality/                    # Health và readiness checks
│   └── sources/open_meteo/         # Lập kế hoạch và tải dữ liệu nguồn
├── tests/                          # Unit tests và contract tests
├── tools/                          # Tạo GeoJSON
├── transform/
│   ├── macros/                     # Incremental scope và data quality
│   ├── models/                     # Staging, intermediate và Gold marts
│   ├── seeds/                      # Geography references
│   └── tests/                      # dbt singular tests
├── .env.example                    # Mẫu cấu hình local
├── docker-compose.yml              # Toàn bộ single-node runtime
├── pyproject.toml                  # Package, dependencies và CLI entry points
└── uv.lock                         # Dependency lockfile
```

### Cấu trúc thư mục Raw Lakehouse

![Cấu trúc thư mục Raw Lakehouse trên MinIO](docs/raw-lakehouse-directory.png)

Bucket `vn-climate` sử dụng prefix phân cấp để tách loại dữ liệu, model và cửa
sổ thời gian ngay từ Bronze. Object key thực tế có cấu trúc:

```text
vn-climate/
└── bronze/files/open_meteo/
    ├── forecast/incremental/YYYY/MM/DD/HH/
    │   └── run_YYYYMMDDTHHMMSS/response_NNN.json
    └── historical_weather_hourly/
        ├── backfill/year=YYYY/month=MM/
        │   └── run_YYYYMMDDTHHMMSS/response_NNN.json   # ERA5, trước 2017
        └── ifs/year=YYYY/month=MM/
            └── run_YYYYMMDDTHHMMSS/response_NNN.json   # ECMWF IFS, từ 2017
```

| Thành phần path | Ý nghĩa |
|---|---|
| `bronze/files` | Landing zone lưu phản hồi nguồn ở dạng nguyên bản, chưa chuẩn hóa sang schema phân tích. |
| `forecast/incremental` | Forecast 72 giờ được phân vùng theo slot UTC; cùng một slot luôn dùng một `run_id` ổn định để retry không tạo vintage mới. |
| `historical_weather_hourly/backfill` | Archive ERA5 trước năm 2017, phân vùng theo `year=` và `month=` để backfill và kiểm tra coverage theo tháng. |
| `historical_weather_hourly/ifs` | Archive ECMWF IFS từ năm 2017 trở đi, tách khỏi ERA5 để không trộn weather model có độ phân giải khác nhau. |
| `run_*` | Định danh một lần thu thập; giữ các response của cùng request window trong một nhóm có thể audit và replay. |
| `response_NNN.json` | Phản hồi HTTP nguyên bản của một batch weather grid. Với batch mặc định 25 locations, 126 phường/xã tạo tối đa sáu file cho một forecast run. |

Fetcher ghi chính xác response bytes vào object storage và bỏ qua tên file đã tồn
tại trong cùng partition, nên retry có tính idempotent. Khi Auto Loader nạp sang
Silver staging, object key được giữ trong `_source_file`; nhờ đó mỗi bản ghi có
thể truy ngược về file nguồn, run thu thập và cửa sổ thời gian ban đầu.

## Nguồn dữ liệu

| Nguồn | Loại dữ liệu | Phạm vi | Mục đích |
|---|---|---|---|
| Open-Meteo Forecast API | Thời tiết theo giờ | 72 giờ tiếp theo | Dự báo mưa và tính rainfall-pressure signal |
| Open-Meteo Archive API | Thời tiết lịch sử theo giờ | ERA5 trước 2017, ECMWF IFS từ 2017 | Phát lại dữ liệu mưa lịch sử |
| Geography seeds | Tọa độ, ranh giới và ánh xạ weather grid | 126 phường/xã Hà Nội | Liên kết dữ liệu thời tiết với địa giới hành chính |

Forecast và archive sử dụng grain riêng. Mỗi bản ghi staging giữ metadata ingestion
và `_source_file`; các mô hình thời tiết lịch sử không bị trộn âm thầm vào cùng
một grid. Xem [data contracts](docs/04-data-contracts.md) để biết đầy đủ grain,
quality gates và Gold models.

## Các giai đoạn pipeline

Hai data DAG dùng cùng chuỗi bảy task. DAG forecast xử lý slot giờ tại
`data_interval_end`; DAG archive xử lý tháng tại `data_interval_start`.

![Airflow DAG xử lý Open-Meteo forecast](docs/airflow-forecast-dag.png)

| Thứ tự | Airflow task | Xử lý thực tế |
|---:|---|---|
| 1 | `ingest_bronze` | Gọi Open-Meteo theo slot forecast 72 giờ hoặc tháng archive, chia request theo weather grid và lưu phản hồi JSON nguyên bản vào MinIO bằng object key bất biến. |
| 2 | `validate_bronze` | Chạy Provero trên đúng forecast run hoặc tháng archive vừa tải; yêu cầu có dữ liệu, đủ metadata và hourly fields, đúng cửa sổ thời gian, tọa độ hợp lệ, không trùng `(file, location, time)` và các giá trị mưa nằm trong phạm vi cho phép. |
| 3 | `load_silver_staging` | Auto Loader discovery các object chưa xử lý, đăng ký và lease file trong PostgreSQL, parse JSON rồi append vào bảng Silver staging. File đã commit không bị nạp lại. |
| 4 | `build_silver_intermediate` | Mở hoặc tiếp tục processing run ổn định theo Airflow `run_id`, lấy checkpoint bounds và chỉ build `int_weather_forecast_hourly` hoặc `int_weather_archive_hourly`. Checkpoint chưa được cập nhật ở bước này. |
| 5 | `validate_silver_intermediate` | Kiểm tra curated Silver không rỗng, key/thời gian/lượng mưa không null, giá trị nằm đúng phạm vi và grain không trùng. Forecast còn phải bảo đảm mỗi run có đủ horizon 72 giờ. |
| 6 | `publish_gold` | Dùng bounds của cùng processing run để chạy `dbt build` cho các marts mang tag `forecast` hoặc `archive`, bao gồm data tests. Chỉ sau khi thành công mới ghi metrics, lưu `published_snapshot_id` và tiến checkpoint. |
| 7 | `check_pipeline_health` | Chạy health check theo scope tương ứng với `--require-gold`, xác nhận đầu ra Gold bắt buộc sẵn sàng sau publication. |

Các task chạy qua pool `lakehouse_single_writer_pool` và mỗi DAG chỉ có một run
hoạt động tại một thời điểm. Task lỗi được retry hai lần, cách nhau 180 giây.
Nếu lỗi xảy ra từ bước build Silver đến publish Gold, processing run tương ứng
được đóng ở trạng thái lỗi và checkpoint không tiến; mọi task failure đều gọi
health callback để gửi thông báo khi webhook đã được cấu hình.

| DAG | Lịch mặc định | Chức năng |
|---|---|---|
| `open_meteo_forecast_hourly` | Phút 15 mỗi giờ | Cập nhật dự báo 72 giờ |
| `open_meteo_archive_monthly` | 02:30 ngày đầu tháng | Nạp thêm dữ liệu archive |
| `lakehouse_maintenance_daily` | 03:30 mỗi ngày | Retention snapshot và dọn file an toàn |

## Dashboard

Dashboard Streamlit gồm ba luồng khám phá chính:

### Bản đồ dự báo

![Dashboard bản đồ dự báo và áp lực mưa tại Hà Nội](docs/dashboard-forecast-map.png)

Màn hình bản đồ sử dụng forecast snapshot đã được kiểm thử và công bố gần nhất,
giúp toàn bộ KPI, polygon và bảng xếp hạng cùng đọc một phiên bản dữ liệu nhất
quán. Người dùng có thể chọn mốc thời gian trong horizon, chỉ báo lượng mưa và
basemap mà không làm thay đổi snapshot nguồn.

- **KPI tổng quan:** thể hiện độ phủ phường/xã, lượng mưa tích lũy 24 giờ lớn
  nhất, số phường vượt dải nền và số phường có áp lực cao.
- **Chú giải ngưỡng:** phân loại lượng mưa 24 giờ thành các dải màu cố định từ
  dưới 50 mm đến trên 300 mm để so sánh trực quan giữa các địa bàn.
- **Bản đồ chuyên đề:** tô màu 126 phường/xã theo chỉ báo đang chọn; tooltip cung
  cấp lượng mưa tại thời điểm, lượng mưa tích lũy, điểm/mức áp lực và lý do kích hoạt.
- **Bảng ưu tiên:** hai tab áp lực mưa và mưa 24 giờ hỗ trợ xếp hạng nhanh các
  phường/xã cần theo dõi tại mốc đang xem.

`pressure_score` là điểm ưu tiên vận hành từ tín hiệu mưa dự báo, revision và độ
bền qua nhiều forecast run; đây không phải xác suất hoặc dự báo độ sâu ngập.

### Chi tiết phường/xã

Đi sâu vào chuỗi thời gian của một địa bàn: lượng mưa theo cửa sổ, pressure
signal và độ dai dẳng.

### Phát lại quá khứ

Phát lại lượng mưa lịch sử theo ngày, giờ địa phương và nguồn archive.

## Chạy local

Yêu cầu [Git](https://git-scm.com/downloads) và
[Docker Desktop](https://www.docker.com/products/docker-desktop/). Chạy trên
PowerShell:

```powershell
git clone https://github.com/DKSang/vn-climate-risk-monitor.git
cd vn-climate-risk-monitor
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

`.env.example` đã có credential local đơn giản; thay đổi nếu máy có người khác
truy cập và không commit `.env`. Service `bootstrap` chạy một lần rồi thoát với
code `0` là trạng thái bình thường.

| Giao diện | Địa chỉ | Tài khoản local |
|---|---|---|
| Streamlit | [http://localhost:8501](http://localhost:8501) | Không yêu cầu đăng nhập |
| Airflow | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| MinIO | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |

Trong Airflow, unpause và trigger `open_meteo_forecast_hourly`; trigger thêm
`open_meteo_archive_monthly` khi cần dữ liệu lịch sử. Xem
[runbook vận hành](docs/05-operations.md) cho backfill, health check, rewind,
backup và restore.

## Phát triển và kiểm thử

Cài [uv](https://docs.astral.sh/uv/getting-started/installation/) và dependency
từ lockfile:

```powershell
uv sync --frozen
```

Chạy các quality gates trước khi chia sẻ thay đổi:

```powershell
uv run pytest -q
uv run ruff check .
$env:PYTHONUTF8 = "1"
uv run dbt parse --project-dir transform --profiles-dir transform
docker compose config
uv run python scripts/check_docs.py
```

`PYTHONUTF8=1` giúp dbt đọc nhất quán tên địa danh tiếng Việt khi chạy trực tiếp
trên Windows. Container và CI đã sử dụng UTF-8 locale.

## Tài liệu

- [Kiến trúc hệ thống](docs/03-architecture.md) — ownership, publication và trade-offs.
- [Data contracts](docs/04-data-contracts.md) — source grain, quality gates và Gold models.
- [Vận hành](docs/05-operations.md) — bootstrap, retry, rewind, maintenance và recovery.

## Giới hạn thiết kế

- Chạy theo mô hình local single-node, single-writer; không hỗ trợ HA hoặc scale-out.
- Pressure signal phản ánh áp lực khí tượng, không phải xác suất hay độ sâu ngập.
- Geography là dữ liệu seed có version, không phải nguồn realtime.
- Credential trong `.env` chỉ phù hợp cho môi trường local, không thay thế secret
  management của production.
