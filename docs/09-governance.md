# Bước 9 — Governance & Continuous Improvement

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-09-10

## 1. Mục tiêu và phạm vi

Governance của dự án trả lời năm câu hỏi: dữ liệu nào được phép dùng, ai chịu
trách nhiệm, output nào là contract, thay đổi được kiểm soát thế nào, và khi hệ
thống sai thì phát hiện–phục hồi–học lại ra sao.

Đây là MVP portfolio single-node, không phải hệ thống cảnh báo thiên tai chính
thức. Vì vậy governance ưu tiên control nhỏ nhưng thực thi được trong Git, CI,
dbt, Airflow và runbook. Không dựng thêm data catalog, policy engine hay hội đồng
phê duyệt giả lập.

Cách tiếp cận dựa trên:

- [NIST CSF 2.0](https://www.nist.gov/publications/nist-cybersecurity-framework-csf-20):
  xác định context, risk, policy, role, oversight và improvement.
- [Google SRE — Implementing SLOs](https://sre.google/workbook/implementing-slos/):
  pipeline đo freshness, coverage và correctness; SLO phải dẫn tới quyết định.
- [dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts):
  contract dành cho model được consumer phụ thuộc, nhưng không áp dụng máy móc
  khi adapter/custom materialization chưa hỗ trợ hoặc model còn biến động.
- [NIST SP 800-61r3](https://www.nist.gov/publications/incident-response-recommendations-and-considerations-cybersecurity-risk-management-csf):
  incident response là vòng đời liên tục, bài học phải quay lại control.

## 2. Trách nhiệm

Trong repo một người, **Repository maintainer** có thể giữ nhiều vai trò nhưng
trách nhiệm vẫn phải tách về mặt quyết định. Khi có thêm người, thay role bằng
tên/team thật và thêm `.github/CODEOWNERS`; không dùng account placeholder.

| Role | Accountable for | Không được tự ý |
|---|---|---|
| Product owner | Mục tiêu, wording, ngưỡng chấp nhận rủi ro | Gọi pressure là xác suất ngập/cảnh báo chính thức |
| Data owner | Nguồn được phép dùng, contract Gold, retention | Publish nguồn chưa kiểm quyền sử dụng |
| Data engineer/custodian | Ingestion, storage, transform, quality, backup | Bypass gate hoặc sửa checkpoint trực tiếp |
| Data steward/reviewer | Seed địa lý, điểm úng, observation/geocode | Đặt `geocode_verified=TRUE` khi chưa review |
| Operator | Schedule, incident, restore, credential rotation | Restore vào production khi writer chưa quiesce |
| Consumer | Dashboard/API dùng đúng semantic contract | Suy diễn độ phân giải đường phố từ weather grid |

Owner mặc định hiện hành là repository maintainer. Mọi thay đổi semantic cần
một lượt review độc lập khi dự án có người thứ hai; trước đó checklist PR và CI
là control bắt buộc tối thiểu.

## 3. Phân loại và quyền truy cập

| Class | Dữ liệu | Quyền mặc định | Control |
|---|---|---|---|
| Public-source | Open-Meteo, S13, văn bản/điểm công khai | Read cho consumer | Attribution, giữ source URL/version |
| Curated-public | Gold rainfall, pressure, ward/flood reference | Read-only serving | dbt tests + published snapshot |
| Internal-operational | checkpoint, run/error metadata, logs, backup manifest | Operator/maintainer | Không expose qua dashboard công cộng |
| Secret | password, access key, webhook | Chỉ runtime/operator | `.env` ignored, `/run/secrets`, rotate khi lộ |

Dự án không chủ ý thu thập PII. Không thêm tên cá nhân, số điện thoại, biển số,
định danh thiết bị hoặc dữ liệu camera vào seed/Gold. Nếu nguồn mới chứa PII,
dừng onboarding và thực hiện privacy/legal review trước khi land dữ liệu.

Hiện deployment là single-node và service dùng credential vận hành chung. Đây
không phải mô hình IAM cho production nhiều người. Trước khi mở public hoặc cấp
quyền cho nhiều operator, phải tách ít nhất role read-only serving, writer
pipeline và admin/backup trong PostgreSQL/MinIO.

## 4. Data product contracts

Gold là interface duy nhất cho serving. Bronze/Silver và tên file vật lý là
implementation detail, không phải API cho consumer.

| Contract | Grain | Consumer | Reliability rule |
|---|---|---|---|
| `fct_rain_forecast_current_hourly` | run × grid × valid hour | Map, ward drilldown | Một current run, không giờ expired |
| `fct_rain_pressure_alert` | run × ward × valid hour | Map, ward drilldown | Level/coverage/score nhất quán; đủ ward-hour |
| `fct_rain_archive_hourly` | model × grid × valid hour | Archive replay | Unique grain; rolling window NULL khi thiếu giờ |
| `fct_flood_event_observation` | observation | Archive replay | Chỉ row đủ source/time/verified ward mới replay |
| `dim_ward`, `dim_grid`, `bridge_ward_grid` | business key từng bảng | Mọi trang | 126 ward và đúng một mapping/ward/model |

Contract chi tiết ở
[transform/models/marts/schema.yml](../transform/models/marts/schema.yml); logic
chéo bảng ở [transform/tests](../transform/tests). DuckLake dùng custom
materialization nên không bật `contract.enforced` chỉ để có nhãn governance;
shape được bảo vệ bằng khai báo cột, schema tests, singular tests, CI và consumer
smoke test. Khi adapter hỗ trợ contract ổn định, đánh giá lại cho các model trong
bảng trên, không áp đại trà cho intermediate.

`published_snapshot_id` là ranh giới publish: consumer chỉ thấy snapshot của
Gold run `SUCCEEDED`. Build/test lỗi không làm checkpoint tiến và không trở thành
serving version.

## 5. SLO ban đầu và error budget

Đây là mục tiêu nội bộ best-effort, không phải SLA với người dùng. Cửa sổ đánh
giá rolling 30 ngày; review sau 30 ngày dữ liệu thật đầu tiên.

| SLI | Cách đo | SLO ban đầu |
|---|---|---|
| Forecast freshness | Tỷ lệ health evaluations có age ≤ 24h | ≥ 95% |
| Forecast coverage | Published run đủ 72 source hours và 126 locations | 100% |
| Data correctness | Gold publication vượt toàn bộ selected dbt tests | 100% |
| Serving integrity | Health thấy publication snapshot còn đọc được | 100% |
| Archive completeness | Closed model/grid/month đủ số giờ lịch | 100% |
| Dashboard availability | Tỷ lệ probe `/_stcore/health` thành công | ≥ 95% |

Nguồn đo là Airflow run/task history, `logs/health.json`, processing audit và
dashboard probe. Không lấy số đo hiện tại rồi quảng bá thành SLA.

Error budget cho hai SLO 95% là 5% evaluation points trong 30 ngày. Khi dùng quá
50% budget trước nửa cửa sổ, ưu tiên reliability/quality thay feature. Khi hết
budget, chỉ merge sửa correctness, security, recovery hoặc giảm rủi ro cho đến
khi SLO trở lại. SLO 100% là fail-closed: không publish dữ liệu vi phạm.

## 6. Quy trình thay đổi

Mọi thay đổi đi qua pull request checklist ở
[.github/pull_request_template.md](../.github/pull_request_template.md) và CI.

1. **Phân loại**: implementation-only, backward-compatible, semantic, breaking,
   hay emergency.
2. **Impact**: liệt kê source/model/consumer, grain, volume, backfill, license,
   privacy và rollback.
3. **Validate**: unit test, dbt parse/test, freshness, Provero và health theo
   phạm vi thay đổi.
4. **Publish**: chỉ Gold run `SUCCEEDED`; semantic change/full refresh phải có
   `REASON` để ghi processing audit.
5. **Observe**: kiểm tra run kế tiếp, current horizon, publication snapshot và
   dashboard.
6. **Close**: cập nhật contract/runbook/risk register nếu assumption thay đổi.

Breaking change đối với contract đang có consumer phải dùng tên/version mới và
migration window, trừ khi toàn bộ consumer nằm trong cùng repo và được migrate
atomically trong một release. Xóa model yêu cầu chứng minh không còn `ref`, SQL,
dashboard query, selector, test hoặc tài liệu tham chiếu.

Thay đổi pressure threshold là **semantic change**: cập nhật methodology/schema
test, chạy audited full-refresh và kiểm tra lại phân bố level. Không điều chỉnh
ngưỡng chỉ để tạo thêm cảnh báo.

## 7. Source onboarding và deprecation

Nguồn mới chỉ được nhận khi có đủ:

- business question và consumer cụ thể;
- owner, license/terms, attribution và mục đích sử dụng phù hợp;
- grain, key, timezone, units, null/schema-drift rule;
- quota/cost, expected volume, freshness và failure behavior;
- immutable landing/replay path, DQ checks và deletion/retention decision;
- đánh giá PII và rủi ro suy diễn sai.

Nguồn/model bị deprecate khi không còn consumer hoặc không còn đáp ứng license,
quality hay economics. Quy trình: đánh dấu và thông báo, dừng schedule, chứng
minh không còn consumer, giữ raw/audit theo retention, rồi xóa code và catalog
qua thay đổi có review. Không xóa object MinIO thủ công để “dọn” DuckLake.

## 8. Incident và recovery

| Severity | Ví dụ | Hành động |
|---|---|---|
| SEV-1 | Credential lộ, dữ liệu sai đã publish, mất catalog/object | Dừng writer/serving liên quan, rotate/rollback/restore, thông báo owner |
| SEV-2 | Forecast stale >24h, pipeline fail liên tiếp, publication mất | Chặn publish, xử lý trong ngày, dùng snapshot tốt gần nhất |
| SEV-3 | Một run retry thành công, dashboard lỗi không mất dữ liệu | Ghi issue và xử lý theo backlog |

Vòng xử lý: detect → triage → contain → recover → validate → learn. Bằng chứng
tối thiểu gồm thời gian UTC, impact, run/snapshot/object key liên quan, lệnh xử
lý, người quyết định và test xác nhận. Không sửa tay checkpoint; dùng
`reprocess-from`/`abandon` có reason. Không restore khi writer còn chạy.

Post-incident record tối thiểu:

```text
Incident/SEV/UTC:
Impact và dữ liệu bị ảnh hưởng:
Detection signal:
Timeline UTC:
Root cause / contributing conditions:
Containment và recovery:
Validation evidence:
Corrective action (owner, due date):
Control/SLO/runbook cần thay đổi:
```

Backup policy cho deployment ngoài local: full lakehouse tối thiểu hàng tuần vào
failure domain độc lập, verify mỗi artifact, giữ ít nhất hai generation đã xác
minh và restore drill hàng quý trên database/bucket dùng một lần. RPO mục tiêu
ban đầu ≤ 7 ngày. RTO chỉ được chốt sau drill thật đầu tiên; mục tiêu tạm thời
≤ 4 giờ không được quảng bá là đã đạt.

## 9. Risk register hiện hành

| ID | Risk | L/I | Treatment và trigger |
|---|---|---|---|
| R1 | Free API outage/quota/license đổi | H/H | Retry, checkpoint, stale state; review terms hàng quý |
| R2 | Pressure bị hiểu là dự báo ngập chính thức | M/H | Wording bắt buộc, không xác suất; incident nếu public claim sai |
| R3 | Single-node mất cả compute và local volumes | M/H | External backup; không tuyên bố HA |
| R4 | Nhiều ward dùng chung weather grid | H/M | Hiển thị grid count; cấm nội suy độ phân giải đường phố |
| R5 | ERA5 và IFS khác phân phối | H/M | Giữ `weather_model`; không baseline gộp mù |
| R6 | Chưa có restore drill độc lập | H/H | Block production claim; hoàn tất drill và ghi RPO/RTO thực đo |
| R7 | Shared admin credential khi mở rộng user | M/H | Tách read/write/admin trước public/multi-operator deployment |
| R8 | Heuristic chưa calibration bằng ground truth | H/H | Chỉ dùng ưu tiên vận hành; không gọi risk probability |

`L/I` là likelihood/impact định tính. Review risk register hàng tháng, sau mỗi
SEV-1/2, khi đổi source/terms/architecture hoặc trước public deployment.

## 10. Continuous improvement và Definition of Done

Review hàng tháng dùng đúng một trang bằng chứng: SLO 30 ngày, incident,
failed/retried runs, storage growth, backup drill gần nhất, dependency/source
terms và ba improvement có ROI cao nhất. Mỗi action phải có owner, due date và
success metric; xóa action không còn giá trị thay vì tích backlog vô hạn.

Phase 9 hoàn tất ở cấp repository khi:

- ownership, classification, contracts, SLO, change/incident/risk policy được
  version-control;
- PR và CI kiểm soát code, dbt project và link tài liệu;
- Gold publish fail-closed qua audited snapshot;
- health/quality gates và recovery commands chạy được;
- giới hạn MVP, accepted risks và promotion gates được công bố trung thực.

Hai việc phụ thuộc hạ tầng/organizational state không được ghi nhận giả: restore
drill độc lập (R6) và least-privilege multi-user IAM (R7). Chúng là promotion
gate trước production/public deployment, không ngăn hoàn tất governance cho
portfolio single-node hiện hành.
