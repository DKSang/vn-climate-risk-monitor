# Hanoi Flood & Climate Risk Monitor

Lakehouse theo dõi rủi ro mưa lớn, ngập úng và proxy lũ cho Hà Nội.

```text
MinIO + PostgreSQL/DuckLake + DuckDB/dbt
Bronze → Silver → Gold
```

Open-Meteo chưa được ingest. Repository hiện đã có lakehouse địa lý, contract
incremental ingestion và package boundary cho collector/loader/state.

```bash
make up
make bootstrap
make transform
```

Xem [tài liệu kiến trúc](docs/03-architecture.md) và
[cấu trúc repository](docs/03a-repo-structure.md).
