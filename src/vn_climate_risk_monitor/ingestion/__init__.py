"""Ingestion của dự án.

  * ``fetch``  — gọi Open-Meteo, land JSON as-is lên MinIO
  * ``run``    — nối package ``autoloader`` với cấu hình nguồn (YAML + SQL)

Logic nạp file → bảng nằm ở package ``autoloader`` (generic, tái sử dụng được).
"""
