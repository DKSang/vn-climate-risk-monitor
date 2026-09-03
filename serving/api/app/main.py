"""Read-only operational API for health checks and scheduler status."""

from __future__ import annotations

from html import escape

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from vn_climate_risk_monitor.health import collect_health

app = FastAPI(
    title="Hanoi Climate Risk Monitor Operations",
    version="0.1.0",
    description="Read-only health API. No forecast data is modified by this service.",
)


@app.get("/health/live")
def liveness() -> dict[str, str]:
    return {"status": "ALIVE"}


@app.get("/health")
def health(
    scope: str = Query("all", pattern="^(forecast|archive|all)$"),
    require_gold: bool = True,
) -> dict:
    report = collect_health(scope=scope, require_gold=require_gold)  # type: ignore[arg-type]
    return report.to_dict()


@app.get("/health/ready")
def readiness() -> dict:
    report = collect_health(scope="forecast", require_gold=True)
    if report.status == "UNHEALTHY":
        raise HTTPException(status_code=503, detail=report.to_dict())
    return report.to_dict()


@app.get("/ops", response_class=HTMLResponse)
def operations_dashboard() -> str:
    report = collect_health(scope="all", require_gold=True)
    rows = "".join(
        "<tr>"
        f"<td>{escape(check.name)}</td>"
        f"<td>{escape(check.status)}</td>"
        f"<td>{escape(check.message)}</td>"
        "</tr>"
        for check in report.checks
    )
    return f"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="60">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Climate monitor operations</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 1100px; margin: 2rem auto; padding: 0 1rem; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #bbb; padding: .55rem; text-align: left; }}
    th {{ background: #eee; }}
  </style>
</head>
<body>
  <h1>Hanoi Climate Risk Monitor</h1>
  <p>Trạng thái: <strong>{escape(report.status)}</strong></p>
  <p>Kiểm tra lúc {escape(report.checked_at_utc)}. Trang tự làm mới mỗi 60 giây.</p>
  <table><thead><tr><th>Check</th><th>Status</th><th>Thông tin</th></tr></thead>
  <tbody>{rows}</tbody></table>
</body>
</html>"""
