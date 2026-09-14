"""Fetch and normalize versioned historical flood observations.

The first supported source is the Flourish table embedded in the VnExpress
live report for 2025-10-07.  The raw wording is deliberately retained: news
absence is not a negative flood label, and normalization must remain auditable.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

DEFAULT_EMBED_URL = "https://public.flourish.studio/visualisation/25498819/embed"
DEFAULT_ARTICLE_URL = (
    "https://vnexpress.net/ha-noi-mua-lon-nhieu-tuyen-pho-nguy-co-ngap-4948058.html"
)
DEFAULT_EVENT_ID = "hanoi_matmo_2025_10_07"
DEFAULT_AS_OF = "2025-10-07T10:00:00+07:00"
DEFAULT_OUTPUT = Path("transform/seeds/flood_event_observations_seed.csv")

CSV_FIELDS = (
    "observation_id",
    "event_id",
    "location_name_raw",
    "depth_text_raw",
    "traffic_text_raw",
    "depth_min_cm",
    "depth_max_cm",
    "traffic_status",
    "observed_at_local",
    "as_of_local",
    "is_flooded",
    "source_grade",
    "source_publisher",
    "source_url",
    "source_visualisation_id",
    "source_visualisation_version",
    "source_updated_at_utc",
)


@dataclass(frozen=True)
class FlourishSnapshot:
    rows: list[list[str]]
    visualisation_id: int
    visualisation_version: int
    updated_at_utc: datetime


def _assignment_json(document: str, variable: str) -> Any:
    marker = f"{variable} = "
    start = document.find(marker)
    if start < 0:
        raise ValueError(f"Không tìm thấy biến {variable} trong Flourish embed")
    value, _ = json.JSONDecoder().raw_decode(document[start + len(marker) :])
    return value


def parse_flourish_snapshot(document: str) -> FlourishSnapshot:
    """Extract the data and immutable source metadata from an embed document."""
    data = _assignment_json(document, "_Flourish_data")
    rows = [item["columns"] for item in data["rows"]]
    visualisation_id = int(
        re.search(r"_Flourish_visualisation_id\s*=\s*(\d+)", document).group(1)  # type: ignore[union-attr]
    )
    version = int(
        re.search(
            r"_Flourish_visualisation_version_number\s*=\s*(\d+)", document
        ).group(1)  # type: ignore[union-attr]
    )
    updated_ms = int(
        re.search(r'"last_updated":new Date\((\d+)\)', document).group(1)  # type: ignore[union-attr]
    )
    return FlourishSnapshot(
        rows=rows,
        visualisation_id=visualisation_id,
        visualisation_version=version,
        updated_at_utc=datetime.fromtimestamp(updated_ms / 1000, tz=UTC),
    )


def parse_depth_cm(value: str) -> tuple[float | None, float | None]:
    """Return the last centimetre range, ignoring lengths expressed in metres."""
    matches = list(
        re.finditer(
            r"(\d+(?:[.,]\d+)?)\s*(?:[-–]\s*(\d+(?:[.,]\d+)?))?\s*cm",
            value,
            re.IGNORECASE,
        )
    )
    if not matches:
        return None, None
    match = matches[-1]
    lower = float(match.group(1).replace(",", "."))
    upper = float((match.group(2) or match.group(1)).replace(",", "."))
    return min(lower, upper), max(lower, upper)


def parse_traffic_status(value: str) -> str:
    normalized = value.casefold()
    if "nước đã rút" in normalized:
        return "receded"
    if "phân luồng" in normalized:
        return "diverted"
    if "không lưu thông được" in normalized:
        return "impassable"
    if "vẫn lưu thông được" in normalized or "lưu thông bình thường" in normalized:
        return "passable"
    if "lưu thông chậm" in normalized:
        return "slow"
    return "unknown"


def parse_observed_at(value: str, event_date: str = "2025-10-07") -> str:
    match = re.search(r"lúc\s+(\d{1,2})h(\d{1,2})", value, re.IGNORECASE)
    if not match:
        return ""
    hour, minute = (int(part) for part in match.groups())
    return f"{event_date}T{hour:02d}:{minute:02d}:00+07:00"


def normalize_snapshot(
    snapshot: FlourishSnapshot,
    *,
    event_id: str = DEFAULT_EVENT_ID,
    as_of_local: str = DEFAULT_AS_OF,
    article_url: str = DEFAULT_ARTICLE_URL,
) -> list[dict[str, object]]:
    event_date = as_of_local[:10]
    normalized: list[dict[str, object]] = []
    for index, columns in enumerate(snapshot.rows, start=1):
        if len(columns) != 3:
            raise ValueError(f"Dòng Flourish {index} không có đúng 3 cột")
        location, depth_text, traffic_text = (str(value).strip() for value in columns)
        depth_min, depth_max = parse_depth_cm(depth_text)
        status = parse_traffic_status(traffic_text)
        normalized.append(
            {
                "observation_id": f"VNE_20251007_{index:03d}",
                "event_id": event_id,
                "location_name_raw": location,
                "depth_text_raw": depth_text,
                "traffic_text_raw": traffic_text,
                "depth_min_cm": "" if depth_min is None else f"{depth_min:g}",
                "depth_max_cm": "" if depth_max is None else f"{depth_max:g}",
                "traffic_status": status,
                "observed_at_local": parse_observed_at(traffic_text, event_date),
                "as_of_local": as_of_local,
                "is_flooded": status != "receded",
                "source_grade": "B",
                "source_publisher": "VnExpress",
                "source_url": article_url,
                "source_visualisation_id": snapshot.visualisation_id,
                "source_visualisation_version": snapshot.visualisation_version,
                "source_updated_at_utc": snapshot.updated_at_utc.isoformat(),
            }
        )
    return normalized


def fetch_document(url: str) -> str:
    request = Request(url, headers={"User-Agent": "vn-climate-risk-monitor/0.1"})
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def write_seed(rows: list[dict[str, object]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_EMBED_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--expected-version", type=int, default=141)
    args = parser.parse_args()

    snapshot = parse_flourish_snapshot(fetch_document(args.url))
    if snapshot.visualisation_version != args.expected_version:
        raise SystemExit(
            "Flourish version đã đổi: "
            f"expected={args.expected_version}, actual={snapshot.visualisation_version}. "
            "Hãy review diff trước khi chấp nhận version mới."
        )
    rows = normalize_snapshot(snapshot)
    active = sum(bool(row["is_flooded"]) for row in rows)
    if len(rows) != 123 or active != 122:
        raise SystemExit(
            f"Snapshot không khớp invariant: raw={len(rows)}, flooded={active}"
        )
    write_seed(rows, args.output)
    print(
        f"Đã ghi {len(rows)} quan sát ({active} đang ngập) từ "
        f"Flourish v{snapshot.visualisation_version} vào {args.output}"
    )


if __name__ == "__main__":
    main()
