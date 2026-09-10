"""Geocode nháp cho quan sát ngập, kèm gán phường bằng point-in-polygon.

Script này KHÔNG sinh ra dữ liệu đáng tin. Nó chỉ đề xuất toạ độ để người soát.

── Vì sao là seed RIÊNG, không thêm cột vào seed quan sát ────────────────────
`flood_event_observations_seed.csv` do `flood_observations.py` sinh ra và bị
GHI ĐÈ mỗi lần fetch lại. Thêm cột toạ độ vào đó nghĩa là mọi công sức soát tay
biến mất ở lần fetch kế tiếp. Tách làm hai file: một file máy sinh, một file
người sửa, nối với nhau bằng `observation_id`.

── Vì sao mọi dòng mặc định geocode_verified = false ─────────────────────────
Gán sai phường = sai ô lưới = sai lượng mưa = nhãn hỏng, và nhãn hỏng tệ hơn
thiếu nhãn. Nominatim với địa chỉ dạng cột mốc đường ("ĐLTL đoạn Km 8+200")
rất dễ trả về một kết quả TRÔNG hợp lý nhưng sai. `is_replay_eligible` vì vậy
chỉ nhận dòng đã được người xác nhận.

Chạy lại script là AN TOÀN: mặc định mọi dòng đã có đều được giữ nguyên để
không xoá review đang làm dở. Chỉ `--refresh-unverified` mới hỏi lại các dòng
chưa xác nhận; dòng `geocode_verified = true` luôn được giữ nguyên.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb

OBSERVATION_SEED = Path("transform/seeds/flood_event_observations_seed.csv")
GEOCODE_SEED = Path("transform/seeds/flood_observation_geocode_seed.csv")
WARD_GEOJSON_GLOB = "reference/s13_wards/*.geojson"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Chính sách Nominatim: tối đa 1 request/giây và bắt buộc User-Agent định danh.
NOMINATIM_MIN_INTERVAL_SECONDS = 1.1
USER_AGENT = "vn-climate-risk-monitor/0.1 (flood observation geocoding)"

# Khung Hà Nội sau sắp xếp 2025. `bounded=1` để Nominatim không trả về một phố
# trùng tên ở tỉnh khác — lỗi im lặng nguy hiểm nhất của geocoding tiếng Việt.
HANOI_VIEWBOX = "105.28,21.39,106.03,20.53"

CSV_FIELDS = (
    "observation_id",
    "location_name_raw",
    "query_sent",
    "latitude",
    "longitude",
    "ward_code",
    "ward_name",
    "geocode_source",
    "geocode_match_type",
    "geocode_verified",
    "review_note",
    "anchor_type",
    "confidence",
    "coordinate_basis",
    "coordinate_changed_from_input",
    "ward_changed_from_input",
    "original_latitude",
    "original_longitude",
    "original_ward_code",
    "original_ward_name",
    "needs_manual_validation",
    "verified_at_utc",
    "verification_method",
)


# Viết tắt trong báo cáo thoát nước Hà Nội. Nominatim không hiểu chúng, và
# "ĐLTL" trả về 0 kết quả trong khi "Đại lộ Thăng Long" trả về đúng tuyến.
# Thứ tự quan trọng: cụm dài phải đứng trước để không bị cụm ngắn ăn mất.
ROAD_ABBREVIATIONS = (
    (r"\bĐLTL\b", "Đại lộ Thăng Long"),
    (r"\bĐCT\b", "Đường cao tốc"),
    (r"\bQL\s*(\d+)", r"Quốc lộ \1"),
    (r"\bTL\s*(\d+)", r"Tỉnh lộ \1"),
    (r"\bĐT\s*(\d+)", r"Đường tỉnh \1"),
    (r"\bKĐT\b", "Khu đô thị"),
    (r"\bKCN\b", "Khu công nghiệp"),
    (r"\bBV\b", "Bệnh viện"),
    (r"\bTT\b", "Thị trấn"),
)


@dataclass(frozen=True)
class GeocodeResult:
    latitude: float | None
    longitude: float | None
    query: str
    match_type: str


def expand_abbreviations(name: str) -> str:
    expanded = name
    for pattern, replacement in ROAD_ABBREVIATIONS:
        expanded = re.sub(pattern, replacement, expanded)
    return expanded


def road_prefix(name: str) -> str:
    """Bỏ phần mô tả đoạn để thử lại ở mức tuyến đường.

    "Phố Triều Khúc (đoạn ngõ 66 đến đình làng)" -> "Phố Triều Khúc"
    Nominatim gần như không bao giờ biết một đoạn cụ thể, nhưng thường biết tuyến.
    """
    trimmed = re.split(r"\s*\(|\s+đoạn\s+|:\s*|,\s*", name)[0]
    trimmed = re.sub(r"\s*Km\s*\d+.*$", "", trimmed, flags=re.IGNORECASE)
    return trimmed.strip()


def query_nominatim(query: str) -> tuple[float, float] | None:
    parameters = urlencode(
        {
            "q": query,
            "format": "jsonv2",
            "limit": 1,
            "countrycodes": "vn",
            "viewbox": HANOI_VIEWBOX,
            "bounded": 1,
        }
    )
    request = Request(
        f"{NOMINATIM_URL}?{parameters}", headers={"User-Agent": USER_AGENT}
    )
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not payload:
        return None
    return float(payload[0]["lat"]), float(payload[0]["lon"])


def geocode(name: str) -> GeocodeResult:
    """Thử tên đầy đủ trước, rồi lùi về tên tuyến đường.

    `road_prefix` có ĐỘ CHÍNH XÁC THẤP và phải soát kỹ hơn: nó trả về điểm đại
    diện của cả tuyến, không phải cột mốc. Đo được: Km14+500 và Km17+700 của
    Quốc lộ 32 — cách nhau ~3 km ngoài thực địa — nhận cùng một toạ độ.
    """
    expanded = expand_abbreviations(name)
    attempts = [(expanded, "full_name")]
    prefix = road_prefix(expanded)
    if prefix and prefix != expanded:
        attempts.append((prefix, "road_prefix"))

    for text, match_type in attempts:
        query = f"{text}, Hà Nội, Việt Nam"
        found = query_nominatim(query)
        time.sleep(NOMINATIM_MIN_INTERVAL_SECONDS)
        if found:
            return GeocodeResult(found[0], found[1], query, match_type)
    return GeocodeResult(None, None, f"{name}, Hà Nội, Việt Nam", "no_match")


def ward_lookup_connection() -> duckdb.DuckDBPyConnection:
    """Nạp 126 ranh giới phường vào bộ nhớ cho phép thử point-in-polygon."""
    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial;")
    connection.execute(
        "CREATE TABLE wards(ward_code VARCHAR, ward_name VARCHAR, geom GEOMETRY)"
    )
    files = sorted(glob.glob(WARD_GEOJSON_GLOB))
    if len(files) != 126:
        raise SystemExit(f"Cần đúng 126 ranh giới phường, tìm thấy {len(files)}")
    for path in files:
        connection.execute(
            "INSERT INTO wards SELECT code, fullName, geom FROM ST_Read(?)", [path]
        )
    return connection


def resolve_ward(
    connection: duckdb.DuckDBPyConnection, latitude: float, longitude: float
) -> tuple[str, str] | None:
    row = connection.execute(
        "SELECT ward_code, ward_name FROM wards "
        "WHERE ST_Contains(geom, ST_Point(?, ?)) LIMIT 1",
        [longitude, latitude],
    ).fetchone()
    return (row[0], row[1]) if row else None


def read_existing(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        return {row["observation_id"]: row for row in csv.DictReader(handle)}


def is_verified(row: dict[str, str]) -> bool:
    return str(row.get("geocode_verified", "")).strip().lower() in {"true", "1", "yes"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, default=OBSERVATION_SEED)
    parser.add_argument("--output", type=Path, default=GEOCODE_SEED)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Chỉ geocode N dòng đầu (để thử nhanh); 0 = tất cả",
    )
    parser.add_argument(
        "--refresh-unverified",
        action="store_true",
        help=(
            "Geocode lại cả dòng chưa verified. Mặc định giữ nguyên mọi dòng đã "
            "có để không xoá review_note/anchor thủ công còn đang chờ duyệt."
        ),
    )
    arguments = parser.parse_args()

    with arguments.observations.open(encoding="utf-8") as handle:
        observations = list(csv.DictReader(handle))
    if not observations:
        raise SystemExit(f"{arguments.observations} rỗng — chạy fetch trước")

    existing = read_existing(arguments.output)
    verified_count = sum(1 for row in existing.values() if is_verified(row))
    connection = ward_lookup_connection()

    pending = [
        row
        for row in observations
        if row["observation_id"] not in existing
        or (
            arguments.refresh_unverified
            and not is_verified(existing.get(row["observation_id"], {}))
        )
    ]
    if arguments.limit:
        pending = pending[: arguments.limit]

    print(
        f"{len(observations)} quan sát · giữ nguyên {len(existing)} dòng "
        f"({verified_count} đã xác nhận) · "
        f"geocode {len(pending)} dòng (~{len(pending) * 2 * 1.1 / 60:.0f} phút tối đa)"
    )

    results: dict[str, dict[str, str]] = dict(existing)
    matched = ward_hit = 0
    for index, observation in enumerate(pending, start=1):
        name = observation["location_name_raw"]
        result = geocode(name)
        ward = (
            resolve_ward(connection, result.latitude, result.longitude)
            if result.latitude is not None
            else None
        )
        if result.latitude is not None:
            matched += 1
        if ward:
            ward_hit += 1
        results[observation["observation_id"]] = {
            "observation_id": observation["observation_id"],
            "location_name_raw": name,
            "query_sent": result.query,
            "latitude": "" if result.latitude is None else f"{result.latitude:.6f}",
            "longitude": "" if result.longitude is None else f"{result.longitude:.6f}",
            "ward_code": ward[0] if ward else "",
            "ward_name": ward[1] if ward else "",
            "geocode_source": "nominatim" if result.latitude is not None else "",
            "geocode_match_type": result.match_type
            if ward
            else ("outside_hanoi" if result.latitude is not None else "no_match"),
            # LUÔN false. Chỉ người soát mới được đổi thành true.
            "geocode_verified": "false",
            "review_note": "",
            "anchor_type": result.match_type,
            "confidence": "",
            "coordinate_basis": "nominatim",
            "coordinate_changed_from_input": "false",
            "ward_changed_from_input": "false",
            "original_latitude": "",
            "original_longitude": "",
            "original_ward_code": "",
            "original_ward_name": "",
            "needs_manual_validation": "true",
            "verified_at_utc": "",
            "verification_method": "",
        }
        if index % 20 == 0:
            print(f"  … {index}/{len(pending)}")

    ordered = [
        results[row["observation_id"]]
        for row in observations
        if row["observation_id"] in results
    ]
    verified_after = sum(1 for row in ordered if is_verified(row))
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    with arguments.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CSV_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(ordered)

    print(
        f"\nĐã ghi {len(ordered)} dòng vào {arguments.output}\n"
        f"  Nominatim trả kết quả : {matched}/{len(pending)}\n"
        f"  Rơi đúng vào 126 phường: {ward_hit}/{len(pending)}\n"
        f"  Đã xác nhận           : {verified_after}/{len(ordered)}\n"
        "\nChỉ dòng geocode_verified=true mới vào archive replay; "
        "chạy lại mặc định giữ nguyên toàn bộ review."
    )


if __name__ == "__main__":
    main()
