"""Đồng bộ bộ ranh giới S13 đã ghim phiên bản cho 126 phường/xã Hà Nội."""

from __future__ import annotations

import csv
import io
import json
import logging
import tarfile
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

S13_REPOSITORY = "ThangLeQuoc/vietnamese-provinces-database"
S13_COMMIT = "438d04e16af02d8974703412f4d0d2cd86116024"
S13_ARCHIVE_URL = f"https://github.com/{S13_REPOSITORY}/archive/{S13_COMMIT}.tar.gz"
S13_MEMBER_PREFIX = "json/geojson/01_ha_noi/wards/"

SEED_PATH = Path("transform/seeds/ward_coordinates_seed.csv")
OUTPUT_PATH = Path("dashboard/data/hanoi_wards.geojson")


def expected_ward_codes() -> set[str]:
    with SEED_PATH.open(encoding="utf-8") as handle:
        return {
            row["commune_code"].zfill(5)
            for row in csv.DictReader(handle)
            if row["province_code"] == "01"
        }


def fetch_archive() -> bytes:
    request = urllib.request.Request(
        S13_ARCHIVE_URL,
        headers={"User-Agent": "HanoiClimateRiskMonitor/1.0"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def sync_boundaries(archive_bytes: bytes) -> list[dict]:
    expected = expected_ward_codes()
    found: dict[str, dict] = {}
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
        for member in archive.getmembers():
            relative = member.name.partition("/")[2]
            if not relative.startswith(S13_MEMBER_PREFIX) or not relative.endswith(
                ".geojson"
            ):
                continue
            source = archive.extractfile(member)
            if source is None:
                continue
            payload = json.loads(source.read().decode("utf-8"))
            features = payload.get("features", [])
            if len(features) != 1:
                raise ValueError(f"{member.name}: cần đúng một feature")
            code = str(features[0].get("properties", {}).get("code", "")).zfill(5)
            if code not in expected:
                continue
            found[code] = features[0]

    missing = expected - found.keys()
    extra = found.keys() - expected
    if missing or extra or len(found) != 126:
        raise ValueError(
            f"Bộ S13 không hợp lệ: found={len(found)}, missing={sorted(missing)}, "
            f"extra={sorted(extra)}"
        )
    return [found[code] for code in sorted(found)]


def write_aggregate(features: list[dict]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "type": "FeatureCollection",
        "source": S13_REPOSITORY,
        "source_commit": S13_COMMIT,
        "features": features,
    }
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def main() -> None:
    logger.info("Tải S13 tại commit %s", S13_COMMIT)
    features = sync_boundaries(fetch_archive())
    write_aggregate(features)
    logger.info("Đã xác minh và ghi %d ranh giới Hà Nội", len(features))


if __name__ == "__main__":
    main()
