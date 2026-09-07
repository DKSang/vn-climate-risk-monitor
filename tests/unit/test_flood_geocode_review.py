"""Guardrails for the manually reviewed flood-observation geocode seed."""

from __future__ import annotations

import csv
from pathlib import Path

SEED = Path("transform/seeds/flood_observation_geocode_seed.csv")


def _rows() -> list[dict[str, str]]:
    with SEED.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_geocode_seed_keeps_one_row_per_observation() -> None:
    rows = _rows()
    observation_ids = [row["observation_id"] for row in rows]
    assert len(rows) == 123
    assert len(set(observation_ids)) == len(observation_ids)


def test_verified_geocodes_have_complete_review_evidence() -> None:
    verified = [row for row in _rows() if row["geocode_verified"] == "true"]
    assert len(verified) == 18
    for row in verified:
        assert row["latitude"] and row["longitude"]
        assert len(row["ward_code"]) == 5
        assert row["confidence"] == "high"
        assert row["needs_manual_validation"] == "false"
        assert row["verified_at_utc"]
        assert row["verification_method"]


def test_corrected_landmarks_use_current_s13_wards() -> None:
    rows = {row["observation_id"]: row for row in _rows()}
    expected = {
        "VNE_20251007_012": "00592",  # Bệnh viện 19-8
        "VNE_20251007_020": "00622",  # Cầu Ngà
        "VNE_20251007_025": "00175",  # Phạm Hùng–Dương Đình Nghệ
    }
    assert {key: rows[key]["ward_code"] for key in expected} == expected


def test_training_feature_aggregates_catalogue_before_join() -> None:
    sql = Path("transform/models/marts/fct_flood_training_feature.sql").read_text(
        encoding="utf-8"
    )
    assert "catalogue_by_ward AS" in sql
    assert "LEFT JOIN catalogue_by_ward catalogue" in sql
    assert "LEFT JOIN {{ ref('dim_flood_point') }} fp" not in sql
