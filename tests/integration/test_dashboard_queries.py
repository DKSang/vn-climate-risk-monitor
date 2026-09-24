"""The dashboard reads Gold only through the latest published DuckLake snapshot."""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest
import streamlit as st
from staging import stage
from streamlit.testing.v1 import AppTest

from dashboard import queries
from pipeline import lake
from pipeline.init import main as init_lake
from pipeline.open_meteo.clean import build_clean_forecast
from pipeline.open_meteo.gold import build_gold_forecast
from pipeline.open_meteo.load import CREATE_STAGING
from pipeline.settings import load_settings

NOW = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
WARDS = 126


def reset_lake() -> None:
    """A brand-new lake: snapshot history survives DROP TABLE, so drop the catalog."""
    with psycopg.connect(load_settings().postgres.dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS ducklake CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS meta CASCADE")
    client, bucket = lake.minio_client(), load_settings().minio.bucket
    if client.bucket_exists(bucket):
        for obj in client.list_objects(bucket, recursive=True):
            client.remove_object(bucket, obj.object_name)
    init_lake()
    st.cache_data.clear()
    st.cache_resource.clear()


def test_before_the_first_publication_there_is_no_snapshot_to_read() -> None:
    reset_lake()

    assert queries.published_snapshot() is None


@pytest.fixture(scope="module")
def published() -> int:
    reset_lake()
    with lake.connect() as con:
        con.execute(CREATE_STAGING)
    stage(NOW, rain=0.4, inserted_at=datetime.now(UTC), file="run.json")
    build_clean_forecast()
    build_gold_forecast()
    return queries.published_snapshot()


def test_every_ward_has_rain_and_a_pressure_level_for_an_hour(published: int) -> None:
    wards = queries.ward_hour(published, NOW)

    assert len(wards) == WARDS
    assert wards["ward_code"].is_unique
    assert set(wards["pressure_level"]) <= {
        "NORMAL",
        "WATCH",
        "ELEVATED",
        "HIGH",
        "UNKNOWN",
    }
    assert (wards["precipitation_mm"] == 0.4).all()


def test_gold_changes_after_the_publication_are_not_shown(published: int) -> None:
    with lake.connect() as con:
        con.execute("UPDATE gold.fct_rain_pressure_alert SET pressure_level = 'HIGH'")
    st.cache_data.clear()  # read the lake again, not an earlier cached result

    assert queries.published_snapshot() == published
    assert "HIGH" not in set(queries.ward_hour(published, NOW)["pressure_level"])


@pytest.mark.parametrize("page", ["Tổng quan", "Chi tiết phường", "Bảng dữ liệu"])
@pytest.mark.parametrize("ward", ["", "00004"])
def test_every_report_page_renders(published: int, page: str, ward: str) -> None:
    app = AppTest.from_file("../../dashboard/app.py", default_timeout=120)
    app.session_state["page"] = page
    app.session_state["ward"] = ward

    app.run()

    assert not app.exception, app.exception
    assert not app.error
    assert any("report-title" in md.value for md in app.markdown)
