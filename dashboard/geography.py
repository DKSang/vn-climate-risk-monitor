"""Hanoi's 126 ward boundaries, pinned in the repo."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

GEOJSON_PATH = Path(__file__).parent / "data" / "hanoi_wards.geojson"


@st.cache_data
def ward_boundaries() -> dict:
    with GEOJSON_PATH.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    for feature in payload["features"]:
        props = feature["properties"]
        props["ward_code"] = str(props["code"]).zfill(5)
    return payload
