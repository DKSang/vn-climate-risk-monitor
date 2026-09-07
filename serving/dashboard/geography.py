"""Ranh giới hành chính dùng chung cho các bản đồ Streamlit."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
GEOJSON_PATH = ROOT_DIR / "serving" / "dashboard" / "data" / "hanoi_wards.geojson"


@st.cache_data
def load_hanoi_geojson() -> dict:
    """Đọc đúng bộ 126 ranh giới S13 đã ghim phiên bản."""
    with GEOJSON_PATH.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    for feature in payload.get("features", []):
        props = feature.setdefault("properties", {})
        props["ward_code"] = str(props.get("code", "")).zfill(5)
        props["name"] = props.get("fullName") or props.get("name") or "Không rõ"
    return payload
