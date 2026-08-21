"""Validation shared by Open-Meteo collectors before parser-specific loading."""

from __future__ import annotations

import json

from requests import Response


def response_attempt_count(response: Response) -> int:
    retries = getattr(getattr(response, "raw", None), "retries", None)
    history = getattr(retries, "history", ())
    return 1 + len(history)


def received_location_count(content: bytes, expected_count: int) -> int:
    """Validate a single/multi-location JSON root and return its cardinality."""
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Open-Meteo response is not valid JSON") from error

    if isinstance(payload, dict):
        if payload.get("error") is True:
            reason = payload.get("reason", "unknown API error")
            raise ValueError(f"Open-Meteo returned an error payload: {reason}")
        received_count = 1
    elif isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError("Open-Meteo response list must contain only objects")
        received_count = len(payload)
    else:
        raise TypeError("Open-Meteo response root must be an object or array")

    if received_count != expected_count:
        raise ValueError(
            f"Expected {expected_count} response locations, received {received_count}"
        )
    return received_count
