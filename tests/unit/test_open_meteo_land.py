import json
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError

from pipeline.open_meteo import fetch
from pipeline.open_meteo.fetch import land


class PutMinio:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, bucket, key, data, length, content_type=""):
        self.objects[key] = data.read()


class BucketMinio:
    def __init__(self, *, exists: bool) -> None:
        self.exists = exists
        self.created: list[str] = []

    def bucket_exists(self, bucket: str) -> bool:
        return self.exists

    def make_bucket(self, bucket: str) -> None:
        self.created.append(bucket)


def test_ensure_bucket_creates_a_missing_bucket() -> None:
    client = BucketMinio(exists=False)

    fetch.ensure_bucket(client, "vn-climate")

    assert client.created == ["vn-climate"]


def _http_error(code: int) -> HTTPError:
    return HTTPError("https://x", code, "err", hdrs=None, fp=BytesIO(b""))


def _ok(body: bytes):
    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return body

    return Resp()


def test_exact_http_response_bytes_are_put_without_reserialization() -> None:
    body = b'{\n  "latitude": 21.0,\r\n  "generationtime_ms": 1.25\n}\n'
    client = PutMinio()
    with patch(
        "pipeline.open_meteo.fetch.urlopen",
        return_value=_ok(body),
    ):
        land(client, "vn-climate", "https://x", "k.json")
    assert client.objects["k.json"] == body


def test_array_body_keeps_elements() -> None:
    client = PutMinio()
    with patch(
        "pipeline.open_meteo.fetch.urlopen",
        return_value=_ok(b'[{"latitude": 1}, {"latitude": 2}]'),
    ):
        land(client, "vn-climate", "https://x", "k.json")
    assert json.loads(client.objects["k.json"]) == [{"latitude": 1}, {"latitude": 2}]


def test_error_json_is_detected_before_it_can_poison_an_immutable_key() -> None:
    client = PutMinio()
    try:
        with patch(
            "pipeline.open_meteo.fetch.urlopen",
            return_value=_ok(b'{"error": true, "reason": "Rate limit"}'),
        ):
            land(client, "vn-climate", "https://x", "k.json")
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError")
    assert client.objects == {}


def test_429_then_200_puts_once() -> None:
    client = PutMinio()
    calls = {"n": 0}

    def urlopen(request, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _http_error(429)
        return _ok(b'{"ok": true}')

    with (
        patch("pipeline.open_meteo.fetch.urlopen", side_effect=urlopen),
        patch("pipeline.open_meteo.fetch.time.sleep"),
    ):
        land(client, "vn-climate", "https://x", "k.json")
    assert client.objects["k.json"] == b'{"ok": true}'


def test_five_429s_do_not_put() -> None:
    client = PutMinio()
    with (
        patch(
            "pipeline.open_meteo.fetch.urlopen",
            side_effect=_http_error(429),
        ),
        patch("pipeline.open_meteo.fetch.time.sleep") as slept,
    ):
        try:
            land(client, "vn-climate", "https://x", "k.json")
        except HTTPError:
            pass
        else:
            raise AssertionError("expected HTTPError")
    assert client.objects == {}
    assert slept.call_count == 4


def test_400_does_not_retry() -> None:
    client = PutMinio()
    with (
        patch(
            "pipeline.open_meteo.fetch.urlopen",
            side_effect=_http_error(400),
        ),
        patch("pipeline.open_meteo.fetch.time.sleep") as slept,
    ):
        try:
            land(client, "vn-climate", "https://x", "k.json")
        except HTTPError:
            pass
        else:
            raise AssertionError("expected HTTPError")
    assert client.objects == {}
    slept.assert_not_called()
