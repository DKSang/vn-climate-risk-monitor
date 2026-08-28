import json
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError

from fetch import land


class PutMinio:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, bucket, key, data, length, content_type=""):
        self.objects[key] = data.read()


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


def test_dict_body_is_put_as_one_element_array() -> None:
    client = PutMinio()
    with patch(
        "fetch.urlopen",
        return_value=_ok(b'{"latitude": 21.0}'),
    ):
        land(client, "vn-climate", "https://x", "k.json")
    assert json.loads(client.objects["k.json"]) == [{"latitude": 21.0}]


def test_array_body_keeps_elements() -> None:
    client = PutMinio()
    with patch(
        "fetch.urlopen",
        return_value=_ok(b'[{"latitude": 1}, {"latitude": 2}]'),
    ):
        land(client, "vn-climate", "https://x", "k.json")
    assert json.loads(client.objects["k.json"]) == [{"latitude": 1}, {"latitude": 2}]


def test_error_json_does_not_put() -> None:
    client = PutMinio()
    try:
        with patch(
            "fetch.urlopen",
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
        patch("fetch.urlopen", side_effect=urlopen),
        patch("fetch.time.sleep"),
    ):
        land(client, "vn-climate", "https://x", "k.json")
    assert json.loads(client.objects["k.json"]) == [{"ok": True}]


def test_five_429s_do_not_put() -> None:
    client = PutMinio()
    with (
        patch(
            "fetch.urlopen",
            side_effect=_http_error(429),
        ),
        patch("fetch.time.sleep") as slept,
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
            "fetch.urlopen",
            side_effect=_http_error(400),
        ),
        patch("fetch.time.sleep") as slept,
    ):
        try:
            land(client, "vn-climate", "https://x", "k.json")
        except HTTPError:
            pass
        else:
            raise AssertionError("expected HTTPError")
    assert client.objects == {}
    slept.assert_not_called()
