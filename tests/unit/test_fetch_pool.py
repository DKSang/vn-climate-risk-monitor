"""Test pool song song — trọng tâm là chính sách dừng khi một request hỏng."""

from __future__ import annotations

import threading

from vn_climate_risk_monitor.sources.open_meteo.fetch import FetchTask, run_fetch_pool


def tasks(count: int, units: int = 1) -> list[FetchTask]:
    return [
        FetchTask(url=f"https://x/{i}", key=f"k/{i:03}.json", units=units)
        for i in range(count)
    ]


def quiet(_: str) -> None:
    return None


def test_every_task_lands() -> None:
    landed: list[str] = []
    lock = threading.Lock()

    def land_one(task: FetchTask) -> None:
        with lock:
            landed.append(task.key)

    result = run_fetch_pool(
        tasks(20),
        land_one=land_one,
        workers=4,
        report=quiet,
    )

    assert result.landed == 20
    assert result.ok
    assert sorted(landed) == sorted(t.key for t in tasks(20))


def test_one_failure_stops_the_batch() -> None:
    """429 thì dừng, đừng đốt thêm request."""
    seen: list[str] = []
    lock = threading.Lock()

    def land_one(task: FetchTask) -> None:
        with lock:
            seen.append(task.key)
        if task.key.endswith("002.json"):
            raise RuntimeError("Rate limit")

    result = run_fetch_pool(
        tasks(40),
        land_one=land_one,
        workers=2,
        report=quiet,
    )

    assert not result.ok
    assert len(result.failures) == 1
    assert "Rate limit" in result.failures[0]
    assert len(seen) < 40, "phải dừng sớm, không chạy hết cả lô"
    assert result.landed + result.cancelled + 1 == 40


def test_failure_message_names_the_object_key() -> None:
    def land_one(task: FetchTask) -> None:
        raise ValueError("boom")

    result = run_fetch_pool(
        tasks(1),
        land_one=land_one,
        workers=1,
        report=quiet,
    )

    assert result.failures == ["k/000.json: ValueError: boom"]


def test_single_worker_matches_sequential_behaviour() -> None:
    order: list[str] = []
    result = run_fetch_pool(
        tasks(5),
        land_one=lambda t: order.append(t.key),
        workers=1,
        report=quiet,
    )

    assert result.landed == 5
    assert order == [t.key for t in tasks(5)]
