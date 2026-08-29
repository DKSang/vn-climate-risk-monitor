"""Hàng đợi song song: nhiều worker GET→PUT.

Chính sách lỗi: hỏng một cái là DỪNG cả lô. File đã land vẫn nằm trên MinIO;
lần chạy sau tự bỏ qua.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field


@dataclass(frozen=True)
class FetchTask:
    """Một request: đi đâu, ghi vào key nào, tốn bao nhiêu đơn vị API (ước lượng)."""

    url: str
    key: str
    units: int


@dataclass
class PoolResult:
    landed: int = 0
    failures: list[str] = field(default_factory=list)
    cancelled: int = 0

    @property
    def ok(self) -> bool:
        return not self.failures


def run_fetch_pool(
    tasks: Sequence[FetchTask],
    *,
    land_one: Callable[[FetchTask], None],
    workers: int = 4,
    report: Callable[[str], None] = print,
) -> PoolResult:
    """Chạy ``tasks`` qua ``workers`` luồng."""
    if workers < 1:
        raise ValueError("workers phải >= 1")
    result = PoolResult()
    stop = threading.Event()
    lock = threading.Lock()
    total = len(tasks)

    def work(task: FetchTask) -> None:
        if stop.is_set():
            with lock:
                result.cancelled += 1
            return
        try:
            land_one(task)
        except Exception as error:  # noqa: BLE001 — gom lỗi rồi dừng cả lô
            with lock:
                result.failures.append(
                    f"{task.key}: {type(error).__name__}: {error}"
                )
            stop.set()
            return
        with lock:
            result.landed += 1
            report(f"  [{result.landed}/{total}] {task.key}")

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch") as pool:
        list(pool.map(work, tasks))
    return result
