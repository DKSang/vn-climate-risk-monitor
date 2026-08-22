"""Typed identities and records for incremental ingestion state."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID, uuid5

LOGICAL_RUN_NAMESPACE = UUID("cb6040e9-4fba-4f57-a8f2-b0bf99c78238")


class RunStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def build_logical_run_id(
    *,
    pipeline_name: str,
    dataset: str,
    scope: str,
    logical_key: str,
) -> UUID:
    """Build a deterministic identity shared by all attempts of one logical run."""
    components = (pipeline_name, dataset, scope, logical_key)
    if any(not component.strip() for component in components):
        raise ValueError("logical run identity components must not be empty")
    return uuid5(LOGICAL_RUN_NAMESPACE, "\x1f".join(components))


@dataclass(frozen=True)
class RunAttempt:
    attempt_id: UUID
    logical_run_id: UUID
    attempt_number: int
    logical_key: str
    status: RunStatus


@dataclass(frozen=True)
class ClaimedObject:
    """Một file đã claim — engine chỉ cần danh tính và vị trí của nó."""

    file_id: UUID
    object_key: str
