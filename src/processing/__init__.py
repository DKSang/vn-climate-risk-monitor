"""Processing framework: incremental transform có checkpoint và audit.

Song song với ``autoloader`` chứ không nằm trong nó. Hai checkpoint khác nhau::

    autoloader  → ingestion.ingestion_files   File đã vào Bronze chưa?
    processing  → processing.processing_state Process đã xử lý tới mốc nào?

Nguyên tắc: checkpoint là START TIME của lần chạy thành công gần nhất, KHÔNG
BAO GIỜ là ``MAX(source.timestamp)``.
"""

from processing.config import (
    CheckpointConfig,
    ProcessConfig,
    RunnerConfig,
    SourceBinding,
    parse_duration,
)
from processing.dbt import DbtBuildError, build_vars, run_dbt, to_sql_timestamp
from processing.runner import (
    Bounds,
    ProcessingResult,
    SourceBounds,
    compute_bounds,
    run_process,
)
from processing.schema import ensure_processing_state
from processing.softdelete import (
    SoftDeleteConfig,
    SoftDeleteError,
    SoftDeleteResult,
    apply_soft_delete,
)
from processing.state import ProcessingRepository, ProcessingStateError

__all__ = [
    "Bounds",
    "CheckpointConfig",
    "DbtBuildError",
    "ProcessConfig",
    "ProcessingRepository",
    "ProcessingResult",
    "ProcessingStateError",
    "RunnerConfig",
    "SoftDeleteConfig",
    "SoftDeleteError",
    "SoftDeleteResult",
    "SourceBinding",
    "SourceBounds",
    "apply_soft_delete",
    "build_vars",
    "compute_bounds",
    "ensure_processing_state",
    "parse_duration",
    "run_dbt",
    "run_process",
    "to_sql_timestamp",
]
