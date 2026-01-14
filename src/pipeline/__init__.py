"""Pipeline module with Prefect flows and utilities."""

from pipeline.wrappers import (
    wrap_job_as_task,
    create_task_a,
    create_task_b,
    create_task_c,
)
from pipeline.flows import (
    JobSpec,
    PipelineDefinition,
    PipelineRunner,
    make_job_task,
    run_pipeline,
    create_default_pipeline,
)

from pipeline.parallel import (
    ParallelExecutionResult,
    get_max_workers,
    run_parallel_from_files,
    run_parallel_from_database,
)

__all__ = [
    # Wrappers (kept for backward compatibility)
    "wrap_job_as_task",
    "create_task_a",
    "create_task_b",
    "create_task_c",
    # New scalable API
    "JobSpec",
    "PipelineDefinition",
    "PipelineRunner",
    "make_job_task",
    "run_pipeline",
    "create_default_pipeline",
    # Parallel execution
    "ParallelExecutionResult",
    "get_max_workers",
    "run_parallel_from_files",
    "run_parallel_from_database",]
