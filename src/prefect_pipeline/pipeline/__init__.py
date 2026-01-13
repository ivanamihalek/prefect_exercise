"""Pipeline module with Prefect flows and utilities."""

from prefect_pipeline.pipeline.wrappers import (
    wrap_job_as_task,
    create_task_a,
    create_task_b,
    create_task_c,
)
from prefect_pipeline.pipeline.flows import (
    JobSpec,
    PipelineDefinition,
    PipelineRunner,
    make_job_task,
    run_pipeline,
    create_default_pipeline,
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
]
