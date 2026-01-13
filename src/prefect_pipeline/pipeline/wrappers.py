"""Prefect task wrappers for job classes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, TypeVar

from prefect import task, get_run_logger
from prefect.tasks import Task

from prefect_pipeline.jobs import BaseJob, JobResult, JobA, JobB, JobC
from prefect_pipeline.jobs.job_c import JobCResult
from prefect_pipeline.config import get_config

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


def wrap_job_as_task(
        job_class: type[BaseJob[InputT, OutputT]],
        task_name: str | None = None,
        **task_kwargs: Any
) -> Callable[[Any], JobResult[OutputT]]:
    """
    Wrap a job class's single_job method as a Prefect task.

    This allows keeping the original class definitions clean
    without decorators while still using them in Prefect flows.

    Args:
        job_class: The job class to wrap
        task_name: Optional custom task name
        **task_kwargs: Additional arguments passed to @task decorator

    Returns:
        A Prefect task function
    """
    name = task_name or f"{job_class.__name__}_task"

    @task(name=name, **task_kwargs)
    def wrapped_task(input_data: Any, **job_init_kwargs: Any) -> JobResult[OutputT]:
        logger = get_run_logger()

        config = get_config()
        if config.log_level != "OFF":
            logger.info(f"Starting {name} with input: {input_data}")

        job_instance = job_class(**job_init_kwargs)
        result = job_instance.execute(input_data)

        if config.log_level != "OFF":
            if result.success:
                logger.info(f"{name} completed successfully")
            else:
                logger.error(f"{name} failed: {result.error}")

        return result

    return wrapped_task


def create_task_a(
        output_dir: Path | None = None,
        **task_kwargs: Any
) -> Task[..., JobResult[Path]]:
    """
    Create a Prefect task for Job A.

    Args:
        output_dir: Optional output directory override
        **task_kwargs: Additional task configuration

    Returns:
        Configured Prefect task
    """

    @task(name="job_a_process_file", **task_kwargs)
    def task_a(file_path: Path | str) -> JobResult[Path]:
        logger = get_run_logger()
        config = get_config()

        if config.log_level != "OFF":
            logger.info(f"Job A: Processing file {file_path}")

        job = JobA(output_dir=output_dir)
        result = job.execute(file_path)

        if result.success and config.log_level != "OFF":
            logger.info(f"Job A: Output written to {result.output}")

        return result

    return task_a


def create_task_b(
        db_path: Path | None = None,
        **task_kwargs: Any
) -> Task[..., JobResult[int]]:
    """
    Create a Prefect task for Job B.

    Args:
        db_path: Optional database path override
        **task_kwargs: Additional task configuration

    Returns:
        Configured Prefect task
    """

    @task(name="job_b_write_database", **task_kwargs)
    def task_b(processed_data_path: Path | str | dict[str, Any]) -> JobResult[int]:
        logger = get_run_logger()
        config = get_config()

        if config.log_level != "OFF":
            logger.info(f"Job B: Writing to database from {processed_data_path}")

        job = JobB(db_path=db_path)
        result = job.execute(processed_data_path)

        if result.success and config.log_level != "OFF":
            logger.info(f"Job B: Created batch with ID {result.output}")

        return result

    return task_b


def create_task_c(
        db_path: Path | None = None,
        **task_kwargs: Any
) -> Task[..., JobResult[JobCResult]]:
    """
    Create a Prefect task for Job C.

    Args:
        db_path: Optional database path override
        **task_kwargs: Additional task configuration

    Returns:
        Configured Prefect task
    """

    @task(name="job_c_finalize", **task_kwargs)
    def task_c(batch_id: int) -> JobResult[JobCResult]:
        logger = get_run_logger()
        config = get_config()

        if config.log_level != "OFF":
            logger.info(f"Job C: Finalizing batch {batch_id}")

        job = JobC(db_path=db_path)
        result = job.execute(batch_id)

        if result.success and config.log_level != "OFF":
            logger.info(
                f"Job C: Finalized {result.output.records_finalized} records"
            )

        return result

    return task_c
