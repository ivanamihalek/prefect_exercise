"""Parallel pipeline execution utilities."""

from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from prefect_pipeline.config import get_config, set_config, PipelineConfig
from prefect_pipeline.database import (
    initialize_database,
    close_database,
    PipelineInput,
    InputStatus,
    db,
)
from prefect_pipeline.jobs import JobResult


@dataclass
class ParallelExecutionResult:
    """Result of parallel pipeline execution."""

    total: int
    succeeded: int
    failed: int
    results: list[dict[str, Any]] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100

    def __post_init__(self) -> None:
        if self.completed_at is None:
            self.completed_at = datetime.utcnow()


def _run_single_pipeline(args: tuple[str, Path, Path, str]) -> dict[str, Any]:
    """
    Worker function to run a single pipeline.

    This function runs in a separate process, so it must:
    1. Re-initialize the database connection
    2. Re-create the pipeline runner
    3. Handle all errors gracefully

    Args:
        args: Tuple of (file_path, output_dir, db_path, log_level)

    Returns:
        Dictionary with execution results
    """
    file_path_str, output_dir, db_path, log_level = args
    file_path = Path(file_path_str)

    result_dict: dict[str, Any] = {
        "file_path": file_path_str,
        "success": False,
        "error": None,
        "output": None,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
    }

    try:
        # Set up configuration for this process
        config = PipelineConfig(
            database_path=db_path,
            output_directory=output_dir,
            log_level=log_level,  # type: ignore[arg-type]
        )
        set_config(config)

        # Initialize database in this process
        initialize_database(db_path)

        try:
            # Import here to avoid circular imports
            from prefect_pipeline.pipeline.flows import (
                PipelineDefinition,
                create_default_pipeline,
                run_pipeline,
            )

            # Create pipeline and run
            pipeline = create_default_pipeline(
                output_dir=output_dir,
                db_path=db_path,
            )

            job_result = run_pipeline(
                pipeline=pipeline,
                input_data=file_path,
                start_from=None,
                stop_after=None,
            )

            result_dict["success"] = job_result.success
            result_dict["error"] = job_result.error
            result_dict["output"] = str(job_result.output) if job_result.output else None

        finally:
            close_database()

    except Exception as e:
        result_dict["success"] = False
        result_dict["error"] = str(e)

    result_dict["completed_at"] = datetime.utcnow().isoformat()
    return result_dict


def _update_input_status(
        db_path: Path,
        input_id: int,
        result: dict[str, Any],
) -> None:
    """
    Update the status of a pipeline input in the database.

    Args:
        db_path: Path to the database
        input_id: ID of the PipelineInput record
        result: Result dictionary from pipeline execution
    """
    initialize_database(db_path)
    try:
        pipeline_input = PipelineInput.get_by_id(input_id)
        if result["success"]:
            pipeline_input.mark_completed()
        else:
            pipeline_input.mark_failed(result["error"] or "Unknown error")
    finally:
        close_database()


def get_max_workers(requested: int | None = None) -> int:
    """
    Determine the number of worker processes to use.

    Args:
        requested: Requested number of workers (None = use CPU count)

    Returns:
        Number of workers, capped at CPU count
    """
    cpu_count = multiprocessing.cpu_count()

    if requested is None:
        return cpu_count

    if requested < 1:
        return 1

    return min(requested, cpu_count)


def run_parallel_from_files(
        file_paths: list[Path | str],
        output_dir: Path,
        db_path: Path,
        log_level: str = "INFO",
        max_workers: int | None = None,
        progress_callback: Callable[[int, int, dict[str, Any]], None] | None = None,
) -> ParallelExecutionResult:
    """
    Run the pipeline in parallel for multiple input files.

    Args:
        file_paths: List of input file paths
        output_dir: Output directory for processed files
        db_path: Path to the database
        log_level: Logging level
        max_workers: Maximum number of parallel workers (None = CPU count)
        progress_callback: Optional callback(completed, total, result) for progress

    Returns:
        ParallelExecutionResult with summary and individual results
    """
    if not file_paths:
        return ParallelExecutionResult(
            total=0,
            succeeded=0,
            failed=0,
        )

    workers = get_max_workers(max_workers)
    started_at = datetime.utcnow()

    # Prepare arguments for each worker
    work_items = [
        (str(fp), output_dir, db_path, log_level)
        for fp in file_paths
    ]

    results: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        future_to_path = {
            executor.submit(_run_single_pipeline, args): args[0]
            for args in work_items
        }

        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_path), 1):
            file_path = future_to_path[future]

            try:
                result = future.result()
            except Exception as e:
                result = {
                    "file_path": file_path,
                    "success": False,
                    "error": str(e),
                    "output": None,
                }

            results.append(result)

            if result["success"]:
                succeeded += 1
            else:
                failed += 1

            if progress_callback:
                progress_callback(i, len(work_items), result)

    return ParallelExecutionResult(
        total=len(file_paths),
        succeeded=succeeded,
        failed=failed,
        results=results,
        started_at=started_at,
    )


def run_parallel_from_database(
        db_path: Path,
        output_dir: Path,
        log_level: str = "INFO",
        max_workers: int | None = None,
        limit: int | None = None,
        progress_callback: Callable[[int, int, dict[str, Any]], None] | None = None,
) -> ParallelExecutionResult:
    """
    Run the pipeline in parallel for pending inputs from the database.

    Args:
        db_path: Path to the database
        output_dir: Output directory for processed files
        log_level: Logging level
        max_workers: Maximum number of parallel workers (None = CPU count)
        limit: Maximum number of inputs to process (None = all pending)
        progress_callback: Optional callback(completed, total, result) for progress

    Returns:
        ParallelExecutionResult with summary and individual results
    """
    # Get pending inputs from database
    initialize_database(db_path)
    try:
        pending_inputs = PipelineInput.get_pending(limit=limit)

        if not pending_inputs:
            return ParallelExecutionResult(
                total=0,
                succeeded=0,
                failed=0,
            )

        # Mark all as processing
        input_map: dict[str, int] = {}
        file_paths: list[Path] = []

        for pipeline_input in pending_inputs:
            pipeline_input.mark_processing()
            file_path = Path(pipeline_input.file_path)
            file_paths.append(file_path)
            input_map[str(file_path)] = pipeline_input.id

    finally:
        close_database()

    # Define callback that updates database
    def db_progress_callback(
            completed: int,
            total: int,
            result: dict[str, Any]
    ) -> None:
        # Update database with result
        file_path = result["file_path"]
        if file_path in input_map:
            _update_input_status(db_path, input_map[file_path], result)

        # Call user's callback if provided
        if progress_callback:
            progress_callback(completed, total, result)

    # Run parallel execution
    return run_parallel_from_files(
        file_paths=file_paths,
        output_dir=output_dir,
        db_path=db_path,
        log_level=log_level,
        max_workers=max_workers,
        progress_callback=db_progress_callback,
    )
