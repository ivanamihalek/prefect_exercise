"""Scalable Prefect flow definitions for sequential job pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Self

from prefect import flow, task, get_run_logger

from prefect_pipeline.config import get_config, set_config, PipelineConfig
from prefect_pipeline.database import initialize_database, close_database
from prefect_pipeline.jobs import BaseJob, JobResult


# =============================================================================
# Pipeline Definition Classes
# =============================================================================

@dataclass
class JobSpec:
    """Specification for a single job in the pipeline."""

    name: str
    job_class: type[BaseJob[Any, Any]]
    config_factory: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    description: str = ""

    def create_instance(self) -> BaseJob[Any, Any]:
        """Create a job instance with configuration."""
        config = self.config_factory()
        return self.job_class(**config)


class PipelineDefinition:
    """
    Defines a sequence of jobs to be executed.

    This class is the core of the scalable design - it holds
    the job sequence and provides methods to query and slice it.
    """

    def __init__(self, name: str = "pipeline") -> None:
        self.name = name
        self._jobs: list[JobSpec] = []
        self._job_index: dict[str, int] = {}

    def add_job(
            self,
            name: str,
            job_class: type[BaseJob[Any, Any]],
            config_factory: Callable[[], dict[str, Any]] | None = None,
            description: str = "",
    ) -> Self:
        """
        Add a job to the pipeline.

        Args:
            name: Unique identifier for the job
            job_class: The job class to instantiate
            config_factory: Callable that returns kwargs for job constructor
            description: Human-readable description

        Returns:
            Self for method chaining
        """
        if name in self._job_index:
            raise ValueError(f"Job '{name}' already exists in pipeline")

        spec = JobSpec(
            name=name,
            job_class=job_class,
            config_factory=config_factory or (lambda: {}),
            description=description,
        )

        self._job_index[name] = len(self._jobs)
        self._jobs.append(spec)
        return self

    @property
    def job_names(self) -> list[str]:
        """Get ordered list of job names."""
        return [job.name for job in self._jobs]

    @property
    def first_job(self) -> str:
        """Get the name of the first job."""
        if not self._jobs:
            raise ValueError("Pipeline has no jobs")
        return self._jobs[0].name

    @property
    def last_job(self) -> str:
        """Get the name of the last job."""
        if not self._jobs:
            raise ValueError("Pipeline has no jobs")
        return self._jobs[-1].name

    def __len__(self) -> int:
        return len(self._jobs)

    def __contains__(self, name: str) -> bool:
        return name in self._job_index

    def get_job_index(self, name: str) -> int:
        """Get the index of a job by name."""
        if name not in self._job_index:
            raise ValueError(
                f"Unknown job: '{name}'. Available jobs: {self.job_names}"
            )
        return self._job_index[name]

    def get_job(self, name: str) -> JobSpec:
        """Get a job specification by name."""
        return self._jobs[self.get_job_index(name)]

    def get_jobs_in_range(
            self,
            start_from: str | None = None,
            stop_after: str | None = None,
    ) -> list[JobSpec]:
        """
        Get jobs in the specified range (inclusive).

        Args:
            start_from: Job name to start from (None = first job)
            stop_after: Job name to stop after (None = last job)

        Returns:
            List of JobSpec in the range
        """
        if not self._jobs:
            return []

        start_idx = 0 if start_from is None else self.get_job_index(start_from)
        stop_idx = len(self._jobs) - 1 if stop_after is None else self.get_job_index(stop_after)

        if start_idx > stop_idx:
            raise ValueError(
                f"start_from ('{start_from}' @ {start_idx}) must come before "
                f"stop_after ('{stop_after}' @ {stop_idx})"
            )

        return self._jobs[start_idx:stop_idx + 1]

    def validate_range(
            self,
            start_from: str | None = None,
            stop_after: str | None = None,
    ) -> tuple[str, str]:
        """
        Validate and normalize a job range.

        Returns:
            Tuple of (start_job_name, stop_job_name)
        """
        start = start_from or self.first_job
        stop = stop_after or self.last_job

        # This will raise if invalid
        self.get_jobs_in_range(start, stop)

        return start, stop


# =============================================================================
# Task Factory
# =============================================================================

def make_job_task(spec: JobSpec) -> Callable[[Any], JobResult[Any]]:
    """
    Create a Prefect task for a job specification.

    This factory creates tasks dynamically, allowing the pipeline
    to scale to any number of jobs.
    """

    @task(name=f"job_{spec.name}")
    def job_task(input_data: Any) -> JobResult[Any]:
        logger = get_run_logger()
        config = get_config()

        if config.log_level != "OFF":
            logger.info(f"Starting job '{spec.name}'")
            if spec.description:
                logger.debug(f"Description: {spec.description}")

        job = spec.create_instance()
        result = job.execute(input_data)

        if config.log_level != "OFF":
            if result.success:
                logger.info(f"Job '{spec.name}' completed successfully")
            else:
                logger.error(f"Job '{spec.name}' failed: {result.error}")

        return result

    return job_task


# =============================================================================
# The Single Flow Definition - Handles All Cases
# =============================================================================

@flow(name="sequential_pipeline")
def run_pipeline(
        pipeline: PipelineDefinition,
        input_data: Any,
        start_from: str | None = None,
        stop_after: str | None = None,
) -> JobResult[Any]:
    """
    Run a pipeline of jobs sequentially.

    This single flow handles all entry points by specifying
    start_from and stop_after parameters.

    Args:
        pipeline: The pipeline definition containing job specs
        input_data: Initial input for the starting job
        start_from: Job name to start from (None = first job)
        stop_after: Job name to stop after (None = last job)

    Returns:
        JobResult from the last executed job

    Examples:
        # Run full pipeline
        run_pipeline(pipeline, input_file)

        # Run from job B to end
        run_pipeline(pipeline, processed_data, start_from="B")

        # Run only job A
        run_pipeline(pipeline, input_file, stop_after="A")

        # Run jobs B and C only
        run_pipeline(pipeline, processed_data, start_from="B", stop_after="C")
    """
    logger = get_run_logger()
    config = get_config()

    # Get the jobs to run
    jobs_to_run = pipeline.get_jobs_in_range(start_from, stop_after)
    job_names = [j.name for j in jobs_to_run]

    if config.log_level != "OFF":
        logger.info(f"Pipeline '{pipeline.name}' executing jobs: {job_names}")

    if not jobs_to_run:
        logger.warning("No jobs to run")
        return JobResult(success=True, output=input_data)

    # Execute jobs sequentially
    current_data = input_data
    result: JobResult[Any] | None = None

    for i, spec in enumerate(jobs_to_run):
        if config.log_level != "OFF":
            logger.info(f"Step {i + 1}/{len(jobs_to_run)}: {spec.name}")

        # Create and run the task
        job_task = make_job_task(spec)
        result = job_task(current_data)

        if not result.success:
            if config.log_level != "OFF":
                logger.error(
                    f"Pipeline stopped at job '{spec.name}': {result.error}"
                )
            return result

        # Output of this job becomes input to next job
        current_data = result.output

    if config.log_level != "OFF":
        logger.info(f"Pipeline completed successfully")

    return result  # type: ignore[return-value]


# =============================================================================
# Pipeline Factory
# =============================================================================

def create_default_pipeline(
        output_dir: Path | None = None,
        db_path: Path | None = None,
) -> PipelineDefinition:
    """
    Create the default A -> B -> C pipeline.

    This factory demonstrates how to configure a pipeline.
    For N jobs, simply add more add_job() calls.
    """
    from prefect_pipeline.jobs import JobA, JobB, JobC

    config = get_config()
    _output_dir = output_dir or config.output_directory
    _db_path = db_path or config.database_path

    pipeline = PipelineDefinition(name="default_pipeline")

    # Add jobs in order - scales to any number of jobs
    pipeline.add_job(
        name="A",
        job_class=JobA,
        config_factory=lambda od=_output_dir: {"output_dir": od},
        description="Process input file and produce JSON",
    )

    pipeline.add_job(
        name="B",
        job_class=JobB,
        config_factory=lambda dp=_db_path: {"db_path": dp},
        description="Write processed data to database",
    )

    pipeline.add_job(
        name="C",
        job_class=JobC,
        config_factory=lambda dp=_db_path: {"db_path": dp},
        description="Finalize records in database",
    )

    return pipeline


# =============================================================================
# Pipeline Runner - Convenience Wrapper
# =============================================================================

class PipelineRunner:
    """
    Convenience class for running pipelines with stored configuration.

    Provides a clean API for pipeline execution with automatic
    database initialization and cleanup.
    """

    def __init__(
            self,
            output_dir: Path | None = None,
            db_path: Path | None = None,
            log_level: str = "INFO",
            pipeline: PipelineDefinition | None = None,
    ) -> None:
        """
        Initialize the pipeline runner.

        Args:
            output_dir: Output directory for processed files
            db_path: Path to the SQLite database
            log_level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'OFF')
            pipeline: Custom pipeline definition (uses default if None)
        """
        self.output_dir = output_dir or Path("./data/output")
        self.db_path = db_path or Path("./data/pipeline.db")
        self.log_level = log_level

        # Set global configuration
        config = PipelineConfig(
            database_path=self.db_path,
            output_directory=self.output_dir,
            log_level=log_level,  # type: ignore[arg-type]
        )
        set_config(config)

        # Create or use provided pipeline
        self._pipeline = pipeline or create_default_pipeline(
            output_dir=self.output_dir,
            db_path=self.db_path,
        )

    @property
    def available_jobs(self) -> list[str]:
        """Get list of available job names."""
        return self._pipeline.job_names

    def run(
            self,
            input_data: Any,
            start_from: str | None = None,
            stop_after: str | None = None,
    ) -> JobResult[Any]:
        """
        Run the pipeline.

        Args:
            input_data: Input for the starting job
            start_from: Job name to start from (None = first job)
            stop_after: Job name to stop after (None = last job)

        Returns:
            JobResult from the last executed job

        Examples:
            runner = PipelineRunner()

            # Full pipeline
            result = runner.run(Path("input.txt"))

            # Start from middle
            result = runner.run(processed_data, start_from="B")

            # Run single job
            result = runner.run(input_file, start_from="A", stop_after="A")
        """
        initialize_database(self.db_path)

        try:
            return run_pipeline(
                pipeline=self._pipeline,
                input_data=input_data,
                start_from=start_from,
                stop_after=stop_after,
            )
        finally:
            close_database()

    # Convenience methods for common patterns

    def run_full(self, input_data: Any) -> JobResult[Any]:
        """Run the complete pipeline."""
        return self.run(input_data)

    def run_single(self, job_name: str, input_data: Any) -> JobResult[Any]:
        """Run a single job."""
        return self.run(input_data, start_from=job_name, stop_after=job_name)

    def run_from(self, job_name: str, input_data: Any) -> JobResult[Any]:
        """Run from a specific job to the end."""
        return self.run(input_data, start_from=job_name)

    def run_until(self, job_name: str, input_data: Any) -> JobResult[Any]:
        """Run from the beginning until a specific job."""
        return self.run(input_data, stop_after=job_name)
