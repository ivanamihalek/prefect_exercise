"""Base class for all jobs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


@dataclass
class JobResult(Generic[OutputT]):
    """Result wrapper for job execution."""

    success: bool
    output: OutputT | None = None
    error: str | None = None
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.completed_at is None:
            self.completed_at = datetime.utcnow()


class BaseJob(ABC, Generic[InputT, OutputT]):
    """
    Abstract base class for all pipeline jobs.

    Each job must implement the single_job method that performs
    the actual work.
    """

    def __init__(self) -> None:
        self._job_name = self.__class__.__name__

    @property
    def job_name(self) -> str:
        """Get the name of this job."""
        return self._job_name

    @abstractmethod
    def validate_input(self, input_data: Any) -> InputT:
        """
        Validate and transform input data.

        Args:
            input_data: Raw input data to validate

        Returns:
            Validated and typed input data

        Raises:
            ValidationError: If input is invalid
        """
        ...

    @abstractmethod
    def single_job(self, input_data: InputT) -> OutputT:
        """
        Execute the job's main logic.

        Args:
            input_data: Validated input data

        Returns:
            Job output
        """
        ...

    def execute(self, raw_input: Any) -> JobResult[OutputT]:
        """
        Execute the job with validation and error handling.

        Args:
            raw_input: Raw input data

        Returns:
            JobResult containing output or error information
        """
        started_at = datetime.utcnow()

        try:
            validated_input = self.validate_input(raw_input)
            output = self.single_job(validated_input)
            return JobResult(
                success=True,
                output=output,
                started_at=started_at,
                metadata={"job_name": self.job_name}
            )
        except Exception as e:
            return JobResult(
                success=False,
                error=str(e),
                started_at=started_at,
                metadata={"job_name": self.job_name, "error_type": type(e).__name__}
            )