"""Job C: Database finalization job."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from config import get_config
from database import (
    initialize_database,
    ProcessingBatch,
    ProcessedRecord,
    db,
)
from jobs import BaseJob
from validators import validate_database_record_id, ValidationError


class JobCResult:
    """Result from Job C execution."""

    def __init__(
            self,
            batch_id: int,
            records_finalized: int,
            status: str
    ) -> None:
        self.batch_id = batch_id
        self.records_finalized = records_finalized
        self.status = status
        self.completed_at = datetime.utcnow()

    def __repr__(self) -> str:
        return (
            f"JobCResult(batch_id={self.batch_id}, "
            f"records_finalized={self.records_finalized}, "
            f"status='{self.status}')"
        )


class JobC(BaseJob[int, JobCResult]):
    """
    Job C finalizes records in the database.

    Takes a batch ID as input and finalizes all records
    in that batch. Can only run when data is available
    from Job B.
    """

    def __init__(self, db_path: Path | None = None) -> None:
        super().__init__()
        self._db_path = db_path or get_config().database_path
        self._ensure_db_initialized()

    def _ensure_db_initialized(self) -> None:
        """Ensure database is initialized."""
        if db.is_closed():
            initialize_database(self._db_path)

    def validate_input(self, input_data: Any) -> int:
        """
        Validate batch ID input.

        Args:
            input_data: Raw batch ID input

        Returns:
            Validated batch ID

        Raises:
            ValidationError: If input is invalid or batch doesn't exist
        """
        batch_id = validate_database_record_id(input_data)

        self._ensure_db_initialized()

        # Check that batch exists and has data
        batch = ProcessingBatch.get_or_none(ProcessingBatch.id == batch_id)
        if batch is None:
            raise ValidationError(
                f"Batch with ID {batch_id} does not exist",
                "batch_id"
            )

        if batch.status != "completed":
            raise ValidationError(
                f"Batch {batch_id} is not ready (status: {batch.status})",
                "batch_id"
            )

        if batch.record_count == 0:
            raise ValidationError(
                f"Batch {batch_id} has no records to process",
                "batch_id"
            )

        return batch_id

    def single_job(self, input_data: int) -> JobCResult:
        """
        Finalize records for the given batch.

        Args:
            input_data: Validated batch ID

        Returns:
            JobCResult with finalization details
        """
        self._ensure_db_initialized()

        batch_id = input_data
        records_finalized = 0

        with db.atomic():
            # Get all non-finalized records in the batch
            records = (
                ProcessedRecord
                .select()
                .where(ProcessedRecord.batch_id == batch_id)
                .where(ProcessedRecord.is_finalized == False)
            )

            for record in records:
                record.is_finalized = True
                record.save()
                records_finalized += 1

            # Update batch status
            batch = ProcessingBatch.get_by_id(batch_id)
            batch.status = "finalized"
            batch.save()

        return JobCResult(
            batch_id=batch_id,
            records_finalized=records_finalized,
            status="finalized"
        )
