"""Job B: Database writing job."""

from __future__ import annotations

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
from validators import validate_processed_data


class JobB(BaseJob[dict[str, Any], int]):
    """
    Job B writes processed data to the database.

    Takes processed data (from Job A) as input and writes it
    to the database using Peewee ORM.
    """

    def __init__(self, db_path: Path | None = None) -> None:
        super().__init__()
        self._db_path = db_path or get_config().database_path
        self._ensure_db_initialized()

    def _ensure_db_initialized(self) -> None:
        """Ensure database is initialized."""
        if db.is_closed():
            initialize_database(self._db_path)

    def validate_input(self, input_data: Any) -> dict[str, Any]:
        """
        Validate input data.

        Args:
            input_data: Raw input (can be dict or path to JSON file)

        Returns:
            Validated data dictionary

        Raises:
            ValidationError: If input is invalid
        """
        return validate_processed_data(input_data)

    def single_job(self, input_data: dict[str, Any]) -> int:
        """
        Write processed data to database.

        Args:
            input_data: Validated processed data

        Returns:
            Batch ID of the created batch
        """
        self._ensure_db_initialized()

        with db.atomic():
            # Create batch
            batch = ProcessingBatch.create(
                source_file=input_data['source_file'],
                status="processing",
                record_count=0
            )

            # Create records
            records_created = 0
            for record_data in input_data['records']:
                ProcessedRecord.create(
                    batch=batch,
                    name=record_data['name'],
                    value=record_data['value'],
                    is_finalized=False
                )
                records_created += 1

            # Update batch
            batch.record_count = records_created
            batch.status = "completed"
            batch.save()

        return batch.id