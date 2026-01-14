"""Peewee database models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path

from peewee import (
    SqliteDatabase,
    Model,
    AutoField,
    CharField,
    TextField,
    DateTimeField,
    IntegerField,
    ForeignKeyField,
    BooleanField,
)

from config import get_config

# Deferred database - will be initialized later
db = SqliteDatabase(None)


class BaseModel(Model):
    """Base model with common configuration."""

    class Meta:
        database = db

class InputStatus(str, Enum):
    """Status of a pipeline input."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class PipelineInput(BaseModel):
    """
    Represents an input to be processed by the pipeline.

    This table serves as a queue of work items that can be
    processed in parallel.
    """

    id: int = AutoField()
    file_path: str = CharField(max_length=1000)
    status: str = CharField(max_length=50, default=InputStatus.PENDING.value)
    priority: int = IntegerField(default=0)  # Higher = more urgent
    created_at: datetime = DateTimeField(default=datetime.utcnow)
    started_at: datetime | None = DateTimeField(null=True)
    completed_at: datetime | None = DateTimeField(null=True)
    error_message: str | None = TextField(null=True)
    result_batch_id: int | None = IntegerField(null=True)

    class Meta:
        table_name = "pipeline_inputs"

    @classmethod
    def add_input(
            cls,
            file_path: str | Path,
            priority: int = 0,
    ) -> "PipelineInput":
        """Add a new input to the queue."""
        return cls.create(
            file_path=str(file_path),
            status=InputStatus.PENDING.value,
            priority=priority,
        )

    @classmethod
    def get_pending(cls, limit: int | None = None) -> list["PipelineInput"]:
        """Get pending inputs ordered by priority (highest first)."""
        query = (
            cls.select()
            .where(cls.status == InputStatus.PENDING.value)
            .order_by(cls.priority.desc(), cls.created_at.asc())
        )
        if limit:
            query = query.limit(limit)
        return list(query)

    @classmethod
    def get_by_status(cls, status: InputStatus) -> list["PipelineInput"]:
        """Get all inputs with a specific status."""
        return list(
            cls.select()
            .where(cls.status == status.value)
            .order_by(cls.created_at.desc())
        )

    def mark_processing(self) -> None:
        """Mark this input as being processed."""
        self.status = InputStatus.PROCESSING.value
        self.started_at = datetime.utcnow()
        self.save()

    def mark_completed(self, result_batch_id: int | None = None) -> None:
        """Mark this input as completed."""
        self.status = InputStatus.COMPLETED.value
        self.completed_at = datetime.utcnow()
        self.result_batch_id = result_batch_id
        self.save()

    def mark_failed(self, error_message: str) -> None:
        """Mark this input as failed."""
        self.status = InputStatus.FAILED.value
        self.completed_at = datetime.utcnow()
        self.error_message = error_message
        self.save()

class ProcessingBatch(BaseModel):
    """Represents a batch of processed records."""

    id: int = AutoField()
    source_file: str = CharField(max_length=500)
    created_at: datetime = DateTimeField(default=datetime.utcnow)
    status: str = CharField(max_length=50, default="pending")
    record_count: int = IntegerField(default=0)

    class Meta:
        table_name = "processing_batches"


class ProcessedRecord(BaseModel):
    """Represents a single processed record."""

    id: int = AutoField()
    batch: ProcessingBatch = ForeignKeyField(
        ProcessingBatch,
        backref="records",
        on_delete="CASCADE"
    )
    name: str = CharField(max_length=255)
    value: str = TextField()
    processed_at: datetime = DateTimeField(default=datetime.utcnow)
    is_finalized: bool = BooleanField(default=False)

    class Meta:
        table_name = "processed_records"


def initialize_database(db_path: Path | None = None) -> None:
    """
    Initialize the database connection and create tables.

    Args:
        db_path: Optional path to the database file. If None, uses config.
    """
    if db_path is None:
        db_path = get_config().database_path

    db.init(str(db_path))
    db.connect(reuse_if_open=True)
    db.create_tables([ProcessingBatch, ProcessedRecord], safe=True)

def close_database() -> None:
    """Close the database connection."""
    if not db.is_closed():
        db.close()

def get_batch_by_id(batch_id: int) -> ProcessingBatch | None:
    """Get a processing batch by ID."""
    return ProcessingBatch.get_or_none(ProcessingBatch.id == batch_id)

def get_pending_batches() -> list[ProcessingBatch]:
    """Get all pending batches."""
    return list(
        ProcessingBatch.select()
        .where(ProcessingBatch.status == "completed")
        .where(ProcessingBatch.record_count > 0)
    )