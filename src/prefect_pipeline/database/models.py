"""Peewee database models."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

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

from prefect_pipeline.config import get_config

# Deferred database - will be initialized later
db = SqliteDatabase(None)


class BaseModel(Model):
    """Base model with common configuration."""

    class Meta:
        database = db


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