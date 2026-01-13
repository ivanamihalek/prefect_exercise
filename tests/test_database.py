"""Tests for database models and operations."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from prefect_pipeline.database import (
    initialize_database,
    close_database,
    ProcessingBatch,
    ProcessedRecord,
    db,
)


class TestDatabaseInitialization:
    """Tests for database initialization."""

    def test_initialize_creates_tables(self, temp_dir: Path) -> None:
        """Test that initialization creates required tables."""
        db_path = temp_dir / "test_init.db"
        initialize_database(db_path)

        try:
            # Tables should exist
            assert ProcessingBatch.table_exists()
            assert ProcessedRecord.table_exists()
        finally:
            close_database()

    def test_initialize_idempotent(self, temp_dir: Path) -> None:
        """Test that multiple initializations don't cause errors."""
        db_path = temp_dir / "test_idempotent.db"

        initialize_database(db_path)
        initialize_database(db_path)  # Should not raise

        close_database()

    def test_close_database_idempotent(self, temp_dir: Path) -> None:
        """Test that closing multiple times doesn't cause errors."""
        db_path = temp_dir / "test_close.db"
        initialize_database(db_path)

        close_database()
        close_database()  # Should not raise


class TestProcessingBatch:
    """Tests for ProcessingBatch model."""

    def test_create_batch(self, test_db: None) -> None:
        """Test creating a processing batch."""
        batch = ProcessingBatch.create(
            source_file="/path/to/file.txt",
            status="pending",
            record_count=0,
        )

        assert batch.id is not None
        assert batch.source_file == "/path/to/file.txt"
        assert batch.status == "pending"
        assert batch.record_count == 0

    def test_batch_default_values(self, test_db: None) -> None:
        """Test batch creation with default values."""
        batch = ProcessingBatch.create(
            source_file="/path/to/file.txt",
        )

        assert batch.status == "pending"
        assert batch.record_count == 0
        assert batch.created_at is not None

    def test_batch_with_records(self, test_db: None) -> None:
        """Test batch with associated records."""
        batch = ProcessingBatch.create(
            source_file="/path/to/file.txt",
            status="completed",
            record_count=2,
        )

        ProcessedRecord.create(
            batch=batch,
            name="record_1",
            value="value_1",
        )
        ProcessedRecord.create(
            batch=batch,
            name="record_2",
            value="value_2",
        )

        # Query records through backref
        records = list(batch.records)
        assert len(records) == 2
        assert records[0].name == "record_1"
        assert records[1].name == "record_2"

    def test_batch_update_status(self, test_db: None) -> None:
        """Test updating batch status."""
        batch = ProcessingBatch.create(
            source_file="/path/to/file.txt",
            status="pending",
        )

        batch.status = "completed"
        batch.save()

        # Reload from database
        reloaded = ProcessingBatch.get_by_id(batch.id)
        assert reloaded.status == "completed"

    def test_batch_query_by_status(self, test_db: None) -> None:
        """Test querying batches by status."""
        ProcessingBatch.create(source_file="file1.txt", status="pending")
        ProcessingBatch.create(source_file="file2.txt", status="completed")
        ProcessingBatch.create(source_file="file3.txt", status="completed")

        completed = list(
            ProcessingBatch.select()
            .where(ProcessingBatch.status == "completed")
        )

        assert len(completed) == 2


class TestProcessedRecord:
    """Tests for ProcessedRecord model."""

    def test_create_record(self, test_db: None) -> None:
        """Test creating a processed record."""
        batch = ProcessingBatch.create(source_file="test.txt")

        record = ProcessedRecord.create(
            batch=batch,
            name="test_record",
            value="test_value",
        )

        assert record.id is not None
        assert record.batch_id == batch.id
        assert record.name == "test_record"
        assert record.value == "test_value"
        assert record.is_finalized is False

    def test_record_default_values(self, test_db: None) -> None:
        """Test record creation with default values."""
        batch = ProcessingBatch.create(source_file="test.txt")

        record = ProcessedRecord.create(
            batch=batch,
            name="test_record",
            value="test_value",
        )

        assert record.is_finalized is False
        assert record.processed_at is not None

    def test_record_finalization(self, test_db: None) -> None:
        """Test finalizing a record."""
        batch = ProcessingBatch.create(source_file="test.txt")
        record = ProcessedRecord.create(
            batch=batch,
            name="test_record",
            value="test_value",
        )

        assert record.is_finalized is False

        record.is_finalized = True
        record.save()

        reloaded = ProcessedRecord.get_by_id(record.id)
        assert reloaded.is_finalized is True

    def test_cascade_delete(self, test_db: None) -> None:
        """Test that deleting batch cascades to records."""
        batch = ProcessingBatch.create(source_file="test.txt")
        record = ProcessedRecord.create(
            batch=batch,
            name="test_record",
            value="test_value",
        )
        record_id = record.id

        # Delete batch
        batch.delete_instance()

        # Record should be deleted too
        assert ProcessedRecord.get_or_none(ProcessedRecord.id == record_id) is None

    def test_query_unfinalized_records(self, test_db: None) -> None:
        """Test querying unfinalized records."""
        batch = ProcessingBatch.create(source_file="test.txt")

        ProcessedRecord.create(batch=batch, name="r1", value="v1", is_finalized=False)
        ProcessedRecord.create(batch=batch, name="r2", value="v2", is_finalized=True)
        ProcessedRecord.create(batch=batch, name="r3", value="v3", is_finalized=False)

        unfinalized = list(
            ProcessedRecord.select()
            .where(ProcessedRecord.is_finalized == False)
        )

        assert len(unfinalized) == 2

    def test_query_records_by_batch(self, test_db: None) -> None:
        """Test querying records by batch ID."""
        batch1 = ProcessingBatch.create(source_file="file1.txt")
        batch2 = ProcessingBatch.create(source_file="file2.txt")

        ProcessedRecord.create(batch=batch1, name="r1", value="v1")
        ProcessedRecord.create(batch=batch1, name="r2", value="v2")
        ProcessedRecord.create(batch=batch2, name="r3", value="v3")

        batch1_records = list(
            ProcessedRecord.select()
            .where(ProcessedRecord.batch_id == batch1.id)
        )

        assert len(batch1_records) == 2


class TestDatabaseTransactions:
    """Tests for database transaction handling."""

    def test_atomic_transaction_success(self, test_db: None) -> None:
        """Test successful atomic transaction."""
        with db.atomic():
            batch = ProcessingBatch.create(source_file="test.txt")
            ProcessedRecord.create(batch=batch, name="r1", value="v1")
            ProcessedRecord.create(batch=batch, name="r2", value="v2")

        # Both should be persisted
        assert ProcessingBatch.select().count() == 1
        assert ProcessedRecord.select().count() == 2

    def test_atomic_transaction_rollback(self, test_db: None) -> None:
        """Test atomic transaction rollback on error."""
        try:
            with db.atomic():
                batch = ProcessingBatch.create(source_file="test.txt")
                ProcessedRecord.create(batch=batch, name="r1", value="v1")
                raise ValueError("Simulated error")
        except ValueError:
            pass

        # Nothing should be persisted due to rollback
        assert ProcessingBatch.select().count() == 0
        assert ProcessedRecord.select().count() == 0
