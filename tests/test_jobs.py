"""Tests for job classes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from prefect_pipeline.jobs import JobA, JobB, JobC, JobResult
from prefect_pipeline.validators import ValidationError


class TestJobA:
    """Tests for Job A."""

    def test_single_job_success(
            self,
            sample_input_file: Path,
            temp_dir: Path
    ) -> None:
        """Test successful job execution."""
        job = JobA(output_dir=temp_dir)
        output_path = job.single_job(sample_input_file)

        assert output_path.exists()
        assert output_path.suffix == ".json"

        with open(output_path) as f:
            data = json.load(f)

        assert "records" in data
        assert len(data["records"]) == 3
        assert data["source_file"] == str(sample_input_file)

    def test_execute_success(
            self,
            sample_input_file: Path,
            temp_dir: Path
    ) -> None:
        """Test execute method returns JobResult."""
        job = JobA(output_dir=temp_dir)
        result = job.execute(sample_input_file)

        assert isinstance(result, JobResult)
        assert result.success is True
        assert result.output is not None
        assert result.error is None

    def test_execute_invalid_input(self, temp_dir: Path) -> None:
        """Test execute with invalid input."""
        job = JobA(output_dir=temp_dir)
        result = job.execute("/nonexistent/file.txt")

        assert result.success is False
        assert result.error is not None
        assert "does not exist" in result.error

    def test_validate_input_wrong_extension(
            self,
            temp_dir: Path
    ) -> None:
        """Test validation rejects wrong file extension."""
        wrong_file = temp_dir / "test.xyz"
        wrong_file.write_text("content")

        job = JobA(output_dir=temp_dir)
        with pytest.raises(ValidationError) as exc_info:
            job.validate_input(wrong_file)
        assert "Invalid file extension" in str(exc_info.value)


class TestJobB:
    """Tests for Job B."""

    def test_single_job_success(
            self,
            test_db: None,
            sample_processed_data: dict[str, Any],
            test_config: Any,
    ) -> None:
        """Test successful database write."""
        job = JobB(db_path=test_config.database_path)
        batch_id = job.single_job(sample_processed_data)

        assert isinstance(batch_id, int)
        assert batch_id > 0

    def test_execute_with_file_input(
            self,
            test_db: None,
            sample_processed_file: Path,
            test_config: Any,
    ) -> None:
        """Test execute with file path input."""
        job = JobB(db_path=test_config.database_path)
        result = job.execute(sample_processed_file)

        assert result.success is True
        assert isinstance(result.output, int)

    def test_execute_invalid_data(
            self,
            test_db: None,
            test_config: Any,
    ) -> None:
        """Test execute with invalid data."""
        job = JobB(db_path=test_config.database_path)
        result = job.execute({"invalid": "data"})

        assert result.success is False
        assert "Missing required fields" in result.error


class TestJobC:
    """Tests for Job C."""

    def test_single_job_success(
            self,
            test_db: None,
            sample_processed_data: dict[str, Any],
            test_config: Any,
    ) -> None:
        """Test successful finalization."""
        # First, create a batch using Job B
        job_b = JobB(db_path=test_config.database_path)
        batch_id = job_b.single_job(sample_processed_data)

        # Now finalize with Job C
        job_c = JobC(db_path=test_config.database_path)
        result = job_c.single_job(batch_id)

        assert result.batch_id == batch_id
        assert result.records_finalized == 2
        assert result.status == "finalized"

    def test_validate_nonexistent_batch(
            self,
            test_db: None,
            test_config: Any,
    ) -> None:
        """Test validation fails for nonexistent batch."""
        job = JobC(db_path=test_config.database_path)

        with pytest.raises(ValidationError) as exc_info:
            job.validate_input(9999)
        assert "does not exist" in str(exc_info.value)

    def test_execute_invalid_batch_id(
            self,
            test_db: None,
            test_config: Any,
    ) -> None:
        """Test execute with invalid batch ID."""
        job = JobC(db_path=test_config.database_path)
        result = job.execute(-1)

        assert result.success is False
        assert "must be positive" in result.error
