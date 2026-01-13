"""Tests for pipeline flows and wrappers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest

from prefect_pipeline.config import PipelineConfig, set_config, get_config
from prefect_pipeline.database import (
    initialize_database,
    close_database,
    ProcessingBatch,
    ProcessedRecord,
)
from prefect_pipeline.jobs import JobA, JobB, JobC, JobResult
from prefect_pipeline.jobs.job_c import JobCResult
from prefect_pipeline.pipeline.wrappers import (
    wrap_job_as_task,
    create_task_a,
    create_task_b,
    create_task_c,
)
from prefect_pipeline.pipeline.flows import (
    run_full_pipeline,
    run_from_a,
    run_from_b,
    run_from_c,
    PipelineRunner,
)


class TestWrapJobAsTask:
    """Tests for the wrap_job_as_task utility."""

    def test_wrapped_task_has_correct_name(self) -> None:
        """Test that wrapped task has the expected name."""
        wrapped = wrap_job_as_task(JobA, task_name="custom_name")
        assert wrapped.name == "custom_name"

    def test_wrapped_task_default_name(self) -> None:
        """Test that wrapped task uses class name by default."""
        wrapped = wrap_job_as_task(JobA)
        assert wrapped.name == "JobA_task"


class TestCreateTaskA:
    """Tests for create_task_a wrapper."""

    def test_task_creation(self, temp_dir: Path) -> None:
        """Test that task is created correctly."""
        task = create_task_a(output_dir=temp_dir)
        assert task.name == "job_a_process_file"

    def test_task_execution_success(
            self,
            sample_input_file: Path,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test successful task execution."""
        task = create_task_a(output_dir=temp_dir)

        # Execute the task function directly (not as Prefect task)
        result = task.fn(sample_input_file)

        assert isinstance(result, JobResult)
        assert result.success is True
        assert result.output is not None
        assert result.output.exists()

    def test_task_execution_failure(
            self,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test task execution with invalid input."""
        task = create_task_a(output_dir=temp_dir)

        result = task.fn(temp_dir / "nonexistent.txt")

        assert isinstance(result, JobResult)
        assert result.success is False
        assert result.error is not None


class TestCreateTaskB:
    """Tests for create_task_b wrapper."""

    def test_task_creation(self, test_config: PipelineConfig) -> None:
        """Test that task is created correctly."""
        task = create_task_b(db_path=test_config.database_path)
        assert task.name == "job_b_write_database"

    def test_task_execution_success(
            self,
            test_db: None,
            sample_processed_data: dict[str, Any],
            test_config: PipelineConfig,
    ) -> None:
        """Test successful task execution."""
        task = create_task_b(db_path=test_config.database_path)

        result = task.fn(sample_processed_data)

        assert isinstance(result, JobResult)
        assert result.success is True
        assert isinstance(result.output, int)
        assert result.output > 0

    def test_task_execution_with_file_path(
            self,
            test_db: None,
            sample_processed_file: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test task execution with file path input."""
        task = create_task_b(db_path=test_config.database_path)

        result = task.fn(sample_processed_file)

        assert result.success is True
        assert isinstance(result.output, int)


class TestCreateTaskC:
    """Tests for create_task_c wrapper."""

    def test_task_creation(self, test_config: PipelineConfig) -> None:
        """Test that task is created correctly."""
        task = create_task_c(db_path=test_config.database_path)
        assert task.name == "job_c_finalize"

    def test_task_execution_success(
            self,
            test_db: None,
            sample_processed_data: dict[str, Any],
            test_config: PipelineConfig,
    ) -> None:
        """Test successful task execution."""
        # First create a batch
        job_b = JobB(db_path=test_config.database_path)
        batch_id = job_b.single_job(sample_processed_data)

        task = create_task_c(db_path=test_config.database_path)
        result = task.fn(batch_id)

        assert isinstance(result, JobResult)
        assert result.success is True
        assert isinstance(result.output, JobCResult)
        assert result.output.batch_id == batch_id

    def test_task_execution_invalid_batch(
            self,
            test_db: None,
            test_config: PipelineConfig,
    ) -> None:
        """Test task execution with invalid batch ID."""
        task = create_task_c(db_path=test_config.database_path)

        result = task.fn(9999)

        assert result.success is False
        assert "does not exist" in result.error


class TestRunFullPipeline:
    """Tests for run_full_pipeline flow."""

    def test_full_pipeline_success(
            self,
            sample_input_file: Path,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test successful full pipeline execution."""
        result = run_full_pipeline(
            input_file=sample_input_file,
            output_dir=temp_dir / "output",
            db_path=test_config.database_path,
        )

        assert result.success is True
        assert isinstance(result.output, JobCResult)
        assert result.output.status == "finalized"
        assert result.output.records_finalized > 0

    def test_full_pipeline_creates_database_records(
            self,
            sample_input_file: Path,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test that pipeline creates expected database records."""
        result = run_full_pipeline(
            input_file=sample_input_file,
            output_dir=temp_dir / "output",
            db_path=test_config.database_path,
        )

        assert result.success is True

        # Verify database state
        initialize_database(test_config.database_path)
        try:
            batches = list(ProcessingBatch.select())
            assert len(batches) == 1
            assert batches[0].status == "finalized"

            records = list(ProcessedRecord.select())
            assert len(records) > 0
            assert all(r.is_finalized for r in records)
        finally:
            close_database()

    def test_full_pipeline_invalid_input(
            self,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test pipeline with invalid input file."""
        with pytest.raises(RuntimeError) as exc_info:
            run_full_pipeline(
                input_file=temp_dir / "nonexistent.txt",
                output_dir=temp_dir / "output",
                db_path=test_config.database_path,
            )
        assert "Job A failed" in str(exc_info.value)


class TestRunFromA:
    """Tests for run_from_a flow."""

    def test_run_from_a_full(
            self,
            sample_input_file: Path,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test running full pipeline from A."""
        result = run_from_a(
            input_file=sample_input_file,
            output_dir=temp_dir / "output",
            db_path=test_config.database_path,
        )

        assert result.success is True
        assert isinstance(result.output, JobCResult)

    def test_run_from_a_stop_after_a(
            self,
            sample_input_file: Path,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from A, stopping after A."""
        result = run_from_a(
            input_file=sample_input_file,
            output_dir=temp_dir / "output",
            db_path=test_config.database_path,
            stop_after='a',
        )

        assert result.success is True
        assert isinstance(result.output, Path)
        assert result.output.exists()

    def test_run_from_a_stop_after_b(
            self,
            sample_input_file: Path,
            temp_dir: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from A, stopping after B."""
        result = run_from_a(
            input_file=sample_input_file,
            output_dir=temp_dir / "output",
            db_path=test_config.database_path,
            stop_after='b',
        )

        assert result.success is True
        assert isinstance(result.output, int)  # Batch ID


class TestRunFromB:
    """Tests for run_from_b flow."""

    def test_run_from_b_with_dict(
            self,
            sample_processed_data: dict[str, Any],
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from B with dict input."""
        result = run_from_b(
            processed_data=sample_processed_data,
            db_path=test_config.database_path,
        )

        assert result.success is True
        assert isinstance(result.output, JobCResult)

    def test_run_from_b_with_file(
            self,
            sample_processed_file: Path,
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from B with file input."""
        result = run_from_b(
            processed_data=sample_processed_file,
            db_path=test_config.database_path,
        )

        assert result.success is True
        assert isinstance(result.output, JobCResult)

    def test_run_from_b_stop_after_b(
            self,
            sample_processed_data: dict[str, Any],
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from B, stopping after B."""
        result = run_from_b(
            processed_data=sample_processed_data,
            db_path=test_config.database_path,
            stop_after='b',
        )

        assert result.success is True
        assert isinstance(result.output, int)  # Batch ID


class TestRunFromC:
    """Tests for run_from_c flow."""

    def test_run_from_c_success(
            self,
            test_db: None,
            sample_processed_data: dict[str, Any],
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from C."""
        # Create a batch first
        job_b = JobB(db_path=test_config.database_path)
        batch_id = job_b.single_job(sample_processed_data)

        # Close and reinitialize to simulate fresh run
        close_database()

        result = run_from_c(
            batch_id=batch_id,
            db_path=test_config.database_path,
        )

        assert result.success is True
        assert isinstance(result.output, JobCResult)
        assert result.output.batch_id == batch_id

    def test_run_from_c_invalid_batch(
            self,
            test_config: PipelineConfig,
    ) -> None:
        """Test running pipeline from C with invalid batch."""
        result = run_from_c(
            batch_id=9999,
            db_path=test_config.database_path,
        )

        assert result.success is False
        assert "does not exist" in result.error


class TestPipelineRunner:
    """Tests for PipelineRunner class."""

    def test_runner_initialization(self, temp_dir: Path) -> None:
        """Test runner initialization."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        assert runner.output_dir == temp_dir / "output"
        assert runner.db_path == temp_dir / "test.db"
        assert runner.log_level == "OFF"

    def test_runner_sets_config(self, temp_dir: Path) -> None:
        """Test that runner sets global config."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="WARNING",
        )

        config = get_config()
        assert config.log_level == "WARNING"

    def test_runner_run_full(
            self,
            sample_input_file: Path,
            temp_dir: Path,
    ) -> None:
        """Test runner.run_full method."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        result = runner.run_full(sample_input_file)

        assert result.success is True
        assert isinstance(result.output, JobCResult)

    def test_runner_run_from_a(
            self,
            sample_input_file: Path,
            temp_dir: Path,
    ) -> None:
        """Test runner.run_from_a method."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        result = runner.flow_from_a(sample_input_file, stop_after='a')

        assert result.success is True
        assert isinstance(result.output, Path)

    def test_runner_run_from_b(
            self,
            sample_processed_data: dict[str, Any],
            temp_dir: Path,
    ) -> None:
        """Test runner.run_from_b method."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        result = runner.flow_from_b(sample_processed_data)

        assert result.success is True
        assert isinstance(result.output, JobCResult)

    def test_runner_run_from_c(
            self,
            sample_processed_data: dict[str, Any],
            temp_dir: Path,
    ) -> None:
        """Test runner.run_from_c method."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        # First create a batch
        initialize_database(runner.db_path)
        job_b = JobB(db_path=runner.db_path)
        batch_id = job_b.single_job(sample_processed_data)
        close_database()

        result = runner.run_from_c(batch_id)

        assert result.success is True
        assert result.output.batch_id == batch_id


class TestPipelineIntegration:
    """Integration tests for the complete pipeline."""

    def test_end_to_end_pipeline(
            self,
            temp_dir: Path,
    ) -> None:
        """Test complete end-to-end pipeline execution."""
        # Create input file
        input_file = temp_dir / "integration_test.txt"
        input_file.write_text(
            "First record\n"
            "Second record\n"
            "Third record\n"
            "Fourth record\n"
        )

        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "integration.db",
            log_level="OFF",
        )

        result = runner.run_full(input_file)

        # Verify result
        assert result.success is True
        assert result.output.records_finalized == 4

        # Verify output file exists
        output_files = list((temp_dir / "output").glob("*.json"))
        assert len(output_files) == 1

        # Verify database state
        initialize_database(temp_dir / "integration.db")
        try:
            batches = list(ProcessingBatch.select())
            assert len(batches) == 1
            assert batches[0].status == "finalized"
            assert batches[0].record_count == 4

            records = list(ProcessedRecord.select())
            assert len(records) == 4
            assert all(r.is_finalized for r in records)
        finally:
            close_database()

    def test_pipeline_with_empty_file(
            self,
            temp_dir: Path,
    ) -> None:
        """Test pipeline with empty input file."""
        # Create empty input file
        input_file = temp_dir / "empty.txt"
        input_file.write_text("")

        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "empty.db",
            log_level="OFF",
        )

        # Pipeline should handle empty file
        # Job A should succeed (produces file with 0 records)
        result = runner.flow_from_a(input_file, stop_after='a')
        assert result.success is True

    def test_pipeline_preserves_data_integrity(
            self,
            temp_dir: Path,
    ) -> None:
        """Test that pipeline preserves data integrity."""
        # Create input with specific content
        input_file = temp_dir / "integrity_test.txt"
        original_lines = [
            "Line with special chars: @#$%",
            "Line with unicode: 日本語",
            "Line with numbers: 12345",
        ]
        input_file.write_text("\n".join(original_lines))

        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "integrity.db",
            log_level="OFF",
        )

        result = runner.run_full(input_file)
        assert result.success is True

        # Verify data in database matches input
        initialize_database(temp_dir / "integrity.db")
        try:
            records = list(
                ProcessedRecord.select()
                .order_by(ProcessedRecord.id)
            )

            assert len(records) == 3
            for i, record in enumerate(records):
                assert record.value == original_lines[i]
        finally:
            close_database()

    def test_multiple_pipeline_runs(
            self,
            temp_dir: Path,
    ) -> None:
        """Test running pipeline multiple times."""
        runner = PipelineRunner(
            output_dir=temp_dir / "output",
            db_path=temp_dir / "multiple.db",
            log_level="OFF",
        )

        # Run pipeline multiple times with different inputs
        for i in range(3):
            input_file = temp_dir / f"input_{i}.txt"
            input_file.write_text(f"Content for file {i}\nLine 2\n")

            result = runner.run_full(input_file)
            assert result.success is True

        # Verify all batches are created
        initialize_database(temp_dir / "multiple.db")
        try:
            batches = list(ProcessingBatch.select())
            assert len(batches) == 3

            records = list(ProcessedRecord.select())
            assert len(records) == 6  # 2 records per batch
        finally:
            close_database()