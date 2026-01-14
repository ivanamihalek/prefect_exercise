"""Tests for parallel pipeline execution."""

from __future__ import annotations

import multiprocessing
from pathlib import Path
from typing import Any

import pytest

from prefect_pipeline.config import PipelineConfig, set_config
from prefect_pipeline.database import (
    initialize_database,
    close_database,
    PipelineInput,
    InputStatus,
)
from prefect_pipeline.pipeline.parallel import (
    ParallelExecutionResult,
    get_max_workers,
    run_parallel_from_files,
    run_parallel_from_database,
)


class TestGetMaxWorkers:
    """Tests for get_max_workers function."""

    def test_none_returns_cpu_count(self) -> None:
        """Test that None returns CPU count."""
        result = get_max_workers(None)
        assert result == multiprocessing.cpu_count()

    def test_less_than_one_returns_one(self) -> None:
        """Test that values less than 1 return 1."""
        assert get_max_workers(0) == 1
        assert get_max_workers(-5) == 1

    def test_respects_requested_value(self) -> None:
        """Test that valid values are respected."""
        cpu_count = multiprocessing.cpu_count()
        if cpu_count >= 2:
            assert get_max_workers(2) == 2

    def test_caps_at_cpu_count(self) -> None:
        """Test that values are capped at CPU count."""
        cpu_count = multiprocessing.cpu_count()
        assert get_max_workers(cpu_count + 10) == cpu_count


class TestParallelExecutionResult:
    """Tests for ParallelExecutionResult dataclass."""

    def test_success_rate_empty(self) -> None:
        """Test success rate with no items."""
        result = ParallelExecutionResult(total=0, succeeded=0, failed=0)
        assert result.success_rate == 0.0

    def test_success_rate_all_success(self) -> None:
        """Test success rate when all succeed."""
        result = ParallelExecutionResult(total=10, succeeded=10, failed=0)
        assert result.success_rate == 100.0

    def test_success_rate_partial(self) -> None:
        """Test success rate with partial success."""
        result = ParallelExecutionResult(total=10, succeeded=7, failed=3)
        assert result.success_rate == 70.0


class TestRunParallelFromFiles:
    """Tests for run_parallel_from_files function."""

    def test_empty_file_list(self, temp_dir: Path) -> None:
        """Test with empty file list."""
        result = run_parallel_from_files(
            file_paths=[],
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        assert result.total == 0
        assert result.succeeded == 0
        assert result.failed == 0

    def test_single_file(self, temp_dir: Path) -> None:
        """Test with single file."""
        # Create input file
        input_file = temp_dir / "input.txt"
        input_file.write_text("line 1\nline 2\n")

        result = run_parallel_from_files(
            file_paths=[input_file],
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
            max_workers=1,
        )

        assert result.total == 1
        assert result.succeeded == 1
        assert result.failed == 0

    def test_multiple_files(self, temp_dir: Path) -> None:
        """Test with multiple files."""
        # Create input files
        files = []
        for i in range(3):
            f = temp_dir / f"input_{i}.txt"
            f.write_text(f"content {i}\n")
            files.append(f)

        result = run_parallel_from_files(
            file_paths=files,
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
            max_workers=2,
        )

        assert result.total == 3
        assert result.succeeded == 3
        assert result.failed == 0

    def test_handles_nonexistent_file(self, temp_dir: Path) -> None:
        """Test handling of nonexistent file."""
        result = run_parallel_from_files(
            file_paths=[temp_dir / "nonexistent.txt"],
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
        )

        assert result.total == 1
        assert result.succeeded == 0
        assert result.failed == 1

    def test_progress_callback(self, temp_dir: Path) -> None:
        """Test that progress callback is called."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("content\n")

        callback_calls: list[tuple[int, int, dict]] = []

        def callback(completed: int, total: int, result: dict) -> None:
            callback_calls.append((completed, total, result))

        run_parallel_from_files(
            file_paths=[input_file],
            output_dir=temp_dir / "output",
            db_path=temp_dir / "test.db",
            log_level="OFF",
            progress_callback=callback,
        )

        assert len(callback_calls) == 1
        assert callback_calls[0][0] == 1  # completed
        assert callback_calls[0][1] == 1  # total


class TestRunParallelFromDatabase:
    """Tests for run_parallel_from_database function."""

    def test_empty_queue(self, temp_dir: Path) -> None:
        """Test with empty input queue."""
        db_path = temp_dir / "test.db"
        initialize_database(db_path)
        close_database()

        result = run_parallel_from_database(
            db_path=db_path,
            output_dir=temp_dir / "output",
            log_level="OFF",
        )

        assert result.total == 0

    def test_processes_pending_inputs(self, temp_dir: Path) -> None:
        """Test processing of pending inputs."""
        db_path = temp_dir / "test.db"

        # Create input files and add to queue
        input_file = temp_dir / "queued.txt"
        input_file.write_text("queued content\n")

        initialize_database(db_path)
        PipelineInput.add_input(input_file)
        close_database()

        result = run_parallel_from_database(
            db_path=db_path,
            output_dir=temp_dir / "output",
            log_level="OFF",
        )

        assert result.total == 1
        assert result.succeeded == 1

        # Verify database was updated
        initialize_database(db_path)
        try:
            inputs = PipelineInput.get_by_status(InputStatus.COMPLETED)
            assert len(inputs) == 1
        finally:
            close_database()

    def test_respects_limit(self, temp_dir: Path) -> None:
        """Test that limit parameter is respected."""
        db_path = temp_dir / "test.db"

        # Create multiple input files
        initialize_database(db_path)
        for i in range(5):
            f = temp_dir / f"input_{i}.txt"
            f.write_text(f"content {i}\n")
            PipelineInput.add_input(f)
        close_database()

        result = run_parallel_from_database(
            db_path=db_path,
            output_dir=temp_dir / "output",
            log_level="OFF",
            limit=2,
        )

        assert result.total == 2

    def test_updates_status_on_failure(self, temp_dir: Path) -> None:
        """Test that failed inputs are marked correctly."""
        db_path = temp_dir / "test.db"

        # Add nonexistent file to queue
        initialize_database(db_path)
        PipelineInput.add_input(temp_dir / "nonexistent.txt")
        close_database()

        result = run_parallel_from_database(
            db_path=db_path,
            output_dir=temp_dir / "output",
            log_level="OFF",
        )

        assert result.failed == 1

        # Verify database was updated
        initialize_database(db_path)
        try:
            failed = PipelineInput.get_by_status(InputStatus.FAILED)
            assert len(failed) == 1
            assert failed[0].error_message is not None
        finally:
            close_database()
