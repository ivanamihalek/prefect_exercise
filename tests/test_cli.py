"""Tests for CLI commands."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from prefect_pipeline.cli import main
from prefect_pipeline.database import (
    initialize_database,
    close_database,
    PipelineInput,
    InputStatus,
)


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


class TestListJobs:
    """Tests for list-jobs command."""

    def test_lists_jobs(self, cli_runner: CliRunner, temp_dir: Path) -> None:
        """Test that jobs are listed."""
        result = cli_runner.invoke(
            main,
            ["--db-path", str(temp_dir / "test.db"), "list-jobs"]
        )

        assert result.exit_code == 0
        assert "Available jobs" in result.output
        assert "A" in result.output
        assert "B" in result.output
        assert "C" in result.output


class TestRunAll:
    """Tests for run-all command."""

    def test_single_file(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test running with single file."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("test content\n")

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "--output-dir", str(temp_dir / "output"),
                "--log-level", "OFF",
                "run-all",
                str(input_file),
            ]
        )

        assert result.exit_code == 0
        assert "Succeeded:" in result.output

    def test_multiple_files(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test running with multiple files."""
        files = []
        for i in range(2):
            f = temp_dir / f"input_{i}.txt"
            f.write_text(f"content {i}\n")
            files.append(str(f))

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "--output-dir", str(temp_dir / "output"),
                "--log-level", "OFF",
                "run-all",
                *files,
                "--max-workers", "1",
            ]
        )

        assert result.exit_code == 0
        assert "Total:     2" in result.output

    def test_from_db_no_inputs(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test from-db with empty queue."""
        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "--output-dir", str(temp_dir / "output"),
                "--log-level", "OFF",
                "run-all",
                "--from-db",
            ]
        )

        assert result.exit_code == 0
        assert "Total:     0" in result.output

    def test_error_both_sources(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test error when both sources specified."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("content\n")

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "run-all",
                str(input_file),
                "--from-db",
            ]
        )

        assert result.exit_code == 1
        assert "Cannot specify both" in result.output

    def test_error_no_source(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test error when no source specified."""
        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "run-all",
            ]
        )

        assert result.exit_code == 1
        assert "Must specify either" in result.output


class TestInputsCommands:
    """Tests for inputs subcommands."""

    def test_add_single(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test adding single input."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("content\n")

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "inputs", "add",
                str(input_file),
            ]
        )

        assert result.exit_code == 0
        assert "Added 1 input(s)" in result.output

    def test_add_with_priority(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test adding input with priority."""
        input_file = temp_dir / "input.txt"
        input_file.write_text("content\n")

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "inputs", "add",
                str(input_file),
                "--priority", "10",
            ]
        )

        assert result.exit_code == 0
        assert "priority: 10" in result.output

    def test_list_empty(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test listing empty queue."""
        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(temp_dir / "test.db"),
                "inputs", "list",
            ]
        )

        assert result.exit_code == 0
        assert "No inputs found" in result.output

    def test_list_with_inputs(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test listing inputs."""
        db_path = temp_dir / "test.db"
        input_file = temp_dir / "input.txt"
        input_file.write_text("content\n")

        # Add input directly
        initialize_database(db_path)
        PipelineInput.add_input(input_file)
        close_database()

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(db_path),
                "inputs", "list",
            ]
        )

        assert result.exit_code == 0
        assert "input.txt" in result.output

    def test_clear_with_confirmation(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test clearing inputs with confirmation."""
        db_path = temp_dir / "test.db"

        initialize_database(db_path)
        PipelineInput.add_input(temp_dir / "dummy.txt")
        close_database()

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(db_path),
                "inputs", "clear",
                "--status", "pending",
                "--yes",
            ]
        )

        assert result.exit_code == 0
        assert "Deleted" in result.output

    def test_retry_failed(
            self,
            cli_runner: CliRunner,
            temp_dir: Path,
    ) -> None:
        """Test retrying failed inputs."""
        db_path = temp_dir / "test.db"

        initialize_database(db_path)
        inp = PipelineInput.add_input(temp_dir / "dummy.txt")
        inp.mark_failed("test error")
        close_database()

        result = cli_runner.invoke(
            main,
            [
                "--db-path", str(db_path),
                "inputs", "retry-failed",
            ]
        )

        assert result.exit_code == 0
        assert "Reset 1 failed" in result.output

        # Verify status changed
        initialize_database(db_path)
        try:
            pending = PipelineInput.get_pending()
            assert len(pending) == 1
        finally:
            close_database()
