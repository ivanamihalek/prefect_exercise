"""Pytest configuration and fixtures."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Generator, Any

import pytest

from prefect_pipeline.config import PipelineConfig, set_config
from prefect_pipeline.database import initialize_database, close_database, db


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_config(temp_dir: Path) -> Generator[PipelineConfig, None, None]:
    """Create test configuration."""
    config = PipelineConfig(
        database_path=temp_dir / "test.db",
        output_directory=temp_dir / "output",
        log_level="OFF",
    )
    set_config(config)
    yield config


@pytest.fixture
def test_db(test_config: PipelineConfig) -> Generator[None, None, None]:
    """Initialize test database."""
    initialize_database(test_config.database_path)
    yield
    close_database()


@pytest.fixture
def sample_input_file(temp_dir: Path) -> Path:
    """Create a sample input file."""
    input_file = temp_dir / "input.txt"
    input_file.write_text("line one\nline two\nline three\n")
    return input_file


@pytest.fixture
def sample_json_file(temp_dir: Path) -> Path:
    """Create a sample JSON input file."""
    input_file = temp_dir / "input.json"
    data = {"key": "value", "items": [1, 2, 3]}
    input_file.write_text(json.dumps(data))
    return input_file


@pytest.fixture
def sample_processed_data(temp_dir: Path) -> dict[str, Any]:
    """Create sample processed data matching Job A output format."""
    return {
        "source_file": str(temp_dir / "input.txt"),
        "records": [
            {"name": "record_0", "value": "line one", "line_number": 1},
            {"name": "record_1", "value": "line two", "line_number": 2},
        ],
        "processed_at": "2024-01-01T00:00:00",
        "total_lines": 2,
        "non_empty_lines": 2,
    }


@pytest.fixture
def sample_processed_file(
    temp_dir: Path,
    sample_processed_data: dict[str, Any]
) -> Path:
    """Create a sample processed JSON file."""
    output_file = temp_dir / "processed.json"
    output_file.write_text(json.dumps(sample_processed_data))
    return output_file
