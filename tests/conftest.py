"""Shared pytest fixtures for all tests."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Generator, Any

import pytest
from peewee import SqliteDatabase

from src.config import PipelineConfig, set_config, get_config
from src.database import initialize_database, close_database, db
from src.database.models import PipelineInput, ProcessingBatch, ProcessedRecord


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_config(temp_dir: Path) -> Generator[PipelineConfig, None, None]:
    """Create a test configuration with temporary paths."""
    db_path = temp_dir / "test.db"
    output_dir = temp_dir / "output"
    
    config = PipelineConfig(
        database_path=db_path,
        output_directory=output_dir,
        log_level="OFF"  # Suppress logs during tests
    )
    
    # Set as global config
    original_config = None
    try:
        original_config = get_config()
    except Exception:
        pass
    
    set_config(config)
    
    yield config
    
    # Restore original config if it existed
    if original_config:
        set_config(original_config)


@pytest.fixture
def test_db(test_config: PipelineConfig) -> Generator[SqliteDatabase, None, None]:
    """Initialize a test database with all tables."""
    initialize_database(test_config.database_path)
    
    # Also create PipelineInput table
    db.create_tables([PipelineInput], safe=True)
    
    yield db
    
    # Cleanup
    close_database()


@pytest.fixture
def sample_input_file(temp_dir: Path) -> Path:
    """Create a sample input text file for testing."""
    file_path = temp_dir / "sample_input.txt"
    content = """Line 1
Line 2
Line 3
"""
    file_path.write_text(content, encoding='utf-8')
    return file_path


@pytest.fixture
def sample_json_file(temp_dir: Path) -> Path:
    """Create a sample JSON file for testing."""
    file_path = temp_dir / "sample_data.json"
    data = {
        "key1": "value1",
        "key2": "value2",
        "items": [1, 2, 3]
    }
    file_path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    return file_path


@pytest.fixture
def sample_csv_file(temp_dir: Path) -> Path:
    """Create a sample CSV file for testing."""
    file_path = temp_dir / "sample_data.csv"
    content = """header1,header2,header3
value1,value2,value3
value4,value5,value6
"""
    file_path.write_text(content, encoding='utf-8')
    return file_path


@pytest.fixture
def processed_data_file(temp_dir: Path) -> Path:
    """Create a processed data file (Job A output format)."""
    file_path = temp_dir / "processed_output.json"
    data = {
        "source_file": "/path/to/source.txt",
        "records": [
            {"name": "record_0", "value": "Line 1", "line_number": 1},
            {"name": "record_1", "value": "Line 2", "line_number": 2},
            {"name": "record_2", "value": "Line 3", "line_number": 3},
        ],
        "processed_at": "2024-01-01T12:00:00",
        "total_lines": 3,
        "non_empty_lines": 3
    }
    file_path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    return file_path


@pytest.fixture
def cleanup_database() -> Generator[None, None, None]:
    """Cleanup database after tests."""
    yield
    
    # Clean all records
    try:
        ProcessedRecord.delete().execute()
        ProcessingBatch.delete().execute()
        PipelineInput.delete().execute()
    except Exception:
        pass


@pytest.fixture
def sample_batch(test_db: SqliteDatabase) -> ProcessingBatch:
    """Create a sample processing batch in the database."""
    batch = ProcessingBatch.create(
        source_file="test_file.txt",
        status="pending",
        record_count=3
    )
    
    # Add some records
    for i in range(3):
        ProcessedRecord.create(
            batch=batch,
            name=f"record_{i}",
            value=f"Line {i+1}",
            is_finalized=False
        )
    
    return batch


@pytest.fixture
def multiple_input_files(temp_dir: Path) -> list[Path]:
    """Create multiple input files for parallel processing tests."""
    files = []
    for i in range(5):
        file_path = temp_dir / f"input_{i}.txt"
        content = f"Content for file {i}\nLine 2\nLine 3\n"
        file_path.write_text(content, encoding='utf-8')
        files.append(file_path)
    return files
