"""Tests for input validators."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from prefect_pipeline.validators import (
    ValidationError,
    validate_file_path,
    validate_processed_data,
    validate_database_record_id,
)


class TestValidateFilePath:
    """Tests for validate_file_path function."""

    def test_valid_path_exists(self, sample_input_file: Path) -> None:
        """Test validation of existing file."""
        result = validate_file_path(sample_input_file)
        assert result == sample_input_file
        assert isinstance(result, Path)

    def test_valid_string_path(self, sample_input_file: Path) -> None:
        """Test validation with string path."""
        result = validate_file_path(str(sample_input_file))
        assert result == sample_input_file

    def test_none_raises_error(self) -> None:
        """Test that None raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_file_path(None)
        assert "cannot be None" in str(exc_info.value)

    def test_nonexistent_file_raises_error(self, temp_dir: Path) -> None:
        """Test that nonexistent file raises ValidationError."""
        fake_path = temp_dir / "nonexistent.txt"
        with pytest.raises(ValidationError) as exc_info:
            validate_file_path(fake_path)
        assert "does not exist" in str(exc_info.value)

    def test_invalid_extension(self, sample_input_file: Path) -> None:
        """Test that invalid extension raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_file_path(
                sample_input_file,
                allowed_extensions=('.json',)
            )
        assert "Invalid file extension" in str(exc_info.value)

    def test_valid_extension(self, sample_json_file: Path) -> None:
        """Test validation with valid extension."""
        result = validate_file_path(
            sample_json_file,
            allowed_extensions=('.json', '.txt')
        )
        assert result == sample_json_file

    def test_invalid_type_raises_error(self) -> None:
        """Test that invalid type raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_file_path(123)
        assert "must be str or Path" in str(exc_info.value)

    def test_directory_path_raises_error(self, temp_dir: Path) -> None:
        """Test that directory path raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_file_path(temp_dir)
        assert "not a file" in str(exc_info.value)


class TestValidateProcessedData:
    """Tests for validate_processed_data function."""

    def test_valid_dict(self, sample_processed_data: dict) -> None:
        """Test validation of valid dictionary."""
        result = validate_processed_data(sample_processed_data)
        assert result == sample_processed_data

    def test_valid_file_path(self, sample_processed_file: Path) -> None:
        """Test validation with file path input."""
        result = validate_processed_data(sample_processed_file)
        assert "records" in result

    def test_none_raises_error(self) -> None:
        """Test that None raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_processed_data(None)
        assert "cannot be None" in str(exc_info.value)

    def test_missing_required_fields(self) -> None:
        """Test that missing fields raise ValidationError."""
        incomplete_data = {"source_file": "test.txt"}
        with pytest.raises(ValidationError) as exc_info:
            validate_processed_data(incomplete_data)
        assert "Missing required fields" in str(exc_info.value)

    def test_invalid_records_type(self) -> None:
        """Test that invalid records type raises ValidationError."""
        invalid_data = {
            "source_file": "test.txt",
            "records": "not a list",
            "processed_at": "2024-01-01",
        }
        with pytest.raises(ValidationError) as exc_info:
            validate_processed_data(invalid_data)
        assert "'records' must be a list" in str(exc_info.value)


class TestValidateDatabaseRecordId:
    """Tests for validate_database_record_id function."""

    def test_valid_int(self) -> None:
        """Test validation of valid integer."""
        assert validate_database_record_id(1) == 1
        assert validate_database_record_id(100) == 100

    def test_valid_string_int(self) -> None:
        """Test validation of string that can be converted to int."""
        assert validate_database_record_id("42") == 42

    def test_none_raises_error(self) -> None:
        """Test that None raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_database_record_id(None)
        assert "cannot be None" in str(exc_info.value)

    def test_negative_raises_error(self) -> None:
        """Test that negative ID raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_database_record_id(-1)
        assert "must be positive" in str(exc_info.value)

    def test_zero_raises_error(self) -> None:
        """Test that zero raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_database_record_id(0)
        assert "must be positive" in str(exc_info.value)

    def test_invalid_string_raises_error(self) -> None:
        """Test that non-numeric string raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_database_record_id("abc")
        assert "must be an integer" in str(exc_info.value)