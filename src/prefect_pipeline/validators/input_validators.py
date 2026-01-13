"""Input validation utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

class ValidationError(Exception):
    """Raised when input validation fails."""

    def __init__(self, message: str, field: str | None = None) -> None:
        self.field = field
        self.message = message
        super().__init__(f"Validation error{f' for {field}' if field else ''}: {message}")


def validate_file_path(
        file_path: Any,
        must_exist: bool = True,
        allowed_extensions: tuple[str, ...] | None = None,
) -> Path:
    """
    Validate a file path input.

    Args:
        file_path: The path to validate (can be str or Path)
        must_exist: Whether the file must already exist
        allowed_extensions: Tuple of allowed file extensions (e.g., ('.json', '.txt'))

    Returns:
        Validated Path object

    Raises:
        ValidationError: If validation fails
    """
    if file_path is None:
        raise ValidationError("File path cannot be None", "file_path")

    if not isinstance(file_path, (str, Path)):
        raise ValidationError(
            f"File path must be str or Path, got {type(file_path).__name__}",
            "file_path"
        )

    path = Path(file_path)

    if must_exist and not path.exists():
        raise ValidationError(f"File does not exist: {path}", "file_path")

    if must_exist and not path.is_file():
        raise ValidationError(f"Path is not a file: {path}", "file_path")

    if allowed_extensions and path.suffix.lower() not in allowed_extensions:
        raise ValidationError(
            f"Invalid file extension '{path.suffix}'. "
            f"Allowed: {allowed_extensions}",
            "file_path"
        )

    return path


def validate_processed_data(data: Any) -> dict[str, Any]:
    """
    Validate processed data from Job A.

    Args:
        data: The data to validate

    Returns:
        Validated data dictionary

    Raises:
        ValidationError: If validation fails
    """
    if data is None:
        raise ValidationError("Processed data cannot be None", "data")

    if isinstance(data, (str, Path)):
        # If it's a file path, read and parse it
        path = validate_file_path(data, must_exist=True, allowed_extensions=('.json',))
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON in file: {e}", "data") from e

    if not isinstance(data, dict):
        raise ValidationError(
            f"Data must be a dictionary, got {type(data).__name__}",
            "data"
        )

    required_fields = {'source_file', 'records', 'processed_at'}
    missing_fields = required_fields - set(data.keys())
    if missing_fields:
        raise ValidationError(
            f"Missing required fields: {missing_fields}",
            "data"
        )

    if not isinstance(data['records'], list):
        raise ValidationError("'records' must be a list", "data.records")

    return data


def validate_database_record_id(record_id: Any) -> int:
    """
    Validate a database record ID.

    Args:
        record_id: The record ID to validate

    Returns:
        Validated record ID as integer

    Raises:
        ValidationError: If validation fails
    """
    if record_id is None:
        raise ValidationError("Record ID cannot be None", "record_id")

    try:
        record_id = int(record_id)
    except (TypeError, ValueError) as e:
        raise ValidationError(
            f"Record ID must be an integer, got {type(record_id).__name__}",
            "record_id"
        ) from e

    if record_id < 1:
        raise ValidationError(
            f"Record ID must be positive, got {record_id}",
            "record_id"
        )

    return record_id