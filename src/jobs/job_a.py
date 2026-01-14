"""Job A: File processing job."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import get_config
from src.jobs.base import BaseJob
from src.validators import validate_file_path


class JobA(BaseJob[Path, Path]):
    """
    Job A processes an input file and produces an output file.

    Takes a file path as input, processes it, and writes output
    that can be consumed by Job B.
    """

    ALLOWED_EXTENSIONS: tuple[str, ...] = ('.txt', '.csv', '.json')

    def __init__(self, output_dir: Path | None = None) -> None:
        super().__init__()
        self._output_dir = output_dir or get_config().output_directory

    def validate_input(self, input_data: Any) -> Path:
        """
        Validate input file path.

        Args:
            input_data: Raw file path input

        Returns:
            Validated Path object

        Raises:
            ValidationError: If input is invalid
        """
        return validate_file_path(
            input_data,
            must_exist=True,
            allowed_extensions=self.ALLOWED_EXTENSIONS
        )

    def single_job(self, input_data: Path) -> Path:
        """
        Process the input file and produce output.

        Args:
            input_data: Validated input file path

        Returns:
            Path to the output file
        """
        # Read input file
        content = self._read_input_file(input_data)

        # Process content
        processed_data = self._process_content(content, input_data)

        # Write output
        output_path = self._write_output(processed_data, input_data)

        return output_path

    def _read_input_file(self, file_path: Path) -> str:
        """Read content from input file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    def _process_content(self, content: str, source_path: Path) -> dict[str, Any]:
        """
        Process raw content into structured data.

        This is a toy implementation - in real world, this would
        do actual data processing.
        """
        lines = content.strip().split('\n')
        records = []

        for i, line in enumerate(lines):
            if line.strip():
                records.append({
                    "name": f"record_{i}",
                    "value": line.strip(),
                    "line_number": i + 1
                })

        return {
            "source_file": str(source_path),
            "records": records,
            "processed_at": datetime.utcnow().isoformat(),
            "total_lines": len(lines),
            "non_empty_lines": len(records)
        }

    def _write_output(self, data: dict[str, Any], source_path: Path) -> Path:
        """Write processed data to output file."""
        self._output_dir.mkdir(parents=True, exist_ok=True)

        output_filename = f"{source_path.stem}_processed.json"
        output_path = self._output_dir / output_filename

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        return output_path
