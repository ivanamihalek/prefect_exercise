## Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest tests/test_pipeline.py

# Run a specific test class
pytest tests/test_pipeline.py::TestPipelineRunner

# Run a specific test
pytest tests/test_pipeline.py::TestPipelineIntegration::test_end_to_end_pipeline

# Run with coverage report
pytest --cov=prefect_pipeline --cov-report=html

# Open coverage report (macOS)
open htmlcov/index.html
```

## Demo test
```bash
# Quick verification that everything works
pytest tests/test_pipeline.py::TestPipelineIntegration::test_end_to_end_pipeline -v
```
Expected output:
```text
tests/test_pipeline.py::TestPipelineIntegration::test_end_to_end_pipeline PASSED
```

---