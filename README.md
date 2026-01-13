# Prefect Class-Based Pipeline

A production-ready data processing pipeline built with Prefect, Peewee and Python 3.12+.  
Jobs are defined as classes (A, B, C) with `single_job()` methods - 
no decorators in business logic. Prefect decorators are applied via wrappers.

## Features
### Class-Based Jobs
- **JobA**: Reads an input file, splits into records, writes JSON
- **JobB**: Reads JSON or dict, writes to SQLite via Peewee ORM
- **JobC**: Finalizes database records by batch ID
- All jobs inherit from `BaseJob` with built-in validation and error handling

### Scalable Pipeline Architecture
- **Single flow definition** handles all entry points
- **`PipelineDefinition`**: Define N jobs with simple `add_job()` calls
- **Flexible execution**: Start from any job, stop after any job
- **No code duplication**: Adding jobs doesn't require new flow functions

### Input Validation
- Type and value checking for file paths, JSON structures, and IDs
- Validation errors are caught and returned in `JobResult`

### Prefect Integration
- Jobs wrapped as Prefect tasks dynamically via `make_job_task()`
- Single `run_pipeline` flow orchestrates any job sequence
- Full Prefect observability and retry capabilities

### CLI & Configuration
- `pipeline` command-line tool built with Click
- Configurable logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `OFF`)
- Global `PipelineConfig` for output directory, database path, and logging

### Testing
- Comprehensive pytest suite covering validators, jobs, database, tasks, flows, and end-to-end scenarios
---

## Requirements

    Python 3.12+
    Prefect 3.0+
    Peewee 3.17+
    Click 8.1+
    Pydantic 2.0+


---
## Project Structure
```text
prefect_pipeline/
├── pyproject.toml
├── README.md
├── src/
│ └── prefect_pipeline/
│ ├── init.py
│ ├── cli.py # Command-line interface
│ ├── config.py # Configuration management
│ ├── database/
│ │ ├── init.py
│ │ └── models.py # Peewee ORM models
│ ├── jobs/
│ │ ├── init.py
│ │ ├── base.py # BaseJob abstract class
│ │ ├── job_a.py # File processing job
│ │ ├── job_b.py # Database writing job
│ │ └── job_c.py # Finalization job
│ ├── pipeline/
│ │ ├── init.py
│ │ ├── wrappers.py # Prefect task wrappers
│ │ └── flows.py # PipelineDefinition & flows
│ └── validators/
│ ├── init.py
│ └── input_validators.py # Input validation utilities
└── tests/
├── init.py
├── conftest.py # Pytest fixtures
├── test_database.py
├── test_jobs.py
├── test_pipeline.py
└── test_validators.py
```
---
## Installation

1. Clone the repository  
2. Create & activate a Python 3.12+ venv  
3. Install dependencies (in development mode):

```bash
pip install -e ".[dev]"
```
---

## Quick Demo
1. Create a Sample Input File

```bash

mkdir demo_data
cat <<EOF > demo_data/sample.txt
hello world
prefect pipeline
end to end
EOF
```
2. List Available Jobs
```bash
pipeline list-jobs
```
(pipeline was created by pip, see `[project.scripts]` section in project.toml)

outout:
```text
Available jobs:
  1. A
  2. B
  3. C
```
3. Run full pipeline via CLI

```bash
pipeline run-all demo_data/sample.txt
```

4. Run with Custom Start/Stop Points
```bash
# Run only Job A (file processing)
pipeline run demo_data/sample.txt --stop-after A

# Run Jobs A and B (skip finalization)
pipeline run demo_data/sample.txt --stop-after B

# Run from Job B (assumes you have processed JSON)
pipeline run ./data/output/sample_processed.json --start-from B

# Run only Job C (finalize existing batch)
pipeline run 1 --start-from C --stop-after C
```

5. Configure Logging
```bash
# Verbose logging
pipeline --log-level DEBUG run demo_data/sample.txt

# Suppress all logs
pipeline --log-level OFF run demo_data/sample.txt

# Custom output directory and database
pipeline --output-dir ./my_output --db-path ./my_data.db run demo_data/sample.txt
```
6. Inspect Results
```bash
# List output files
ls ./data/output/

# Query the database
sqlite3 ./data/pipeline.db "SELECT * FROM processing_batches;"
sqlite3 ./data/pipeline.db "SELECT * FROM processed_records;"
```


----
## Python API Demo

Basic usage:

```python
from pathlib import Path
from prefect_pipeline.pipeline import PipelineRunner

# Create a runner with configuration
runner = PipelineRunner(
    output_dir=Path("./output"),
    db_path=Path("./pipeline.db"),
    log_level="INFO",  # or "OFF" to disable logging
)

# Run the full pipeline
result = runner.run(Path("input.txt"))

# Check the result
if result.success:
    print(f"Success! Output: {result.output}")
else:
    print(f"Failed: {result.error}")
```

Flexible execution:
```python
# Run full pipeline
result = runner.run(Path("input.txt"))

# Run from a specific job to the end
result = runner.run(Path("processed.json"), start_from="B")

# Run up to a specific job
result = runner.run(Path("input.txt"), stop_after="A")

# Run a single job
result = runner.run(processed_data, start_from="B", stop_after="B")

# Convenience methods
result = runner.run_full(Path("input.txt"))           # Full pipeline
result = runner.run_single("A", Path("input.txt"))    # Single job
result = runner.run_from("B", processed_data)         # From B to end
result = runner.run_until("B", Path("input.txt"))     # Start to B
```
List available jobs:
```python
print(runner.available_jobs)
# ['A', 'B', 'C']
```
---


CLI Reference
```text
Usage: pipeline [OPTIONS] COMMAND [ARGS]...

Options:
  --log-level [DEBUG|INFO|WARNING|ERROR|CRITICAL|OFF]
                                  Set logging level. Use 'OFF' to disable.
  --output-dir PATH               Output directory for processed files.
  --db-path PATH                  Path to SQLite database.
  --help                          Show this message and exit.

Commands:
  list-jobs  List all available jobs in the pipeline.
  run        Run the pipeline with flexible start/stop points.
  run-all    Run the complete pipeline (A -> B -> C).
```
pipeline run
```text
Usage: pipeline run [OPTIONS] INPUT_DATA

  Run the pipeline with flexible start/stop points.

Arguments:
  INPUT_DATA  Input for the starting job (file path, batch ID, etc.)

Options:
  --start-from TEXT  Job name to start from.
  --stop-after TEXT  Job name to stop after.
  --help             Show this message and exit.

```
---
Architecture Overview
```text
┌──────────────────────────────────────────────────────────────────┐
│                         CLI (cli.py)                             │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                    PipelineRunner (flows.py)                     │
│  - Stores configuration (output_dir, db_path, log_level)        │
│  - Provides convenient run methods                               │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                 PipelineDefinition (flows.py)                    │
│  - Holds ordered list of JobSpec                                 │
│  - Provides job range queries (start_from, stop_after)           │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│               run_pipeline (Prefect @flow)                       │
│  - Single flow handles all entry points                          │
│  - Iterates through jobs in range                                │
│  - Chains outputs to inputs                                      │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│              make_job_task (Prefect @task factory)               │
│  - Dynamically creates Prefect tasks from JobSpec                │
│  - Handles logging and error wrapping                            │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                    BaseJob.execute()                             │
│  - Calls validate_input() then single_job()                      │
│  - Returns JobResult with success/failure info                   │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                JobA / JobB / JobC (jobs/*.py)                    │
│  - validate_input(): Input validation logic                      │
│  - single_job(): Core business logic (no decorators!)            │
└──────────────────────────────────────────────────────────────────┘

```