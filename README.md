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

---

## CLI Reference
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

Pipeline run:
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

Examples:
```bash
# Single file (as before)
pipeline run-all input.txt

# Multiple files from command line (parallel)
pipeline run-all file1.txt file2.txt file3.txt --max-workers 4

# Using glob pattern
pipeline run-all data/*.txt --max-workers 2

# From database
pipeline run-all --from-db --max-workers 4

# From database with limit
pipeline run-all --from-db --limit 10 --max-workers 2

# Add inputs to queue
pipeline inputs add file1.txt file2.txt --priority 10

# List queued inputs
pipeline inputs list --status pending

# Clear completed inputs
pipeline inputs clear --status completed --yes

# Retry failed inputs
pipeline inputs retry-failed
```

-