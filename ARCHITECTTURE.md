## Project Structure
```plaintext
prefect_pipeline/
├── pyproject.toml                        # Project configuration and dependencies
├── README.md                             # Project overview, features, installation, and usage instructions
├── src/                                  # Source code for the pipeline
│   └── prefect_pipeline/                  # Package directory
│       ├── __init__.py                   # Package initialization
│       ├── cli.py                        # Command-line interface for executing the pipeline
│       ├── config.py                     # Configuration management (global settings)
│       ├── database/                     # Database models and utilities
│       │   ├── __init__.py               # Database package initialization
│       │   └── models.py                 # Peewee ORM models representing inputs, batches, and records
│       ├── jobs/                         # Job definitions
│       │   ├── __init__.py               # Job package initialization
│       │   ├── base.py                   # Abstract base class for all jobs (BaseJob)
│       │   ├── job_a.py                  # JobA implementation (file processing)
│       │   ├── job_b.py                  # JobB implementation (write to database)
│       │   └── job_c.py                  # JobC implementation (finalize records)
│       ├── pipeline/                     # Pipeline flows and execution logic
│       │   ├── __init__.py               # Pipeline package initialization
│       │   ├── wrappers.py               # Prefect task wrappers for job classes
│       │   ├── flows.py                  # Pipeline flows and definitions, including dynamic job sequences
│       │   └── parallel.py               # Utilities for parallel execution of jobs
│       └── validators/                   # Input validation utilities
│           ├── __init__.py               # Validators package initialization
│           └── input_validators.py       # Functions for validating input types and structures
└── tests/                                # Test suite for ensuring code quality
    ├── __init__.py                       # Test package initialization
    ├── conftest.py                       # Pytest fixtures for setting up test environment
    ├── test_database.py                   # Tests for database models and operations
    ├── test_jobs.py                       # Tests for job implementations
    ├── test_pipeline.py                   # Tests for pipeline flows and execution logic
    └── test_validators.py                 # Tests for input validation functions


```

## Architecture overview
```text
┌──────────────────────────────────────────────────────────────────┐
│                         CLI (cli.py)                             │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                    PipelineRunner (pipeline.py)                  │
│  - Stores configuration (output_dir, db_path, log_level)         │
│  - Provides convenient run methods                               │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                    PipelineDefinition (flows.py)                 │
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
│                 JobA / JobB / JobC (jobs/*.py)                   │
│  - validate_input(): Input validation logic                      │
│  - single_job(): Core business logic (no decorators!)            │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────┐
│               Database Models (models.py)                     │
│  - PipelineInput: Queue for input files                       │
│  - ProcessingBatch: Groups of processed records               │
│  - ProcessedRecord: Individual records                        │
└───────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Parallel Execution (parallel.py)                      │
│  - run_parallel_from_files: Process multiple input files in parallel     |
│  - run_parallel_from_database: Process pending inputs from the database  |
│  - Handle job progress and status updates                                │
└──────────────────────────────────────────────────────────────────────────┘

```