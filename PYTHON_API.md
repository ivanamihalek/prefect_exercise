
## Python API Demo

Basic usage:

```python
from pathlib import Path
from src import PipelineRunner

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

Run in parallel:

```python
from pathlib import Path
from src import (
    run_parallel_from_files,
    run_parallel_from_database,
)

# Parallel execution from file list
result = run_parallel_from_files(
    file_paths=[Path("a.txt"), Path("b.txt"), Path("c.txt")],
    output_dir=Path("./output"),
    db_path=Path("./pipeline.db"),
    max_workers=4,
    progress_callback=lambda c, t, r: print(f"{c}/{t}: {r['file_path']}")
)

print(f"Processed {result.total}, {result.succeeded} succeeded")

# Parallel execution from database queue
result = run_parallel_from_database(
    db_path=Path("./pipeline.db"),
    output_dir=Path("./output"),
    max_workers=4,
    limit=100,
)
```