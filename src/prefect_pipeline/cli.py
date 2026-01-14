"""Command-line interface for the pipeline."""

from __future__ import annotations

import logging
import multiprocessing
import sys
from pathlib import Path

import click

from prefect_pipeline.config import PipelineConfig, set_config
from prefect_pipeline.database import (
    initialize_database,
    close_database,
    PipelineInput,
    InputStatus,
)
from prefect_pipeline.pipeline import (
    PipelineRunner,
    get_max_workers,
    run_parallel_from_files,
    run_parallel_from_database,
)


def configure_logging(log_level: str) -> None:
    """Configure logging based on the specified level."""
    if log_level == "OFF":
        logging.disable(logging.CRITICAL)
        logging.getLogger("prefect").setLevel(logging.CRITICAL + 10)
    else:
        logging.disable(logging.NOTSET)
        numeric_level = getattr(logging, log_level)
        logging.basicConfig(
            level=numeric_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        logging.getLogger("prefect").setLevel(numeric_level)


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "OFF"]),
    default="INFO",
    help="Set logging level. Use 'OFF' to disable logging entirely.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("./data/output"),
    help="Output directory for processed files.",
)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path),
    default=Path("./data/pipeline.db"),
    help="Path to SQLite database.",
)
@click.pass_context
def main(
        ctx: click.Context,
        log_level: str,
        output_dir: Path,
        db_path: Path,
) -> None:
    """
    Prefect Pipeline CLI.

    Run data processing pipelines with configurable logging and storage.
    """
    configure_logging(log_level)

    ctx.ensure_object(dict)
    ctx.obj["log_level"] = log_level
    ctx.obj["output_dir"] = output_dir
    ctx.obj["db_path"] = db_path
    ctx.obj["runner"] = PipelineRunner(
        output_dir=output_dir,
        db_path=db_path,
        log_level=log_level,
    )


@main.command()
@click.pass_context
def list_jobs(ctx: click.Context) -> None:
    """List all available jobs in the pipeline."""
    runner: PipelineRunner = ctx.obj["runner"]

    click.echo("Available jobs:")
    for i, name in enumerate(runner.available_jobs, 1):
        click.echo(f"  {i}. {name}")


@main.command()
@click.argument("input_data", type=str)
@click.option(
    "--start-from",
    type=str,
    default=None,
    help="Job name to start from.",
)
@click.option(
    "--stop-after",
    type=str,
    default=None,
    help="Job name to stop after.",
)
@click.pass_context
def run(
        ctx: click.Context,
        input_data: str,
        start_from: str | None,
        stop_after: str | None,
) -> None:
    """
    Run the pipeline with flexible start/stop points.

    INPUT_DATA: Input for the starting job (file path, batch ID, etc.)
    """
    runner: PipelineRunner = ctx.obj["runner"]

    actual_start = start_from or runner.available_jobs[0]

    if actual_start == "C":
        try:
            parsed_input: Path | int = int(input_data)
        except ValueError:
            click.echo(
                click.style(
                    f"Error: Job C requires a batch ID (integer), got: {input_data}",
                    fg="red"
                )
            )
            sys.exit(1)
    else:
        parsed_input = Path(input_data)
        if not parsed_input.exists():
            click.echo(
                click.style(f"Error: File not found: {parsed_input}", fg="red")
            )
            sys.exit(1)

    click.echo(f"Running pipeline...")
    if start_from:
        click.echo(f"  Starting from: {start_from}")
    if stop_after:
        click.echo(f"  Stopping after: {stop_after}")

    result = runner.run(
        input_data=parsed_input,
        start_from=start_from,
        stop_after=stop_after,
    )

    if result.success:
        click.echo(click.style("Pipeline completed successfully!", fg="green"))
        click.echo(f"Result: {result.output}")
    else:
        click.echo(click.style(f"Pipeline failed: {result.error}", fg="red"))
        sys.exit(1)


@main.command("run-all")
@click.argument("input_files", type=click.Path(exists=True, path_type=Path), nargs=-1)
@click.option(
    "--from-db",
    is_flag=True,
    default=False,
    help="Read inputs from database instead of command line.",
)
@click.option(
    "--max-workers",
    type=int,
    default=None,
    help=f"Maximum parallel workers (default: CPU count = {multiprocessing.cpu_count()}).",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of inputs to process (only with --from-db).",
)
@click.pass_context
def run_all(
        ctx: click.Context,
        input_files: tuple[Path, ...],
        from_db: bool,
        max_workers: int | None,
        limit: int | None,
) -> None:
    """
    Run the complete pipeline for one or more inputs.

    Inputs can come from:

    \b
    a) Command line arguments (one or more file paths)
       pipeline run-all file1.txt file2.txt file3.txt

    \b
    b) The database (pending inputs)
       pipeline run-all --from-db

    When multiple inputs are provided, they are processed in parallel,
    with the number of workers limited by --max-workers (default: CPU count).

    \b
    Examples:
        # Single file
        pipeline run-all input.txt

        # Multiple files in parallel
        pipeline run-all *.txt --max-workers 4

        # From database
        pipeline run-all --from-db --max-workers 2

        # From database with limit
        pipeline run-all --from-db --limit 10
    """
    output_dir: Path = ctx.obj["output_dir"]
    db_path: Path = ctx.obj["db_path"]
    log_level: str = ctx.obj["log_level"]

    # Validate arguments
    if from_db and input_files:
        click.echo(
            click.style(
                "Error: Cannot specify both --from-db and input files.",
                fg="red"
            )
        )
        sys.exit(1)

    if not from_db and not input_files:
        click.echo(
            click.style(
                "Error: Must specify either input files or --from-db.",
                fg="red"
            )
        )
        sys.exit(1)

    if limit is not None and not from_db:
        click.echo(
            click.style(
                "Warning: --limit is only used with --from-db, ignoring.",
                fg="yellow"
            )
        )

    # Determine actual worker count
    actual_workers = get_max_workers(max_workers)

    # Progress callback for CLI output
    def progress_callback(completed: int, total: int, result: dict) -> None:
        status = click.style("✓", fg="green") if result["success"] else click.style("✗", fg="red")
        file_name = Path(result["file_path"]).name
        click.echo(f"  [{completed}/{total}] {status} {file_name}")
        if not result["success"] and result.get("error"):
            click.echo(f"           Error: {result['error']}")

    # Execute based on input source
    if from_db:
        click.echo(f"Processing pending inputs from database...")
        click.echo(f"  Database: {db_path}")
        click.echo(f"  Max workers: {actual_workers}")
        if limit:
            click.echo(f"  Limit: {limit}")
        click.echo()

        result = run_parallel_from_database(
            db_path=db_path,
            output_dir=output_dir,
            log_level=log_level,
            max_workers=max_workers,
            limit=limit,
            progress_callback=progress_callback,
        )
    else:
        file_list = list(input_files)
        click.echo(f"Processing {len(file_list)} file(s)...")
        click.echo(f"  Max workers: {actual_workers}")
        click.echo()

        result = run_parallel_from_files(
            file_paths=file_list,
            output_dir=output_dir,
            db_path=db_path,
            log_level=log_level,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )

    # Print summary
    click.echo()
    click.echo("=" * 50)
    click.echo("Execution Summary")
    click.echo("=" * 50)
    click.echo(f"  Total:     {result.total}")
    click.echo(f"  Succeeded: {click.style(str(result.succeeded), fg='green')}")
    click.echo(f"  Failed:    {click.style(str(result.failed), fg='red' if result.failed else 'green')}")
    click.echo(f"  Success rate: {result.success_rate:.1f}%")

    if result.failed > 0:
        sys.exit(1)


# =============================================================================
# Input Queue Management Commands
# =============================================================================

@main.group()
def inputs() -> None:
    """Manage pipeline input queue."""
    pass


@inputs.command("add")
@click.argument("file_paths", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True)
@click.option(
    "--priority",
    type=int,
    default=0,
    help="Priority level (higher = processed first).",
)
@click.pass_context
def inputs_add(
        ctx: click.Context,
        file_paths: tuple[Path, ...],
        priority: int,
) -> None:
    """
    Add file(s) to the input queue.

    \b
    Examples:
        pipeline inputs add file1.txt
        pipeline inputs add *.txt --priority 10
    """
    db_path: Path = ctx.obj["db_path"]

    initialize_database(db_path)
    try:
        for file_path in file_paths:
            PipelineInput.add_input(file_path, priority=priority)
            click.echo(f"  Added: {file_path} (priority: {priority})")

        click.echo(click.style(f"\nAdded {len(file_paths)} input(s) to queue.", fg="green"))
    finally:
        close_database()


@inputs.command("list")
@click.option(
    "--status",
    type=click.Choice(["pending", "processing", "completed", "failed", "all"]),
    default="all",
    help="Filter by status.",
)
@click.option(
    "--limit",
    type=int,
    default=50,
    help="Maximum number of inputs to show.",
)
@click.pass_context
def inputs_list(
        ctx: click.Context,
        status: str,
        limit: int,
) -> None:
    """List inputs in the queue."""
    db_path: Path = ctx.obj["db_path"]

    initialize_database(db_path)
    try:
        if status == "all":
            query = PipelineInput.select().order_by(
                PipelineInput.created_at.desc()
            ).limit(limit)
            inputs_list = list(query)
        else:
            input_status = InputStatus(status)
            inputs_list = PipelineInput.get_by_status(input_status)[:limit]

        if not inputs_list:
            click.echo("No inputs found.")
            return

        click.echo(f"{'ID':<6} {'Status':<12} {'Priority':<8} {'File Path'}")
        click.echo("-" * 70)

        for inp in inputs_list:
            status_color = {
                "pending": "yellow",
                "processing": "blue",
                "completed": "green",
                "failed": "red",
            }.get(inp.status, "white")

            status_str = click.style(f"{inp.status:<12}", fg=status_color)
            click.echo(f"{inp.id:<6} {status_str} {inp.priority:<8} {inp.file_path}")

        click.echo(f"\nShowing {len(inputs_list)} input(s).")

    finally:
        close_database()


@inputs.command("clear")
@click.option(
    "--status",
    type=click.Choice(["pending", "completed", "failed", "all"]),
    required=True,
    help="Status of inputs to clear.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.pass_context
def inputs_clear(
        ctx: click.Context,
        status: str,
        yes: bool,
) -> None:
    """Clear inputs from the queue."""
    db_path: Path = ctx.obj["db_path"]

    if not yes:
        if status == "all":
            msg = "This will delete ALL inputs from the queue."
        else:
            msg = f"This will delete all {status.upper()} inputs from the queue."

        if not click.confirm(f"{msg} Continue?"):
            click.echo("Aborted.")
            return

    initialize_database(db_path)
    try:
        if status == "all":
            deleted = PipelineInput.delete().execute()
        else:
            deleted = (
                PipelineInput
                .delete()
                .where(PipelineInput.status == status)
                .execute()
            )

        click.echo(click.style(f"Deleted {deleted} input(s).", fg="green"))

    finally:
        close_database()


@inputs.command("retry-failed")
@click.pass_context
def inputs_retry_failed(ctx: click.Context) -> None:
    """Reset failed inputs to pending status."""
    db_path: Path = ctx.obj["db_path"]

    initialize_database(db_path)
    try:
        updated = (
            PipelineInput
            .update(
                status=InputStatus.PENDING.value,
                error_message=None,
                started_at=None,
                completed_at=None,
            )
            .where(PipelineInput.status == InputStatus.FAILED.value)
            .execute()
        )

        click.echo(click.style(f"Reset {updated} failed input(s) to pending.", fg="green"))

    finally:
        close_database()


if __name__ == "__main__":
    main()
