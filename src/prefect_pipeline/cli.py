"""Command-line interface for the pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

from prefect_pipeline.config import PipelineConfig, set_config
from prefect_pipeline.pipeline import PipelineRunner


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

    Examples:

        \b
        # Run full pipeline
        pipeline run input.txt

        \b
        # Run from job B with processed JSON
        pipeline run processed.json --start-from B

        \b
        # Run only job A
        pipeline run input.txt --stop-after A

        \b
        # Run jobs B and C
        pipeline run processed.json --start-from B --stop-after C
    """
    runner: PipelineRunner = ctx.obj["runner"]

    # Determine input type based on starting job
    actual_start = start_from or runner.available_jobs[0]

    # Convert input based on job type
    if actual_start == "C":
        # Job C expects an integer batch ID
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
        # Jobs A and B expect file paths
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


# Backward-compatible commands

@main.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.pass_context
def run_all(ctx: click.Context, input_file: Path) -> None:
    """Run the complete pipeline (A -> B -> C)."""
    runner: PipelineRunner = ctx.obj["runner"]

    click.echo(f"Running full pipeline with input: {input_file}")
    result = runner.run_full(input_file)

    if result.success:
        click.echo(click.style("Pipeline completed successfully!", fg="green"))
        click.echo(f"Result: {result.output}")
    else:
        click.echo(click.style(f"Pipeline failed: {result.error}", fg="red"))
        sys.exit(1)


if __name__ == "__main__":
    main()
