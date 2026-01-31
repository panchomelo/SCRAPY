#!/usr/bin/env python
"""
Scrapy Engine CLI.

Command-line interface for debugging and manual extraction.
Use the API for production integrations.

Usage:
    scrapy scrape https://example.com --source web
    scrapy status <job_id>
    scrapy jobs --status pending
    scrapy stats
    scrapy serve --port 8000
"""

import asyncio
import json
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from src.core.config import get_settings
from src.database.models import ExtractionSource, JobStatus
from src.utils.logging import configure_logging

__version__ = "1.0.0"

console = Console()


def version_callback(value: bool) -> None:
    """Show version and exit."""
    if value:
        console.print(f"[blue]Scrapy Engine[/blue] v{__version__}")
        console.print("[dim]Modular scraping for RAG pipelines[/dim]")
        raise typer.Exit()


# Initialize Typer app with modern configuration
app = typer.Typer(
    name="scrapy",
    help="**Scrapy Engine** - Modular scraping for RAG pipelines 🚀",
    add_completion=False,
    rich_markup_mode="markdown",  # Enable markdown in help text
    no_args_is_help=True,  # Show help when no command is provided
)


# Global callback for version option (modern Typer pattern)
@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=version_callback,
            is_eager=True,  # Process before other options
        ),
    ] = False,
) -> None:
    """
    **Scrapy Engine** - Modular scraping for RAG pipelines.

    Use commands below to extract content from various sources.
    For production use, start the API server with `scrapy serve`.
    """
    pass


def run_async(coro):
    """Helper to run async functions in sync context."""
    return asyncio.run(coro)


@app.command()
def scrape(
    url: Annotated[
        str,
        typer.Argument(
            help="URL to extract content from.",
            show_default=False,
        ),
    ],
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Extraction source: `web`, `pdf`, `excel`, `social`.",
            show_default=True,
            rich_help_panel="Extraction Options",
        ),
    ] = "web",
    output: Annotated[
        str | None,
        typer.Option(
            "--output",
            "-o",
            help="Output file path (JSON). If not specified, prints to stdout.",
            rich_help_panel="Output Options",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Enable verbose output with full JSON response.",
            rich_help_panel="Output Options",
        ),
    ] = False,
) -> None:
    """
    Extract content from a URL.

    **Examples:**

    ```bash
    scrapy scrape https://example.com
    scrapy scrape https://example.com/doc.pdf --source pdf
    scrapy scrape https://example.com -o result.json
    ```
    """
    configure_logging()

    async def _scrape():
        from src.core.engine import ScrapyEngine

        try:
            source_enum = ExtractionSource(source.lower())
        except ValueError:
            console.print(f"[red]Invalid source: {source}[/red]")
            console.print(f"Valid sources: {', '.join(s.value for s in ExtractionSource)}")
            raise typer.Exit(1)

        console.print(f"[blue]Extracting from:[/blue] {url}")
        console.print(f"[blue]Source type:[/blue] {source_enum.value}")

        async with ScrapyEngine() as engine:
            try:
                result = await engine.extract(
                    source=source_enum,
                    target=url,
                )

                result_dict = result.model_dump(mode="json")

                if output:
                    with open(output, "w", encoding="utf-8") as f:
                        json.dump(result_dict, f, indent=2, ensure_ascii=False)
                    console.print(f"[green]✓ Result saved to:[/green] {output}")
                else:
                    if verbose:
                        console.print_json(data=result_dict)
                    else:
                        console.print(f"\n[green]Title:[/green] {result.metadata.title or 'N/A'}")
                        console.print(f"[green]Content length:[/green] {len(result.content)} chars")
                        console.print("\n[dim]Content preview:[/dim]")
                        console.print(
                            result.content[:500] + "..."
                            if len(result.content) > 500
                            else result.content
                        )

                console.print("\n[green]✓ Extraction completed successfully[/green]")

            except Exception as e:
                console.print(f"[red]✗ Extraction failed:[/red] {e}")
                raise typer.Exit(1)

    run_async(_scrape())


@app.command()
def status(
    job_id: Annotated[
        str,
        typer.Argument(
            help="Job UUID to check.",
            metavar="JOB_ID",
            show_default=False,
        ),
    ],
) -> None:
    """
    Get status of an extraction job.

    **Example:**

    ```bash
    scrapy status 550e8400-e29b-41d4-a716-446655440000
    ```
    """
    configure_logging()

    async def _status():
        from src.database.connection import get_session, init_db
        from src.database.repository import JobRepository

        await init_db()

        async with get_session() as session:
            try:
                job = await JobRepository.get_by_id(session, job_id)

                table = Table(title=f"Job {job_id[:8]}...")
                table.add_column("Field", style="cyan")
                table.add_column("Value", style="white")

                table.add_row("ID", job.id)
                table.add_row("Status", _status_color(job.status))
                table.add_row("Source", job.source.value)
                table.add_row("URL", job.source_url or "N/A")
                table.add_row("Created", str(job.created_at))
                table.add_row("Completed", str(job.completed_at) if job.completed_at else "N/A")

                if job.error:
                    table.add_row("Error", f"[red]{job.error}[/red]")

                console.print(table)

            except Exception as e:
                console.print(f"[red]Job not found:[/red] {job_id}")
                console.print(f"[dim]{e}[/dim]")
                raise typer.Exit(1)

    run_async(_status())


@app.command()
def jobs(
    status_filter: Annotated[
        str | None,
        typer.Option(
            "--status",
            "-s",
            help="Filter by status: `pending`, `extracting`, `completed`, `failed`.",
            metavar="STATUS",
            rich_help_panel="Filters",
        ),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            "-l",
            help="Maximum number of jobs to show.",
            show_default=True,
            rich_help_panel="Filters",
        ),
    ] = 20,
) -> None:
    """
    List extraction jobs.

    **Examples:**

    ```bash
    scrapy jobs
    scrapy jobs --status pending
    scrapy jobs --limit 50
    ```
    """
    configure_logging()

    async def _jobs():
        from src.database.connection import get_session, init_db
        from src.database.repository import JobRepository

        await init_db()

        async with get_session() as session:
            if status_filter:
                try:
                    filter_enum = JobStatus(status_filter.lower())
                except ValueError:
                    console.print(f"[red]Invalid status: {status_filter}[/red]")
                    console.print(f"Valid statuses: {', '.join(s.value for s in JobStatus)}")
                    raise typer.Exit(1)

                jobs_list = await JobRepository.list_by_status(session, filter_enum, limit=limit)
            else:
                jobs_list = await JobRepository.list_recent(session, limit=limit)

            if not jobs_list:
                console.print("[yellow]No jobs found[/yellow]")
                return

            table = Table(title=f"Extraction Jobs ({len(jobs_list)} shown)")
            table.add_column("ID", style="cyan", max_width=12)
            table.add_column("Status", style="white")
            table.add_column("Source", style="blue")
            table.add_column("URL", style="dim", max_width=40)
            table.add_column("Created", style="dim")

            for job in jobs_list:
                table.add_row(
                    job.id[:12] + "...",
                    _status_color(job.status),
                    job.source.value,
                    (job.source_url[:37] + "...")
                    if job.source_url and len(job.source_url) > 40
                    else (job.source_url or "N/A"),
                    str(job.created_at.strftime("%Y-%m-%d %H:%M")),
                )

            console.print(table)

    run_async(_jobs())


@app.command()
def stats() -> None:
    """
    Show job statistics.

    **Example:**

    ```bash
    scrapy stats
    ```
    """
    configure_logging()

    async def _stats():
        from src.database.connection import get_session, init_db
        from src.database.repository import JobRepository

        await init_db()

        async with get_session() as session:
            stats_data = await JobRepository.get_stats(session)

            table = Table(title="Job Statistics")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="white", justify="right")

            table.add_row("Total Jobs", str(stats_data.get("total", 0)))
            table.add_row("Pending", f"[yellow]{stats_data.get('pending', 0)}[/yellow]")
            table.add_row("Processing", f"[blue]{stats_data.get('processing', 0)}[/blue]")
            table.add_row("Completed", f"[green]{stats_data.get('completed', 0)}[/green]")
            table.add_row("Failed", f"[red]{stats_data.get('failed', 0)}[/red]")
            table.add_row("Success Rate", f"{stats_data.get('success_rate', 0):.1f}%")

            console.print(table)

    run_async(_stats())


@app.command()
def serve(
    host: Annotated[
        str,
        typer.Option(
            "--host",
            "-h",
            help="Host to bind to.",
            show_default=True,
            rich_help_panel="Server Options",
        ),
    ] = "0.0.0.0",
    port: Annotated[
        int,
        typer.Option(
            "--port",
            "-p",
            help="Port to bind to.",
            show_default=True,
            rich_help_panel="Server Options",
        ),
    ] = 8000,
    reload: Annotated[
        bool,
        typer.Option(
            "--reload",
            "-r",
            help="Enable auto-reload for development.",
            rich_help_panel="Development Options",
        ),
    ] = False,
    workers: Annotated[
        int,
        typer.Option(
            "--workers",
            "-w",
            help="Number of worker processes (ignored with `--reload`).",
            show_default=True,
            rich_help_panel="Server Options",
        ),
    ] = 1,
) -> None:
    """
    Start the API server.

    **Examples:**

    ```bash
    scrapy serve
    scrapy serve --port 8080
    scrapy serve --reload  # Development mode
    ```
    """
    import uvicorn

    settings = get_settings()

    console.print("[blue]Starting Scrapy Engine API[/blue]")
    console.print(f"[dim]Host:[/dim] {host}")
    console.print(f"[dim]Port:[/dim] {port}")
    console.print(f"[dim]Debug:[/dim] {settings.debug}")

    if settings.debug:
        console.print(f"[dim]Docs:[/dim] http://{host}:{port}/docs")

    # Use uvicorn.Config + Server for more control (recommended by docs)
    config = uvicorn.Config(
        app="api.app:app",  # Import string for multiprocessing/reload support
        host=host,
        port=port,
        reload=reload,
        workers=workers if not reload else 1,
        log_level="debug" if settings.debug else "info",
        # Modern best practices from uvicorn docs
        lifespan="on",  # Explicitly enable lifespan events (FastAPI uses them)
        access_log=settings.debug,  # Disable access logs in production for performance
        proxy_headers=True,  # Trust X-Forwarded-* headers (for reverse proxy)
        forwarded_allow_ips="*" if settings.debug else "127.0.0.1",  # Restrict in prod
        server_header=False,  # Hide "server: uvicorn" header (security)
        date_header=True,  # Include Date header (HTTP compliance)
    )
    server = uvicorn.Server(config)
    server.run()


def _status_color(status: JobStatus) -> str:
    """Get colored status string."""
    colors = {
        JobStatus.PENDING: "[yellow]pending[/yellow]",
        JobStatus.EXTRACTING: "[blue]extracting[/blue]",
        JobStatus.COMPLETED: "[green]completed[/green]",
        JobStatus.FAILED: "[red]failed[/red]",
    }
    return colors.get(status, str(status.value))


if __name__ == "__main__":
    app()
