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

import typer
from rich.console import Console
from rich.table import Table

from src.core.config import get_settings
from src.database.models import ExtractionSource, JobStatus
from src.utils.logging import configure_logging

# Initialize Typer app
app = typer.Typer(
    name="scrapy",
    help="Scrapy Engine - Modular scraping for RAG pipelines",
    add_completion=False,
)

console = Console()


def run_async(coro):
    """Helper to run async functions in sync context."""
    return asyncio.run(coro)


@app.command()
def scrape(
    url: str = typer.Argument(..., help="URL to extract content from"),
    source: str = typer.Option(
        "web",
        "--source",
        "-s",
        help="Extraction source: web, pdf, excel, social",
    ),
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path (JSON). If not specified, prints to stdout",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose output",
    ),
) -> None:
    """
    Extract content from a URL.

    Examples:
        scrapy scrape https://example.com
        scrapy scrape https://example.com/doc.pdf --source pdf
        scrapy scrape https://example.com -o result.json
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
    job_id: str = typer.Argument(..., help="Job UUID to check"),
) -> None:
    """
    Get status of an extraction job.

    Example:
        scrapy status 550e8400-e29b-41d4-a716-446655440000
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
    status_filter: str | None = typer.Option(
        None,
        "--status",
        "-s",
        help="Filter by status: pending, extracting, completed, failed",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        "-l",
        help="Maximum number of jobs to show",
    ),
) -> None:
    """
    List extraction jobs.

    Examples:
        scrapy jobs
        scrapy jobs --status pending
        scrapy jobs --limit 50
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

    Example:
        scrapy stats
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
    host: str = typer.Option(
        "0.0.0.0",
        "--host",
        "-h",
        help="Host to bind to",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        "-p",
        help="Port to bind to",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        "-r",
        help="Enable auto-reload for development",
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Number of worker processes",
    ),
) -> None:
    """
    Start the API server.

    Examples:
        scrapy serve
        scrapy serve --port 8080
        scrapy serve --reload  # Development mode
    """
    import uvicorn

    settings = get_settings()

    console.print("[blue]Starting Scrapy Engine API[/blue]")
    console.print(f"[dim]Host:[/dim] {host}")
    console.print(f"[dim]Port:[/dim] {port}")
    console.print(f"[dim]Debug:[/dim] {settings.debug}")

    if settings.debug:
        console.print(f"[dim]Docs:[/dim] http://{host}:{port}/docs")

    uvicorn.run(
        "api.app:app",
        host=host,
        port=port,
        reload=reload,
        workers=workers if not reload else 1,
        log_level="debug" if settings.debug else "info",
    )


@app.command()
def version() -> None:
    """Show version information."""
    console.print("[blue]Scrapy Engine[/blue] v1.0.0")
    console.print("[dim]Modular scraping for RAG pipelines[/dim]")


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
