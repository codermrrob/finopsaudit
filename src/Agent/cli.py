"""
Main CLI for the Entity Extraction Agent.

This standalone CLI tool uses an LLM to discover potential new business entities
from the 'residue' of resource names after known terms have been removed.
"""
import logging
from pathlib import Path
import sys
# Add the project root's 'src' directory to the Python path
# This allows for absolute imports from 'src' when running as a script
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import typer
from typing_extensions import Annotated
from dotenv import load_dotenv

from Utils.logging import setup_logging
from .Workflow import Workflow
from Configuration.AgentConfig import AgentConfig

app = typer.Typer(
    name="entity-extraction-agent",
    help="A CLI tool to discover new business entities from resource name residues using an LLM.",
    add_completion=False,
)

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    output_base: Annotated[
        Path,
        typer.Option(
            "--output-base",
            help="Base directory containing tenant configuration and audit outputs.",
            rich_help_panel="Paths",
            prompt=True,
        ),
    ],
    tenant: Annotated[
        str,
        typer.Option(
            "--tenant",
            help="Tenant identifier used to locate configuration and audit data.",
            rich_help_panel="Paths",
            prompt=True,
        ),
    ],
    year: Annotated[
        int,
        typer.Option(
            help="The year of the data to process (e.g., 2024).",
            rich_help_panel="Data Selection",
            prompt=True,
        ),
    ],
    month: Annotated[
        str,
        typer.Option(
            help="The month of the data to process (e.g., '06').",
            rich_help_panel="Data Selection",
            prompt=True,
        ),
    ],
    llm_model: Annotated[
        str,
        typer.Option(
            help="The name of the LLM model to use (e.g., a Gemini model)."
        ),
    ] = AgentConfig.AGENT_DEFAULT_MODEL.value,
    temperature: Annotated[
        float,
        typer.Option(
            help="The temperature setting for the LLM.", min=0.0, max=2.0
        ),
    ] = 1.0,
    log_level: Annotated[
        str,
        typer.Option(
            help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
            envvar="LOG_LEVEL",
        ),
    ] = "INFO",
    log_file: Annotated[
        Path | None,
        typer.Option(
            help="Path to a file for logging. If not provided, logs to stderr only.",
            rich_help_panel="Logging",
        ),
    ] = None,
):
    """Main entry point for the entity extraction agent.
    
    Initializes logging and runs the entity extraction workflow.
    """
    # Use output_base as the directory for logs, ensuring it's created
    log_dir = output_base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Ensure a default log file name if not provided
    log_file_name = log_file if log_file else "agent.log"

    # Correctly pass arguments by keyword
    setup_logging(log_dir=str(log_dir), log_level=log_level.upper(), log_file_name=log_file_name)
    logger.info(f"Log level set to {log_level.upper()}")

    if ctx.invoked_subcommand is not None:
        return

    logger.info(
        "Starting entity extraction for %s-%s with model '%s' and temperature %.2f.",
        year,
        month,
        llm_model,
        temperature,
    )

    try:
        workflow = Workflow(
            output_base=str(output_base),
            tenant=tenant,
            llm_model=llm_model,
            temperature=temperature,
            year=year,
            month=month,
        )
        workflow.run()
        logger.info("Workflow completed successfully.")
    except Exception as e:
        logger.exception(f"An error occurred during the workflow: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
