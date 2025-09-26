import typer
import sys
import logging
from pathlib import Path
from typing_extensions import Annotated

# Add the project root's 'src' directory to the Python path
# This allows for absolute imports from 'src' when running as a script
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from Audit.AuditReadiness import AuditReadiness
from Configuration import AuditConfig
from Utils.logging import setup_logging

# Create a Typer app instance
app = typer.Typer(
    name="audit-runner",
    help="A CLI to run the full FinOps audit readiness workflow.",
    add_completion=False
)

# Get a logger instance for this module
logger = logging.getLogger(__name__)

@app.command()
def run(
    tenant: Annotated[str, typer.Option(
        ...,  # '...' makes the option required
        help="The tenant ID to process (e.g., 'Contoso').",
        rich_help_panel="Required Parameters"
    )],
    year: Annotated[int, typer.Option(
        ...,
        help="The year of the data to process (e.g., 2024).",
        rich_help_panel="Required Parameters"
    )],
    month: Annotated[str, typer.Option(
        ...,
        help="The month of the data to process, as a two-digit string (e.g., '01' for January).",
        rich_help_panel="Required Parameters"
    )],
    output_base: Annotated[Path, typer.Option(
        help="The base directory for all output data.",
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        rich_help_panel="Input/Output Configuration"
    )] = Path("./output"),
    log_dir: Annotated[Path, typer.Option(
        help="Directory to store log files. Will be created if it doesn't exist.",
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        rich_help_panel="Logging Configuration"
    )] = Path("./logs"),
    log_level: Annotated[str, typer.Option(
        help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).",
        case_sensitive=False,
        rich_help_panel="Logging Configuration"
    )] = "INFO"
):
    """Runs the end-to-end audit readiness workflow for a specific tenant and month."""
    # 1. Setup Logging
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        numeric_log_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_log_level, int):
            print(f"Warning: Invalid log level '{log_level}'. Defaulting to INFO.", file=sys.stderr)
            numeric_log_level = logging.INFO
        
        setup_logging(log_dir=str(log_dir), log_level=numeric_log_level)
        logger.info(f"Logging initialized. Level: {log_level.upper()}, Directory: {log_dir}")
    except Exception as e:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger.error(f"Critical error setting up logging: {e}. Switched to basicConfig.", exc_info=True)

    logger.info(f"Starting audit for Tenant: {tenant}, Period: {year}-{month}")
    logger.info(f"Output directory: {output_base}")

    try:
        # 2. Initialize and run the workflow
        audit_readiness = AuditReadiness(
            output_base=str(output_base), # Convert Path back to string for the class
            tenant=tenant,
            year=year,
            month=month
        )
        
        audit_readiness.run()
        
        logger.info("Audit workflow completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred during the audit workflow: {e}", exc_info=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
