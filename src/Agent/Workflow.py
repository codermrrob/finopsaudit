"""
Orchestrates the entity extraction workflow.
"""

from .ResidueAnalyzer import ResidueAnalyzer
import logging

logger = logging.getLogger(__name__)

class Workflow:
    """Manages the end-to-end process of entity extraction."""

    def __init__(self, output_base: str, tenant: str, llm_model: str, temperature: float, year: int, month: str):
        """
        Initializes the workflow.

        Args:
            llm_model (str): The name of the LLM model to use.
            temperature (float): The temperature setting for the LLM.
            year (int): The year of the data to process.
            month (str): The month of the data to process.
        """
        self.output_base = output_base
        self.tenant = tenant
        self.llm_model = llm_model
        self.temperature = temperature
        self.year = year
        self.month = month
        # The persistence and output path are now managed within ResidueAnalyzer
        pass

    def run(self):
        """Executes the full analysis workflow."""
        logger.info("Starting entity extraction workflow...")

        # Initialize and run the ResidueAnalyzer, which now handles the entire process.
        analyzer = ResidueAnalyzer(output_base=self.output_base, tenant=self.tenant, year=self.year, month=self.month)
        
        # This method now finds residues, runs extraction, and persists results.
        analyzer.get_meaningful_residues()
        
        logger.info("Entity extraction process complete.")

