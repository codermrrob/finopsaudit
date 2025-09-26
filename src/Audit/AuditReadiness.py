# File: auditReadiness.py
import logging

logger = logging.getLogger(__name__)
from Configuration import AuditConfig
from .AuditPhaseOne import AuditPhaseOne
from .AuditPhaseTwo import AuditPhaseTwo
from .AuditInitialiser import AuditInitialiser
from DataManager import DataManager
from FileSystem import LocalFileSystem


class AuditReadiness:
    """Orchestrates the entire audit readiness process, running all phases in sequence."""

    def __init__(self, output_base: str, tenant: str, year: int, month: str):
        self.settings = AuditConfig()
        self.tenant = tenant
        self.output_base = output_base
        self.year = year
        self.month = month

        # Ensure the audit environment is initialised
        fs = LocalFileSystem()
        data_manager = DataManager(fs)
        AuditInitialiser(output_base, tenant, data_manager)

    def run(self):
        """Runs all audit phases sequentially."""
        logger.info("Starting audit readiness workflow...")

        # --- Phase 1: Initial Masking and Residual Analysis ---
        phase_one = AuditPhaseOne(
            output_base=self.output_base,
            tenant=self.tenant,
            year=self.year,
            month=self.month
        )
        phase_one_output_df = phase_one.run()

        # --- Phase 2: Entity Candidate Extraction ---
        logger.info("Starting Audit Phase 2: Entity Candidate Extraction...")
        phase_two = AuditPhaseTwo(
            phase_one_df=phase_one_output_df,
            year=self.year,
            month=self.month,
            output_base=self.output_base,
            tenant=self.tenant
        )
        phase_two.run()
        logger.info("Audit Phase 2 completed.")


        logger.info("Audit workflow finished.")
