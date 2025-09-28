"""
Manages the collection of all known terms for entity matching.
"""

import pandas as pd
from pathlib import Path

from Configuration import AgentConfig, AuditConfig
import logging

logger = logging.getLogger(__name__)

class KnowledgeBase:
    """Loads and provides access to all known entities, terms, and exclusions."""

    def __init__(self, output_base: str, tenant: str, protect_set_path: str):
        """
        Initializes the KnowledgeBase.

        Args:
            protect_set_path (str): Path to the protect_set_combined CSV file.
        """
        self.protect_set_path = protect_set_path
        self.output_base = output_base
        self.tenant = tenant
        self.tenant_config_path = Path(self.output_base) / self.tenant / f"{self.tenant}Config"
        self._all_terms: list[str] = []
        self._load_all_terms()

    def get_all_terms_sorted(self) -> list[str]:
        """Returns all known terms, sorted by length in descending order."""
        return self._all_terms

    def _load_all_terms(self):
        """Consolidates all terms from all sources."""
        protect_set_terms = self._load_protect_set()
        environment_terms = self._get_environment_terms()
        region_terms = self._get_region_terms()
        tech_terms = self._get_tech_terms()

        # Consolidate, lowercase, and remove duplicates
        all_terms_set = set(
            [str(term).lower() for term in protect_set_terms]
            + [term.lower() for term in environment_terms]
            + [term.lower() for term in region_terms]
            + [term.lower() for term in tech_terms]
        )

        # Sort by length, descending, to ensure longest match first
        self._all_terms = sorted(list(all_terms_set), key=len, reverse=True)
        logger.info(f"KnowledgeBase initialized with {len(self._all_terms)} unique terms.")

    def _load_terms_from_file(self, file_path: Path) -> list[str]:
        """Loads terms from a text file, one term per line."""
        if not file_path.exists():
            logger.warning(f"Terms file not found at {file_path}. Returning empty list.")
            return []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Failed to read terms from {file_path}: {e}")
            return []

    def _load_protect_set(self) -> list[str]:
        """Loads terms from the protect_set_combined CSV file."""
        try:
            df = pd.read_csv(self.protect_set_path)
            if 'chunk' in df.columns:
                return df['chunk'].dropna().tolist()
            else:
                logger.warning(f"'chunk' column not found in {self.protect_set_path}. Returning empty list.")
                return []
        except FileNotFoundError:
            logger.error(f"Protect set file not found at {self.protect_set_path}. Returning empty list.")
            return []

    def _get_environment_terms(self) -> list[str]:
        """Loads environment-related terms from the configured file."""
        environments_path = self.tenant_config_path / AuditConfig.TENANT_CONFIG_ENVIRONMENTS_PATH
        return self._load_terms_from_file(environments_path)

    def _get_region_terms(self) -> list[str]:
        """Loads region-related terms from the configured file."""
        regions_path = self.tenant_config_path / AuditConfig.TENANT_CONFIG_REGIONS_PATH
        return self._load_terms_from_file(regions_path)

    def _get_tech_terms(self) -> list[str]:
        """Loads technical/exclusion terms from the configured file."""
        exclusions_path = self.tenant_config_path / AuditConfig.TENANT_CONFIG_EXCLUSIONS_PATH
        return self._load_terms_from_file(exclusions_path)