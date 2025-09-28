"""
Contains the ResidueAnalyzer class for reducing resource names.
"""

import pandas as pd
import yaml
from pathlib import Path
from Configuration import (
    AgentConfig,
    AuditConfig,
    AuditReportColumns,
    ModelProvider,
    ProtectSetColumns,
)
from .EntityExtractor import EntityExtractor

import logging

logger = logging.getLogger(__name__)


class ResidueAnalyzer:
    """Reads masked resource names, extracts residue, and filters out known entities."""

    def __init__(self, output_base: str, tenant: str, year: int, month: str):
        """Initializes the analyzer and loads protected chunks."""
        self.placeholders = AuditConfig.PLACEHOLDERS.values()
        self.year = year
        self.month = month
        self.output_base = output_base
        self.tenant = tenant
        self.tenant_audit_path = Path(self.output_base) / self.tenant / f"audit"
        self.tenant_agent_path = Path(self.output_base) / self.tenant / f"agent"

        # Construct paths relative to the tenant's audit directory
        self.input_path = self.tenant_audit_path / f"masked_names_for_agent.{self.year}_{self.month}.csv"
        self.protect_set_path = self.tenant_audit_path / f"protect_set_combined.{self.year}_{self.month}.p2.csv"

        self.protected_chunks = self._load_protected_chunks()
        self.entity_extractor = EntityExtractor(model_provider=ModelProvider.GEMINI_FLASH)

    def _load_protected_chunks(self) -> list[str]:
        """Loads and sorts protected chunks from the specified CSV file."""
        try:
            df = pd.read_csv(self.protect_set_path)
            # Sort by length of the protect-set chunk string, descending
            df['length'] = df[ProtectSetColumns.CHUNK].str.len()
            df_sorted = df.sort_values(by='length', ascending=False)
            logger.info(f"Loaded {len(df_sorted)} protected chunks.")
            return df_sorted[ProtectSetColumns.CHUNK].tolist()
        except FileNotFoundError:
            logger.warning(f"Protected chunks file not found at: {self.protect_set_path}")
            return []
        except Exception as e:
            logger.error(f"Error loading protected chunks: {e}")
            return []

    def get_meaningful_residues(self) -> list[tuple[str, str]]:
        """
        Reads masked names, extracts residues, filters them, and returns meaningful ones.

        Returns:
            list[tuple[str, str]]: A list of (original_resource_name, residue) tuples.
        """
        try:
            df = pd.read_csv(self.input_path)
        except FileNotFoundError:
            logger.error(f"Masked names input file not found at: {self.input_path}")
            logger.error("Please run the 'audit' command first to generate the required input file.")
            return []

        residues_with_origin = []
        for _, row in df.iterrows():
            original_name = row[AuditReportColumns.RESOURCE_NAME]
            masked_name = row[AuditReportColumns.MASKED_NAME]

            # Ensure masked_name is a string to avoid errors with NaN values
            if not isinstance(masked_name, str):
                masked_name = ''
            
            residue = masked_name
            for placeholder in self.placeholders:
                residue = residue.replace(placeholder, '')

            # If residue contains a protected chunk, skip it
            if any(chunk in residue for chunk in self.protected_chunks):
                continue

            if self._is_meaningful(residue):
                residues_with_origin.append((original_name, residue))
        
        logger.info(f"Found {len(residues_with_origin)} meaningful residues for LLM analysis.")

        # Process residues with the EntityExtractor
        if residues_with_origin:
            extraction_results = self.entity_extractor.process(residues_with_origin)
            self._persist_results(extraction_results)

        return residues_with_origin

    def _is_meaningful(self, residue: str) -> bool:
        """
        Checks if a residue is meaningful enough to send to an LLM.
        Since the input is now pre-masked, this is a simpler check.

        Args:
            residue (str): The residue string to check.

        Returns:
            bool: True if the residue is meaningful, False otherwise.
        """
        # Rule 1: Must be longer than 2 characters to be worth analyzing.
        if len(residue) <= AgentConfig.AGENT_MIN_MEANINGFUL_RESIDUE_LENGTH:
            return False

        return True

    def _persist_results(self, results: list):
        """Transforms and persists the extraction results to a YAML file in an entity-centric format."""
        output_path = self.tenant_agent_path / f"suggested_entities.{self.year}_{self.month}.p3.yml"

        if not any(result.entities for result in results):
            logger.info("No entities were extracted, so no output file will be generated.")
            return

        # Aggregate results by entity
        aggregated_entities = {}
        for result in results:
            if not result.entities:
                continue
            for entity in result.entities:
                if entity.entity_name not in aggregated_entities:
                    aggregated_entities[entity.entity_name] = {
                        'abbreviations': set(),
                        'found_in_chunks': []
                    }
                aggregated_entities[entity.entity_name]['abbreviations'].update(entity.abbreviations)
                aggregated_entities[entity.entity_name]['found_in_chunks'].append(result.original_name)

        # Format for YAML output
        output_data = []
        for name, data in aggregated_entities.items():
            output_data.append({
                'entity_name': name,
                'abbreviations': sorted(list(data['abbreviations'])),
                'found_in_chunks': sorted(list(set(data['found_in_chunks']))) # Unique chunks
            })

        # Sort the final list by entity name for consistency
        output_data.sort(key=lambda x: x['entity_name'])

        try:
            with open(output_path, 'w') as f:
                yaml.dump(output_data, f, indent=2, default_flow_style=False, sort_keys=False)
            logger.info(f"Successfully saved {len(output_data)} suggested entities to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save suggested entities: {e}")
