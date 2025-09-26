# File: suggested_entities.py
import pandas as pd
import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
from Configuration import ResourcesPerDayJsonColumns

class SuggestedEntities:
    """Generates a suggested entities YAML file from audit data."""

    def __init__(self, phase_one_df: pd.DataFrame, protect_set_df: pd.DataFrame, output_dir: Path, year: int, month: str):
        self.phase_one_df = phase_one_df
        self.protect_set_df = protect_set_df
        self.output_dir = output_dir
        self.year = year
        self.month = month

    def generate_yaml(self):
        """Creates and saves the suggested entities YAML file."""
        logger.info("Generating suggested entities YAML...")
        if self.protect_set_df.empty:
            logger.warning("Protect set is empty. Skipping suggested entities YAML generation.")
            return

        entities = []
        for _, row in self.protect_set_df.iterrows():
            chunk = row['chunk']
            found_in_chunks = self.phase_one_df[
                self.phase_one_df[ResourcesPerDayJsonColumns.RESOURCE_NAME].str.contains(chunk, case=False, na=False)
            ][ResourcesPerDayJsonColumns.RESOURCE_NAME].unique().tolist()

            if found_in_chunks:
                entities.append({
                    'entity_name': chunk,
                    'found_in_chunks': found_in_chunks
                })

        if not entities:
            logger.warning("No entities found to generate the YAML file.")
            return

        output_path = self.output_dir / f"suggested_entities.{self.year}_{self.month}.p2.yml"
        with open(output_path, 'w') as f:
            yaml.dump(entities, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Suggested entities YAML saved to {output_path}")
