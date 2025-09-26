# File: entity_merger.py
import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class EntityMerger:
    """Merges two entity YAML files."""

    def __init__(self, file1_path: Path, file2_path: Path, output_path: Path):
        self.file1_path = file1_path
        self.file2_path = file2_path
        self.output_path = output_path

    def merge(self):
        """Loads, merges, and saves the entity YAML files."""
        logger.info(f"Merging {self.file1_path.name} and {self.file2_path.name}...")

        try:
            with open(self.file1_path, 'r') as f:
                data1 = yaml.safe_load(f) or []
        except FileNotFoundError:
            logger.warning(f"{self.file1_path} not found. Starting with an empty list.")
            data1 = []

        try:
            with open(self.file2_path, 'r') as f:
                data2 = yaml.safe_load(f) or []
        except FileNotFoundError:
            logger.warning(f"{self.file2_path} not found. Nothing to merge.")
            data2 = []

        merged_entities = {entity['entity_name']: entity for entity in data1}

        for entity in data2:
            entity_name = entity['entity_name']
            if entity_name in merged_entities:
                # Merge found_in_chunks and remove duplicates
                existing_chunks = set(merged_entities[entity_name].get('found_in_chunks', []))
                new_chunks = set(entity.get('found_in_chunks', []))
                merged_entities[entity_name]['found_in_chunks'] = sorted(list(existing_chunks.union(new_chunks)))
            else:
                merged_entities[entity_name] = entity

        final_list = sorted(list(merged_entities.values()), key=lambda x: x['entity_name'])

        with open(self.output_path, 'w') as f:
            yaml.dump(final_list, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Merged entity file saved to {self.output_path}")
