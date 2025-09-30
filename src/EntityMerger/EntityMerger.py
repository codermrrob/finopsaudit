"""
Contains the EntityMerger class for combining and analyzing entity suggestions.
"""

import yaml
from pathlib import Path
from collections import defaultdict
import logging
import datetime

from Configuration import EntityMergerConfig, SimilarityConfig
from .EntityNameSimilarity import EntityNameSimilarity

logger = logging.getLogger(__name__)

class EntityMerger:
    """Merges, analyzes, and groups entity suggestions from audit phases."""

    def __init__(self, output_base: str, tenant: str, year: int, month: str):
        """Initializes the merger with paths and configuration."""
        self.year = year
        self.month = month

        logger.info("EntityMerger initialized.")
        logger.info(f"  Tenant: {tenant}, Period: {year}-{month}")

        # Define paths
        self.p2_suggestions_path = Path(output_base) / tenant / "audit" / EntityMergerConfig.P2_SUGGESTIONS_FILENAME.format(year=year, month=month)
        self.p3_suggestions_path = Path(output_base) / tenant / "agent" / EntityMergerConfig.P3_SUGGESTIONS_FILENAME.format(year=year, month=month)
        self.output_path = Path(output_base) / tenant / "entities"
        self.master_suggestions_path = self.output_path / EntityMergerConfig.MASTER_SUGGESTIONS_FILENAME.format(year=year, month=month)
        logger.info(f"  Output file path set to: {self.master_suggestions_path}")

        self.output_path.mkdir(parents=True, exist_ok=True)

    def _load_yaml(self, file_path: Path) -> list:
        """Safely loads a YAML file, returning an empty list if not found."""
        if not file_path.exists():
            logger.warning(f"Suggestions file not found, treating as empty: {file_path}")
            return []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or []
        except Exception as e:
            logger.error(f"Error loading YAML file {file_path}: {e}")
            return []

    def _perform_initial_merge(self, p2_suggestions: list, p3_suggestions: list) -> dict:
        """Performs a simple key-based merge of entity suggestions."""
        merged = defaultdict(lambda: {
            'abbreviations': set(),
            'found_in_chunks': set(),
            'validation_source': []
        })

        for entity in p2_suggestions:
            name = entity[SimilarityConfig.ENTITY_NAME_KEY]
            merged[name]['abbreviations'].update(entity.get('abbreviations', []))
            merged[name]['found_in_chunks'].update(entity.get('found_in_chunks', []))
            if 'statistical' not in merged[name]['validation_source']:
                merged[name]['validation_source'].append('statistical')

        for entity in p3_suggestions:
            name = entity[SimilarityConfig.ENTITY_NAME_KEY]
            merged[name]['abbreviations'].update(entity.get('abbreviations', []))
            merged[name]['found_in_chunks'].update(entity.get('found_in_chunks', []))
            if 'llm_inferred' not in merged[name]['validation_source']:
                merged[name]['validation_source'].append('llm_inferred')
        
        return {name: data for name, data in merged.items()}

    def merge_suggestions(self):
        """Orchestrates the full entity merging and similarity analysis workflow."""
        logger.info("Step 1: Loading entity suggestions...")
        p2_suggestions = self._load_yaml(self.p2_suggestions_path)
        p3_suggestions = self._load_yaml(self.p3_suggestions_path)
        logger.info(f"Loaded {len(p2_suggestions)} suggestions from Phase 2 (statistical).")
        logger.info(f"Loaded {len(p3_suggestions)} suggestions from Phase 3 (LLM-inferred).")

        logger.info("Step 2: Performing initial key-based merge...")
        initial_merge_dict = self._perform_initial_merge(p2_suggestions, p3_suggestions)
        initial_entities = [{'entity_name': name, **data} for name, data in initial_merge_dict.items()]
        logger.info(f"Found {len(initial_entities)} unique entities after initial merge.")

        logger.info("Step 3: Starting similarity analysis...")
        similarity_analyzer = EntityNameSimilarity(initial_entities)
        groups = similarity_analyzer.find_similar_groups()
        logger.info("Similarity analysis complete.")

        logger.info("Step 4: Aggregating final groups and identifying distinct entities...")
        final_groups = []
        grouped_entities = set()

        for group in groups:
            canonical_name = group[SimilarityConfig.CANONICAL_KEY]
            all_abbreviations = set(initial_merge_dict[canonical_name]['abbreviations'])
            all_chunks = set(initial_merge_dict[canonical_name]['found_in_chunks'])
            grouped_entities.add(canonical_name)

            for member in group[SimilarityConfig.MEMBERS_KEY]:
                member_name = member[SimilarityConfig.ENTITY_NAME_KEY]
                all_abbreviations.update(initial_merge_dict[member_name]['abbreviations'])
                all_chunks.update(initial_merge_dict[member_name]['found_in_chunks'])
                grouped_entities.add(member_name)
            
            final_groups.append({
                SimilarityConfig.CANONICAL_KEY: canonical_name,
                'abbreviations': sorted(list(all_abbreviations)),
                'found_in_chunks': sorted(list(all_chunks)),
                'status': 'pending',
                SimilarityConfig.MEMBERS_KEY: group[SimilarityConfig.MEMBERS_KEY]
            })

        # Identify distinct entities (those not in any group)
        distinct_entities = []
        for entity in initial_entities:
            if entity['entity_name'] not in grouped_entities:
                distinct_entities.append({
                    'entity_name': entity['entity_name'],
                    'abbreviations': sorted(list(entity['abbreviations'])),
                    'found_in_chunks': sorted(list(entity['found_in_chunks'])),
                    'validation_source': sorted(entity['validation_source']),
                    'status': 'pending'
                })

        # Prepare final YAML output
        output_data = {
            'snapshot_id': f"{self.year}_{self.month}",
            'generation_date': datetime.datetime.now().isoformat(),
            'method': 'tfidf_char_3_5_cosine',
            'thresholds': {
                'duplicate': SimilarityConfig.DUPLICATE_THRESHOLD,
                'related': SimilarityConfig.RELATED_THRESHOLD
            },
            'linking': f'reciprocal_top_k_{SimilarityConfig.RECIPROCAL_TOP_K}',
            'groups': final_groups,
            SimilarityConfig.DISTINCT_ENTITIES_KEY: distinct_entities
        }

        try:
            with open(self.master_suggestions_path, 'w', encoding='utf-8') as f:
                yaml.dump(output_data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
            logger.info(f"Successfully merged and analyzed entities into {self.master_suggestions_path}")
            logger.info(f"Found {len(final_groups)} groups and {len(distinct_entities)} distinct entities.")
        except Exception as e:
            logger.error(f"Error writing master suggestions file: {e}")
