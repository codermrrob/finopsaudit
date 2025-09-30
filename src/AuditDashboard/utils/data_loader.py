"""
Contains data loading and processing functions for the Entity Analysis dashboard pages.
"""

import streamlit as st
import pandas as pd
import yaml
from pathlib import Path
import logging

from Configuration import EntityMergerConfig, SimilarityConfig, AuditReportColumns, ResourcesPerDayJsonColumns

logger = logging.getLogger(__name__)

@st.cache_data
def load_entity_analysis_data(tenant: str, year: str, month: str, output_base: str) -> dict:
    """
    Loads and processes all data needed for the entity analysis dashboard.

    This function is cached to ensure data is loaded only once per session.

    Returns:
        A dictionary containing DataFrames and other processed data.
    """
    base_path = Path(output_base)
    month_str = month

    # --- Define Paths ---
    master_suggestions_path = base_path / tenant / "entities" / EntityMergerConfig.MASTER_SUGGESTIONS_FILENAME.format(year=year, month=month_str)
    # Correct the filename format to match the actual file: audit_readiness.{year}_{month}.p1.csv
    phase_one_output_path = base_path / tenant / "audit" / f"audit_readiness.p1.{year}_{month_str}.csv"

    # --- Load Files ---
    if not master_suggestions_path.exists() or not phase_one_output_path.exists():
        st.error(f"Required data files not found for {tenant} {year}-{month_str}. Please run the full audit and merge process first.")
        return {}

    try:
        with open(master_suggestions_path, 'r') as f:
            entity_data = yaml.safe_load(f)
        
        p1_df = pd.read_csv(phase_one_output_path)
    except Exception as e:
        st.error(f"Error loading data files: {e}")
        return {}

    # --- Process Data ---
    total_cost = p1_df[ResourcesPerDayJsonColumns.COST].sum()

    distinct_entities_map = {e['entity_name']: e for e in entity_data.get(SimilarityConfig.DISTINCT_ENTITIES_KEY, [])}

    all_entities = []
    # Process groups
    for group in entity_data.get('groups', []):
        group_chunks = set(group.get('found_in_chunks', []))
        for member in group.get('members', []):
            member_entity = distinct_entities_map.get(member['entity_name'])
            if member_entity:
                group_chunks.update(member_entity.get('found_in_chunks', []))

        all_entities.append({
            'entity_name': group[SimilarityConfig.CANONICAL_KEY],
            'is_canonical': True,
            'found_in_chunks': list(group_chunks),
            'members': group.get('members', [])
        })

    # Process distinct entities that are not in any group
    grouped_members = {m['entity_name'] for g in entity_data.get('groups', []) for m in g.get('members', [])}
    for entity_name, entity in distinct_entities_map.items():
        if entity_name not in grouped_members and entity.get('found_in_chunks'):
            # Check if this distinct entity is a canonical for some group
            is_canonical_of_a_group = any(g[SimilarityConfig.CANONICAL_KEY] == entity_name for g in entity_data.get('groups', []))
            if not is_canonical_of_a_group:
                 all_entities.append({
                    'entity_name': entity['entity_name'],
                    'is_canonical': False,
                    'found_in_chunks': entity['found_in_chunks'],
                    'members': []
                })

    if not all_entities:
        st.warning("No suggested entities found in the master file.")
        return {}

    # --- Cross-reference with Cost Data ---
    entity_costs = []
    for entity in all_entities:
        # Create a regex pattern from the found_in_chunks
        # This is slow, but necessary for accurate cost mapping.
        # A more performant approach would be to pre-calculate this in the pipeline.
        pattern = '|'.join(entity['found_in_chunks'])
        if not pattern:
            continue
        
        matched_resources = p1_df[p1_df[AuditReportColumns.RESOURCE_NAME].str.contains(pattern, na=False, case=False)]
        cost = matched_resources[ResourcesPerDayJsonColumns.COST].sum()
        
        entity_costs.append({
            'entity_name': entity['entity_name'],
            'cost': cost,
            'num_members': len(entity['members'])
        })

    cost_df = pd.DataFrame(entity_costs).sort_values(by='cost', ascending=False).reset_index(drop=True)
    cost_df['cost_pct_of_total'] = (cost_df['cost'] / total_cost) * 100
    cost_df['cumulative_cost_pct'] = cost_df['cost_pct_of_total'].cumsum()

    return {
        'entity_data': entity_data,
        'cost_df': cost_df,
        'total_cost': total_cost,
        'p1_df': p1_df
    }
