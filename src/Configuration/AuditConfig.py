
"""
Audit constants used in statistical analysis of resource names.

This module contains all constants used in the audit workflow.
"""

class AuditConfig:
    """Constants used in the audit workflow."""

    AUDIT_TOKEN_COUNT_IN_CLUSTER_MIN: int = 2
    """Minimum number of tokens a cluster must have to be considered in the audit."""
    AUDIT_MEAN_INTRA_DISTANCE_MAX: float = 0.5
    """Maximum mean intra-cluster distance for a cluster to be considered in the audit."""
    AUDIT_RELATIVE_COST_PCT_MIN: float = 0.01
    """Minimum relative cost percentage for a token to be included in the audit."""
    AUDIT_DISTINCT_RESOURCE_COUNT_MIN: int = 3
    """Minimum number of distinct resources a token must appear in to be included in the audit."""

    # --- Audit Phase Two Settings ---
    AUDIT_ENTITY_CANDIDATES_FILENAME: str = 'entity_candidates.csv'
    """Output filename for the entity candidates generated in Audit Phase Two."""
    AUDIT_FITNESS_CERTAINTY_SINGLE: float = 1.0
    """Fitness certainty score for entity candidates derived from a single, non-delimited residual string."""
    AUDIT_FITNESS_CERTAINTY_DELIMITED: float = 1.0
    """Fitness certainty score for entity candidates derived from a delimited residual string."""
    AUDIT_OVERSTRIP_PCT: float = 0.8
    """Threshold for the `overstrip_flag`. A resource is flagged if `pct_removed` exceeds this value."""
    AUDIT_RESIDUAL_MIN_LEN: int = 3
    """Threshold for the `overstrip_flag`. A resource is flagged if `residual_len` is less than this value."""

    # --- Audit Phase 2: Protect Set Settings ---
    AUDIT_P_SET_MIN_SUPPORT: int = 3
    """Minimum number of distinct resource names a chunk must appear in to be included in the Protect Set."""
    AUDIT_P_SET_MAX_SIZE: int = 500
    """The maximum number of chunks to include in the final Protect Set, sorted by length and support."""
    AUDIT_P_SET_COST_THRESHOLD_PCT: float = 0.01
    """Minimum cost a token's associated resources must represent (as a percentage of total monthly cost) to be included in the cost-based protect set."""
    FINAL_SELECTION_MIN_FREQUENCY: int = 5
    """Minimum cost a token's associated resources must represent (as a percentage of total monthly cost) to be included in the cost-based protect set."""

    # --- Audit Phase 2: Glued Name Analysis Settings ---
    AUDIT_GLUED_COVERAGE_NO_P_HITS: bool = True
    """If True, attempt full coverage analysis on glued names even if they have no Protect Set hits. If False, skip them."""

    # --- ECS Calculation Parameters ---
    ECS_TIGHTNESS_NORMALIZATION_FACTOR: float = 0.5
    """Normalization factor for the tightness score in ECS calculation."""
    ECS_COST_NORMALIZATION_FACTOR: float = 2.0
    """Normalization factor for the cost score in ECS calculation (cost as a percentage)."""
    """Normalization factor for the reach score in ECS calculation."""
    ECS_TIGHTNESS_WEIGHT: float = 0.50
    """Weight for the tightness component in the final ECS score."""
    ECS_COST_WEIGHT: float = 0.35
    """Weight for the cost component in the final ECS score."""
    ECS_REACH_WEIGHT: float = 0.15
    """Weight for the reach component in the final ECS score."""

    # --- Pattern Matching Settings ---
    PLACEHOLDERS: dict = {
        'GUID': '⟂GUID⟂',
        'TECH': '⟂TECH⟂',
        'ENV': '⟂ENV⟂',
        'REG': '⟂REG⟂',
        'NUM': '⟂NUM⟂',
        'HEX': '⟂HEX⟂'
    }
    """Placeholders for masked terms."""

    AUDIT_FILTER_OUT_RESOURCE_TYPE: str = 'Metric alert rule'
    """Resource type to filter out during Audit Phase One."""
    AUDIT_ACRONYM_MIN_LEN: int = 3
    """Minimum length for a residual part to be considered an acronym."""
    AUDIT_ACRONYM_MAX_LEN: int = 4
    """Maximum length for a residual part to be considered an acronym."""
    AUDIT_HEAVY_SCAFFOLD_PCT: float = 0.5
    """Percentage of characters removed to flag as heavy scaffold."""
    AUDIT_HEAVY_SCAFFOLD_HITS: int = 3
    """Number of mask hits to flag as heavy scaffold."""
    AUDIT_ENV_CONFLICT_MIN_COUNT: int = 2
    """Minimum number of embedded environment terms to flag a conflict."""

    AUDIT_TOP_TOKENS_COUNT: int = 10
    """Number of top tokens to include in the corpus summary."""

    KNOWN_ENTITIES_YAML_KEY: str = 'known_entities'
    """The top-level key in 'known_entities.yaml' under which the list of entity definitions is found.
       Affects: Loading of known business/technical entities for name analysis."""

    DUCKDB_VIEW_NAME: str = "resources_per_day_json"
    """Name of the specific view within DuckDB (see DUCKDB_PATH) that serves as the primary data source for analysis.
       This view should provide resource details, costs, and tags."""

    TENANT_CONFIG_EXCLUSIONS_PATH: str = "exclusions.txt"
    """Path to the exclusions.yaml file for the tenant."""
    TENANT_CONFIG_ENVIRONMENTS_PATH: str = "environments.txt"
    """Path to the environments.yaml file for the tenant."""
    TENANT_CONFIG_REGIONS_PATH: str = "regions.txt"
    """Path to the regions.yaml file for the tenant."""