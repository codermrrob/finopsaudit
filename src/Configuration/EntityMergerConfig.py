"""
Configuration for the EntityMerger module.
"""

class SimilarityConfig:
    """Constants for the entity name similarity analysis."""
    # Thresholds
    DUPLICATE_THRESHOLD = 0.92
    RELATED_THRESHOLD = 0.80
    CONFIDENCE_THRESHOLD = 0.95

    # Reciprocal Top-K Linking
    RECIPROCAL_TOP_K = 5

    # TF-IDF Parameters
    NGRAM_RANGE = (3, 5)

    # YAML Property Names
    CANONICAL_KEY = "canonical"
    MEMBERS_KEY = "members"
    SIMILARITY_KEY = "similarity"
    LINK_TYPE_KEY = "link_type"
    ENTITY_NAME_KEY = "entity_name"
    DUPLICATE_VALUE = "duplicate"
    RELATED_VALUE = "related"
    DISTINCT_ENTITIES_KEY = "distinct_entities"

class EntityMergerConfig:
    """Constants for the entity merging process."""
    
    # Input filename from Phase 2 (statistical suggestions)
    P2_SUGGESTIONS_FILENAME = "suggested_entities.{year}_{month}.p2.yml"
    
    # Input filename from Phase 3 (LLM-inferred suggestions)
    P3_SUGGESTIONS_FILENAME = "suggested_entities.{year}_{month}.p3.yml"
    
    # Output filename for the master list
    MASTER_SUGGESTIONS_FILENAME = "master_suggested_entities.{year}_{month}.yml"
