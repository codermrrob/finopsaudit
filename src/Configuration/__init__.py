"""
Initializes the Configuration package.

This module provides centralized access to all application settings,
constants, and configuration values across different domains.
"""

# Import from database configuration
from .DatabaseConfig import (
    ResourcesPerDayJsonColumns,
    ResourcesPerDayJsonColumnSets,
    DATABASE_MEMORY_LIMIT,
    DATABASE_THREADS,
    DATABASE_CONNECTION_TIMEOUT
)

# Import from audit configuration
from .AuditConfig import (
    AuditConfig,
    AuditReportColumns,
    ProtectSetColumns,
    GluedNamesColumns,
    CorpusRollupColumns,
)

# Import from agent configuration
from .AgentConfig import AgentConfig, ModelProvider, ModelConfig, ResponseSchema

# Import from global configuration
from .GlobalConfig import RegularExpressions

# You would also import from other config files when you create them
# from .settings import AppSettings, settings
# from .constants import SUPPORTED_FORMATS, DEFAULT_COMPRESSION

__all__ = [
    # Database constants
    "ResourcesPerDayJsonColumns",
    "ResourcesPerDayJsonColumnSets", 
    "DATABASE_MEMORY_LIMIT",
    "DATABASE_THREADS",
    "DATABASE_CONNECTION_TIMEOUT",
    
    # Audit constants
    "AuditConfig",
    "AuditReportColumns",
    "ProtectSetColumns",
    "GluedNamesColumns",
    "CorpusRollupColumns",
    
    # Agent constants
    "AgentConfig",
    "ModelProvider",
    "ModelConfig",
    "ResponseSchema",
    
    # Global constants
    "RegularExpressions",
    
    # Add other exports as you create more config files
    # "AppSettings",
    # "settings",
    # "SUPPORTED_FORMATS",
    # "DEFAULT_COMPRESSION"
]