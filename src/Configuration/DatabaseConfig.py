"""
Database table definitions, column constants, and database-related configuration.

This module contains all database-specific constants including table schemas,
column names, and predefined column sets for common operations.
"""

class ResourcesPerDayJsonColumns:
    """Column constants for the resources_per_day_json table."""
    
    RESOURCE_ID = "ResourceId"
    RESOURCE_NAME = "ResourceName"
    RESOURCE_GROUP = "ResourceGroup"
    RESOURCE_TYPE = "ResourceType"
    REGION_ID = "RegionId"
    REGION_NAME = "RegionName"
    SUB_ACCOUNT_ID = "SubAccountId"
    SUB_ACCOUNT_NAME = "SubAccountName"
    BILLING_ACCOUNT_ID = "BillingAccountId"
    BILLING_ACCOUNT_NAME = "BillingAccountName"
    PROVIDER_NAME = "ProviderName"
    COST = "Cost"
    TAGS = "Tags"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"

class ResourcesPerDayJsonColumnSets:
    """Predefined column sets for resources_per_day_json table operations."""
    
    AGGREGATION = [
        ResourcesPerDayJsonColumns.RESOURCE_ID,
        ResourcesPerDayJsonColumns.RESOURCE_NAME,
        ResourcesPerDayJsonColumns.COST,
        ResourcesPerDayJsonColumns.BILLING_ACCOUNT_NAME,
        ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME,
        ResourcesPerDayJsonColumns.TAGS,
        ResourcesPerDayJsonColumns.RESOURCE_GROUP,
    ]
    
    PARTITION_COLUMNS = [
        ResourcesPerDayJsonColumns.YEAR,
        ResourcesPerDayJsonColumns.MONTH,
        ResourcesPerDayJsonColumns.DAY,
    ]


# Add more table classes as needed...

# Place Holder for CostsRawColumns
# class CostsRawColumns:
#     """Column constants for the costs_raw table."""
    
#     BILLING_PERIOD = "BillingPeriod"
#     BILLED_COST = "BilledCost"
#     EFFECTIVE_COST = "EffectiveCost"
#     # ... add other columns as needed

# class CostsRawColumnSets:
#     """Predefined column sets for costs_raw table operations."""
    
#     REQUIRED_COLUMNS = [
#         CostsRawColumns.BILLING_PERIOD,
#         CostsRawColumns.EFFECTIVE_COST,
#     ]


# Database configuration settings
DATABASE_MEMORY_LIMIT = "2GB"
DATABASE_THREADS = 4
DATABASE_CONNECTION_TIMEOUT = 30