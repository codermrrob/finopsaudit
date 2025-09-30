"""
Configuration constants for the Audit Readiness Dashboard.
"""

class AuditDashboardConfig:
    """Holds configuration settings for the Streamlit dashboard."""
    
    # Page Titles
    SUMMARY_PAGE_TITLE = "Entity Analysis Summary"
    DEEP_DIVE_PAGE_TITLE = "Inconsistency Deep Dive"

    # Chart Settings
    TREEMAP_TOP_N = 15
    COST_COVERAGE_SLIDER_MAX = 100
    COST_COVERAGE_SLIDER_DEFAULT = 20
    