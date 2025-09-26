

class RegularExpressions:
    """Collection of regular expressions used throughout the application."""

    GUID_REGEX: str = r'(?:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}|[0-9a-f]{32})'
    """Regex for matching GUIDs."""

    NUMERIC_REGEX: str = r'\d+'
    """Regex for matching runs of digits."""

    RESOURCE_GROUP_REGEX_PATTERN: str = r"/resourceGroups/([^/]+)"
    """Regex pattern to extract Azure resource group names from resource ID strings.
       Affects: Enrichment of data with 'resource_group' column."""

    HEX_ID_REGEX_PATTERN: str = r'(?:[a-f0-9]*[a-f]){2,}[a-f0-9]*|[a-f0-9]{7,}'
    """Regex pattern to identify and exclude hexadecimal strings. It matches strings that are either at least 7 hex chars long, or contain at least two letters (a-f) to avoid matching simple numbers. This runs after the GUID check."""

    VERSION_CODE_REGEX_PATTERN: str = r'^[0-9]+[a-f0-9]+$'
    """Regex pattern to identify and exclude version-like codes (e.g., '3a4f', '123b') during tokenization.
       Affects: Quality of tokens by removing version identifiers."""

    DELIMITERS_REGEX_PATTERN: str = r'[-_.:/]+'
    """Regex pattern defining delimiters (hyphen, underscore, period, colon) for splitting resource names into initial tokens.
       Affects: Granularity of initial token splitting."""

    POTENTIAL_ENTITY_REGEX_PATTERN: str = r"[A-Za-z]{3,}"
    """Regex pattern to identify potential entities in resource names."""

    TERM_REGEX_FORMAT: str = r"(?:(?<=^)|(?<=[^A-Za-z0-9]))(?:{alternation})(?:(?=$)|(?=[^A-Za-z0-9]))"
    """Format string for creating boundary-aware regexes for general terms (e.g., ENV, REG)."""

    # Special format for TECH terms that allows a trailing digit (e.g., 'sql2019').
    TECH_TERM_REGEX_FORMAT: str = r"(?<![A-Za-z0-9])(?:{alternation})(?=$|[^A-Za-z0-9]|\d)"
    """Format string for creating boundary-aware regexes for technical terms, allowing a trailing digit."""