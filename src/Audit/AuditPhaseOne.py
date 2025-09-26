# File: audit_phase_one.py
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)
from Configuration import AuditConfig, RegularExpressions, ResourcesPerDayJsonColumns
from Database import DataAggregator
from DataManager import DataManager
from pathlib import Path
from .ConfigLoader import ConfigLoader
from Database import DuckDBManager
from FileSystem import LocalFileSystem

class AuditPhaseOne:
    """Handles Phase 1 of the audit readiness process: data loading, term masking, and residual analysis."""

    def __init__(self, output_base: str, tenant: str, year: int, month: str):
        self.settings = AuditConfig()
        self.output_base = output_base
        self.tenant = tenant
        self.year = year
        self.month = month
        self.config_loader = ConfigLoader(self.settings)
        
        # Create DuckDBManager and use it for DataAggregator
        self.db_manager = DuckDBManager(output_base, tenant)
        self.data_aggregator = DataAggregator(self.db_manager)
        self.dm = DataManager(LocalFileSystem())
        
        self.audit_path = Path(self.output_base) / self.tenant / "audit"
        self.audit_path.mkdir(parents=True, exist_ok=True)

        # Define tenant-specific config path
        tenant_config_path = Path(self.output_base) / self.tenant / f"{self.tenant}Config"

        self.placeholders = self.settings.PLACEHOLDERS

        # Load data and terms
        self.df = self._load_data()
        logger.info("Loading and sorting terms from tenant config...")
        
        exclusions_path = tenant_config_path / self.settings.TENANT_CONFIG_EXCLUSIONS_PATH
        environments_path = tenant_config_path / self.settings.TENANT_CONFIG_ENVIRONMENTS_PATH
        regions_path = tenant_config_path / self.settings.TENANT_CONFIG_REGIONS_PATH

        self.tech_terms = sorted(list(self.config_loader.load_exclusions(exclusions_path)), key=len, reverse=True)
        self.env_terms = sorted(list(self.config_loader.load_environments(environments_path)), key=len, reverse=True)
        self.reg_terms = sorted(list(self.config_loader.load_regions(regions_path)), key=len, reverse=True)
        logger.info(f"Loaded {len(self.tech_terms)} tech terms, {len(self.env_terms)} env terms, {len(self.reg_terms)} region terms.")

        # Compile regexes
        self.guid_rx = self._compile_guid_regex()
        self.hex_rx = self._compile_hex_regex()
        self.tech_rx = self._compile_tech_regex(self.tech_terms)
        self.env_rx = self._compile_term_regex(self.env_terms)
        self.reg_rx = self._compile_term_regex(self.reg_terms)
        self.num_rx = self._compile_numeric_regex()

        # ... rest of your existing initialization code remains the same

    def _load_data(self) -> pd.DataFrame:
        """Loads and prepares the monthly data."""
        logger.info(f"Loading monthly data for {self.year}-{self.month}...")
        df = self.data_aggregator.load_monthly_NFKC(
            view_name=self.settings.DUCKDB_VIEW_NAME,
            year=self.year,
            month=self.month
        )
        logger.info("Data loaded and resource group extracted.")

        # Filter out 'Metric alert rule' resources
        initial_rows = len(df)
        df = df[df[ResourcesPerDayJsonColumns.RESOURCE_TYPE] != self.settings.AUDIT_FILTER_OUT_RESOURCE_TYPE]
        rows_filtered = initial_rows - len(df)
        if rows_filtered > 0:
            logger.info(f"Filtered out {rows_filtered} records with resource_type 'Metric alert rule'.")
        return df

    def _compile_guid_regex(self) -> re.Pattern:
        """Compiles a regex for matching GUIDs."""
        guid_pattern = RegularExpressions.GUID_REGEX
        pattern = f"(?:(?<=^)|(?<=[^A-Za-z0-9]))({guid_pattern})(?:(?=$)|(?=[^A-Za-z0-9]))"
        return re.compile(pattern, flags=re.IGNORECASE)

    def _compile_tech_regex(self, terms: list[str]) -> re.Pattern:
        """Compiles a boundary-aware regex for TECH terms, allowing for a trailing digit."""
        if not terms:
            return re.compile("a^")  # A regex that will never match
        alternation = "|".join(re.escape(t) for t in terms)
        pattern = RegularExpressions.TECH_TERM_REGEX_FORMAT.format(alternation=alternation)
        return re.compile(pattern, flags=re.IGNORECASE)

    def _compile_term_regex(self, terms: list[str]) -> re.Pattern:
        """Compiles a boundary-aware regex from a list of terms."""
        if not terms:
            return re.compile("a^") # A regex that will never match
        alternation = "|".join(re.escape(t) for t in terms)
        pattern = RegularExpressions.TERM_REGEX_FORMAT.format(alternation=alternation)
        return re.compile(pattern, flags=re.IGNORECASE)

    def _compile_numeric_regex(self) -> re.Pattern:
        """Compiles a boundary-aware regex for digit runs."""
        numeric_pattern = RegularExpressions.NUMERIC_REGEX
        pattern = f"(?:(?<=^)|(?<=[^A-Za-z0-9]))({numeric_pattern})(?:(?=$)|(?=[^A-Za-z0-9]))"
        return re.compile(pattern)

    def _compile_hex_regex(self) -> re.Pattern:
        """Compiles a boundary-aware regex for hex strings."""
        pattern = RegularExpressions.TERM_REGEX_FORMAT.format(alternation=RegularExpressions.HEX_ID_REGEX_PATTERN)
        return re.compile(pattern, flags=re.IGNORECASE)

    def _calculate_shannon_entropy(self, text: str) -> float:
        """Calculates the Shannon entropy for a given string."""
        if not text:
            return 0.0
        
        import math
        from collections import Counter
        
        entropy = 0
        length = len(text)
        counts = Counter(text)
        
        for count in counts.values():
            p_x = count / length
            entropy -= p_x * math.log2(p_x)
            
        return entropy

    def _apply_masks_to_row(self, row: pd.Series) -> pd.Series:
        """Applies all masking rules and calculates enrichment metrics for a single row."""
        original_name = row.get(ResourcesPerDayJsonColumns.RESOURCE_NAME, '')
        if not isinstance(original_name, str):
            original_name = ''

        # --- 1. Masking and Basic Metrics ---
        masked_name = original_name
        mask_hits = {k: 0 for k in self.placeholders}
        chars_removed_map = {k: 0 for k in self.placeholders}

        def get_replacer(placeholder_key):
            def repl(match):
                mask_hits[placeholder_key] += 1
                chars_removed_map[placeholder_key] += len(match.group(0))
                return self.placeholders[placeholder_key]
            return repl

        # Apply masks in order
        masked_name = self.guid_rx.sub(get_replacer('GUID'), masked_name)
        masked_name = self.hex_rx.sub(get_replacer('HEX'), masked_name)
        masked_name = self.tech_rx.sub(get_replacer('TECH'), masked_name)
        masked_name = self.env_rx.sub(get_replacer('ENV'), masked_name)
        masked_name = self.reg_rx.sub(get_replacer('REG'), masked_name)
        masked_name = self.num_rx.sub(get_replacer('NUM'), masked_name)

        # Create residual preview
        residual = masked_name
        for placeholder in self.placeholders.values():
            residual = residual.replace(placeholder, '')
        residual = re.sub(RegularExpressions.DELIMITERS_REGEX_PATTERN, '-', residual).strip('-_')

        # --- 2. Numeric Metrics Calculation ---
        orig_len = len(original_name)
        removed_chars = sum(chars_removed_map.values())
        pct_removed = removed_chars / max(1, orig_len)
        residual_len = len(residual)
        mask_hits_total = sum(mask_hits.values())

        row['masked_name'] = masked_name
        row['residual_preview'] = residual
        row['orig_len'] = orig_len
        row['removed_chars'] = removed_chars
        row['pct_removed'] = pct_removed
        row['residual_len'] = residual_len
        row['entropy_orig'] = self._calculate_shannon_entropy(original_name)
        row['entropy_resid'] = self._calculate_shannon_entropy(residual)
        row['mask_hits_total'] = mask_hits_total
        for key, count in mask_hits.items():
            row[f'mask_hits_{key.lower()}'] = count

        # --- 3. Flag Calculation ---
        row['overstrip_flag'] = (pct_removed > self.settings.AUDIT_OVERSTRIP_PCT) or \
                                (residual_len < self.settings.AUDIT_RESIDUAL_MIN_LEN)
        
        alpha_parts = re.findall(RegularExpressions.POTENTIAL_ENTITY_REGEX_PATTERN, residual)
        row['acronym_only_residual'] = bool(alpha_parts) and all(self.settings.AUDIT_ACRONYM_MIN_LEN <= len(p) <= self.settings.AUDIT_ACRONYM_MAX_LEN for p in alpha_parts)
        
        row['heavy_scaffold'] = (pct_removed >= self.settings.AUDIT_HEAVY_SCAFFOLD_PCT) or (mask_hits_total >= self.settings.AUDIT_HEAVY_SCAFFOLD_HITS)
        row['is_glued'] = '-' not in original_name and '_' not in original_name

        # --- 4. Embedded Detections (for glued names) ---
        embedded_env_list = []
        embedded_tech_list = []
        if row['is_glued']:
            # Find embedded ENV terms
            name_lower = original_name.lower()
            embedded_env_list = sorted(list(set([term for term in self.env_terms if term in name_lower])))

            # Find embedded TECH+digits terms
            tech_digit_pattern = r'(' + '|'.join(re.escape(t) for t in self.tech_terms) + r')\d+'
            embedded_tech_list = sorted(list(set(re.findall(tech_digit_pattern, name_lower, re.IGNORECASE))))

        row['embedded_env_list'] = embedded_env_list
        row['embedded_tech_list'] = embedded_tech_list
        row['env_conflict'] = len(embedded_env_list) >= self.settings.AUDIT_ENV_CONFLICT_MIN_COUNT

        return row

    def run(self) -> pd.DataFrame:
        """Runs the entire audit readiness process and returns the processed DataFrame."""
        logger.info("Starting Audit Phase 1 processing...")
        if self.df.empty:
            logger.warning("Input DataFrame is empty. Skipping processing.")
            return self.df

        processed_df = self.df.apply(self._apply_masks_to_row, axis=1)
        logger.info("Finished Audit Phase 1 processing.")
        
        dm = self.dm
        output_filename = f"audit_readiness.p1.{self.year}_{self.month}.csv"
        output_path = self.audit_path / output_filename
        dm.write_csv(processed_df, output_path)
        logger.info(f"Audit Phase 1 report saved to {output_path}")

        # Save the masked names for the agent
        agent_input_df = processed_df[[ResourcesPerDayJsonColumns.RESOURCE_NAME, 'masked_name']]
        agent_filename = f"masked_names_for_agent.{self.year}_{self.month}.csv"
        agent_output_path = self.audit_path / agent_filename
        dm.write_csv(agent_input_df, agent_output_path)
        logger.info(f"Masked names for agent saved to {agent_output_path}")

        logger.debug(f"Sample of processed data:\n{processed_df.head().to_string()}")
        return processed_df
