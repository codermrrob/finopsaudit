# File: audit_phase_two.py
import pandas as pd
import numpy as np
import re
import json
import unicodedata
from collections import defaultdict, Counter
import logging

logger = logging.getLogger(__name__)
from Configuration import AuditConfig, RegularExpressions, ResourcesPerDayJsonColumns
from DataManager import DataManager
from .ConfigLoader import ConfigLoader
from .SuggestedEntities import SuggestedEntities
from pathlib import Path
from FileSystem import LocalFileSystem

class AuditPhaseTwo:
    """Handles Phase 2 of the audit readiness process: Protect Set, Glued Name Analysis, and Corpus Roll-ups."""

    def __init__(self, phase_one_df: pd.DataFrame, year: int, month: str, output_base: str, tenant: str):
        self.settings = AuditConfig()
        self.phase_one_df = phase_one_df.copy()
        self.year = year
        self.month = month
        self.DataManager = DataManager(LocalFileSystem())
        self.config_loader = ConfigLoader(self.settings)
        self.audit_path = Path(output_base) / tenant / "audit"

        # Load exclusion terms from tenant config
        tenant_config_path = Path(output_base) / tenant / f"{tenant}Config"
        self.tech_terms = self.config_loader.load_exclusions(tenant_config_path / "exclusions.txt")
        self.env_terms = self.config_loader.load_environments(tenant_config_path / "environments.txt")
        self.reg_terms = self.config_loader.load_regions(tenant_config_path / "regions.txt")
        self.exclusion_sets = {
            'TECH': self.tech_terms,
            'ENV': self.env_terms,
            'REG': self.reg_terms
        }

    def run(self):
        """Orchestrates the entire Phase 2 workflow."""
        logger.info("Starting Audit Phase 2...")
        if self.phase_one_df.empty:
            logger.warning("Phase 1 input DataFrame is empty. Skipping Phase 2.")
            return

        # Step 1: Build Protect Sets (Frequency and Cost based)
        protect_set_freq_df = self._build_protect_set()
        protect_set_cost_df = self._build_protect_set_by_cost()

        # Step 2: Create a combined Protect Set for holistic analysis
        self._create_combined_protect_set(protect_set_freq_df, protect_set_cost_df)

        # Step 3: Analyze Glued Names (using the original frequency-based set)
        glued_results_df = self._analyze_glued_names(protect_set_freq_df)

        # Step 4: Generate Corpus Roll-ups
        self._generate_corpus_rollups(glued_results_df)

        # Step 5: Generate suggested entities from Phase 2 data
        self._generate_suggested_entities(protect_set_freq_df)

        logger.info("Audit Phase 2 completed successfully.")

    def _build_protect_set(self) -> pd.DataFrame:
        """Builds the Protect Set from delimited resource names."""
        logger.info("Building Protect Set...")

        # filtering operation to get only delimited resource names
        delim_df = self.phase_one_df[self.phase_one_df[ResourcesPerDayJsonColumns.RESOURCE_NAME].str.contains(RegularExpressions.DELIMITERS_REGEX_PATTERN, na=False)].copy()

        spans = defaultdict(lambda: defaultdict(list))
        for _, row in delim_df.iterrows():
            name = row[ResourcesPerDayJsonColumns.RESOURCE_NAME]
            for segment in re.split(RegularExpressions.DELIMITERS_REGEX_PATTERN, name):
                for match in re.finditer(RegularExpressions.POTENTIAL_ENTITY_REGEX_PATTERN, segment):
                    span_orig = match.group(0)
                    span_norm = unicodedata.normalize("NFKC", span_orig).lower()
                    
                    is_excluded = False
                    for term_set in self.exclusion_sets.values():
                        if span_norm in term_set:
                            is_excluded = True
                            break
                    if is_excluded:
                        continue

                    spans[span_norm]['resource_ids'].append(row[ResourcesPerDayJsonColumns.RESOURCE_ID])
                    spans[span_norm]['case_variants'].append(span_orig)
                    spans[span_norm]['sub_accounts'].append(row[ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME])
                    spans[span_norm]['resource_groups'].append(row[ResourcesPerDayJsonColumns.RESOURCE_GROUP])

        records = []
        for span, data in spans.items():
            support_names = len(set(data['resource_ids']))
            if support_names >= self.settings.AUDIT_P_SET_MIN_SUPPORT:
                # Determine display form
                case_counts = Counter(data['case_variants'])
                max_freq = max(case_counts.values())
                tied_variants = [v for v, c in case_counts.items() if c == max_freq]
                
                display_form = sorted(tied_variants)[0] # Default tie-break
                if any(v.islower() for v in tied_variants):
                    display_form = sorted([v for v in tied_variants if v.islower()])[0]
                elif any(v.istitle() for v in tied_variants):
                    display_form = sorted([v for v in tied_variants if v.istitle()])[0]

                records.append({
                    'chunk': span,
                    'display_form': display_form,
                    'length': len(span),
                    'support_names': support_names,
                    'spread_subs': len(set(data['sub_accounts'])),
                    'spread_rgs': len(set(data['resource_groups'])),
                    'sample_name_1': self.phase_one_df.loc[self.phase_one_df[ResourcesPerDayJsonColumns.RESOURCE_ID] == data['resource_ids'][0], ResourcesPerDayJsonColumns.RESOURCE_NAME].iloc[0],
                    'sample_name_2': self.phase_one_df.loc[self.phase_one_df[ResourcesPerDayJsonColumns.RESOURCE_ID] == data['resource_ids'][-1], ResourcesPerDayJsonColumns.RESOURCE_NAME].iloc[0] if len(data['resource_ids']) > 1 else None
                })

        if not records:
            logger.warning("No records generated for Protect Set. It will be empty.")
            return pd.DataFrame()

        protect_set_df = pd.DataFrame(records)
        protect_set_df.sort_values(by=['length', 'support_names'], ascending=[False, False], inplace=True)
        protect_set_df = protect_set_df.head(self.settings.AUDIT_P_SET_MAX_SIZE)

        output_path = self.audit_path / f"protect_set.{self.year}_{self.month}.p2.csv"
        self.DataManager.write_csv(protect_set_df, output_path)
        logger.info(f"Protect Set saved to {output_path} with {len(protect_set_df)} chunks.")
        return protect_set_df

    def _build_protect_set_by_cost(self) -> pd.DataFrame:
        """Builds a Protect Set based on aggregated cost of resources associated with each token."""
        logger.info("Building Protect Set based on cost...")
        delim_df = self.phase_one_df[self.phase_one_df[ResourcesPerDayJsonColumns.RESOURCE_NAME].str.contains(RegularExpressions.DELIMITERS_REGEX_PATTERN, na=False)].copy()

        if delim_df.empty:
            logger.warning("No delimited names found for cost-based protect set. It will be empty.")
            return pd.DataFrame()

        total_monthly_cost = self.phase_one_df[ResourcesPerDayJsonColumns.COST].sum()
        if total_monthly_cost == 0:
            logger.warning("Total monthly cost is zero. Cannot build cost-based protect set.")
            return pd.DataFrame()

        spans = defaultdict(lambda: defaultdict(list))
        for _, row in delim_df.iterrows():
            name = row[ResourcesPerDayJsonColumns.RESOURCE_NAME]
            cost = row.get(ResourcesPerDayJsonColumns.COST, 0.0)
            for segment in re.split(RegularExpressions.DELIMITERS_REGEX_PATTERN, name):
                for match in re.finditer(RegularExpressions.POTENTIAL_ENTITY_REGEX_PATTERN, segment):
                    span_orig = match.group(0)
                    span_norm = unicodedata.normalize("NFKC", span_orig).lower()

                    is_excluded = any(span_norm in term_set for term_set in self.exclusion_sets.values())
                    if is_excluded:
                        continue

                    spans[span_norm]['resource_ids'].append(row[ResourcesPerDayJsonColumns.RESOURCE_ID])
                    spans[span_norm]['case_variants'].append(span_orig)
                    spans[span_norm]['sub_accounts'].append(row[ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME])
                    spans[span_norm]['resource_groups'].append(row[ResourcesPerDayJsonColumns.RESOURCE_GROUP])
                    spans[span_norm]['costs'].append(cost)

        records = []
        for span, data in spans.items():
            total_cost = sum(data['costs'])
            cost_pct_of_total = (total_cost / total_monthly_cost)

            if cost_pct_of_total >= self.settings.AUDIT_P_SET_COST_THRESHOLD_PCT:
                case_counts = Counter(data['case_variants'])
                max_freq = max(case_counts.values())
                tied_variants = [v for v, c in case_counts.items() if c == max_freq]
                display_form = sorted(tied_variants)[0]
                if any(v.islower() for v in tied_variants):
                    display_form = sorted([v for v in tied_variants if v.islower()])[0]
                elif any(v.istitle() for v in tied_variants):
                    display_form = sorted([v for v in tied_variants if v.istitle()])[0]

                records.append({
                    'chunk': span,
                    'display_form': display_form,
                    'length': len(span),
                    'support_names': len(set(data['resource_ids'])),
                    'spread_subs': len(set(data['sub_accounts'])),
                    'spread_rgs': len(set(data['resource_groups'])),
                    'total_cost': total_cost,
                    'cost_pct_of_total': cost_pct_of_total
                })

        if not records:
            logger.warning("No records generated for cost-based Protect Set. It will be empty.")
            return pd.DataFrame()

        protect_set_cost_df = pd.DataFrame(records)
        protect_set_cost_df.sort_values(by=['total_cost'], ascending=False, inplace=True)

        output_path = self.audit_path / f"protect_set_cost.{self.year}_{self.month}.p2.csv"
        self.DataManager.write_csv(protect_set_cost_df, output_path)
        logger.info(f"Cost-based Protect Set saved to {output_path} with {len(protect_set_cost_df)} chunks.")
        return protect_set_cost_df

    def _create_combined_protect_set(self, freq_df: pd.DataFrame, cost_df: pd.DataFrame):
        """Merges the frequency-based and cost-based protect sets into a single file."""
        logger.info("Creating combined Protect Set...")

        # Ensure dataframes are not empty before merging
        if freq_df.empty and cost_df.empty:
            logger.warning("Both frequency and cost protect sets are empty. Skipping combined set generation.")
            return

        # Add flags before merging
        freq_df['in_frequency_set'] = True
        cost_df['in_cost_set'] = True

        # Outer merge on 'chunk' to keep all unique tokens
        combined_df = pd.merge(
            freq_df.drop(columns=['sample_name_1', 'sample_name_2']),
            cost_df,
            on='chunk',
            how='outer',
            suffixes=('_freq', '_cost')
        )

        # Coalesce columns from the two dataframes
        for col in ['display_form', 'length', 'support_names', 'spread_subs', 'spread_rgs']:
            combined_df[col] = combined_df[f'{col}_freq'].combine_first(combined_df[f'{col}_cost'])
            combined_df.drop(columns=[f'{col}_freq', f'{col}_cost'], inplace=True)

        # Fill boolean flags and numeric NAs
        combined_df['in_frequency_set'] = combined_df['in_frequency_set'].astype('boolean').fillna(False)
        combined_df['in_cost_set'] = combined_df['in_cost_set'].astype('boolean').fillna(False)
        for col in ['total_cost', 'cost_pct_of_total']:
            if col in combined_df.columns:
                combined_df[col] = combined_df[col].fillna(0)

        # Reorder columns for clarity
        final_cols = [
            'chunk', 'display_form', 'length',
            'support_names', 'spread_subs', 'spread_rgs', 'total_cost', 'cost_pct_of_total',
            'in_frequency_set', 'in_cost_set'
        ]
        # Ensure all columns exist before trying to reorder
        final_cols = [col for col in final_cols if col in combined_df.columns]
        combined_df = combined_df[final_cols]

        combined_df.sort_values(by=['support_names', 'total_cost'], ascending=[False, False], inplace=True)

        output_path = self.audit_path / f"protect_set_combined.{self.year}_{self.month}.p2.csv"
        self.DataManager.write_csv(combined_df, output_path)
        logger.info(f"Combined Protect Set saved to {output_path} with {len(combined_df)} chunks.")


    def _compile_guid_regex(self) -> re.Pattern:
        return re.compile(f"(?<![A-Za-z0-9])({RegularExpressions.GUID_REGEX})(?![A-Za-z0-9])", flags=re.IGNORECASE)

    def _compile_tech_regex(self, terms: list[str]) -> re.Pattern:
        if not terms:
            return re.compile("a^")
        alternation = "|".join(re.escape(t) for t in terms)
        pattern = f"(?<![A-Za-z0-9])(?:{alternation})(?![A-Za-z0-9])(?=[^A-Za-z]|$)"
        return re.compile(pattern, flags=re.IGNORECASE)

    def _compile_term_regex(self, terms: list[str]) -> re.Pattern:
        if not terms:
            return re.compile("a^")
        alternation = "|".join(re.escape(t) for t in terms)
        pattern = f"(?<![A-Za-z0-9])(?:{alternation})(?![A-Za-z0-9])"
        return re.compile(pattern, flags=re.IGNORECASE)

    def _compile_numeric_regex(self) -> re.Pattern:
        return re.compile(r"(?<![A-Za-z0-9])\d+(?![A-Za-z0-9])")

    def _analyze_glued_names(self, protect_set_df: pd.DataFrame):
        logger.info("Analyzing glued names...")
        glued_df = self.phase_one_df[self.phase_one_df['is_glued']].copy()
        if glued_df.empty:
            logger.info("No glued names found to analyze.")
            return pd.DataFrame()

        p_chunks = sorted(protect_set_df['chunk'].tolist(), key=len, reverse=True)
        
        # Compile class regexes
        class_regexes = {
            'GUID': self._compile_guid_regex(),
            'TECH': self._compile_tech_regex(self.tech_terms),
            'ENV': self._compile_term_regex(self.env_terms),
            'REG': self._compile_term_regex(self.reg_terms),
            'NUM': self._compile_numeric_regex()
        }
        class_precedence = ['GUID', 'TECH', 'ENV', 'REG', 'NUM']

        results = []
        for _, row in glued_df.iterrows():
            name = unicodedata.normalize("NFKC", row[ResourcesPerDayJsonColumns.RESOURCE_NAME])
            
            # Find non-overlapping P-set hits
            protected_hits = []
            covered = [False] * len(name)
            for chunk in p_chunks:
                for match in re.finditer(re.escape(chunk), name, re.IGNORECASE):
                    start, end = match.span()
                    if not any(covered[start:end]):
                        protected_hits.append({"chunk": chunk, "start": start, "end": end})
                        for i in range(start, end): covered[i] = True
            
            # Coverage test on the remainder
            glued_explained = True
            coverage_fail_offset = -1
            coverage_sequence = []
            
            if not protected_hits and not self.settings.AUDIT_GLUED_COVERAGE_NO_P_HITS:
                glued_explained = False
            else:
                i = 0
                while i < len(name):
                    if covered[i] or name[i].isspace():
                        i += 1
                        continue
                    
                    best_match = None
                    best_cls = None
                    for cls in class_precedence:
                        match = class_regexes[cls].match(name, i)
                        if match:
                            # Longest-match tie-breaking is implicitly handled by class_precedence order
                            # if lengths are equal. For true longest match, this is correct.
                            if not best_match or len(match.group(0)) > len(best_match.group(0)):
                                best_match = match
                                best_cls = cls
                    
                    if best_match:
                        start, end = best_match.span()
                        coverage_sequence.append({"class": best_cls, "start": start, "end": end})
                        for j in range(start, end): covered[j] = True
                        i = end
                    else:
                        glued_explained = False
                        coverage_fail_offset = i
                        break
            
            # Build masked string if explained
            glued_masked_string = None
            if glued_explained:
                temp_name = list(name)
                for item in coverage_sequence:
                    for i in range(item['start'], item['end']):
                        temp_name[i] = ''
                    temp_name[item['start']] = f"⟂{item['class']}⟂"
                glued_masked_string = "".join(temp_name)

            results.append({
                ResourcesPerDayJsonColumns.RESOURCE_ID: row[ResourcesPerDayJsonColumns.RESOURCE_ID],
                'glued_explained': glued_explained,
                'protected_hits': json.dumps(protected_hits),
                'coverage_sequence': json.dumps(coverage_sequence),
                'coverage_fail_offset': coverage_fail_offset,
                'glued_masked_string': glued_masked_string
            })

        glued_results_df = pd.DataFrame(results)
        output_path = self.audit_path / f"glued_phase2_results.{self.year}_{self.month}.p2.csv"
        self.DataManager.write_csv(glued_results_df, output_path)
        logger.info(f"Glued name analysis saved to {output_path} with {len(glued_results_df)} records.")
        return glued_results_df

    def _generate_corpus_rollups(self, glued_results_df: pd.DataFrame):
        logger.info("Generating corpus roll-ups...")

        # Merge Phase 1 and Phase 2 results
        if glued_results_df.empty:
            merged_df = self.phase_one_df.copy()
            merged_df['glued_explained'] = pd.NA
        else:
            merged_df = pd.merge(self.phase_one_df, glued_results_df, on=ResourcesPerDayJsonColumns.RESOURCE_ID, how='left')

        scopes = {
            'overall': [None],
            ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME: merged_df[ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME].unique(),
            ResourcesPerDayJsonColumns.RESOURCE_GROUP: merged_df[ResourcesPerDayJsonColumns.RESOURCE_GROUP].unique(),
            ResourcesPerDayJsonColumns.BILLING_ACCOUNT_NAME: merged_df[ResourcesPerDayJsonColumns.BILLING_ACCOUNT_NAME].unique()
        }

        summary_records = []
        for scope_type, scope_values in scopes.items():
            for scope_value in scope_values:
                if scope_type == 'overall':
                    scope_df = merged_df
                    logger.info(f"Processing scope: overall")
                else:
                    scope_df = merged_df[merged_df[scope_type] == scope_value]
                    logger.info(f"Processing scope: {scope_type} = {scope_value}")

                if scope_df.empty:
                    continue

                # Calculate metrics
                record = {
                    'scope_type': scope_type, 
                    'scope_value': scope_value if scope_value else 'overall',
                    'n_resources': len(scope_df),
                    'top_tech_tokens': '',
                    'top_env_tokens': '',
                    'top_reg_tokens': ''
                }
                
                for col in ['pct_removed', 'residual_len', 'entropy_orig', 'entropy_resid']:
                    record[f'{col}_mean'] = scope_df[col].mean()
                    record[f'{col}_median'] = scope_df[col].median()
                record['pct_removed_p90'] = scope_df['pct_removed'].quantile(0.9)
                record['residual_len_p10'] = scope_df['residual_len'].quantile(0.1)

                for flag in ['overstrip_flag', 'acronym_only_residual', 'heavy_scaffold', 'is_glued', 'env_conflict']:
                    record[f'{flag}_rate'] = scope_df[flag].mean()

                glued_rows = scope_df[scope_df['is_glued'] == True]
                if not glued_rows.empty:
                    record['glued_explained_rate'] = glued_rows['glued_explained'].mean()
                    record['embedded_env_rate'] = glued_rows['embedded_env_list'].apply(lambda x: len(x) > 0).mean()
                else:
                    record['glued_explained_rate'] = np.nan
                    record['embedded_env_rate'] = np.nan

                # Top token analysis
                for term_type, terms in self.exclusion_sets.items():
                    if not terms:
                        record[f'top_{term_type.lower()}_tokens'] = ''
                        continue
                    
                    regex = self._compile_tech_regex(terms) if term_type == 'TECH' else self._compile_term_regex(terms)
                    token_counts = Counter()
                    for name in scope_df[ResourcesPerDayJsonColumns.RESOURCE_NAME].dropna():
                        norm_name = unicodedata.normalize("NFKC", name).lower()
                        for match in regex.finditer(norm_name):
                            token_counts[match.group(0)] += 1
                    
                    top_tokens = sorted(token_counts.items(), key=lambda x: (-x[1], -len(x[0]), x[0]))[:self.settings.AUDIT_TOP_TOKENS_COUNT]
                    record[f'top_{term_type.lower()}_tokens'] = '|'.join([f'{t}:{c}' for t, c in top_tokens])

                summary_records.append(record)

        summary_df = pd.DataFrame(summary_records)
        output_path = self.audit_path / f"corpus_readiness_summary.{self.year}_{self.month}.p2.csv"
        self.DataManager.write_csv(summary_df, output_path)
        logger.info(f"Corpus readiness summary saved to {output_path}.")

    def _generate_suggested_entities(self, protect_set_df: pd.DataFrame):
        """Generates the suggested entities file for phase two."""
        logger.info("Generating suggested entities for Phase 2...")
        suggested_entities_generator = SuggestedEntities(
            phase_one_df=self.phase_one_df,
            protect_set_df=protect_set_df,
            output_dir=self.audit_path,
            year=self.year,
            month=self.month
        )
        suggested_entities_generator.generate_yaml()

