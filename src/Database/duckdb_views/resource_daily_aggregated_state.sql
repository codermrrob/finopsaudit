/*
------------------------------------------------------------------------------
View: resource_daily_aggregated_state

Purpose:
    Provides a canonical, daily snapshot of each resource, enriched with a
    normalized representation of its tags and two hash fingerprints to allow
    for efficient change detection and historical state tracking.

    This is used to keep the knowledge graph updated in turn used by AI reporting agent

How it works:
  1. Source
     - Reads from `resources_per_day` (one row per resource_id per day).
     - Brings in identifiers, metadata, and raw tags JSON.

  2. CTE: processed_rows
     - Normalizes the tags JSON into a deterministic semicolon-delimited string:
         • Converts JSON into {Key,Value} structs.
         • Unnests into rows.
         • Filters out null keys.
         • Aggregates as "key=value" pairs sorted by Key and Value.
       → Ensures tags with different JSON key orders still normalize to the
         same string (e.g., {"env":"prod","team":"ops"} == {"team":"ops","env":"prod"}).

  3. Final SELECT
     - Constructs a proper date column (`SnapshotDate`) from year/month/day.
     - Emits all key resource attributes along with the raw tags and normalized tag string.
     - Adds two MD5 hashes:
         • state_hash:
             - Hash of resource_id, resource_name, sub_account_id, region_id,
               normalized_tags_string.
             - Detects material changes to a given resource’s identity/state.
         • full_state_hash:
             - Hash of resource_name, resource_type, sub_account_id, region_id,
               normalized_tags_string.
             - Ignores resource_id, useful for clustering or comparing resources
               across environments/tenants where IDs may differ.

Use cases:
    • Daily state tracking of resources with normalized tag values.
    • Detecting configuration or tag drift between days.
    • Supporting slowly changing dimension (SCD) history tables.
    • Simplifying joins and comparisons using deterministic hashes.

Notes:
    • If year/month/day columns are already integers, casts may be redundant.
    • MD5 is used for compactness but carries a small collision risk; consider
      SHA256 if absolute uniqueness is required.
    • Be mindful of delimiters ('|#|', '=', ';'); escape them if resource data
      can contain these characters.
------------------------------------------------------------------------------
*/

CREATE OR REPLACE VIEW resource_daily_aggregated_state AS
WITH processed_rows AS (
    SELECT
        rpd.year,
        rpd.month,
        rpd.day,
        rpd.ResourceId,
        rpd.ResourceGroup,
        rpd.ResourceName,
        rpd.ResourceType,
        rpd.SubAccountId,
        rpd.SubAccountName,
        rpd.RegionId,
        rpd.RegionName,
        rpd.BillingAccountId,
        rpd.BillingAccountName,
        rpd.ProviderName,
        rpd.Tags,
        (
            SELECT string_agg(
                       (tag_struct."Key" || '=') || COALESCE(tag_struct."Value", ''),
                       ';'
                       ORDER BY tag_struct."Key", tag_struct."Value"
                   )
            FROM unnest(
                     json_transform(
                         rpd.Tags,
                         '[{"Key": "VARCHAR", "Value": "VARCHAR"}]'
                     )
                 ) AS t(tag_struct)
            WHERE tag_struct."Key" IS NOT NULL
        ) AS normalized_tags_string
    FROM resources_per_day_json AS rpd -- Source from the JSON-casted view
)
SELECT
    make_date(
        pr.year,
        CAST(pr.month AS INTEGER),
        CAST(pr.day AS INTEGER)
    ) AS SnapshotDate,
    pr.ResourceId,
    pr.ResourceGroup,
    pr.ResourceName,
    pr.ResourceType,
    pr.SubAccountId,
    pr.SubAccountName,
    pr.RegionId,
    pr.RegionName,
    pr.BillingAccountId,
    pr.BillingAccountName,
    pr.ProviderName,
    pr.Tags,
    md5(
        (
              pr.ResourceId
            || '|#|' || COALESCE(pr.ResourceGroup, '')
            || '|#|' || COALESCE(pr.ResourceName, '')
            || '|#|' || COALESCE(pr.SubAccountId, '')
            || '|#|' || COALESCE(pr.BillingAccountName, '')
            || '|#|' || COALESCE(pr.RegionId, '')
            || '|#|' || COALESCE(pr.normalized_tags_string, '')
        )
    ) AS state_hash,
    md5(
        (
              COALESCE(pr.ResourceGroup, '')
            || '|#|' || COALESCE(pr.ResourceName, '')
            || '|#|' || COALESCE(pr.ResourceType, '')
            || '|#|' || COALESCE(pr.SubAccountId, '')
            || '|#|' || COALESCE(pr.BillingAccountName, '')
            || '|#|' || COALESCE(pr.RegionId, '')
            || '|#|' || COALESCE(pr.normalized_tags_string, '')
        )
    ) AS full_state_hash
FROM processed_rows AS pr;
