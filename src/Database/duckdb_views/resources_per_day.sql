CREATE OR REPLACE VIEW resources_per_day AS
WITH
  costs_and_tags_per_day AS (
    SELECT
      ResourceId,
      year,
      month,
      day,
      ANY_VALUE(Tags) AS Tags,
      SUM(EffectiveCost) AS TotalEffectiveCost
    FROM all_costs_view
    GROUP BY
      ResourceId,
      year,
      month,
      day
  ),
  distinct_metadata_per_day AS (
    SELECT DISTINCT
      ResourceId,
      regexp_extract(ResourceId, '.*/resourcegroups/([^/]+)/.*', 1) AS ResourceGroup,
      ResourceName,
      RegionId,
      RegionName,
      SubAccountId,
      SubAccountName,
      ResourceType,
      BillingAccountId,
      BillingAccountName,
      ProviderName,
      year,
      month,
      day
    FROM all_costs_view
  )
SELECT
  c.ResourceId,
  t.ResourceGroup,
  t.ResourceName,
  t.RegionId,
  t.RegionName,
  t.SubAccountId,
  t.SubAccountName,
  t.ResourceType,
  t.BillingAccountId,
  t.BillingAccountName,
  t.ProviderName,
  c.Tags,
  c.TotalEffectiveCost,
  c.year,
  c.month,
  c.day
FROM costs_and_tags_per_day AS c
LEFT JOIN distinct_metadata_per_day AS t
  ON c.ResourceId  = t.ResourceId
 AND c.year        = t.year
 AND c.month       = t.month
 AND c.day         = t.day
;