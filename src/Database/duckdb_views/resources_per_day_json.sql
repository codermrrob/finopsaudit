CREATE OR REPLACE VIEW resources_per_day_json AS
SELECT
    ResourceId,
    ResourceGroup,
    ResourceName,
    RegionId,
    RegionName,
    SubAccountId,
    SubAccountName,
    ResourceType,
    BillingAccountId,
    BillingAccountName,
    ProviderName,
    TRY_CAST(Tags AS JSON) AS Tags,
    TotalEffectiveCost,
    year,
    month,
    day
FROM resources_per_day;