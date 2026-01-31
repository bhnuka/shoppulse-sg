-- ClickHouse schema for ShopPulse SG (ACRA registry intelligence)

CREATE TABLE IF NOT EXISTS acra_entities_raw (
    uen String COMMENT 'Unique Entity Number',
    entity_name String COMMENT 'Registered entity name',
    entity_status_description LowCardinality(String) COMMENT 'Entity status',
    entity_type_description LowCardinality(String) COMMENT 'Entity type',
    business_constitution_description LowCardinality(String) COMMENT 'Business constitution',
    company_type_description Nullable(String) COMMENT 'Company type',
    registration_incorporation_date Nullable(Date32) COMMENT 'Registration/incorporation date',
    uen_issue_date Nullable(Date32) COMMENT 'UEN issue date',
    primary_ssic_code Nullable(String) COMMENT 'Primary SSIC code',
    secondary_ssic_code Nullable(String) COMMENT 'Secondary SSIC code',
    postal_code Nullable(String) COMMENT 'Postal code',
    registration_month UInt32 MATERIALIZED toYYYYMM(registration_incorporation_date) COMMENT 'YYYYMM derived'
)
ENGINE = MergeTree
PARTITION BY registration_month
ORDER BY (registration_month, uen)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS dim_ssic (
    ssic_code String COMMENT 'SSIC code',
    ssic_description Nullable(String) COMMENT 'SSIC description',
    section Nullable(String) COMMENT 'SSIC section',
    division Nullable(String) COMMENT 'SSIC division'
)
ENGINE = MergeTree
ORDER BY ssic_code;

CREATE TABLE IF NOT EXISTS dim_subzone (
    subzone_id String COMMENT 'Subzone identifier',
    name String COMMENT 'Subzone name',
    planning_area_id Nullable(String) COMMENT 'Planning area identifier',
    geometry String COMMENT 'GeoJSON geometry'
)
ENGINE = MergeTree
ORDER BY subzone_id;

CREATE TABLE IF NOT EXISTS dim_planning_area (
    planning_area_id String COMMENT 'Planning area identifier',
    name String COMMENT 'Planning area name',
    geometry String COMMENT 'GeoJSON geometry'
)
ENGINE = MergeTree
ORDER BY planning_area_id;

CREATE TABLE IF NOT EXISTS dim_postal_geo (
    postal_code String COMMENT 'Postal code',
    latitude Nullable(Float64) COMMENT 'Latitude',
    longitude Nullable(Float64) COMMENT 'Longitude',
    subzone_id Nullable(String) COMMENT 'Subzone identifier',
    planning_area_id Nullable(String) COMMENT 'Planning area identifier',
    updated_at DateTime COMMENT 'Last updated timestamp'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY postal_code;

CREATE TABLE IF NOT EXISTS acra_entities_enriched (
    uen String COMMENT 'Unique Entity Number',
    entity_name String COMMENT 'Registered entity name',
    entity_status_description LowCardinality(String) COMMENT 'Entity status',
    entity_type_description LowCardinality(String) COMMENT 'Entity type',
    business_constitution_description LowCardinality(String) COMMENT 'Business constitution',
    company_type_description Nullable(String) COMMENT 'Company type',
    registration_incorporation_date Nullable(Date32) COMMENT 'Registration/incorporation date',
    uen_issue_date Nullable(Date32) COMMENT 'UEN issue date',
    primary_ssic_code Nullable(String) COMMENT 'Primary SSIC code (raw)',
    secondary_ssic_code Nullable(String) COMMENT 'Secondary SSIC code (raw)',
    primary_ssic_norm Nullable(String) COMMENT 'Primary SSIC normalized',
    secondary_ssic_norm Nullable(String) COMMENT 'Secondary SSIC normalized',
    postal_code Nullable(String) COMMENT 'Postal code',
    registration_month UInt32 COMMENT 'YYYYMM derived',
    latitude Nullable(Float64) COMMENT 'Latitude',
    longitude Nullable(Float64) COMMENT 'Longitude',
    subzone_id Nullable(String) COMMENT 'Subzone identifier',
    planning_area_id Nullable(String) COMMENT 'Planning area identifier'
)
ENGINE = MergeTree
PARTITION BY registration_month
ORDER BY (registration_month, coalesce(primary_ssic_norm, ''), coalesce(subzone_id, ''), uen)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS new_entities_monthly_by_ssic (
    registration_month UInt32,
    primary_ssic_norm Nullable(String),
    entity_status_description LowCardinality(String),
    new_entities UInt64
)
ENGINE = SummingMergeTree
PARTITION BY registration_month
ORDER BY (registration_month, coalesce(primary_ssic_norm, ''), entity_status_description);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_new_entities_monthly_by_ssic
TO new_entities_monthly_by_ssic
AS
SELECT
    registration_month,
    primary_ssic_norm,
    entity_status_description,
    count() AS new_entities
FROM acra_entities_enriched
GROUP BY registration_month, primary_ssic_norm, entity_status_description;

CREATE TABLE IF NOT EXISTS new_entities_monthly_by_subzone (
    registration_month UInt32,
    subzone_id Nullable(String),
    new_entities UInt64
)
ENGINE = SummingMergeTree
PARTITION BY registration_month
ORDER BY (registration_month, coalesce(subzone_id, ''));

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_new_entities_monthly_by_subzone
TO new_entities_monthly_by_subzone
AS
SELECT
    registration_month,
    subzone_id,
    count() AS new_entities
FROM acra_entities_enriched
GROUP BY registration_month, subzone_id;

CREATE TABLE IF NOT EXISTS new_entities_monthly_by_planning_area (
    registration_month UInt32,
    planning_area_id Nullable(String),
    new_entities UInt64
)
ENGINE = SummingMergeTree
PARTITION BY registration_month
ORDER BY (registration_month, coalesce(planning_area_id, ''));

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_new_entities_monthly_by_planning_area
TO new_entities_monthly_by_planning_area
AS
SELECT
    registration_month,
    planning_area_id,
    count() AS new_entities
FROM acra_entities_enriched
GROUP BY registration_month, planning_area_id;

CREATE TABLE IF NOT EXISTS top_ssic_by_area_month (
    registration_month UInt32,
    planning_area_id Nullable(String),
    primary_ssic_norm Nullable(String),
    new_entities UInt64
)
ENGINE = SummingMergeTree
PARTITION BY registration_month
ORDER BY (registration_month, coalesce(planning_area_id, ''), coalesce(primary_ssic_norm, ''));

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_ssic_by_area_month
TO top_ssic_by_area_month
AS
SELECT
    registration_month,
    planning_area_id,
    primary_ssic_norm,
    count() AS new_entities
FROM acra_entities_enriched
GROUP BY registration_month, planning_area_id, primary_ssic_norm;
