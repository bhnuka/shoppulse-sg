# Data Contract — ACRA Corporate Entities (ShopPulse SG)

## Source
- Dataset collection: ACRA Information on Corporate Entities (collection_id=2)
- API: data.gov.sg datastore
- Refresh cadence: monthly (per collection metadata)
- License note: Refer to data.gov.sg dataset terms; sensitive columns removed per Companies Act Section 12(2A).

## Grain
- One row per legal entity (UEN), deduplicated across A–Z datasets.
- Dedup rule: keep latest `registration_incorporation_date` when duplicates appear; if missing, keep the first seen.

## Columns (selected subset)
| Column | Type | Meaning |
|---|---|---|
| uen | String | Unique Entity Number (primary key) |
| entity_name | String | Registered entity name |
| entity_status_description | String (LowCardinality) | Entity status (e.g., Live/Terminated) |
| entity_type_description | String (LowCardinality) | Entity type (e.g., Sole Proprietorship/Partnership) |
| business_constitution_description | String (LowCardinality) | Business constitution |
| company_type_description | String, Nullable | Company type (nullable) |
| registration_incorporation_date | Date, Nullable | Registration / incorporation date |
| uen_issue_date | Date, Nullable | UEN issue date |
| primary_ssic_code | String, Nullable | Primary SSIC code |
| secondary_ssic_code | String, Nullable | Secondary SSIC code |
| postal_code | String, Nullable | 6-digit postal code (normalized) |

## Cleaning rules
- Convert literal "na" (case-insensitive) and empty strings to NULL for all kept fields.
- `postal_code`:
  - keep as String
  - if digits only and length < 6, left-pad with zeros to 6
  - if not digits or length != 6 after padding, set NULL
- SSIC codes (primary/secondary):
  - trim whitespace
  - if digits only and length < 5, left-pad to 5
  - if not digits-only, keep as-is (or NULL if blank)
- Dates:
  - parse to ISO Date (YYYY-MM-DD)
  - invalid values -> NULL

## Quality rules
- Missing columns in source datasets are filled with NULLs.
- Duplicates across datasets are resolved by latest registration date.
- Address-related columns are dropped to avoid storing full addresses.

## Privacy notes
- Only the strict subset above is retained; address components, former names, and audit firm fields are dropped.
- Postal code is retained for geospatial aggregation only.
