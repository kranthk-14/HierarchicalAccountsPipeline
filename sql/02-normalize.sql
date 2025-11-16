-- Data Normalization Module
-- Cleans and validates extracted data

CREATE TEMP TABLE tmp_normalized_relationships AS
WITH deduplicated AS (
    SELECT 
        parent_account_id,
        child_account_id,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY parent_account_id, child_account_id 
            ORDER BY updated_at DESC, created_at DESC
        ) AS rn
    FROM tmp_account_relationships
    WHERE parent_account_id != child_account_id
),
loop_prevention AS (
    SELECT 
        parent_account_id,
        child_account_id,
        created_at,
        updated_at
    FROM deduplicated
    WHERE rn = 1
        AND parent_account_id IS NOT NULL
        AND child_account_id IS NOT NULL
        AND LENGTH(parent_account_id) > 0
        AND LENGTH(child_account_id) > 0
)
SELECT * FROM loop_prevention;

CREATE TEMP TABLE tmp_entity_types AS
SELECT DISTINCT
    partner_account_id AS account_id,
    'PARTNER' AS entity_type,
    is_approved,
    is_external
FROM tmp_partner_accounts
UNION ALL
SELECT DISTINCT
    manager_account_id AS account_id,
    'MANAGER' AS entity_type,
    1 AS is_approved,
    0 AS is_external
FROM tmp_account_mappings;

CREATE TEMP TABLE tmp_clean_mappings AS
SELECT 
    manager_account_id,
    entity_id,
    marketplace_id,
    effective_start_date,
    effective_end_date,
    source,
    advertiser_id,
    ROW_NUMBER() OVER (
        PARTITION BY manager_account_id, entity_id, marketplace_id, effective_start_date
        ORDER BY 
            CASE UPPER(source)
                WHEN 'API' THEN 1
                WHEN 'MANUAL' THEN 2
                ELSE 3
            END,
            effective_end_date DESC
    ) AS priority_rank
FROM tmp_account_mappings
WHERE effective_start_date <= effective_end_date;
