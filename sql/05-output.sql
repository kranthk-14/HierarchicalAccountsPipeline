-- Final Output Module
-- Creates consolidated star schema tables

CREATE TEMP TABLE tmp_final_consolidated AS
WITH hierarchy_data AS (
    SELECT 
        manager_account_id AS account_id,
        entity_id,
        marketplace_id,
        period_start AS effective_start_date,
        period_end AS effective_end_date,
        level_depth,
        root_account_id,
        calculated_root_flag AS is_root_manager,
        calculated_partner_flag AS is_root_partner_manager,
        source,
        'HIERARCHY' AS record_type
    FROM tmp_inheritance_logic
    WHERE manager_account_id IS NOT NULL
),
standalone_data AS (
    SELECT 
        account_id,
        NULL AS entity_id,
        NULL AS marketplace_id,
        inherit_start_date AS effective_start_date,
        inherit_end_date AS effective_end_date,
        0 AS level_depth,
        account_id AS root_account_id,
        inherited_root_flag AS is_root_manager,
        inherited_root_flag AS is_root_partner_manager,
        'INHERITANCE' AS source,
        'STANDALONE' AS record_type
    FROM tmp_post_hierarchy
),
combined_data AS (
    SELECT * FROM hierarchy_data
    UNION ALL
    SELECT * FROM standalone_data
),
deduplicated_final AS (
    SELECT 
        account_id,
        entity_id,
        marketplace_id,
        effective_start_date,
        effective_end_date,
        level_depth,
        root_account_id,
        is_root_manager,
        is_root_partner_manager,
        source,
        record_type,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, entity_id, marketplace_id, 
                         effective_start_date, effective_end_date
            ORDER BY 
                is_root_partner_manager DESC,
                CASE source 
                    WHEN 'API' THEN 1
                    WHEN 'INHERITANCE' THEN 2
                    WHEN 'MANUAL' THEN 3
                    ELSE 4
                END,
                level_depth ASC
        ) AS final_rank
    FROM combined_data
)
SELECT 
    account_id,
    entity_id,
    marketplace_id,
    effective_start_date,
    effective_end_date,
    level_depth,
    root_account_id,
    is_root_manager,
    is_root_partner_manager,
    source,
    record_type
FROM deduplicated_final
WHERE final_rank = 1;

-- Create final star schema fact table
CREATE TABLE fact_account_hierarchy AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY account_id, effective_start_date) AS hierarchy_key,
    account_id,
    entity_id,
    marketplace_id,
    root_account_id,
    level_depth,
    effective_start_date,
    effective_end_date,
    is_root_manager,
    is_root_partner_manager,
    source,
    record_type,
    CURRENT_TIMESTAMP AS created_at
FROM tmp_final_consolidated;

-- Create dimension tables
CREATE TABLE dim_accounts AS
SELECT DISTINCT
    account_id,
    et.entity_type,
    et.is_approved,
    et.is_external,
    CURRENT_TIMESTAMP AS created_at
FROM tmp_final_consolidated f
LEFT JOIN tmp_entity_types et ON f.account_id = et.account_id;

CREATE TABLE dim_date AS
WITH date_spine AS (
    SELECT DISTINCT effective_start_date AS date_key FROM tmp_final_consolidated
    UNION
    SELECT DISTINCT effective_end_date AS date_key FROM tmp_final_consolidated
)
SELECT 
    date_key,
    EXTRACT(year FROM date_key) AS year_num,
    EXTRACT(month FROM date_key) AS month_num,
    EXTRACT(day FROM date_key) AS day_num,
    EXTRACT(quarter FROM date_key) AS quarter_num
FROM date_spine
WHERE date_key IS NOT NULL;
