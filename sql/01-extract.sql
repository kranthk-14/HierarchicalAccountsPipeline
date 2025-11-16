-- Data Extraction Module
-- Creates temporary tables from source data

CREATE TEMP TABLE tmp_account_relationships AS
SELECT 
    entity_id_one AS parent_account_id,
    entity_id_two AS child_account_id,
    created_at,
    updated_at,
    CASE WHEN delete_flag = 'Y' THEN 1 ELSE 0 END AS is_deleted
FROM raw_entity_relationships
WHERE delete_flag != 'Y'
    AND entity_id_one IS NOT NULL
    AND entity_id_two IS NOT NULL;

CREATE TEMP TABLE tmp_partner_accounts AS
SELECT 
    partner_account_id,
    group_account_id,
    CASE WHEN is_registration_approved = 'Y' THEN 1 ELSE 0 END AS is_approved,
    CASE WHEN is_external_partner_account = 'Y' THEN 1 ELSE 0 END AS is_external,
    CASE WHEN is_disabled = 'Y' THEN 1 ELSE 0 END AS is_disabled
FROM raw_partner_accounts
WHERE is_disabled != 'Y';

CREATE TEMP TABLE tmp_account_mappings AS
SELECT 
    manager_account_id,
    entity_id,
    marketplace_id,
    effective_start_date,
    COALESCE(effective_end_date, '2099-12-31') AS effective_end_date,
    UPPER(source) AS source,
    advertiser_id,
    CASE WHEN is_linked = 'Y' THEN 1 ELSE 0 END AS is_linked
FROM raw_partner_adv_entity_mapping
WHERE is_linked = 'Y'
    AND effective_start_date IS NOT NULL;
