-- Hierarchy Building Module
-- Creates recursive account hierarchy with cycle prevention

CREATE TEMP TABLE tmp_hierarchy AS
WITH RECURSIVE account_hierarchy AS (
    -- Base case: Direct parent-child relationships
    SELECT 
        parent_account_id,
        child_account_id,
        parent_account_id AS root_account_id,
        1 AS level_depth,
        CAST(parent_account_id || '/' || child_account_id AS VARCHAR(4000)) AS path_trace
    FROM tmp_normalized_relationships
    
    UNION ALL
    
    -- Recursive case: Build deeper levels
    SELECT 
        rel.parent_account_id,
        rel.child_account_id,
        hier.root_account_id,
        hier.level_depth + 1,
        CAST(hier.path_trace || '/' || rel.child_account_id AS VARCHAR(4000))
    FROM tmp_normalized_relationships rel
    INNER JOIN account_hierarchy hier 
        ON rel.parent_account_id = hier.child_account_id
    WHERE hier.level_depth < 12  -- Prevent excessive depth
        AND hier.path_trace NOT LIKE '%/' || rel.child_account_id || '/%'  -- Cycle prevention
        AND hier.path_trace NOT LIKE rel.child_account_id || '/%'
        AND hier.path_trace != rel.child_account_id
)
SELECT 
    parent_account_id,
    child_account_id,
    root_account_id,
    level_depth,
    path_trace
FROM account_hierarchy;

CREATE TEMP TABLE tmp_partner_flags AS
SELECT 
    h.parent_account_id,
    h.child_account_id,
    h.root_account_id,
    h.level_depth,
    h.path_trace,
    CASE WHEN p_parent.account_id IS NOT NULL THEN 1 ELSE 0 END AS parent_is_partner,
    CASE WHEN p_child.account_id IS NOT NULL THEN 1 ELSE 0 END AS child_is_partner,
    CASE WHEN p_root.account_id IS NOT NULL THEN 1 ELSE 0 END AS root_is_partner
FROM tmp_hierarchy h
LEFT JOIN tmp_entity_types p_parent 
    ON h.parent_account_id = p_parent.account_id 
    AND p_parent.entity_type = 'PARTNER'
LEFT JOIN tmp_entity_types p_child 
    ON h.child_account_id = p_child.account_id 
    AND p_child.entity_type = 'PARTNER'
LEFT JOIN tmp_entity_types p_root 
    ON h.root_account_id = p_root.account_id 
    AND p_root.entity_type = 'PARTNER';

CREATE TEMP TABLE tmp_root_detection AS
SELECT 
    parent_account_id,
    child_account_id,
    root_account_id,
    level_depth,
    path_trace,
    parent_is_partner,
    child_is_partner,
    root_is_partner,
    CASE 
        WHEN parent_account_id = root_account_id THEN 1 
        ELSE 0 
    END AS is_root_manager,
    CASE 
        WHEN parent_is_partner = 1 AND level_depth = 1 THEN 1
        WHEN root_is_partner = 1 AND parent_account_id = root_account_id THEN 1
        ELSE 0 
    END AS is_root_partner_manager
FROM tmp_partner_flags;
