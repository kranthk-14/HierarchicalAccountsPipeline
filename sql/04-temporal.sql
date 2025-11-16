-- Temporal Processing Module
-- Handles time-based inheritance and date breakpoints

CREATE TEMP TABLE tmp_date_breakpoints AS
WITH all_dates AS (
    SELECT DISTINCT effective_start_date AS breakpoint_date
    FROM tmp_clean_mappings
    WHERE priority_rank = 1
    UNION
    SELECT DISTINCT effective_end_date AS breakpoint_date  
    FROM tmp_clean_mappings
    WHERE priority_rank = 1
        AND effective_end_date != '2099-12-31'
    UNION
    SELECT DISTINCT DATE(created_at) AS breakpoint_date
    FROM tmp_normalized_relationships
),
ordered_dates AS (
    SELECT 
        breakpoint_date,
        LAG(breakpoint_date) OVER (ORDER BY breakpoint_date) AS prev_date,
        LEAD(breakpoint_date) OVER (ORDER BY breakpoint_date) AS next_date
    FROM all_dates
    WHERE breakpoint_date IS NOT NULL
)
SELECT 
    breakpoint_date AS period_start,
    COALESCE(
        DATEADD(day, -1, next_date), 
        '2099-12-31'
    ) AS period_end
FROM ordered_dates
ORDER BY breakpoint_date;

CREATE TEMP TABLE tmp_temporal_segments AS
SELECT 
    h.parent_account_id,
    h.child_account_id,
    h.root_account_id,
    h.level_depth,
    h.is_root_manager,
    h.is_root_partner_manager,
    bp.period_start,
    bp.period_end,
    m.manager_account_id,
    m.entity_id,
    m.marketplace_id,
    m.source
FROM tmp_root_detection h
CROSS JOIN tmp_date_breakpoints bp
LEFT JOIN tmp_clean_mappings m
    ON h.parent_account_id = m.manager_account_id
    AND m.priority_rank = 1
    AND bp.period_start >= m.effective_start_date
    AND bp.period_end <= m.effective_end_date;

CREATE TEMP TABLE tmp_inheritance_logic AS
SELECT 
    parent_account_id,
    child_account_id,
    root_account_id,
    level_depth,
    period_start,
    period_end,
    manager_account_id,
    entity_id,
    marketplace_id,
    source,
    CASE 
        WHEN manager_account_id IS NOT NULL THEN is_root_manager
        WHEN parent_is_partner = 1 AND manager_account_id IS NULL THEN 1
        ELSE 0 
    END AS calculated_root_flag,
    CASE 
        WHEN manager_account_id IS NOT NULL THEN is_root_partner_manager
        WHEN parent_is_partner = 1 AND child_is_partner = 1 AND manager_account_id IS NULL THEN 1
        ELSE 0 
    END AS calculated_partner_flag
FROM tmp_temporal_segments;

CREATE TEMP TABLE tmp_post_hierarchy AS
WITH hierarchy_ends AS (
    SELECT 
        child_account_id,
        MAX(period_end) AS hierarchy_end_date
    FROM tmp_inheritance_logic
    WHERE manager_account_id IS NOT NULL
    GROUP BY child_account_id
),
standalone_periods AS (
    SELECT 
        he.child_account_id AS account_id,
        DATEADD(day, 1, he.hierarchy_end_date) AS inherit_start_date,
        '2099-12-31' AS inherit_end_date,
        1 AS inherited_root_flag
    FROM hierarchy_ends he
    INNER JOIN tmp_entity_types et 
        ON he.child_account_id = et.account_id 
        AND et.entity_type = 'PARTNER'
    WHERE he.hierarchy_end_date < '2099-12-31'
)
SELECT * FROM standalone_periods;
