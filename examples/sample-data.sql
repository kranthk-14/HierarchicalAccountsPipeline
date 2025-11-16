-- Sample Data for Testing Hierarchical Accounts Pipeline

-- Sample account relationships
INSERT INTO raw_entity_relationships VALUES
('MA_ROOT_001', 'MA_CHILD_001', '2024-01-01 00:00:00', '2024-01-01 00:00:00', 'N'),
('MA_ROOT_001', 'MA_CHILD_002', '2024-01-01 00:00:00', '2024-01-01 00:00:00', 'N'),
('MA_CHILD_001', 'MA_GRANDCHILD_001', '2024-02-01 00:00:00', '2024-02-01 00:00:00', 'N'),
('MA_PARTNER_001', 'MA_SUB_001', '2024-01-15 00:00:00', '2024-01-15 00:00:00', 'N');

-- Sample partner accounts
INSERT INTO raw_partner_accounts VALUES
('MA_ROOT_001', 'GROUP_001', 'Y', 'N', 'N'),
('MA_PARTNER_001', 'GROUP_002', 'Y', 'Y', 'N'),
('MA_CHILD_001', 'GROUP_001', 'Y', 'N', 'N');

-- Sample account mappings
INSERT INTO raw_partner_adv_entity_mapping VALUES
('MA_ROOT_001', 'ADV_001', 1, '2024-01-01', '2024-06-30', 'API', 'ADVERTISER_001', 'Y'),
('MA_ROOT_001', 'ADV_002', 1, '2024-07-01', '2099-12-31', 'API', 'ADVERTISER_002', 'Y'),
('MA_CHILD_001', 'ADV_003', 1, '2024-01-01', '2024-03-31', 'MANUAL', 'ADVERTISER_003', 'Y'),
('MA_PARTNER_001', 'ADV_004', 2, '2024-01-15', '2099-12-31', 'API', 'ADVERTISER_004', 'Y');

-- Expected output examples:

-- Hierarchy relationships
-- MA_ROOT_001 -> MA_CHILD_001 (level 1)
-- MA_ROOT_001 -> MA_CHILD_002 (level 1)  
-- MA_ROOT_001 -> MA_GRANDCHILD_001 (level 2, via MA_CHILD_001)

-- Temporal inheritance example:
-- MA_CHILD_001 manages ADV_003 until 2024-03-31
-- After hierarchy ends, MA_CHILD_001 becomes root manager if partner-registered

-- Star schema output:
-- fact_account_hierarchy: Contains all account relationships with time periods
-- dim_accounts: Account metadata (partner status, type)
-- dim_date: Date dimension for temporal analysis
