-- =============================================================================
-- Mock Seed Data for ACME_DW
-- =============================================================================
-- Populates sample data so the framework can be tested end-to-end.
-- Replace 'your_snowflake_login' with your actual Snowflake username.
-- =============================================================================

USE DATABASE DEV_ACME_DW;

-- ─── Organization Hierarchy ──────────────────────────────────────────────────

INSERT INTO corp.dim_org_hierarchy
    (region_code, region_name, department_code, department_name, team_code, team_name)
VALUES
    ('R01', 'Northeast',  'D01', 'Sales',       'T01', 'Enterprise Sales'),
    ('R01', 'Northeast',  'D01', 'Sales',       'T02', 'SMB Sales'),
    ('R01', 'Northeast',  'D02', 'Operations',  'T03', 'Logistics'),
    ('R01', 'Northeast',  'D02', 'Operations',  'T04', 'Fulfillment'),
    ('R02', 'Southeast',  'D03', 'Sales',       'T05', 'Enterprise Sales'),
    ('R02', 'Southeast',  'D03', 'Sales',       'T06', 'SMB Sales'),
    ('R02', 'Southeast',  'D04', 'Operations',  'T07', 'Logistics'),
    ('R03', 'Midwest',    'D05', 'Sales',       'T08', 'Enterprise Sales'),
    ('R03', 'Midwest',    'D05', 'Sales',       'T09', 'SMB Sales'),
    ('R03', 'Midwest',    'D06', 'Operations',  'T10', 'Fulfillment'),
    ('R04', 'West',       'D07', 'Sales',       'T11', 'Enterprise Sales'),
    ('R04', 'West',       'D07', 'Sales',       'T12', 'SMB Sales'),
    ('R04', 'West',       'D08', 'Operations',  'T13', 'Logistics'),
    ('R04', 'West',       'D08', 'Operations',  'T14', 'Fulfillment');


-- ─── Customers ───────────────────────────────────────────────────────────────

INSERT INTO corp.dim_customer
    (account_number, customer_name, region_code, shipping_address, payment_terms)
VALUES
    ('ACCT-1001', 'Acme Manufacturing',    'R01', '123 Industrial Ave, Boston, MA',     'NET-30'),
    ('ACCT-1002', 'Beta Technologies',     'R01', '456 Tech Dr, New York, NY',          'NET-45'),
    ('ACCT-1003', 'Coastal Logistics',     'R02', '789 Harbor Blvd, Miami, FL',         'NET-30'),
    ('ACCT-1004', 'Delta Distribution',    'R02', '321 Warehouse Ln, Atlanta, GA',      'NET-60'),
    ('ACCT-1005', 'Echo Enterprises',      'R03', '654 Commerce St, Chicago, IL',       'NET-30'),
    ('ACCT-1006', 'Frontier Foods',        'R03', '987 Market Rd, Detroit, MI',         'NET-15'),
    ('ACCT-1007', 'Global Goods Inc.',     'R04', '111 Pacific Way, San Francisco, CA', 'NET-30'),
    ('ACCT-1008', 'Horizon Healthcare',    'R04', '222 Sunset Blvd, Los Angeles, CA',   'NET-45'),
    ('ACCT-1009', 'Infinity Imports',      'R01', '333 Trade Center, Philadelphia, PA', 'NET-30'),
    ('ACCT-1010', 'Jupiter Services',      'R03', '444 Lake Shore Dr, Milwaukee, WI',   'NET-30');


-- ─── Fiscal Calendar (2023-2025) ─────────────────────────────────────────────

INSERT INTO analytics.dim_fiscal_calendar
SELECT
    DATEADD(DAY, seq4(), '2023-01-01')::DATE      AS calendar_date,
    YEAR(calendar_date)                             AS fiscal_year,
    CONCAT('Q', QUARTER(calendar_date))             AS fiscal_quarter,
    MONTH(calendar_date)                            AS fiscal_month,
    WEEKOFYEAR(calendar_date)                       AS fiscal_week,
    DAYNAME(calendar_date)                          AS day_of_week,
    CASE WHEN DAYOFWEEK(calendar_date) BETWEEN 1 AND 5
         THEN TRUE ELSE FALSE END                   AS is_business_day
FROM TABLE(GENERATOR(ROWCOUNT => 1096));  -- ~3 years


-- ─── Orders ──────────────────────────────────────────────────────────────────

INSERT INTO analytics.fact_orders
    (order_id, customer_key, order_date, order_status, order_total, line_item_count)
SELECT
    CONCAT('ORD-', LPAD(seq4() + 1, 6, '0'))       AS order_id,
    UNIFORM(1, 10, RANDOM())                        AS customer_key,
    DATEADD(DAY, -UNIFORM(0, 730, RANDOM()), CURRENT_DATE()) AS order_date,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'PENDING'
        WHEN 2 THEN 'SHIPPED'
        WHEN 3 THEN 'DELIVERED'
        WHEN 4 THEN 'CANCELLED'
        ELSE        'RETURNED'
    END                                             AS order_status,
    ROUND(UNIFORM(50, 50000, RANDOM())::DECIMAL(12,2) / 100, 2)  AS order_total,
    UNIFORM(1, 20, RANDOM())                        AS line_item_count
FROM TABLE(GENERATOR(ROWCOUNT => 5000));


-- ─── Revenue ─────────────────────────────────────────────────────────────────

INSERT INTO analytics.fact_revenue
    (customer_key, transaction_date, revenue_type, revenue_amount, cost_amount)
SELECT
    UNIFORM(1, 10, RANDOM())                        AS customer_key,
    DATEADD(DAY, -UNIFORM(0, 730, RANDOM()), CURRENT_DATE()) AS transaction_date,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'Product Sales'
        WHEN 2 THEN 'Service Revenue'
        WHEN 3 THEN 'Subscription'
        ELSE        'Consulting'
    END                                             AS revenue_type,
    ROUND(UNIFORM(100, 100000, RANDOM())::DECIMAL(12,2) / 100, 2) AS revenue_amount,
    ROUND(UNIFORM(50,  70000, RANDOM())::DECIMAL(12,2) / 100, 2)  AS cost_amount
FROM TABLE(GENERATOR(ROWCOUNT => 10000));


-- ─── Security: Grant yourself access to all regions ──────────────────────────
-- ⚠️  Replace 'your_snowflake_login' with your actual username

INSERT INTO security.user_access_map
    (employee_login, region_code, access_level, granted_by)
VALUES
    ('your_snowflake_login', 'R01', 'READ', 'ADMIN'),
    ('your_snowflake_login', 'R02', 'READ', 'ADMIN'),
    ('your_snowflake_login', 'R03', 'READ', 'ADMIN'),
    ('your_snowflake_login', 'R04', 'READ', 'ADMIN');


-- ─── Security: Make yourself a report admin ──────────────────────────────────

INSERT INTO security.app_role_helper
    (user_name, role_level, is_active)
VALUES
    ('your_snowflake_login', 'REPORT_ADMIN', TRUE);


-- ─── App Metadata: Sample folder structure ───────────────────────────────────

INSERT INTO app_metadata.global_filter_folders
    (id, parent_id, name, description, allowed_roles)
VALUES
    ('F001', NULL,   'Shared Reports',    'Company-wide shared filter presets', ARRAY_CONSTRUCT('PUBLIC')),
    ('F002', 'F001', 'Northeast Team',    'Northeast region presets',           ARRAY_CONSTRUCT('PUBLIC')),
    ('F003', 'F001', 'West Team',         'West region presets',               ARRAY_CONSTRUCT('PUBLIC')),
    ('F004', NULL,   'My Saved Queries',  'Personal saved queries',            ARRAY_CONSTRUCT('PUBLIC'));


-- ─── Verify Data ─────────────────────────────────────────────────────────────

SELECT 'dim_org_hierarchy'   AS table_name, COUNT(*) AS row_count FROM corp.dim_org_hierarchy
UNION ALL
SELECT 'dim_customer',       COUNT(*) FROM corp.dim_customer
UNION ALL
SELECT 'dim_fiscal_calendar', COUNT(*) FROM analytics.dim_fiscal_calendar
UNION ALL
SELECT 'fact_orders',        COUNT(*) FROM analytics.fact_orders
UNION ALL
SELECT 'fact_revenue',       COUNT(*) FROM analytics.fact_revenue
UNION ALL
SELECT 'user_access_map',   COUNT(*) FROM security.user_access_map
ORDER BY table_name;
