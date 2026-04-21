-- =============================================================================
-- Customer Orders Query Studio - SQL Template
-- =============================================================================
-- This query drives the Customer Orders dashboard.
-- The framework engine replaces:
--   {DB}              → environment-specific database (e.g., DEV_ACME_DW)
--   {current_user}    → logged-in Snowflake user
--   -- WHERE_PLACEHOLDER → dynamically assembled WHERE clause
--   {?filter:cond}    → optional conditional SQL (included only when filter has a value)
-- =============================================================================

WITH security_filters AS (
    SELECT DISTINCT region_code
    FROM {DB}.security.user_access_map
    WHERE LOWER(employee_login) = LOWER('{current_user}')
      AND is_active = TRUE
)

SELECT
    oh.region_name              AS REGION_NAME,
    oh.department_name          AS DEPARTMENT_NAME,
    c.customer_name             AS CUSTOMER_NAME,
    c.account_number            AS ACCOUNT_NUMBER,
    o.order_id                  AS ORDER_ID,
    o.order_date                AS ORDER_DATE,
    o.order_status              AS ORDER_STATUS,
    o.order_total               AS ORDER_TOTAL,
    o.line_item_count           AS LINE_ITEM_COUNT,
    c.shipping_address          AS SHIPPING_ADDRESS,
    c.payment_terms             AS PAYMENT_TERMS

FROM {DB}.analytics.fact_orders o
INNER JOIN {DB}.corp.dim_customer c
    ON o.customer_key = c.customer_key
INNER JOIN {DB}.corp.dim_org_hierarchy oh
    ON c.region_code = oh.region_code
    AND oh.is_current = 1
INNER JOIN security_filters sf
    ON oh.region_code = sf.region_code

-- WHERE_PLACEHOLDER
