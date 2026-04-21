-- =============================================================================
-- Revenue Analysis Query Studio - SQL Template (Complex)
-- =============================================================================
-- Demonstrates advanced patterns:
--   - Security CTE with {current_user} injection
--   - {?filter:condition} optional conditionals
--   - -- WHERE_PLACEHOLDER for dynamic filter injection
--   - -- SECURITY_CTE_WHERE_PLACEHOLDER for CTE-level injection
--   - Multi-table JOINs with fiscal calendar
-- =============================================================================

WITH security_filters AS (
    SELECT DISTINCT region_code
    FROM {DB}.security.user_access_map
    WHERE LOWER(employee_login) = LOWER('{current_user}')
      AND is_active = TRUE
      -- SECURITY_CTE_WHERE_PLACEHOLDER
),

fiscal_scope AS (
    SELECT
        fiscal_year,
        fiscal_quarter,
        fiscal_month,
        calendar_date
    FROM {DB}.analytics.dim_fiscal_calendar
    WHERE 1=1
      {?fiscal_year: AND fiscal_year IN ({fiscal_year})}
      {?fiscal_quarter: AND fiscal_quarter IN ({fiscal_quarter})}
)

SELECT
    CONCAT(oh.region_code, ' - ', oh.region_name)           AS REGION,
    CONCAT(oh.department_code, ' - ', oh.department_name)   AS DEPARTMENT,
    CONCAT(oh.team_code, ' - ', oh.team_name)               AS TEAM,
    c.customer_name                                          AS CUSTOMER_NAME,
    c.account_number                                         AS ACCOUNT_NUMBER,
    fc.fiscal_year                                           AS FISCAL_YEAR,
    fc.fiscal_quarter                                        AS FISCAL_QUARTER,
    fc.fiscal_month                                          AS FISCAL_MONTH,
    r.revenue_type                                           AS REVENUE_TYPE,
    r.revenue_amount                                         AS REVENUE_AMOUNT,
    r.cost_amount                                            AS COST_AMOUNT,
    (r.revenue_amount - r.cost_amount)                       AS MARGIN_AMOUNT,
    CASE
        WHEN r.revenue_amount > 0
        THEN ROUND((r.revenue_amount - r.cost_amount) / r.revenue_amount * 100, 2)
        ELSE 0
    END                                                      AS MARGIN_PERCENT,
    COUNT(*) OVER (
        PARTITION BY c.customer_key, fc.fiscal_year
    )                                                        AS TRANSACTION_COUNT

FROM {DB}.analytics.fact_revenue r
INNER JOIN {DB}.corp.dim_customer c
    ON r.customer_key = c.customer_key
INNER JOIN {DB}.corp.dim_org_hierarchy oh
    ON c.region_code = oh.region_code
    AND oh.is_current = 1
INNER JOIN security_filters sf
    ON oh.region_code = sf.region_code
INNER JOIN fiscal_scope fc
    ON r.transaction_date = fc.calendar_date

-- WHERE_PLACEHOLDER
