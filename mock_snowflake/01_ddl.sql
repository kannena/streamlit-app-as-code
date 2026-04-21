-- =============================================================================
-- Mock Snowflake DDL: ACME_DW Database
-- =============================================================================
-- Run this script to set up the database objects needed by the framework.
-- Adjust warehouse names and roles to match your environment.
-- =============================================================================

-- ─── Database & Schemas ──────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS DEV_ACME_DW;

USE DATABASE DEV_ACME_DW;

CREATE SCHEMA IF NOT EXISTS corp;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS security;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS app_metadata;


-- ─── Organization Hierarchy (corp.dim_org_hierarchy) ─────────────────────────

CREATE TABLE IF NOT EXISTS corp.dim_org_hierarchy (
    org_hierarchy_key   INT AUTOINCREMENT PRIMARY KEY,
    region_code         VARCHAR(10)   NOT NULL,
    region_name         VARCHAR(100),
    department_code     VARCHAR(10),
    department_name     VARCHAR(100),
    team_code           VARCHAR(10),
    team_name           VARCHAR(100),
    is_current          BOOLEAN       DEFAULT TRUE,
    effective_date      DATE          DEFAULT CURRENT_DATE(),
    end_date            DATE
);


-- ─── Customer Dimension (corp.dim_customer) ──────────────────────────────────

CREATE TABLE IF NOT EXISTS corp.dim_customer (
    customer_key        INT AUTOINCREMENT PRIMARY KEY,
    account_number      VARCHAR(20)   NOT NULL,
    customer_name       VARCHAR(200),
    region_code         VARCHAR(10),
    shipping_address    VARCHAR(500),
    payment_terms       VARCHAR(50),
    customer_status     VARCHAR(20)   DEFAULT 'ACTIVE',
    created_date        DATE          DEFAULT CURRENT_DATE()
);


-- ─── Fiscal Calendar (analytics.dim_fiscal_calendar) ─────────────────────────

CREATE TABLE IF NOT EXISTS analytics.dim_fiscal_calendar (
    calendar_date       DATE PRIMARY KEY,
    fiscal_year         INT,
    fiscal_quarter      VARCHAR(6),
    fiscal_month        INT,
    fiscal_week         INT,
    day_of_week         VARCHAR(10),
    is_business_day     BOOLEAN
);


-- ─── Orders Fact (analytics.fact_orders) ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id            VARCHAR(20) PRIMARY KEY,
    customer_key        INT          REFERENCES corp.dim_customer(customer_key),
    order_date          DATE,
    order_status        VARCHAR(20),
    order_total         DECIMAL(12,2),
    line_item_count     INT,
    shipped_date        DATE,
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── Revenue Fact (analytics.fact_revenue) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.fact_revenue (
    revenue_id          INT AUTOINCREMENT PRIMARY KEY,
    customer_key        INT          REFERENCES corp.dim_customer(customer_key),
    transaction_date    DATE,
    revenue_type        VARCHAR(50),
    revenue_amount      DECIMAL(12,2),
    cost_amount         DECIMAL(12,2),
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── Security: User Access Map ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS security.user_access_map (
    user_access_id      INT AUTOINCREMENT PRIMARY KEY,
    employee_login      VARCHAR(100) NOT NULL,
    region_code         VARCHAR(10)  NOT NULL,
    access_level        VARCHAR(20)  DEFAULT 'READ',
    is_active           BOOLEAN      DEFAULT TRUE,
    granted_date        DATE         DEFAULT CURRENT_DATE(),
    granted_by          VARCHAR(100)
);


-- ─── Security: Role Helper (for admin checks) ───────────────────────────────

CREATE TABLE IF NOT EXISTS security.app_role_helper (
    role_id             INT AUTOINCREMENT PRIMARY KEY,
    user_name           VARCHAR(100) NOT NULL,
    role_level          VARCHAR(50)  NOT NULL,
    is_active           BOOLEAN      DEFAULT TRUE,
    updated_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── Audit: User Activity Log ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS audit.app_user_activity (
    activity_id         INT AUTOINCREMENT PRIMARY KEY,
    session_id          VARCHAR(100),
    user_login          VARCHAR(100),
    app_name            VARCHAR(200),
    action_type         VARCHAR(50),
    action_details      VARIANT,
    record_count        INT,
    environment         VARCHAR(10),
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── App Metadata: Global Filter Catalog ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS app_metadata.global_filter_catalog (
    catalog_id          INT AUTOINCREMENT PRIMARY KEY,
    user_login_name     VARCHAR(100),
    app_name            VARCHAR(200),
    favorite_name       VARCHAR(200),
    filter_selections_json VARIANT,
    url                 VARCHAR(500),
    folder_id           VARCHAR(50),
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── App Metadata: Global Filter Folders ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS app_metadata.global_filter_folders (
    id                  VARCHAR(50) PRIMARY KEY,
    parent_id           VARCHAR(50),
    name                VARCHAR(200),
    description         VARCHAR(500),
    allowed_roles       ARRAY,
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── App Metadata: Disclaimer Acceptance ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS app_metadata.disclaimer_acceptance (
    acceptance_id       INT AUTOINCREMENT PRIMARY KEY,
    user_login          VARCHAR(100),
    app_name            VARCHAR(200),
    disclaimer_version  VARCHAR(20),
    environment         VARCHAR(10),
    accepted_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── App Metadata: Subscriptions ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS app_metadata.app_subscriptions (
    subscription_id     INT AUTOINCREMENT PRIMARY KEY,
    user_login          VARCHAR(100),
    app_name            VARCHAR(200),
    schedule_type       VARCHAR(20),
    schedule_day        VARCHAR(20),
    schedule_time       VARCHAR(10),
    filter_selections   VARIANT,
    is_active           BOOLEAN      DEFAULT TRUE,
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP()
);


-- ─── Warehouses (adjust sizes to your needs) ─────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS DEV_ANALYTICS_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE WAREHOUSE IF NOT EXISTS DEV_DEFAULT_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
