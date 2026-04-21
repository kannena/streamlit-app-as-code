# Data Masking Reference Map
# All code sanitization MUST follow this map exactly.
# After writing any file, grep for every "Original" value to verify zero leaks.

---

## DATABASES

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `EDW` | `ACME_DW` | Production database |
| `DEV_EDW` | `DEV_ACME_DW` | Dev environment |
| `QA_EDW` | `QA_ACME_DW` | QA environment |
| `STG_EDW` | `STG_ACME_DW` | Staging environment |
| `PROD_EDW` | `ACME_DW` | Prod has no prefix |

---

## SCHEMAS

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `core` | `corp` | Corporate dimension tables |
| `cdm` | `analytics` | Analytical/business tables |
| `CURATED_ODS` | `staging` | Staging/curated layer |
| `AUDIT` | `audit` | Audit logging tables (keep lowercase) |
| `UTILITY` | `utility` | Utility tables (keep lowercase) |
| `REFERENCE` | `reference` | Reference/lookup tables |

---

## TABLES — Security & RBAC

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `Sec_Emp_to_Div` | `user_access_map` | Schema: `security` |
| `Dim_Corp_Hier` | `dim_org_hierarchy` | Schema: `corp` |
| `EMP_SEC` | `user_access_map` | Alias for same concept |
| `GLOBAL_USER_ROLES` | `app_user_roles` | Schema: `audit` |
| `GLOBAL_APPLICATION_GROUPS` | `app_groups` | Schema: `audit` |
| `GLOBAL_GROUP_MEMBERSHIP` | `app_group_members` | Schema: `audit` |

---

## TABLES — Features

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `streamlit_user_activity` | `app_user_activity` | Schema: `audit` |
| `disclaimer_acceptances` | `disclaimer_acceptances` | Keep as-is, schema: `utility` |
| `USER_FAVORITE_FILTERS` | `user_saved_filters` | Schema: `audit` |
| `USER_SUBSCRIPTIONS` | `user_subscriptions` | Keep as-is, schema: `audit` |
| `GLOBAL_CATALOG` | `filter_catalog` | Schema: `audit` |
| `GLOBAL_CATALOG_FOLDER` | `filter_catalog_folder` | Schema: `audit` |
| `GLOBAL_APP_LINK_PARAMETERS` | `app_link_params` | Schema: `audit` |
| `Dim_Date` | `dim_date` | Schema: `corp` |
| `Dim_Employee` | `dim_employee` | Schema: `corp` |

---

## COLUMNS — Security

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `Employee_Network_User_ID` | `user_login_id` | |
| `Cur_Div_Nbr` | `region_code` | |
| `CUR_DIV_NBR` | `region_code` | Uppercase variant |
| `Cur_Infopro_Div_Nbr` | `region_code` | Legacy variant |
| `Infopro_Div_Nbr` | `legacy_region_code` | |
| `Div_Nbr` | `region_code` | |
| `Div_SK` | `region_sk` | Surrogate key |
| `Cur_Div_SK` | `region_sk` | |
| `Cur_Region_Nbr` | `territory_code` | |
| `Is_Current` | `is_current` | Keep as-is |
| `REV_DISTRIB_CD` | `cost_center_code` | |
| `AR_DIVISION` | `ar_region` | |

---

## COLUMNS — Business/Hierarchy

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `Area` (as hierarchy level) | `territory` | |
| `Business_Unit` | `department` | |
| `Division` (as hierarchy level) | `region` | |
| `Customer_Nbr` | `customer_id` | |
| `Site_Nbr` | `location_id` | |
| `Container_Nbr` | `asset_id` | |

---

## COMPANY / BRANDING

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `Republic Services` | `ACME Corp` | |
| `Republic Services Confidential` | `ACME Corp Confidential` | |
| `RSI` (abbreviation) | `ACME` | |
| `Insight` (product name) | `DataStudio` | |
| `RSI.png` | `logo_left.png` | |
| `Insight.png` | `logo_right.png` | |

---

## COLORS (Company Branding)

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `#004A7C` | `#1a73e8` | Google-style blue (neutral) |
| `#002855` | `#0d47a1` | Darker blue |
| `#003876` | `#1565c0` | Mid blue |

---

## ROLE PREFIXES

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `AZUREAD-SF-STRMLT-QS-FLDR-` | `APP-ROLE-FOLDER-` | |
| `STREAMLIT_QS_FLDR_` | `APP_ROLE_FOLDER_` | Legacy variant |
| `REPORT_ADMIN` | `REPORT_ADMIN` | Keep as-is (generic enough) |

---

## WAREHOUSE NAMES

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `{ENVIRON}STREAMLIT_S_WH` | `{ENVIRON}STREAMLIT_WH` | |
| `{ENVIRON}ANALYTICS_WH` | `{ENVIRON}ANALYTICS_WH` | Keep as-is |

---

## URLS / DOMAINS / POLICIES

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `republicservices.sharepoint.com` | REMOVE entirely | No replacement URL |
| `@republicservices.com` | `@acmecorp.com` | |
| All SharePoint policy URLs (LGL-115, MKT-111, HRS-115) | REMOVE or replace with `https://example.com/policy` | |
| `SC-ISDept`, `LL-PolyProc` | REMOVE | Internal SharePoint sites |

---

## APP / REPORT NAMES

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `Account, Site & Container` | `Customer Assets` | |
| `Account, Site & Container - Query Studio` | `Customer Assets Query Studio` | |
| `Aging Query Studio` | `Sample Aging Report` | |
| `Cash Receipts` | `Payment History` | |
| `Invoice History` | `Invoice Analysis` | |
| `Service History` | `Service Records` | |
| `Global Filter Manager` | EXCLUDED from scope | |

---

## CI/CD SECRETS / ENV VARS

| Original | Sanitized | Notes |
|----------|-----------|-------|
| `SNOWFLAKE_DATA_PRODUCTS_DATABASE` | `SNOWFLAKE_DATABASE` | |
| `SNOWFLAKE_GIT_USER` | `SNOWFLAKE_USER` | |
| `SNOWFLAKE_DATA_PRODUCTS_STREAMLIT_CREATOR_ROLE` | `SNOWFLAKE_DEPLOY_ROLE` | |
| `SNOWFLAKE_STREAMLIT_APPS_HOSTING_WH` | `SNOWFLAKE_DEPLOY_WH` | |
| `SNOWFLAKE_STREAMLIT_APP_HOSTING_COMPUTE_POOL` | `SNOWFLAKE_COMPUTE_POOL` | |
| `SNOWFLAKE_STREAMLIT_COMPUTE_POOL_QUERIES_WH` | `SNOWFLAKE_QUERY_WH` | |

---

## ENVIRONMENT DETECTION LOGIC

| Original Pattern | Sanitized Pattern | Notes |
|------------------|-------------------|-------|
| DB contains "DEV" → prefix `DEV_` | Same | Keep logic, change DB name |
| DB contains "QA" → prefix `QA_` | Same | |
| DB contains "STG" → prefix `STG_` | Same | |
| Else → no prefix (PROD) | Same | |
| `{DB}` placeholder | `{DB}` | Keep as-is — core framework concept |
| `{ENVIRON}` placeholder | `{ENVIRON}` | Keep as-is |
| `{current_user}` placeholder | `{current_user}` | Keep as-is |

---

## VERIFICATION CHECKLIST

After all files are written, run these searches across the entire output directory.
Every search MUST return ZERO results:

```
grep -ri "republic" output/
grep -ri "RSI\b" output/         # word-boundary to avoid false positives
grep -ri "EDW\b" output/         # should only appear as ACME_DW
grep -ri "@republic" output/
grep -ri "sharepoint" output/
grep -ri "Sec_Emp" output/
grep -ri "Dim_Corp_Hier" output/
grep -ri "Infopro" output/
grep -ri "Div_Nbr" output/
grep -ri "CURATED_ODS" output/
grep -ri "AZUREAD" output/
grep -ri "SC-ISDept" output/
grep -ri "LL-PolyProc" output/
grep -ri "LGL-115\|MKT-111\|HRS-115" output/
grep -ri "004A7C\|002855\|003876" output/
```
