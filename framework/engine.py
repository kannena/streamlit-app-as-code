"""
App-as-Code: Metadata-Driven Streamlit Query Studio Engine
==========================================================

This is the core framework engine. It reads a YAML config and a SQL template,
then renders a complete interactive Streamlit application — filters, query
execution, pagination, export, audit logging, and row-level security —
without any per-app Python code.

Architecture:
    config.yaml      ─→ ┐
    queries.sql      ─→ ├─→  engine.py  ─→  Live Streamlit App
    default_config   ─→ ┘

Key Patterns:
    - {DB} placeholder resolves to environment-specific database name
    - {current_user} resolves to logged-in Snowflake user
    - {?filter_name:condition} for optional conditional SQL injection
    - -- WHERE_PLACEHOLDER replaced with dynamic filter conditions
    - Filter dependencies parsed as AND/OR groups
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import os
import time
import yaml
import re
import json
import csv
import io
import zipfile
import importlib
import pandas as pd
from datetime import datetime, date
from io import StringIO

# ---------------------------------------------------------------------------
# Import framework modules (graceful degradation if missing)
# ---------------------------------------------------------------------------

try:
    from audit import AuditLogger, log_event, new_session_id
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

try:
    from subscriptions import create_manager
    SUBSCRIPTION_AVAILABLE = True
except ImportError:
    SUBSCRIPTION_AVAILABLE = False

try:
    from cache import CacheManager
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False

try:
    from disclaimer import DisclaimerHandler
    DISCLAIMER_AVAILABLE = True
except ImportError:
    DISCLAIMER_AVAILABLE = False

EXCEL_ENGINE = None
for _eng in ("xlsxwriter", "openpyxl"):
    if importlib.util.find_spec(_eng):
        EXCEL_ENGINE = _eng
        break

# ---------------------------------------------------------------------------
# Configuration Loading
# ---------------------------------------------------------------------------

def load_app_manifest(path: str = "config.yaml") -> dict:
    """Load the per-app YAML configuration."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        st.error("❌ Configuration file 'config.yaml' not found.")
        return {}


def load_defaults(path: str = "default_config.yaml") -> dict:
    """Load the shared framework configuration and merge defaults."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def combine_configs(framework: dict, app: dict) -> dict:
    """Deep-merge app config over framework defaults."""
    merged = {}
    all_keys = set(list(framework.keys()) + list(app.keys()))
    for key in all_keys:
        f_val = framework.get(key)
        a_val = app.get(key)
        if isinstance(f_val, dict) and isinstance(a_val, dict):
            merged[key] = combine_configs(f_val, a_val)
        elif a_val is not None:
            merged[key] = a_val
        else:
            merged[key] = f_val
    return merged


def read_sql_template(path: str = "queries.sql") -> str:
    """Load the SQL template file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        st.error("❌ SQL template file 'queries.sql' not found.")
        return ""


# ---------------------------------------------------------------------------
# Environment Detection & Placeholder Resolution
# ---------------------------------------------------------------------------

def detect_environment(session, default_env: str = "DEV") -> str:
    """Detect environment from CURRENT_DATABASE()."""
    try:
        val = session.sql("SELECT CURRENT_DATABASE() AS val").collect()[0]["VAL"] or ""
        up = val.upper()
        for env in ("DEV", "QA", "STG", "PROD"):
            if env in up:
                return env
    except Exception:
        pass
    return default_env


def resolve_database(env: str) -> str:
    """Map environment to database name."""
    return "ACME_DW" if env == "PROD" else f"{env}_ACME_DW"


def resolve_placeholders(sql: str, db_name: str) -> str:
    """Replace {DB} tokens with the resolved database name."""
    return sql.replace("{DB}", db_name)


def resolve_env_vars(val: str, env: str) -> str:
    """Replace {ENVIRON} tokens in warehouse names."""
    prefix_map = {"DEV": "DEV_", "QA": "QA_", "STG": "STG_", "PROD": ""}
    return val.replace("{ENVIRON}", prefix_map.get(env.upper(), "DEV_"))


# ---------------------------------------------------------------------------
# User Identity
# ---------------------------------------------------------------------------

def fetch_current_user(session) -> str:
    """
    Get the current username, preferring Streamlit SSO (st.user),
    falling back to Snowflake CURRENT_USER().
    """
    # Method 1: Streamlit SSO
    try:
        user_info = st.user
        for attr in ('user_name', 'login_name', 'email'):
            val = getattr(user_info, attr, None)
            if val and str(val).strip():
                raw = str(val).strip()
                return raw.split("@")[0] if "@" in raw else raw
    except Exception:
        pass
    # Method 2: Snowflake SQL
    try:
        result = session.sql("SELECT CURRENT_USER() AS u").collect()
        if result:
            raw = str(result[0]['U'])
            return raw.split("@")[0] if "@" in raw else raw
    except Exception:
        pass
    return "unknown_user"


# ---------------------------------------------------------------------------
# Security Filter
# ---------------------------------------------------------------------------

def fetch_user_divisions(session, config: dict, db_name: str,
                                 current_user: str, cache_mgr=None) -> list:
    """
    Query the security table to get the user's allowed region codes.
    Results are cached via SessionCacheManager when available.
    """
    sec_config = config.get('security_filter', {})
    if not sec_config.get('enabled', False):
        return []

    # Check cache first
    if cache_mgr:
        cached = cache_mgr.get('security_division', user=current_user)
        if cached is not None:
            return cached

    query = sec_config.get('query', '')
    query = resolve_placeholders(query, db_name)
    query = query.replace('{current_user}', current_user)

    try:
        result = session.sql(query).collect()
        col = sec_config.get('filter_column', 'region_code')
        divisions = [str(row[col.upper()]) for row in result if row[col.upper()]]
    except Exception:
        divisions = []

    if cache_mgr and divisions:
        cache_mgr.set('security_division', divisions, user=current_user)

    return divisions


# ---------------------------------------------------------------------------
# Filter Dependency Resolution
# ---------------------------------------------------------------------------

def parse_dep_groups(depends_on: list) -> list:
    """
    Parse the depends_on list into AND/OR groups.

    Input:  ["territory|department", "region"]
    Output: [["territory", "department"], ["region"]]

    Logic: AND between groups, OR within a group.
    All groups must be satisfied for the filter to be enabled.
    """
    if not depends_on:
        return []
    groups = []
    for item in depends_on:
        if "|" in str(item):
            groups.append([x.strip() for x in str(item).split("|")])
        else:
            groups.append([str(item).strip()])
    return groups


def deps_met(dep_groups: list, filter_values: dict) -> bool:
    """Check whether all dependency groups are satisfied (AND between groups, OR within)."""
    for group in dep_groups:
        # At least one filter in this group must have a value (OR logic)
        group_ok = any(
            filter_values.get(f) not in (None, [], '', {})
            for f in group
        )
        if not group_ok:
            return False  # AND logic — every group must pass
    return True


# ---------------------------------------------------------------------------
# Query Builder
# ---------------------------------------------------------------------------

def assemble_query(sql_template: str, config: dict, filter_values: dict,
                db_name: str, current_user: str, security_divisions: list,
                query_type: str = "data", page: int = 1,
                page_size: int = 2500, selected_columns: list = None) -> str:
    """
    Build the final executable SQL from the template + user selections.

    Steps:
    1. Replace {DB} and {current_user} placeholders
    2. Process {?filter_name:condition} optional conditionals
    3. Collect WHERE conditions from filter values
    4. Inject security division restrictions
    5. Replace -- WHERE_PLACEHOLDER with assembled WHERE clause
    6. Apply pagination (LIMIT/OFFSET) for data queries
    """
    sql = sql_template
    filters_config = config.get('filters', {})
    sec_config = config.get('security_filter', {})

    # Step 1: Core placeholder replacement
    sql = resolve_placeholders(sql, db_name)
    if sec_config.get('enabled', False):
        sql = sql.replace('{current_user}', current_user)
    else:
        sql = sql.replace('{current_user}', '1=1')

    # Step 2: Process optional conditionals  {?filter_name:condition}
    optional_pattern = re.compile(r'\{\?(\w+):([^}]+)\}')
    def _replace_optional(match):
        fname = match.group(1)
        condition = match.group(2)
        val = filter_values.get(fname)
        if val and val not in ([], '', {}):
            return condition
        return ''
    sql = optional_pattern.sub(_replace_optional, sql)

    # Step 3: Build WHERE conditions from filter values
    conditions = []
    security_cte_conditions = []

    # Sort filters by their configured order
    sorted_filters = sorted(
        filters_config.items(),
        key=lambda x: x[1].get('order', 99)
    )

    for filter_name, f_config in sorted_filters:
        val = filter_values.get(filter_name)
        if not val or f_config.get('include_in_where') is False:
            continue

        input_type = f_config.get('input_type', 'text')
        sql_condition = f_config.get('sql_condition', '')
        inject_to_cte = f_config.get('inject_to_security_cte', False)

        condition = None

        if input_type == "checkbox" and isinstance(val, list):
            # Multi-select → IN clause
            if sql_condition:
                quoted = ", ".join(f"'{v}'" for v in val)
                condition = sql_condition.replace(f'{{{filter_name}}}', quoted)
            else:
                col = f_config.get('date_column', filter_name)
                quoted = ", ".join(f"'{v}'" for v in val)
                condition = f"{col} IN ({quoted})"

        elif input_type == "date" and isinstance(val, dict):
            # Date range → BETWEEN
            col = f_config.get('date_column', filter_name)
            start = val.get('start_date', '')
            end = val.get('end_date', '')
            if start and end:
                condition = f"{col} BETWEEN '{start}' AND '{end}'"

        elif input_type == "text" and val:
            # Text → supports comma-separated values
            val_str = str(val).strip()
            if sql_condition:
                if ',' in val_str:
                    items = [v.strip() for v in val_str.split(',') if v.strip()]
                    quoted = ", ".join(f"'{v}'" for v in items)
                    condition = sql_condition.replace(f'{{{filter_name}}}', quoted)
                else:
                    condition = sql_condition.replace(
                        f'{{{filter_name}}}', f"'{val_str}'")
            else:
                condition = f"{filter_name} = '{val_str}'"

        if condition:
            if inject_to_cte:
                security_cte_conditions.append(condition)
            else:
                conditions.append(condition)

    # Step 4: Inject security division restrictions into WHERE
    if sec_config.get('enabled', False) and security_divisions:
        col = sec_config.get('filter_column', 'region_code')
        alias = sec_config.get('table_alias', '')
        prefix = f"{alias}." if alias else ""
        quoted_divs = ", ".join(f"'{d}'" for d in security_divisions)

        if sec_config.get('include_in_where', False):
            conditions.append(f"{prefix}{col} IN ({quoted_divs})")

    # Step 5: Replace placeholders with assembled conditions
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
    else:
        where_clause = ""

    sql = sql.replace("-- WHERE_PLACEHOLDER", where_clause)
    sql = sql.replace("-- CONDITIONS_PLACEHOLDER",
                       " AND ".join(conditions) if conditions else "1=1")

    if security_cte_conditions:
        cte_clause = " AND " + " AND ".join(security_cte_conditions)
        sql = sql.replace("-- SECURITY_CTE_WHERE_PLACEHOLDER", cte_clause)
    else:
        sql = sql.replace("-- SECURITY_CTE_WHERE_PLACEHOLDER", "")

    # Step 6: Query type variants
    if query_type == "count":
        sql = f"SELECT COUNT(*) AS TOTAL_RECORDS FROM ({sql})"
    elif query_type == "data":
        offset = (page - 1) * page_size
        sql = f"{sql}\nLIMIT {page_size} OFFSET {offset}"
    # query_type == "download" → no pagination, return full result

    return sql


# ---------------------------------------------------------------------------
# Filter Rendering
# ---------------------------------------------------------------------------

def render_filter_panel(config: dict, session, db_name: str,
                   filter_values: dict, cache_mgr=None) -> dict:
    """
    Render all filter widgets defined in the YAML config.
    Returns a dict of {filter_name: selected_value}.
    """
    filters_config = config.get('filters', {})
    if not filters_config:
        return {}

    sorted_filters = sorted(
        filters_config.items(),
        key=lambda x: x[1].get('order', 99)
    )

    updated_values = {}

    # Separate mandatory and optional filters
    mandatory = [(n, c) for n, c in sorted_filters if not c.get('for_more', False)]
    optional = [(n, c) for n, c in sorted_filters if c.get('for_more', False)]

    # Render mandatory filters
    _draw_filter_group(mandatory, config, session, db_name,
                         filter_values, updated_values, cache_mgr)

    # Render optional filters in expandable section
    if optional:
        adv_title = config.get('ui', {}).get('sections', {}).get(
            'filters', {}).get('advanced', {}).get('title', '🧩 Optional Filters')
        adv_expanded = config.get('ui', {}).get('sections', {}).get(
            'filters', {}).get('advanced', {}).get('expanded_default', False)
        with st.expander(adv_title, expanded=adv_expanded):
            _draw_filter_group(optional, config, session, db_name,
                                 filter_values, updated_values, cache_mgr)

    return updated_values


def _draw_filter_group(filter_list, config, session, db_name,
                         filter_values, updated_values, cache_mgr):
    """Render a group of filter widgets."""
    filters_config = config.get('filters', {})

    for filter_name, f_config in filter_list:
        # Column selection is handled separately
        if filter_name == "column_selection":
            continue

        label = f_config.get('label', filter_name)
        input_type = f_config.get('input_type', 'text')
        mandatory = f_config.get('mandatory', False)
        depends_on = f_config.get('depends_on', [])

        # Check dependencies
        if depends_on:
            dep_groups = parse_dep_groups(depends_on)
            if not deps_met(dep_groups, filter_values):
                updated_values[filter_name] = None
                continue

        # Render based on input type
        if input_type == "checkbox":
            _draw_checkbox(
                filter_name, f_config, session, db_name,
                filter_values, updated_values, cache_mgr)

        elif input_type == "date":
            _draw_date_range(filter_name, f_config, updated_values)

        elif input_type == "text":
            placeholder = f_config.get('placeholder', f"Enter {label}")
            description = f_config.get('description', '')
            val = st.text_input(
                label, value=filter_values.get(filter_name, ''),
                placeholder=placeholder, help=description,
                key=f"filter_{filter_name}")
            updated_values[filter_name] = val if val else None

        else:
            # Default: text input
            val = st.text_input(
                label, value=filter_values.get(filter_name, ''),
                key=f"filter_{filter_name}")
            updated_values[filter_name] = val if val else None


def _draw_checkbox(filter_name, f_config, session, db_name,
                            filter_values, updated_values, cache_mgr):
    """Render a multi-select checkbox filter with dynamic SQL options."""
    label = f_config.get('label', filter_name)
    filter_sql = f_config.get('sql', '')

    if not filter_sql:
        updated_values[filter_name] = None
        return

    # Resolve placeholders in filter SQL
    resolved_sql = resolve_placeholders(filter_sql, db_name)

    # Replace upstream filter references in the SQL
    for key, val in filter_values.items():
        if val and isinstance(val, list):
            quoted = ", ".join(f"'{v}'" for v in val)
            resolved_sql = resolved_sql.replace(f'{{{key}}}', quoted)
        elif val and isinstance(val, str):
            resolved_sql = resolved_sql.replace(f'{{{key}}}', f"'{val}'")

    try:
        result = session.sql(resolved_sql).collect()
        options = [str(row[0]) for row in result if row[0]] if result else []
    except Exception as e:
        st.warning(f"Error loading {label}: {e}")
        options = []

    if not options:
        updated_values[filter_name] = None
        return

    allow_multiple = f_config.get('allow_multiple', True)

    if allow_multiple:
        selected = st.multiselect(
            label, options=options,
            default=filter_values.get(filter_name, []),
            key=f"filter_{filter_name}")
        updated_values[filter_name] = selected if selected else None
    else:
        selected = st.selectbox(
            label, options=[""] + options,
            index=0, key=f"filter_{filter_name}")
        updated_values[filter_name] = selected if selected else None


def _draw_date_range(filter_name, f_config, updated_values):
    """Render a date range filter."""
    label = f_config.get('label', filter_name)
    pattern = f_config.get('date_pattern', 'range')

    if pattern == "range":
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input(
                f"{label} — From",
                value=None,
                key=f"filter_{filter_name}_from")
        with col2:
            to_date = st.date_input(
                f"{label} — To",
                value=None,
                key=f"filter_{filter_name}_to")

        if from_date and to_date:
            if from_date > to_date:
                st.error("From date must be before or equal to To date")
                updated_values[filter_name] = None
            else:
                updated_values[filter_name] = {
                    'start_date': str(from_date),
                    'end_date': str(to_date)
                }
        else:
            updated_values[filter_name] = None


# ---------------------------------------------------------------------------
# Export Functions
# ---------------------------------------------------------------------------

def format_filter_summary(config: dict, filter_values: dict,
                              current_user: str, record_count: int,
                              security_divisions: list) -> str:
    """Generate a text summary of active filters for export bundling."""
    lines = []
    app_name = config.get('app_info', {}).get('title', 'Query Studio')
    lines.append(f"Report: {app_name}")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"User: {current_user}")
    lines.append(f"Records: {record_count}")
    lines.append("")
    lines.append("Active Filters:")
    lines.append("-" * 40)

    filters_config = config.get('filters', {})
    for fname, fconf in sorted(filters_config.items(),
                                key=lambda x: x[1].get('order', 99)):
        val = filter_values.get(fname)
        if val:
            label = fconf.get('label', fname)
            if isinstance(val, list):
                lines.append(f"  {label}: {', '.join(str(v) for v in val)}")
            elif isinstance(val, dict):
                lines.append(f"  {label}: {val.get('start_date', '')} to {val.get('end_date', '')}")
            else:
                lines.append(f"  {label}: {val}")

    if security_divisions:
        lines.append("")
        lines.append(f"Security Context: {len(security_divisions)} region(s)")

    return "\n".join(lines)


def export_csv_bundle(df: pd.DataFrame, config: dict,
                        filter_values: dict, current_user: str,
                        security_divisions: list) -> bytes:
    """Bundle CSV data + filter summary into a ZIP file."""
    export_config = config.get('export', {})
    app_name = config.get('app_info', {}).get('title', 'query_results')
    timestamp = datetime.now().strftime(
        export_config.get('filename', {}).get('timestamp_format', '%Y%m%d_%H%M%S'))
    base_name = f"{app_name}_{timestamp}".replace(' ', '_')

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        # CSV data
        csv_buf = StringIO()
        df.to_csv(csv_buf, index=False)
        zf.writestr(f"{base_name}.csv", csv_buf.getvalue())

        # Filter summary
        if export_config.get('formats', {}).get('csv', {}).get('include_filter_file', True):
            filter_text = format_filter_summary(
                config, filter_values, current_user,
                len(df), security_divisions)
            zf.writestr(f"{base_name}_filters.txt", filter_text)

    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------

def run_app():
    """Entry point: orchestrates the full application lifecycle."""

    # ── Session & Environment ─────────────────────────────────────────
    try:
        session = get_active_session()
        st.session_state.snowflake_connected = True
    except Exception:
        st.error("❌ Could not connect to Snowflake.")
        return

    env = detect_environment(session)
    db_name = resolve_database(env)
    current_user = fetch_current_user(session)

    # ── Load Configuration ────────────────────────────────────────────
    framework_config = load_defaults()
    app_config = load_app_manifest()
    config = combine_configs(framework_config, app_config)
    sql_template = read_sql_template()

    if not config or not sql_template:
        return

    # ── Page Setup ────────────────────────────────────────────────────
    app_info = config.get('app_info', {})
    st.set_page_config(
        page_title=app_info.get('page_title', 'Query Studio'),
        page_icon=app_info.get('page_icon', '📊'),
        layout=app_info.get('page_layout', 'wide')
    )
    st.title(app_info.get('title', 'Query Studio'))

    # ── Initialize Framework Modules ──────────────────────────────────
    cache_mgr = CacheManager() if CACHE_AVAILABLE else None

    audit_logger = None
    session_id = ""
    if AUDIT_AVAILABLE:
        session_id = new_session_id()
        audit_logger = AuditLogger(
            config, session, app_info.get('title', ''),
            env, db_name)
        log_event(audit_logger, 'session_start', session_id)

    # ── Disclaimer ────────────────────────────────────────────────────
    if DISCLAIMER_AVAILABLE and config.get('disclaimer', {}).get('enabled', False):
        dm = DisclaimerHandler(session, config, db_name, env, audit_logger)
        if dm.needs_acceptance(current_user, app_info.get('title', '')):
            _render_disclaimer(dm, config, current_user,
                                    app_info.get('title', ''), session_id, audit_logger)
            return  # Block app until accepted

    # ── Security Divisions ────────────────────────────────────────────
    security_divisions = fetch_user_divisions(
        session, config, db_name, current_user, cache_mgr)

    # ── Warehouse Context ─────────────────────────────────────────────
    _activate_warehouse(session, config, env)

    # ── Initialize Session State ──────────────────────────────────────
    if 'filter_values' not in st.session_state:
        st.session_state.filter_values = {}
    if 'has_run' not in st.session_state:
        st.session_state.has_run = False
    if 'page' not in st.session_state:
        st.session_state.page = 1

    # ── Render Filters ────────────────────────────────────────────────
    filters_title = config.get('ui', {}).get('sections', {}).get(
        'filters', {}).get('main', {}).get('title', '🔍 Filters')
    filters_expanded = config.get('ui', {}).get('sections', {}).get(
        'filters', {}).get('main', {}).get('expanded_default', True)

    with st.expander(filters_title, expanded=filters_expanded):
        filter_values = render_filter_panel(
            config, session, db_name,
            st.session_state.filter_values, cache_mgr)
        st.session_state.filter_values = filter_values

        # Action buttons
        btn_config = config.get('buttons', {}).get('actions', {})
        col1, col2, col3 = st.columns([1, 1, 4])
        with col1:
            run_clicked = st.button(
                btn_config.get('run_query', {}).get('label', '▶️ Run Query'),
                type="primary")
        with col2:
            clear_clicked = st.button(
                btn_config.get('clear_filters', {}).get('label', '🔄 Clear All'))

    if clear_clicked:
        st.session_state.filter_values = {}
        st.session_state.has_run = False
        st.session_state.page = 1
        st.rerun()

    # ── Execute Query ─────────────────────────────────────────────────
    if run_clicked:
        st.session_state.has_run = True
        st.session_state.page = 1

    if st.session_state.has_run:
        _run_and_render(
            session, config, sql_template, filter_values,
            db_name, current_user, security_divisions,
            audit_logger, session_id, cache_mgr)


# ---------------------------------------------------------------------------
# Execution & Display
# ---------------------------------------------------------------------------

def _run_and_render(session, config, sql_template, filter_values,
                         db_name, current_user, security_divisions,
                         audit_logger, session_id, cache_mgr):
    """Build query, execute, and render results with pagination."""
    pagination = config.get('pagination', {})
    page_size = pagination.get('default_page_size', 2500)
    page = st.session_state.get('page', 1)

    # Get total record count
    count_sql = assemble_query(
        sql_template, config, filter_values,
        db_name, current_user, security_divisions,
        query_type="count")

    try:
        count_result = session.sql(count_sql).collect()
        total_records = count_result[0]['TOTAL_RECORDS'] if count_result else 0
    except Exception as e:
        st.error(f"❌ Error executing query: {e}")
        return

    if total_records == 0:
        empty_conf = config.get('data_display', {}).get('empty_data', {})
        st.info(empty_conf.get('message',
            "📊 No data found. Try adjusting your filters."))
        return

    total_pages = max(1, (total_records + page_size - 1) // page_size)

    # Build and execute data query
    data_sql = assemble_query(
        sql_template, config, filter_values,
        db_name, current_user, security_divisions,
        query_type="data", page=page, page_size=page_size)

    try:
        result_df = session.sql(data_sql).to_pandas()
    except Exception as e:
        st.error(f"❌ Error executing query: {e}")
        return

    # Log query execution
    if audit_logger:
        log_event(audit_logger, 'query_executed', session_id,
                       filter_context=filter_values,
                       record_count=total_records)

    # ── Results Header ────────────────────────────────────────────
    msg_conf = config.get('messages', {}).get('success', {})
    st.success(msg_conf.get('results_display', '').format(
        page=page, total_pages=total_pages,
        record_count=total_records, page_size=page_size))

    # ── Data Table ────────────────────────────────────────────────
    table_conf = config.get('data_display', {}).get('table', {})
    st.dataframe(
        result_df,
        use_container_width=table_conf.get('use_container_width', True),
        height=table_conf.get('height', 400))

    # ── Pagination Controls ───────────────────────────────────────
    if total_pages > 1:
        pcol1, pcol2, pcol3 = st.columns([1, 2, 1])
        with pcol1:
            if st.button("⬅️ Previous", disabled=(page <= 1)):
                st.session_state.page = max(1, page - 1)
                st.rerun()
        with pcol2:
            st.markdown(f"**Page {page} of {total_pages}**")
        with pcol3:
            if st.button("➡️ Next", disabled=(page >= total_pages)):
                st.session_state.page = min(total_pages, page + 1)
                st.rerun()

    # ── Export Buttons ────────────────────────────────────────────
    export_config = config.get('export', {})
    csv_conf = export_config.get('formats', {}).get('csv', {})

    if csv_conf.get('enabled', True):
        if st.button(csv_conf.get('label', '📄 Download CSV')):
            with st.spinner("Preparing download..."):
                # Fetch full dataset for download
                dl_sql = assemble_query(
                    sql_template, config, filter_values,
                    db_name, current_user, security_divisions,
                    query_type="download")
                dl_df = session.sql(dl_sql).to_pandas()

                if csv_conf.get('bundle_as_zip', False):
                    data = export_csv_bundle(
                        dl_df, config, filter_values,
                        current_user, security_divisions)
                    st.download_button(
                        "💾 Download ZIP", data=data,
                        file_name="export.zip",
                        mime="application/zip")
                else:
                    csv_data = dl_df.to_csv(index=False)
                    st.download_button(
                        "💾 Download CSV", data=csv_data,
                        file_name="export.csv",
                        mime="text/csv")

                if audit_logger:
                    log_event(audit_logger, 'data_export', session_id,
                                   details={'format': 'csv'},
                                   record_count=len(dl_df))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _render_disclaimer(dm, config, current_user, app_name,
                             session_id, audit_logger):
    """Render the disclaimer acceptance dialog."""
    disc = config.get('disclaimer', {})
    st.markdown(f"### {disc.get('title', 'Notice')}")

    content = disc.get('content', '')
    max_h = disc.get('body_max_height_px', 500)
    st.markdown(
        f'<div style="max-height:{max_h}px;overflow-y:auto;'
        f'border:1px solid #ccc;padding:16px;border-radius:8px;">'
        f'{content}</div>', unsafe_allow_html=True)

    ui = config.get('ui', {})
    col1, col2 = st.columns(2)
    with col1:
        if st.button(ui.get('accept_button', 'I Accept'), type="primary"):
            version = disc.get('version', 'v1')
            dm.accept(current_user, app_name, version)
            st.session_state[f"disc_ok_{version}_{current_user}"] = True
            if audit_logger:
                log_event(audit_logger, 'disclaimer_accepted', session_id)
            st.rerun()
    with col2:
        if st.button(ui.get('reject_button', 'Exit')):
            version = disc.get('version', 'v1')
            st.session_state[f"disc_rej_{version}_{current_user}"] = True
            st.warning("You must accept the disclaimer to use this application.")
            st.stop()


def _activate_warehouse(session, config, env):
    """Set the warehouse context from config."""
    try:
        db_config = config.get('database', {})
        wh = db_config.get('warehouse', '')
        if wh:
            wh = resolve_env_vars(wh, env)
            session.sql(f"USE WAREHOUSE {wh}").collect()
    except Exception:
        # Try fallback warehouse
        try:
            fallback = config.get('database', {}).get('fallback_warehouse', '')
            if fallback:
                fallback = resolve_env_vars(fallback, env)
                session.sql(f"USE WAREHOUSE {fallback}").collect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_app()
else:
    # When imported as a module in Streamlit-in-Snowflake
    run_app()
