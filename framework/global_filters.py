"""
Global Filters Module
=====================

Manages saved filter presets ("favorites") that can be shared across users
via a role-based folder hierarchy stored in Snowflake.

Features:
    - Save/Load/Delete filter presets per app
    - Folder-based organization with role access control
    - Cascading folder drill-down UI
    - Admin vs. reader role separation
    - Audit logging of all operations
"""

import yaml
import json
import streamlit as st
import pandas as pd
from datetime import datetime, date
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Module-level initialization
# ---------------------------------------------------------------------------

def _load_config():
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        st.error("❌ config.yaml not found.")
        st.stop()

config = _load_config()
APP_NAME = config.get("app_info", {}).get("title", "")
filters_config = config.get("filters", {})

try:
    session = get_active_session()
    st.session_state.snowflake_connected = True
except Exception:
    session = None
    st.session_state.snowflake_connected = False


def detect_env(sess, default_env="DEV"):
    try:
        val = sess.sql("SELECT CURRENT_DATABASE() AS val").collect()[0]["VAL"] or ""
        up = val.upper()
        for k in ("DEV", "QA", "STG", "PROD"):
            if k in up:
                return k
    except Exception:
        pass
    return default_env


def get_db_for_env(env: str) -> str:
    return "ACME_DW" if env == "PROD" else f"{env}_ACME_DW"


def resolve_db_placeholder(sql: str, db_name: str) -> str:
    return sql.replace("{DB}", db_name)


# Audit logger (optional)
try:
    from audit_logger import StreamlitAuditLogger, safe_audit_log
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

ENV_CURRENT = detect_env(session) if session else "DEV"
DB_NAME = get_db_for_env(ENV_CURRENT)

audit_logger = None
if AUDIT_AVAILABLE:
    try:
        if config.get("audit_logging", {}).get("enabled", False):
            audit_logger = StreamlitAuditLogger(
                config, session, APP_NAME, ENV_CURRENT, DB_NAME)
    except Exception:
        audit_logger = None


# ---------------------------------------------------------------------------
# User Identity
# ---------------------------------------------------------------------------

def get_current_user_login() -> str:
    """Get the current user login for filter ownership."""
    try:
        user_info = st.user
        for attr in ("user_name", "login_name", "email"):
            val = getattr(user_info, attr, None)
            if val and str(val).strip():
                raw = str(val).strip()
                return raw.split("@")[0] if "@" in raw else raw
    except Exception:
        pass
    try:
        result = session.sql("SELECT CURRENT_USER() AS u").collect()
        if result:
            return str(result[0]["U"])
    except Exception:
        pass
    return "demo_user"


# ---------------------------------------------------------------------------
# Table Name Resolution
# ---------------------------------------------------------------------------

def _get_table_name(table_key: str) -> str:
    """Resolve a fully qualified table name from the global_filters config."""
    gf_conf = config.get("global_filters", {}).get(table_key, {})
    schema = gf_conf.get("schema", "")
    name = gf_conf.get("name", "")
    if "{DB}" in schema:
        schema = resolve_db_placeholder(schema, DB_NAME)
    return f"{schema}.{name}"


catalog_table = _get_table_name("catalog_table")
folder_table = _get_table_name("folder_table")


# ---------------------------------------------------------------------------
# Filter Value Helpers
# ---------------------------------------------------------------------------

def get_current_filter_values() -> dict:
    """Collect current filter values from session state."""
    values = {}
    for fname, fconf in filters_config.items():
        itype = fconf.get("input_type")
        if itype == "text":
            v = st.session_state.get(f"selected_{fname}", "")
            if v and v.strip():
                values[fname] = v.strip()
        elif itype == "date":
            v = st.session_state.get(f"selected_{fname}")
            if v and isinstance(v, dict):
                values[fname] = v
        else:
            v = st.session_state.get(f"selected_{fname}")
            if isinstance(v, list) and v:
                values[fname] = v
            elif v and str(v).strip():
                values[fname] = v

    col_sel = st.session_state.get("selected_column_selection")
    if col_sel:
        values["column_selection"] = col_sel
    return values


def clear_filters():
    """Reset all filter session state."""
    keys_to_clear = [
        k for k in list(st.session_state.keys())
        if k.startswith("selected_")
        or k.endswith("_multiselect")
        or k.endswith("_input")
        or k.endswith("_from_date")
        or k.endswith("_to_date")
    ]
    for k in keys_to_clear:
        if k.endswith("_multiselect"):
            st.session_state[k] = []
        elif k.endswith("_input"):
            st.session_state[k] = ""
        elif k in st.session_state:
            del st.session_state[k]

    st.session_state.clear_counter = st.session_state.get("clear_counter", 0) + 1
    st.session_state.has_run = False
    st.session_state.page = 1
    st.session_state.total_records = 0


def apply_global_filters(filter_values: dict):
    """Apply loaded filter preset to session state widgets."""
    try:
        clear_filters()
        for fname, value in filter_values.items():
            if fname == "column_selection":
                st.session_state["selected_column_selection"] = value
                st.session_state["column_selection_multiselect"] = value
                continue

            fconf = filters_config.get(fname, {})
            itype = fconf.get("input_type", "")

            if itype == "text":
                st.session_state[f"selected_{fname}"] = value
                st.session_state[f"{fname}_input"] = value
            elif itype == "date" and isinstance(value, dict):
                st.session_state[f"selected_{fname}"] = value
                if "start_date" in value:
                    st.session_state[f"{fname}_from_date"] = _to_date(value["start_date"])
                if "end_date" in value:
                    st.session_state[f"{fname}_to_date"] = _to_date(value["end_date"])
            elif itype == "checkbox":
                st.session_state[f"selected_{fname}"] = value
                st.session_state[f"{fname}_multiselect"] = value
            else:
                st.session_state[f"selected_{fname}"] = value

        st.success("✅ Global filters applied successfully!")
        st.rerun()
    except Exception as e:
        st.error(f"Error applying filters: {e}")


def _to_date(val):
    """Convert string or date to date object."""
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(val, fmt).date()
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Folder Hierarchy
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def get_folder_data(user_key=None):
    """Fetch folders the user has access to based on Snowflake roles."""
    try:
        from utils_permissions import get_user_snowflake_roles
        user_roles = get_user_snowflake_roles(session)
        if not user_roles:
            return None

        roles_str = ", ".join(f"'{r}'" for r in user_roles)
        sql = f"""
            SELECT ID, PARENT_ID, NAME, DESCRIPTION, ALLOWED_ROLES
            FROM {folder_table}
            WHERE ARRAYS_OVERLAP(ALLOWED_ROLES, ARRAY_CONSTRUCT({roles_str}))
            GROUP BY ID, PARENT_ID, NAME, DESCRIPTION, ALLOWED_ROLES
        """
        return st.connection("snowflake").query(sql, ttl=0)
    except Exception as e:
        st.error(f"Error fetching folders: {e}")
        return None


def build_hierarchy_paths(df) -> dict:
    """Build {full_path_string: row_data} from flat folder DataFrame."""
    if df is None or df.empty:
        return {}
    id_map = df.set_index("ID").to_dict("index")
    paths = {}
    for _, row in df.iterrows():
        parts, tid = [], row["ID"]
        while tid and tid in id_map:
            parts.insert(0, id_map[tid]["NAME"])
            tid = id_map[tid]["PARENT_ID"]
            if len(parts) > 20:
                break
        paths[" > ".join(parts)] = row
    return dict(sorted(paths.items()))


def get_folder_children(df, parent_id) -> list:
    """Get immediate children folders of a parent."""
    if df is None or df.empty:
        return []
    if parent_id is None:
        mask = df["PARENT_ID"].isna() | (df["PARENT_ID"] == 0)
    else:
        mask = df["PARENT_ID"] == parent_id
    return df[mask].sort_values("NAME").to_dict("records")


def get_subtree_ids(df, root_id) -> list:
    """Recursively collect all folder IDs under root_id."""
    if df is None or df.empty:
        return []
    ids = []
    if root_id is not None:
        ids.append(root_id)
    if root_id is None:
        children = df[df["PARENT_ID"].isna() | (df["PARENT_ID"] == 0)]["ID"].tolist()
    else:
        children = df[df["PARENT_ID"] == root_id]["ID"].tolist()
    for cid in children:
        ids.extend(get_subtree_ids(df, cid))
    return list(set(ids))


# ---------------------------------------------------------------------------
# Catalog CRUD
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def load_user_globals(user_key=None) -> list:
    """Load all saved filter presets visible to the current user."""
    try:
        from utils_permissions import get_user_snowflake_roles
        user_roles = get_user_snowflake_roles(session)
        if not user_roles:
            return []

        roles_str = ", ".join(f"'{r}'" for r in user_roles)
        sql = f"""
        SELECT C.FAVORITE_NAME, C.FOLDER_ID, C.CREATED_AT, C.UPDATED_AT
          FROM {catalog_table} C
          LEFT JOIN {folder_table} F ON F.ID = C.FOLDER_ID
         WHERE APP_NAME = '{APP_NAME}'
           AND (ARRAYS_OVERLAP(F.ALLOWED_ROLES, ARRAY_CONSTRUCT({roles_str}))
                OR C.FOLDER_ID IS NULL)
         GROUP BY C.FAVORITE_NAME, C.FOLDER_ID, C.CREATED_AT, C.UPDATED_AT
         ORDER BY UPDATED_AT DESC
        """
        result = session.sql(sql).collect()
        return [
            {
                "name": r["FAVORITE_NAME"],
                "folder_id": r["FOLDER_ID"],
                "created_at": r["CREATED_AT"],
                "updated_at": r["UPDATED_AT"],
            }
            for r in result
        ]
    except Exception as e:
        st.error(f"Error loading presets: {e}")
        return []


def load_global_filters(name: str, folder_id) -> dict | None:
    """Load filter JSON for a specific preset."""
    try:
        sql = f"""
        SELECT FILTER_SELECTIONS_JSON
          FROM {catalog_table}
         WHERE APP_NAME      = '{APP_NAME}'
           AND FAVORITE_NAME = '{name}'
           AND FOLDER_ID     = '{folder_id}'
        """
        result = session.sql(sql).collect()
        if not result:
            return None

        data = result[0]["FILTER_SELECTIONS_JSON"]
        if data is None:
            return {}
        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            return json.loads(data)
        if hasattr(data, "items"):
            return dict(data.items())
        return {}
    except Exception as e:
        st.error(f"Error loading preset '{name}': {e}")
        return None


def save_global_filters(name: str, filter_values: dict, folder_id) -> bool:
    """Save or update a filter preset via MERGE."""
    try:
        current_user = get_current_user_login()
        filter_json = json.dumps(filter_values).replace("'", "''").replace('"', '\\"')

        save_sql = f"""
        MERGE INTO {catalog_table} AS target
        USING (
          SELECT
            '{current_user}'                    AS USER_LOGIN_NAME,
            '{APP_NAME}'                        AS APP_NAME,
            '{name}'                            AS FAVORITE_NAME,
            PARSE_JSON('{filter_json}')         AS FILTER_SELECTIONS_JSON,
            '{folder_id}'                       AS FOLDER_ID,
            CURRENT_TIMESTAMP()                 AS UPDATED_AT
        ) AS source
        ON  target.APP_NAME      = source.APP_NAME
        AND target.FAVORITE_NAME = source.FAVORITE_NAME
        AND target.FOLDER_ID     = source.FOLDER_ID
        WHEN MATCHED THEN
          UPDATE SET FILTER_SELECTIONS_JSON = source.FILTER_SELECTIONS_JSON,
                     UPDATED_AT            = source.UPDATED_AT
        WHEN NOT MATCHED THEN
          INSERT (USER_LOGIN_NAME, APP_NAME, FAVORITE_NAME,
                  FILTER_SELECTIONS_JSON, CREATED_AT, UPDATED_AT, FOLDER_ID)
          VALUES (source.USER_LOGIN_NAME, source.APP_NAME, source.FAVORITE_NAME,
                  source.FILTER_SELECTIONS_JSON, CURRENT_TIMESTAMP(),
                  source.UPDATED_AT, source.FOLDER_ID)
        """
        session.sql(save_sql).collect()

        if audit_logger and hasattr(st.session_state, "session_id"):
            try:
                safe_audit_log(
                    audit_logger.log_global_operation,
                    st.session_state.session_id, "save", name, folder_id)
            except Exception:
                pass

        load_user_globals.clear()
        return True
    except Exception as e:
        st.error(f"Error saving preset: {e}")
        return False


def delete_global_filters(name: str, folder_id) -> bool:
    """Delete a filter preset."""
    try:
        sql = f"""
        DELETE FROM {catalog_table}
         WHERE APP_NAME      = '{APP_NAME}'
           AND FAVORITE_NAME = '{name}'
           AND FOLDER_ID     = '{folder_id}'
        """
        session.sql(sql).collect()

        if audit_logger and hasattr(st.session_state, "session_id"):
            try:
                safe_audit_log(
                    audit_logger.log_global_operation,
                    st.session_state.session_id, "delete", name, folder_id)
            except Exception:
                pass

        load_user_globals.clear()
        return True
    except Exception as e:
        st.error(f"Error deleting preset '{name}': {e}")
        return False


# ---------------------------------------------------------------------------
# Admin Check
# ---------------------------------------------------------------------------

def check_report_admin_status() -> bool:
    """Check if the current user has REPORT_ADMIN role."""
    try:
        from init_manager import role_helper_table_name
        current_user = get_current_user_login()
        sql = f"""
            SELECT COUNT(*) AS cnt
            FROM {role_helper_table_name} m
            WHERE USER_NAME = '{current_user}'
              AND IS_ACTIVE = TRUE
              AND ROLE_LEVEL = 'REPORT_ADMIN'
        """
        return session.sql(sql).collect()[0]["CNT"] > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# UI Rendering
# ---------------------------------------------------------------------------

def render_global_filters_section():
    """Main entry point: renders the full global filters UI."""
    if not st.session_state.get("snowflake_connected", False):
        st.info("🔒 Global Filters require a Snowflake connection")
        return

    current_user = get_current_user_login()
    st.session_state["current_user_raw"] = current_user

    is_admin = check_report_admin_status()

    if is_admin:
        save_tab, load_tab, manage_tab = st.tabs(
            ["💾 Save", "📂 Load", "⚙️ Manage"])
        with save_tab:
            _render_save_section()
        with load_tab:
            _render_load_section()
        with manage_tab:
            _render_manage_section()
    else:
        _render_load_section()


def _render_load_section():
    """Load tab: cascading folder drill-down → select and apply a preset."""
    col_h, col_b = st.columns([5, 1])
    with col_h:
        st.markdown("**Load Global Filters**")
        st.caption("Navigate folders to filter the report list.")
    with col_b:
        if st.button("🔄 Refresh", help="Reload latest data"):
            st.cache_data.clear()
            st.rerun()

    _user_key = st.session_state.get("current_user_raw", "")
    globals_data = load_user_globals(user_key=_user_key)
    if not globals_data:
        st.info("📁 No global filters found.")
        return

    raw_folders = get_folder_data(user_key=_user_key)

    # Cascading folder drill-down
    st.markdown(":blue[**Filter by Folder (Optional)**]")
    current_parent = None
    active_folder = None
    level = 0

    while True:
        children = get_folder_children(raw_folders, current_parent)
        if not children:
            break
        folder_map = {f["NAME"]: f["ID"] for f in children}
        sel = st.selectbox(
            f"Level {level + 1}",
            options=list(folder_map.keys()),
            index=None,
            placeholder="Select a folder...",
            key=f"folder_lvl_{level}_{current_parent}",
            label_visibility="collapsed",
        )
        if sel:
            current_parent = folder_map[sel]
            active_folder = current_parent
            level += 1
        else:
            break

    # Filter reports by selected folder subtree
    valid_ids = set(get_subtree_ids(raw_folders, active_folder))
    if active_folder is None:
        valid_ids.update({None, 0})

    visible = [
        g for g in globals_data
        if g.get("folder_id") in valid_ids
        or (g.get("folder_id") is None and active_folder is None)
    ]

    if not visible:
        st.info("No reports found in this location.")
        return

    st.markdown("---")
    report_labels = {f"📄 {r['name']}": r["name"] for r in visible}

    sel_label = st.selectbox(
        f"🗒 Select Preset ({len(visible)})",
        options=[""] + list(report_labels.keys()),
        placeholder="Select a report to load...",
        key="load_global_final_select",
    )

    if sel_label:
        real_name = report_labels[sel_label]
        detail = next((g for g in visible if g["name"] == real_name), None)
        if detail:
            c1, c2 = st.columns([3, 1])
            with c1:
                st.caption(f"Last Updated: {detail['updated_at']}")
            with c2:
                if st.button("📂 Load", type="primary", use_container_width=True):
                    fv = load_global_filters(real_name, detail["folder_id"])
                    if fv:
                        apply_global_filters(fv)


def _render_save_section():
    """Save tab: capture current filters and persist as a named preset."""
    st.markdown("**Save Current Filters**")
    current_filters = get_current_filter_values()
    if not current_filters:
        st.info("🔍 Apply some filters first, then save them here.")
        return

    st.markdown("**📋 Current Filter Settings:**")
    for fname, value in current_filters.items():
        fconf = filters_config.get(fname, {})
        label = fconf.get("label", fname)
        if isinstance(value, list):
            st.write(f"• **{label}:** {len(value)} items selected")
        elif isinstance(value, dict):
            st.write(f"• **{label}:** {value.get('display', str(value))}")
        else:
            st.write(f"• **{label}:** {value}")

    _user_key = st.session_state.get("current_user_raw", "")
    raw_folders = get_folder_data(user_key=_user_key)
    folder_paths = build_hierarchy_paths(raw_folders)

    parent_options = ["No Parent (Root)"] + list(folder_paths.keys())
    sel_parent = st.selectbox("🗂 Parent Folder", options=parent_options, key="parent_save_g")
    parent_id = None
    if sel_parent != "No Parent (Root)":
        try:
            parent_id = folder_paths[sel_parent]["ID"]
        except Exception:
            parent_id = None

    preset_name = st.text_input(
        "Enter a name for this preset:",
        placeholder="e.g., 'East Region Q4 Analysis'",
        key="new_global_filter_name",
    )

    if st.button("💾 Save Preset", type="primary", disabled=not (preset_name and preset_name.strip())):
        if parent_id is None and sel_parent != "No Parent (Root)":
            st.error("Please select a valid folder.")
        elif save_global_filters(preset_name.strip(), current_filters, parent_id):
            st.success(f"✅ Saved preset: '{preset_name.strip()}'")
            st.rerun()


def _render_manage_section():
    """Manage tab: list, preview, rename, and delete presets."""
    st.markdown("**Manage Saved Presets**")

    _user_key = st.session_state.get("current_user_raw", "")
    presets = load_user_globals(user_key=_user_key)
    if not presets:
        st.info("📁 No saved presets to manage.")
        return

    search = st.text_input("🔍 Filter Presets", placeholder="Type to search...",
                           key="search_manage_g")
    if search:
        words = search.lower().split()
        presets = [p for p in presets if all(w in p["name"].lower() for w in words)]

    for preset in presets:
        with st.container():
            c1, c2, c3 = st.columns([4, 1, 1])
            with c1:
                st.write(f"**{preset['name']}**")
                st.caption(f"Updated: {preset['updated_at']}")
            with c2:
                if st.button("👁️", key=f"prev_{preset['name']}_{preset['folder_id']}",
                             help="Preview filters"):
                    fv = load_global_filters(preset["name"], preset["folder_id"])
                    if fv:
                        st.session_state[f"preview_{preset['name']}"] = fv
            with c3:
                if st.button("🗑️", key=f"del_{preset['name']}_{preset['folder_id']}",
                             help="Delete preset"):
                    if delete_global_filters(preset["name"], preset["folder_id"]):
                        st.success(f"Deleted '{preset['name']}'")
                        st.rerun()

            pkey = f"preview_{preset['name']}"
            if st.session_state.get(pkey):
                for fn, fv in st.session_state[pkey].items():
                    label = filters_config.get(fn, {}).get("label", fn)
                    if isinstance(fv, list):
                        st.write(f"• **{label}:** {', '.join(map(str, fv))}")
                    else:
                        st.write(f"• **{label}:** {fv}")
                if st.button("❌ Close", key=f"close_{preset['name']}"):
                    del st.session_state[pkey]
                    st.rerun()

            st.markdown("---")
