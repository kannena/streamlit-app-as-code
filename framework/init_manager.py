import yaml
import re
import threading
import streamlit as st
from snowflake.snowpark.context import get_active_session


def load_config():
    """Load configuration from config.yaml file."""
    try:
        with open("config.yaml", "r") as file:
            return yaml.safe_load(file)
    except Exception:
        return {'app_info': {}, 'branding': {'colors': {}}}


# Initialize Snowflake session
try:
    session = get_active_session()
    st.session_state.snowflake_connected = True
except Exception as e:
    session = None
    st.session_state.snowflake_connected = False


def detect_env(session, default_env="DEV"):
    """Detect current environment from the database name."""
    for query in ["SELECT CURRENT_DATABASE() AS val"]:
        try:
            val = session.sql(query).collect()[0]["VAL"] or ""
            up = val.upper()
            for k in ("DEV", "QA", "STG", "PROD"):
                if k in up:
                    return k
        except Exception:
            continue
    return default_env


def get_db_for_env(env: str) -> str:
    """Return the fully qualified database name for the given environment."""
    return "ACME_DW" if env == "PROD" else f"{env}_ACME_DW"


def resolve_db_placeholder(sql: str, db_name: str) -> str:
    """Replace {DB} placeholder in SQL with the resolved database name."""
    return sql.replace("{DB}", db_name)


def resolve_environ_placeholder(val: str, env: str) -> str:
    """Replace {ENVIRON} placeholder in warehouse names with environment prefix."""
    env_map = {
        "DEV": "DEV_",
        "QA": "QA_",
        "STG": "STG_",
        "PROD": ""
    }
    prefix = env_map.get(env.upper(), "DEV_")
    return val.replace("{ENVIRON}", prefix)


# Detect the current environment
ENV_CURRENT = detect_env(session)

# Resolve database name for current environment
DB_NAME = get_db_for_env(ENV_CURRENT)

config = load_config()


def get_table_name_from_config(table_config_key):
    """Get a fully qualified table name from nested config structure."""
    section = config.get('global_filters', {})
    table_config = section.get(table_config_key, {})
    schema_name = table_config.get('schema', '')
    table_name = table_config.get('name', '')

    if "{DB}" in schema_name:
        schema_name = resolve_db_placeholder(schema_name, DB_NAME)

    return f"{schema_name}.{table_name}"


# =============================================================================
# COMPUTE POOL SERVICE AUTO-SUSPEND OPTIMISATION
# Runs ONCE per container lifecycle via @st.cache_resource.
# Reads target idle timeout from config (compute_pool.service_auto_suspend_secs).
# Finds this app's backing SPCS service and lowers AUTO_SUSPEND_SECS when it
# exceeds the configured target — guards against Snowflake's default.
# Silently no-ops on warehouse-runtime apps where no SPCS service exists.
# =============================================================================
@st.cache_resource
def _set_service_auto_suspend():
    """Fire-and-forget: starts a daemon thread so page load is never blocked."""
    if session is None:
        return

    def _do_alter():
        try:
            cp_config = config.get('compute_pool', {})
            target_secs = cp_config.get('service_auto_suspend_secs', 900)

            app_title = config.get('app_info', {}).get('title', '')
            if not app_title:
                return
            app_pattern = app_title.upper().replace('&', 'AND').replace(' ', '_')
            for suffix in ['_-_QUERY_STUDIO', '_QUERY_STUDIO', '_QS']:
                if app_pattern.endswith(suffix):
                    app_pattern = app_pattern[:-len(suffix)]
                    break
            app_pattern = re.sub(r'[^A-Z0-9_]', '', app_pattern).strip('_')

            db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
            schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]

            result = session.sql(f"SHOW SERVICES IN SCHEMA {db}.{schema}").collect()
            svc_name = None
            for row in result:
                managing_name = str(row["managing_object_name"]).upper()
                if (app_pattern in managing_name
                        and row["status"] == "RUNNING"
                        and row["managing_object_domain"] == "Streamlit"
                        and row["auto_suspend_secs"] > target_secs):
                    svc_name = f"{db}.{schema}.{row['name']}"
                    break

            if svc_name:
                session.sql(
                    f"ALTER SERVICE {svc_name} SET AUTO_SUSPEND_SECS = {target_secs}"
                ).collect()
        except Exception:
            pass

    threading.Thread(target=_do_alter, daemon=True).start()


_set_service_auto_suspend()
