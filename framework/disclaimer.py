"""
Disclaimer Management Module for App-as-Code Query Studio
==========================================================

Provides persistent disclaimer acceptance tracking with configurable
validity periods, audit trails, and environment-aware storage.

Features:
- Cross-session persistence using Snowflake database (MERGE-based)
- Configurable validity period (default 90 days)
- Environment isolation (DEV/QA/STG/PROD)
- Version-aware disclaimer management
- Graceful fallback to session state on database errors
"""

import streamlit as st
import json
from datetime import datetime
from typing import Dict, Any, Optional


class DisclaimerHandler:
    """Manages disclaimer acceptance with persistent storage."""

    def __init__(self, session, config: Dict, db_name: str,
                 environment: str, audit_logger=None):
        self.session = session
        self.config = config
        self.db_name = db_name
        self.environment = environment
        self.audit_logger = audit_logger
        self.disclaimer_config = config.get('disclaimer', {})
        self.persistence_config = self.disclaimer_config.get('persistence', {})

        if self.persistence_config.get('auto_create_table', False):
            self.ensure_table()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def needs_acceptance(self, username: str, app_name: str) -> bool:
        """Return True if the disclaimer dialog should be displayed."""
        if not self.disclaimer_config.get("enabled", False):
            return False

        version = self.disclaimer_config.get("version", "v1")

        # Check persistent acceptance first
        if self.persistence_config.get('enabled', False):
            if self._lookup_acceptance(username, app_name, version):
                return False

        # Session-state fallback (backward compatibility)
        accept_key = f"disc_ok_{version}_{username}"
        reject_key = f"disc_rej_{version}_{username}"

        if st.session_state.get(reject_key):
            return True
        if st.session_state.get(accept_key) and not self.disclaimer_config.get("show_every_session", False):
            return False

        return True

    def accept(self, username: str, app_name: str, version: str) -> bool:
        """Record disclaimer acceptance via MERGE (upsert)."""
        try:
            if not self.persistence_config.get('enabled', False):
                return True

            table = self._full_table_name()
            safe_user = self._sanitize(username)
            safe_app = self._sanitize(app_name)
            safe_ver = self._sanitize(version)
            safe_env = self._sanitize(self.environment)

            session_info = json.dumps({
                'user_agent': 'streamlit_app',
                'app_title': self.config.get('app_info', {}).get('title', ''),
                'timestamp': datetime.now().isoformat(),
                'environment': self.environment
            }).replace("'", "''")

            sql = f"""
            MERGE INTO {table} AS tgt
            USING (
                SELECT
                    '{safe_user}'  AS username,
                    '{safe_app}'   AS app_name,
                    '{safe_ver}'   AS disclaimer_version,
                    '{safe_env}'   AS environment,
                    CURRENT_TIMESTAMP() AS accepted_timestamp,
                    PARSE_JSON('{session_info}') AS session_info
            ) AS src
            ON  tgt.username = src.username
            AND tgt.app_name = src.app_name
            AND tgt.disclaimer_version = src.disclaimer_version
            AND tgt.environment = src.environment
            WHEN MATCHED THEN UPDATE SET
                accepted_timestamp = src.accepted_timestamp,
                session_info       = src.session_info
            WHEN NOT MATCHED THEN INSERT
                (username, app_name, disclaimer_version, environment,
                 accepted_timestamp, session_info)
            VALUES
                (src.username, src.app_name, src.disclaimer_version,
                 src.environment, src.accepted_timestamp, src.session_info)
            """
            self.session.sql(sql).collect()
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    # Persistence helpers
    # ------------------------------------------------------------------ #

    def _lookup_acceptance(self, username: str, app_name: str,
                                     version: str) -> bool:
        try:
            table = self._full_table_name()
            days = self.persistence_config.get('validity_days', 90)
            sql = f"""
            SELECT COUNT(*) AS cnt
            FROM {table}
            WHERE username            = '{self._sanitize(username)}'
              AND app_name            = '{self._sanitize(app_name)}'
              AND disclaimer_version  = '{self._sanitize(version)}'
              AND environment         = '{self._sanitize(self.environment)}'
              AND accepted_timestamp >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            """
            result = self.session.sql(sql).collect()
            return (result[0]['CNT'] if result else 0) > 0
        except Exception:
            return False

    def ensure_table(self):
        """Create the disclaimer tracking table if it does not exist."""
        try:
            table = self._full_table_name()
            self.session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                username              VARCHAR,
                app_name              VARCHAR,
                disclaimer_version    VARCHAR,
                environment           VARCHAR,
                accepted_timestamp    TIMESTAMP_NTZ,
                session_info          VARIANT
            )
            """).collect()
        except Exception:
            pass

    def _full_table_name(self) -> str:
        raw = self.persistence_config.get(
            'table_name', '{DB}.utility.disclaimer_acceptances')
        return raw.replace('{DB}', self.db_name)

    @staticmethod
    def _sanitize(val) -> str:
        return str(val).replace("'", "''") if val else ''
