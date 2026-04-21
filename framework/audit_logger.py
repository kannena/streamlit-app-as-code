"""
Audit Logging Module for App-as-Code Query Studio
==================================================

Tracks user activities for compliance, analytics, and debugging.
Uses asynchronous batch processing so audit writes never block the UI.

Features:
- Thread-safe batch queue with configurable flush thresholds
- Environment-aware database resolution
- Comprehensive activity tracking (queries, exports, sessions, errors)
- Error resilience — audit failures never crash the application
"""

import json
import time
import uuid
import threading
from datetime import datetime
from typing import Dict, Any, Optional
import streamlit as st


class StreamlitAuditLogger:
    """Asynchronous batch audit logger for Streamlit apps."""

    def __init__(self, config: Dict, session, app_name: str,
                 environment: str, db_name: str):
        self.config = config
        self.session = session
        self.app_name = app_name
        self.environment = environment
        self.db_name = db_name

        self.audit_config = config.get('audit_logging', {})
        self.enabled = self.audit_config.get('enabled', False)

        if not self.enabled:
            return

        # Resolve table name with {DB} placeholder
        table_config = self.audit_config.get('table', {})
        schema_template = table_config.get('schema', '{DB}.audit')
        self.table_schema = schema_template.replace('{DB}', db_name)
        self.table_name = table_config.get('name', 'app_user_activity')
        self.full_table_name = f"{self.table_schema}.{self.table_name}"

        # Batch processing settings
        self.async_logging = self.audit_config.get('async_logging', True)
        self.batch_size = self.audit_config.get('batch_size', 10)
        self.batch_timeout = self.audit_config.get('batch_timeout_seconds', 30)

        # Thread-safe batch queue
        self.batch_queue = []
        self.last_batch_time = time.time()
        self.batch_lock = threading.Lock()

        self.activities = self.audit_config.get('activities', {})

    # ------------------------------------------------------------------ #
    # User detection
    # ------------------------------------------------------------------ #

    def _get_current_user(self) -> str:
        """Get current user from Streamlit SSO or Snowflake session."""
        try:
            user_info = st.user
            for attr in ('user_name', 'login_name', 'email'):
                val = getattr(user_info, attr, None)
                if val and str(val).strip():
                    return str(val).split("@")[0] if "@" in str(val) else str(val)
        except Exception:
            pass
        if self.session:
            try:
                result = self.session.sql("SELECT CURRENT_USER() AS u").collect()
                if result:
                    raw = result[0]['U']
                    if raw:
                        return str(raw).split('@')[0] if '@' in str(raw) else str(raw)
            except Exception:
                pass
        return "unknown_user"

    def _get_current_warehouse(self) -> str:
        try:
            if self.session:
                result = self.session.sql("SELECT CURRENT_WAREHOUSE()").collect()
                if result and result[0][0]:
                    return result[0][0]
        except Exception:
            pass
        return "unknown_warehouse"

    # ------------------------------------------------------------------ #
    # Entry creation & batching
    # ------------------------------------------------------------------ #

    def _create_audit_entry(self, activity_type: str, session_id: str,
                            details: Optional[Dict] = None,
                            filter_context: Optional[Dict] = None,
                            record_count: Optional[int] = None,
                            error_message: Optional[str] = None) -> Dict[str, Any]:
        return {
            'app_name': self.app_name,
            'username': self._get_current_user(),
            'activity_type': activity_type,
            'activity_details': json.dumps(details) if details else None,
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'environment': self.environment,
            'warehouse_used': self._get_current_warehouse(),
            'filter_context': json.dumps(filter_context) if filter_context else None,
            'record_count': record_count if record_count is not None else 0,
            'error_message': error_message
        }

    def log_activity(self, activity_type: str, session_id: str, **kwargs):
        """Queue an audit entry. Flushes automatically when thresholds are met."""
        if not self.enabled:
            return
        entry = self._create_audit_entry(activity_type, session_id, **kwargs)
        self._add_to_batch(entry)

    def _add_to_batch(self, audit_entry: Dict[str, Any]):
        if not self.enabled:
            return
        with self.batch_lock:
            self.batch_queue.append(audit_entry)
            elapsed = time.time() - self.last_batch_time
            if len(self.batch_queue) >= self.batch_size or elapsed > self.batch_timeout:
                self._flush_batch()

    def _flush_batch(self):
        """Write queued entries to the audit table."""
        if not self.enabled or not self.batch_queue or not self.session:
            return
        entries = self.batch_queue.copy()
        self.batch_queue.clear()
        self.last_batch_time = time.time()

        if self.async_logging:
            threading.Thread(target=self._write_entries, args=(entries,), daemon=True).start()
        else:
            self._write_entries(entries)

    def _write_entries(self, entries):
        """Insert audit entries into Snowflake — runs in a background thread."""
        for entry in entries:
            try:
                # Escape values for safe SQL
                def esc(v):
                    return str(v).replace("'", "''") if v else ''

                sql = f"""
                INSERT INTO {self.full_table_name}
                (app_name, username, activity_type, activity_details,
                 session_id, timestamp, environment, warehouse_used,
                 filter_context, record_count, error_message)
                VALUES (
                    '{esc(entry["app_name"])}',
                    '{esc(entry["username"])}',
                    '{esc(entry["activity_type"])}',
                    '{esc(entry.get("activity_details"))}',
                    '{esc(entry["session_id"])}',
                    '{esc(entry["timestamp"])}',
                    '{esc(entry["environment"])}',
                    '{esc(entry["warehouse_used"])}',
                    '{esc(entry.get("filter_context"))}',
                    {entry.get("record_count", 0)},
                    '{esc(entry.get("error_message"))}'
                )
                """
                self.session.sql(sql).collect()
            except Exception:
                pass  # Never crash the app for audit failures

    def flush_remaining(self):
        """Force-flush any remaining entries (call on session end)."""
        with self.batch_lock:
            if self.batch_queue:
                self._flush_batch()


# --------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------- #

def generate_session_id() -> str:
    """Generate a unique session identifier."""
    return str(uuid.uuid4())[:12]


def safe_audit_log(audit_logger, activity_type: str, session_id: str, **kwargs):
    """Wrapper that ensures audit logging never raises exceptions."""
    try:
        if audit_logger and hasattr(audit_logger, 'log_activity'):
            audit_logger.log_activity(activity_type, session_id, **kwargs)
    except Exception:
        pass
