"""
Subscription Management Module for App-as-Code Query Studio
============================================================

Enables users to save their current filter selections and schedule
automated report delivery (daily, weekly, monthly).

Features:
- Save current query as a named subscription with frequency
- Capture the complete executable SQL with all filters baked in
- Enhanced scheduling (daily time, weekly day+time, monthly day+time)
- Database-persisted subscription storage with audit trail
"""

import streamlit as st
import json
import datetime
from datetime import time
from typing import Dict, Any, Optional


class SubscriptionManager:
    """Manages report subscriptions with database persistence."""

    def __init__(self, session, config: Dict, db_name: str, environment: str,
                 sql_template: str = "", filters_config: Dict = None,
                 audit_logger=None, build_query_func=None):
        self.session = session
        self.config = config
        self.db_name = db_name
        self.environment = environment
        self.sql_template = sql_template
        self.filters_config = filters_config or {}
        self.audit_logger = audit_logger
        self.build_query_func = build_query_func
        self.app_name = config.get('app_info', {}).get('title', 'StreamlitApp')
        self.frequency_options = ['Daily', 'Weekly', 'Monthly']

    # ------------------------------------------------------------------ #
    # Table resolution
    # ------------------------------------------------------------------ #

    def get_subscription_table_name(self) -> str:
        sub_config = self.config.get('subscriptions', {})
        table_config = sub_config.get('table', {})
        schema = table_config.get('schema', '{DB}.audit').replace('{DB}', self.db_name)
        name = table_config.get('name', 'user_subscriptions')
        return f"{schema}.{name}"

    def check_subscription_table(self) -> bool:
        """Check if the subscription table exists."""
        try:
            if not st.session_state.get('snowflake_connected', False):
                return False
            full_name = self.get_subscription_table_name()
            sub_config = self.config.get('subscriptions', {})
            table_config = sub_config.get('table', {})
            schema_name = table_config.get('schema', '{DB}.audit') \
                .replace('{DB}', self.db_name).split('.')[-1]
            table_name = table_config.get('name', 'user_subscriptions')

            sql = f"""
            SELECT COUNT(*) AS table_count
            FROM {self.db_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name.upper()}'
              AND TABLE_NAME   = '{table_name.upper()}'
            """
            result = self.session.sql(sql).collect()
            exists = result[0]['TABLE_COUNT'] > 0 if result else False
            if not exists:
                st.warning(f"Subscription table `{full_name}` not found.")
            return exists
        except Exception as e:
            st.error(f"Error checking subscription table: {e}")
            return False

    # ------------------------------------------------------------------ #
    # User detection
    # ------------------------------------------------------------------ #

    def get_current_user_info(self):
        """Return (user_id, user_email, error_message)."""
        try:
            user_info = st.user
            email = getattr(user_info, 'email', None)
            if email:
                email = str(email).strip()
            uid = None
            for attr in ('user_name', 'login_name'):
                val = getattr(user_info, attr, None)
                if val and str(val).strip():
                    uid = str(val).split("@")[0] if "@" in str(val) else str(val)
                    break
            if not uid and email:
                uid = email.split("@")[0]
            if uid and email:
                return uid, email, None
        except Exception:
            pass
        try:
            result = self.session.sql("SELECT CURRENT_USER() AS u").collect()
            if result:
                raw = result[0]["U"]
                uid = str(raw).split("@")[0] if "@" in str(raw) else str(raw)
                email = str(raw) if "@" in str(raw) else f"{uid}@example.com"
                return uid, email, None
        except Exception:
            pass
        return None, None, "Could not retrieve user information"

    # ------------------------------------------------------------------ #
    # Schedule formatting
    # ------------------------------------------------------------------ #

    def _format_schedule_summary(self, frequency, schedule_time=None,
                                 schedule_day_of_week=None,
                                 schedule_day_of_month=None) -> str:
        if frequency == "Daily":
            t = schedule_time.strftime('%I:%M %p') if schedule_time else "any time"
            return f"📅 **Schedule**: Daily at {t}"
        elif frequency == "Weekly":
            days = ["Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday"]
            day = days[schedule_day_of_week - 1] if schedule_day_of_week else "Unknown"
            t = schedule_time.strftime('%I:%M %p') if schedule_time else "9:00 AM"
            return f"📅 **Schedule**: Every {day} at {t}"
        elif frequency == "Monthly":
            t = schedule_time.strftime('%I:%M %p') if schedule_time else "9:00 AM"
            if schedule_day_of_month == "LAST_DAY":
                return f"📅 **Schedule**: Last day of every month at {t}"
            elif schedule_day_of_month == "LAST_BUSINESS_DAY":
                return f"📅 **Schedule**: Last business day of every month at {t}"
            else:
                return f"📅 **Schedule**: Day {schedule_day_of_month} of every month at {t}"
        return f"📅 **Schedule**: {frequency}"


def get_subscription_manager(session, config, db_name, environment, **kwargs):
    """Factory function for creating a SubscriptionManager instance."""
    return SubscriptionManager(session, config, db_name, environment, **kwargs)
