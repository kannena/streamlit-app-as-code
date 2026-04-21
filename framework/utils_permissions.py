"""
Permissions Utility for App-as-Code Query Studio
=================================================

Role-based access control (RBAC) helpers that check user permissions
against the app_user_roles table in Snowflake.
"""

import streamlit as st
from typing import Optional


# Role prefix used to map Snowflake roles to application folders
ROLE_SYNC_PREFIX = "APP-ROLE-FOLDER-"

# Group name for report admin privileges
REPORT_ADMIN_GROUP = "REPORT_ADMIN"


def get_user_access_level(session, role_table: str, username: str) -> str:
    """
    Determine the user's access level from the role helper table.

    Returns one of: 'GLOBAL_ADMIN', 'REPORT_ADMIN', 'BASE_USER', 'GUEST'
    """
    try:
        safe_user = str(username).replace("'", "''")
        sql = f"""
        SELECT ROLE_LEVEL
        FROM {role_table}
        WHERE UPPER(USER_NAME) = UPPER('{safe_user}')
          AND IS_ACTIVE = TRUE
        ORDER BY
            CASE ROLE_LEVEL
                WHEN 'GLOBAL_ADMIN'  THEN 1
                WHEN 'REPORT_ADMIN'  THEN 2
                WHEN 'BASE_USER'     THEN 3
                ELSE 4
            END
        LIMIT 1
        """
        result = session.sql(sql).collect()
        if result:
            return result[0]['ROLE_LEVEL']
    except Exception:
        pass
    return "GUEST"


def get_user_roles(session, username: str) -> list:
    """
    Retrieve the list of Snowflake roles granted to the current user.

    Returns a list of role name strings.
    """
    try:
        result = session.sql("SHOW GRANTS TO USER CURRENT_USER()").collect()
        return [str(row['role']) for row in result
                if str(row.get('granted_on', '')).upper() == 'ROLE']
    except Exception:
        return []


def is_report_admin(session, role_table: str, username: str) -> bool:
    """Check whether the user has REPORT_ADMIN or higher privileges."""
    level = get_user_access_level(session, role_table, username)
    return level in ('GLOBAL_ADMIN', 'REPORT_ADMIN')
