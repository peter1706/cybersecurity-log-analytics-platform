"""Pure auth helpers for the Streamlit dashboard login gate."""

from __future__ import annotations

import secrets


def credentials_match(
    username: str,
    password: str,
    *,
    expected_username: str,
    expected_password: str,
) -> bool:
    """Return True when the submitted credentials match the expected pair.

    Uses constant-time comparison for both fields. Empty expected credentials
    are treated as a configuration error (never a successful login).

    Args:
        username: Submitted username.
        password: Submitted password.
        expected_username: Configured dashboard username.
        expected_password: Configured dashboard password.

    Returns:
        Whether both fields match.

    Raises:
        ValueError: If expected username or password is empty.
    """
    if not expected_username or not expected_password:
        raise ValueError("Dashboard login secrets are not configured")
    user_ok = secrets.compare_digest(username, expected_username)
    pass_ok = secrets.compare_digest(password, expected_password)
    return user_ok and pass_ok
