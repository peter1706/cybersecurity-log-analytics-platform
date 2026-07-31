"""Read credentials from container secrets, with an env-var fallback.

Credentials are mounted as files at ``/run/secrets/<name>`` (Docker/Compose
secrets) so they never live in the image, the compose file, or a service's
environment. Every service resolves a credential through :func:`read_secret`;
non-sensitive config (bucket names, window length, log levels) stays on plain
env vars.

The env-var fallback keeps local tooling, unit tests, and ad-hoc ``docker run``
invocations working when no secret file is mounted -- the file always wins when
present, so a mounted secret is never shadowed by a stale env var.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

DEFAULT_SECRETS_DIR = "/run/secrets"


def read_secret(
    name: str,
    *,
    env: str | None = None,
    default: str | None = None,
    env_mapping: Mapping[str, str] | None = None,
) -> str:
    """Return the credential ``name`` from its secret file or an env-var fallback.

    Resolution order:

    1. ``$SECRETS_DIR/<name>`` (``SECRETS_DIR`` defaults to ``/run/secrets``) -- the
       mounted secret file wins whenever it exists and is non-empty.
    2. environment variable ``env`` (defaults to ``name.upper()``).
    3. ``default`` when provided.

    ``env_mapping`` overrides the source of env-var lookups (defaults to
    :data:`os.environ`) so callers that already resolve config from an injected
    mapping can route the fallback through it. Raises :class:`KeyError` when the
    credential resolves to none of the above, so a task fails fast instead of
    authenticating with an empty credential. Raises :class:`ValueError` if
    ``name`` would resolve outside the secrets directory.
    """
    environ: Mapping[str, str] = os.environ if env_mapping is None else env_mapping
    secrets_dir = Path(environ.get("SECRETS_DIR", DEFAULT_SECRETS_DIR))
    path = secrets_dir / name
    if not path.resolve().is_relative_to(secrets_dir.resolve()):
        raise ValueError(f"Invalid secret name {name!r}: escapes the secrets directory")
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        value = ""
    if value:
        return value

    env_key = env or name.upper()
    env_value = environ.get(env_key)
    if env_value:
        return env_value

    if default is not None:
        return default

    raise KeyError(
        f"Secret {name!r} not found: no non-empty file at {path} and environment "
        f"variable {env_key!r} is unset or empty. Mount it as a container secret "
        "(see .env.example / scripts/init_secrets.sh)."
    )
