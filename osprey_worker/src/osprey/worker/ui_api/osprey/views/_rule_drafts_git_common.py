"""Shared helpers for git-forge submission backends.

The GitHub, GitLab, and Tangled adapters all need the same three things: a
cosmetic branch name, a check for whether main.sml already wires a rule in, and
the append that adds the wiring. They also all talk to a remote over HTTP and
must turn a dropped connection into the same structured error the UI renders
rather than an unhandled 500. This module is the one place those live.
"""

from __future__ import annotations

import re
import time
from typing import Any

import requests

from ._rule_drafts_backend import RuleDraftBackendError

DEFAULT_TIMEOUT_SECONDS = 15


def request(method: str, url: str, *, error_action: str, **kwargs: Any) -> requests.Response:
    """Issue an HTTP request, converting transport failures to RuleDraftBackendError.

    A forge outage (connection refused, DNS failure, timeout) is an expected
    operational state for a backend whose job is talking to a remote host, so it
    should surface as the 502 JSON shape the editor knows how to display, not as
    an unhandled Flask 500. HTTP status errors are left for the caller to map,
    since the right status code depends on what was being attempted.
    """
    kwargs.setdefault('timeout', DEFAULT_TIMEOUT_SECONDS)
    try:
        return requests.request(method, url, **kwargs)
    except requests.RequestException as exc:
        raise RuleDraftBackendError(
            f'Could not reach the git host while {error_action}: {exc}',
            status_code=502,
        ) from exc


def generate_branch_name(rule_name: str, author_email: str, *, prefix: str = 'rule-draft') -> str:
    """Cosmetic source-branch label. Timestamped so retries don't collide."""
    short_email = author_email.split('@', 1)[0]
    slug = re.sub(r'[^A-Za-z0-9_-]+', '-', short_email).strip('-') or 'osprey-ui'
    rule_slug = re.sub(r'[^A-Za-z0-9_-]+', '-', rule_name).strip('-') or 'rule'
    return f'{prefix}/{slug}/{rule_slug}-{int(time.time())}'


def require_already_present(main_sml: str, draft_path: str) -> bool:
    pattern = re.compile(
        r"Require\s*\(\s*rule\s*=\s*['\"]" + re.escape(draft_path) + r"['\"]\s*\)",
        re.MULTILINE,
    )
    return bool(pattern.search(main_sml))


def append_require_to_main(main_sml: str, draft_path: str) -> str:
    suffix = f"\nRequire(rule='{draft_path}')\n"
    if not main_sml.endswith('\n'):
        suffix = '\n' + suffix
    return main_sml + suffix
