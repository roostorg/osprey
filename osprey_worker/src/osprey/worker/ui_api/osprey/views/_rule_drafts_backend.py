"""Backend abstraction for rule-draft submission.

The Osprey engine doesn't care where rules live. Different deployments use
different hosting: GitHub or Enterprise, GitLab, Tangled, an internal Gerrit,
or a filesystem on a shared volume. Each is one implementation of the
RuleSubmissionBackend Protocol below.

`load_backend()` reads `OSPREY_RULES_SUBMISSION_BACKEND` and instantiates the
chosen backend with its own env vars. Defaults to `null` so an unconfigured
install ships safe; adopters opt into a backend explicitly.

Adopter docs (env vars per backend, how to choose one): see
`docs/user/manage.md`.

Adding a new backend: implement a class with `submit_draft` and
`list_pending_drafts` matching the Protocol below, add a case in
`load_backend()`, and update the "unknown backend" error message here plus
the "no backend configured" message in `_rule_drafts_null.py`. The existing
`_rule_drafts_github.py` and `_rule_drafts_gitlab.py` modules are working
templates for HTTP-backed adapters; `_rule_drafts_local.py` for filesystem.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Protocol


class RuleDraftBackendError(Exception):
    """Raised by any backend method when the operation cannot complete."""

    def __init__(self, message: str, status_code: int = 502):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class SubmissionResult:
    """Backend-neutral submit_draft return value.

    `title` and `url` are what the UI surfaces in the success banner; `extras`
    carries backend-specific fields (PR number, branch, etc.) for adopters
    whose UI variants want to render more detail.
    """

    title: str
    url: str | None
    main_sml_updated: bool = False
    extras: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        # Spread extras first so the canonical fields always win: a backend that
        # happens to name an extra `title`/`url`/`main_sml_updated` can't shadow
        # the contract fields the UI depends on.
        return {
            **self.extras,
            'title': self.title,
            'url': self.url,
            'main_sml_updated': self.main_sml_updated,
        }


@dataclass(frozen=True)
class PendingDraft:
    """Backend-neutral entry for the pending-drafts list."""

    title: str
    url: str
    author: str
    created_at: str
    touched_files: list[str]
    extras: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        # Spread extras first so backend-specific keys can't shadow the
        # canonical fields the UI depends on.
        return {
            **self.extras,
            'title': self.title,
            'url': self.url,
            'author': self.author,
            'created_at': self.created_at,
            'touched_files': self.touched_files,
        }


class RuleSubmissionBackend(Protocol):
    """The contract every submission backend implements.

    Implementations:
      - submit a draft (create whatever the backend's review unit is)
      - optionally wire the new rule into main.sml as part of the same submission
      - list whatever's currently in review

    Implementations raise `RuleDraftBackendError` for any failure path.
    """

    name: str

    def submit_draft(
        self,
        *,
        draft_path: str,
        sml_source: str,
        rule_name: str,
        summary: str,
        author_email: str,
        is_new_rule: bool,
        wire_into_main: bool,
    ) -> SubmissionResult: ...

    def list_pending_drafts(self) -> list[PendingDraft]: ...


def load_backend() -> RuleSubmissionBackend:
    """Select and instantiate the configured backend.

    `OSPREY_RULES_SUBMISSION_BACKEND` picks one of: github, gitlab, local, null.
    Unset or empty defaults to `null`. Unknown values raise so a typo doesn't
    silently degrade to no-op submission.
    """
    name = (os.environ.get('OSPREY_RULES_SUBMISSION_BACKEND') or 'null').strip().lower()

    # Imports are deferred to keep the Protocol module dependency-free.
    if name == 'null':
        from ._rule_drafts_null import NullBackend

        return NullBackend()
    if name == 'github':
        from ._rule_drafts_github import GitHubBackend

        return GitHubBackend.from_env()
    if name == 'gitlab':
        from ._rule_drafts_gitlab import GitLabBackend

        return GitLabBackend.from_env()
    if name == 'local':
        from ._rule_drafts_local import LocalBackend

        return LocalBackend.from_env()
    raise RuleDraftBackendError(
        f'Unknown OSPREY_RULES_SUBMISSION_BACKEND {name!r}; valid values are github, gitlab, local, null.',
        status_code=500,
    )
