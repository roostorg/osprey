"""Default submission backend: nothing is configured, so every call fails fast.

Ships as the default so an unconfigured upstream install never opens a PR or
writes a file without an adopter explicitly opting into a backend.
"""

from __future__ import annotations

from ._rule_drafts_backend import (
    PendingDraft,
    RuleDraftBackendError,
    SubmissionResult,
)


class NullBackend:
    name = 'null'

    @staticmethod
    def _err() -> RuleDraftBackendError:
        return RuleDraftBackendError(
            'No rule-submission backend is configured. Set OSPREY_RULES_SUBMISSION_BACKEND '
            'to one of: github, gitlab, tangled, local. See docs for what each one needs.',
            status_code=503,
        )

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
    ) -> SubmissionResult:
        raise self._err()

    def list_pending_drafts(self) -> list[PendingDraft]:
        raise self._err()
