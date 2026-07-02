"""Filesystem submission backend for self-hosted setups.

Writes the SML straight to a configured rules directory. No review, no PR.
Adopters whose deploy pipeline already syncs a rules directory into the engine
(etcd push, file watcher, etc.) wire this backend up so the UI drops the SML
in the right place and lets the downstream pipeline take it from there.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from . import _rule_drafts_git_common as git_common
from ._rule_drafts_backend import (
    PendingDraft,
    RuleDraftBackendError,
    SubmissionResult,
)


@dataclass(frozen=True)
class LocalConfig:
    rules_dir: Path

    @classmethod
    def from_env(cls) -> 'LocalConfig':
        raw = os.environ.get('OSPREY_RULES_LOCAL_PATH', '').strip()
        if not raw:
            raise RuleDraftBackendError(
                'OSPREY_RULES_LOCAL_PATH is not configured; set it to the directory the local backend should write to.',
                status_code=503,
            )
        path = Path(raw)
        if not path.is_dir():
            raise RuleDraftBackendError(
                f'OSPREY_RULES_LOCAL_PATH {raw!r} is not a directory.',
                status_code=503,
            )
        return cls(rules_dir=path)


class LocalBackend:
    name = 'local'

    def __init__(self, cfg: LocalConfig):
        self._cfg = cfg

    @classmethod
    def from_env(cls) -> 'LocalBackend':
        return cls(LocalConfig.from_env())

    def _resolve(self, draft_path: str) -> Path:
        """Resolve a draft path within rules_dir, refusing anything that would
        escape via `..` or symlink traversal."""
        candidate = (self._cfg.rules_dir / draft_path).resolve()
        try:
            candidate.relative_to(self._cfg.rules_dir.resolve())
        except ValueError as exc:
            raise RuleDraftBackendError(
                f'Draft path {draft_path!r} escapes the configured rules directory.',
                status_code=400,
            ) from exc
        return candidate

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
        target = self._resolve(draft_path)
        if is_new_rule and target.exists():
            raise RuleDraftBackendError(
                f'A file already exists at {draft_path!r}. Pick a different filename or edit the existing rule.',
                status_code=409,
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(sml_source, encoding='utf-8')

        main_sml_updated = False
        if wire_into_main:
            main_path = self._cfg.rules_dir / 'main.sml'
            if not main_path.exists():
                raise RuleDraftBackendError(
                    f'wire_into_main requested but main.sml does not exist at {main_path}.',
                    status_code=409,
                )
            main_contents = main_path.read_text(encoding='utf-8')
            if not git_common.require_already_present(main_contents, draft_path):
                main_path.write_text(git_common.append_require_to_main(main_contents, draft_path), encoding='utf-8')
                main_sml_updated = True

        verb = 'Created' if is_new_rule else 'Updated'
        return SubmissionResult(
            title=f'{verb} {draft_path} in local rules directory',
            url=None,
            main_sml_updated=main_sml_updated,
            extras={'path_on_disk': str(target)},
        )

    def list_pending_drafts(self) -> list[PendingDraft]:
        # Local backend has no review queue; submissions take effect immediately.
        return []
