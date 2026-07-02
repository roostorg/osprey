"""GitLab submission backend.

Implements RuleSubmissionBackend against the GitLab REST v4 API. Works with
gitlab.com and with self-hosted GitLab; set OSPREY_GITLAB_URL to the instance
root (default https://gitlab.com).

GitLab's model maps cleanly onto the same abstraction as GitHub, just with
different endpoint shapes:
  * "merge requests" instead of "pull requests"
  * `source_branch`/`target_branch` instead of `head`/`base`
  * `PRIVATE-TOKEN` header for PATs
  * file paths in URLs are percent-encoded (slashes and all)
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests

from . import _rule_drafts_git_common as git_common
from ._rule_drafts_backend import (
    PendingDraft,
    RuleDraftBackendError,
    SubmissionResult,
)

DEFAULT_GITLAB_URL = 'https://gitlab.com'


@dataclass(frozen=True)
class GitLabConfig:
    gitlab_url: str
    project: str
    base_branch: str
    rules_path: str
    token: str

    @property
    def project_url(self) -> str:
        # GitLab lets you reference a project by URL-encoded namespace/name.
        return f'{self.gitlab_url.rstrip("/")}/api/v4/projects/{quote(self.project, safe="")}'

    @classmethod
    def from_env(cls) -> 'GitLabConfig':
        gitlab_url = (os.environ.get('OSPREY_GITLAB_URL') or DEFAULT_GITLAB_URL).strip()
        project = os.environ.get('OSPREY_GITLAB_PROJECT', '').strip()
        base = os.environ.get('OSPREY_RULES_BASE_BRANCH', 'main').strip() or 'main'
        rules_path = os.environ.get('OSPREY_RULES_PATH_IN_REPO', '').strip().strip('/')
        token = os.environ.get('OSPREY_GITLAB_TOKEN', '').strip()
        if not project:
            raise RuleDraftBackendError(
                'OSPREY_GITLAB_PROJECT is not configured; set it to "namespace/project" '
                '(e.g. "example-org/osprey-rules").',
                status_code=503,
            )
        if not token:
            raise RuleDraftBackendError(
                'OSPREY_GITLAB_TOKEN is not configured; set a project or personal access token with the `api` scope.',
                status_code=503,
            )
        return cls(gitlab_url=gitlab_url, project=project, base_branch=base, rules_path=rules_path, token=token)


def _headers(cfg: GitLabConfig) -> dict[str, str]:
    return {
        'PRIVATE-TOKEN': cfg.token,
        'Accept': 'application/json',
    }


def _full_path(cfg: GitLabConfig, draft_path: str) -> str:
    draft_path = draft_path.lstrip('/')
    if cfg.rules_path:
        return f'{cfg.rules_path}/{draft_path}'
    return draft_path


def _encoded_file_url(cfg: GitLabConfig, path_in_repo: str) -> str:
    # GitLab's files endpoint takes the full path percent-encoded, slashes included.
    return f'{cfg.project_url}/repository/files/{quote(path_in_repo, safe="")}'


def _get(cfg: GitLabConfig, url: str, **kwargs: Any) -> requests.Response:
    return git_common.request('GET', url, error_action='contacting GitLab', headers=_headers(cfg), **kwargs)


def _post(cfg: GitLabConfig, url: str, json: dict[str, Any] | None = None, **kwargs: Any) -> requests.Response:
    return git_common.request('POST', url, error_action='contacting GitLab', headers=_headers(cfg), json=json, **kwargs)


def _put(cfg: GitLabConfig, url: str, json: dict[str, Any]) -> requests.Response:
    return git_common.request('PUT', url, error_action='contacting GitLab', headers=_headers(cfg), json=json)


def _raise_for_gitlab(response: requests.Response, action: str) -> None:
    if response.ok:
        return
    body = response.text[:500]
    raise RuleDraftBackendError(
        f'GitLab returned {response.status_code} while {action}: {body}',
        status_code=502,
    )


def _get_file_on_ref(cfg: GitLabConfig, path_in_repo: str, ref: str) -> tuple[str, str] | None:
    """Fetch (decoded_content, blob_sha) for a file on `ref`, or None if it doesn't exist."""
    res = _get(cfg, _encoded_file_url(cfg, path_in_repo), params={'ref': ref})
    if res.status_code == 404:
        return None
    _raise_for_gitlab(res, f'reading {path_in_repo!r} on {ref!r}')
    payload = res.json()
    encoded = payload.get('content', '')
    blob_id = payload.get('blob_id') or payload.get('last_commit_id')
    if not blob_id:
        return None
    try:
        decoded = base64.b64decode(encoded).decode('utf-8')
    except Exception as exc:
        raise RuleDraftBackendError(f'could not decode {path_in_repo!r}: {exc}', status_code=502)
    return decoded, blob_id


def _file_exists_on_ref(cfg: GitLabConfig, path_in_repo: str, ref: str) -> bool:
    return _get_file_on_ref(cfg, path_in_repo, ref) is not None


def _create_branch(cfg: GitLabConfig, branch: str, ref: str) -> None:
    res = _post(cfg, f'{cfg.project_url}/repository/branches', params={'branch': branch, 'ref': ref})
    if res.status_code == 400 and 'already exists' in res.text.lower():
        raise RuleDraftBackendError(
            f'Branch {branch!r} already exists on {cfg.project}. Pick a different name.',
            status_code=409,
        )
    _raise_for_gitlab(res, f'creating branch {branch!r}')


def _commit_file(
    cfg: GitLabConfig,
    branch: str,
    path_in_repo: str,
    contents: str,
    message: str,
    is_update: bool,
) -> None:
    payload = {
        'branch': branch,
        'content': contents,
        'commit_message': message,
    }
    url = _encoded_file_url(cfg, path_in_repo)
    if is_update:
        res = _put(cfg, url, json=payload)
    else:
        res = _post(cfg, url, json=payload)
    _raise_for_gitlab(res, f'committing {path_in_repo!r} to {branch!r}')


def _open_mr(cfg: GitLabConfig, source_branch: str, title: str, description: str) -> dict[str, Any]:
    res = _post(
        cfg,
        f'{cfg.project_url}/merge_requests',
        json={
            'source_branch': source_branch,
            'target_branch': cfg.base_branch,
            'title': title,
            'description': description,
        },
    )
    _raise_for_gitlab(res, f'opening MR from {source_branch!r}')
    return res.json()


def _main_sml_path(cfg: GitLabConfig) -> str:
    return _full_path(cfg, 'main.sml')


class GitLabBackend:
    name = 'gitlab'

    def __init__(self, cfg: GitLabConfig):
        self._cfg = cfg

    @classmethod
    def from_env(cls) -> 'GitLabBackend':
        return cls(GitLabConfig.from_env())

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
        cfg = self._cfg
        path_in_repo = _full_path(cfg, draft_path)
        existing = _file_exists_on_ref(cfg, path_in_repo, cfg.base_branch)

        if is_new_rule and existing:
            raise RuleDraftBackendError(
                f'A file already exists at {path_in_repo!r} on {cfg.base_branch}. '
                'Pick a different filename, or edit the existing rule instead of creating a new one.',
                status_code=409,
            )

        branch = git_common.generate_branch_name(rule_name, author_email)
        _create_branch(cfg, branch, cfg.base_branch)

        verb = 'Add' if is_new_rule else 'Update'
        commit_message = f'{verb} rule {rule_name}\n\nAuthored via Osprey UI by {author_email}.'
        _commit_file(
            cfg,
            branch=branch,
            path_in_repo=path_in_repo,
            contents=sml_source,
            message=commit_message,
            is_update=existing,
        )

        main_sml_updated = False
        if wire_into_main:
            main_path = _main_sml_path(cfg)
            fetched = _get_file_on_ref(cfg, main_path, cfg.base_branch)
            if fetched is None:
                raise RuleDraftBackendError(
                    f'wire_into_main requested but {main_path!r} does not exist on {cfg.base_branch}.',
                    status_code=409,
                )
            main_contents, _blob = fetched
            if not git_common.require_already_present(main_contents, draft_path):
                new_main = git_common.append_require_to_main(main_contents, draft_path)
                _commit_file(
                    cfg,
                    branch=branch,
                    path_in_repo=main_path,
                    contents=new_main,
                    message=f'Wire {rule_name} into main.sml\n\nAuthored via Osprey UI by {author_email}.',
                    is_update=True,
                )
                main_sml_updated = True

        title = f'{verb} rule {rule_name}'
        touched = f'`{path_in_repo}`'
        if main_sml_updated:
            touched += f', `{_main_sml_path(cfg)}`'
        description = (
            f'{summary.strip() or "_(no summary provided)_"}\n\n'
            f'---\n'
            f'Drafted in the Osprey rules UI by `{author_email}`.\n'
            f'Touches: {touched}.'
        )
        mr = _open_mr(cfg, source_branch=branch, title=title, description=description)
        mr_iid = mr.get('iid')
        mr_url = mr.get('web_url')
        return SubmissionResult(
            title=f'Merge request !{mr_iid} opened',
            url=mr_url,
            main_sml_updated=main_sml_updated,
            extras={
                'mr_iid': mr_iid,
                'mr_url': mr_url,
                'branch': branch,
                'path_in_repo': path_in_repo,
            },
        )

    def list_pending_drafts(self) -> list[PendingDraft]:
        cfg = self._cfg
        res = _get(
            cfg,
            f'{cfg.project_url}/merge_requests',
            params={'state': 'opened', 'target_branch': cfg.base_branch, 'per_page': 30},
        )
        _raise_for_gitlab(res, 'listing open merge requests')
        open_mrs = res.json()

        out: list[PendingDraft] = []
        for mr in open_mrs:
            iid = mr.get('iid')
            if iid is None:
                continue
            files_res = _get(cfg, f'{cfg.project_url}/merge_requests/{iid}/diffs', params={'per_page': 50})
            if not files_res.ok:
                continue
            diffs = files_res.json()
            touched = [
                d['new_path']
                for d in diffs
                if isinstance(d.get('new_path'), str)
                and (not cfg.rules_path or d['new_path'].startswith(cfg.rules_path + '/'))
                and d['new_path'].endswith('.sml')
            ]
            if not touched:
                continue
            out.append(
                PendingDraft(
                    title=mr.get('title', ''),
                    url=mr.get('web_url', ''),
                    author=(mr.get('author') or {}).get('username', ''),
                    created_at=mr.get('created_at', ''),
                    touched_files=touched,
                    extras={
                        'mr_iid': iid,
                        'branch': mr.get('source_branch', ''),
                    },
                )
            )
        return out
