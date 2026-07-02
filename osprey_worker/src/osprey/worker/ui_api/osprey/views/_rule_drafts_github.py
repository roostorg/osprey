"""GitHub submission backend.

Implements RuleSubmissionBackend by opening a pull request against a configured
repo. Works with github.com and with GitHub Enterprise (set OSPREY_GITHUB_API_URL
to the Enterprise API root, e.g. https://github.mycompany.com/api/v3).
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Any

import requests

from . import _rule_drafts_git_common as git_common
from ._rule_drafts_backend import (
    PendingDraft,
    RuleDraftBackendError,
    SubmissionResult,
)

DEFAULT_GITHUB_API = 'https://api.github.com'


@dataclass(frozen=True)
class GitHubConfig:
    api_url: str
    repo: str
    base_branch: str
    rules_path: str
    token: str

    @property
    def repo_url(self) -> str:
        return f'{self.api_url.rstrip("/")}/repos/{self.repo}'

    @classmethod
    def from_env(cls) -> 'GitHubConfig':
        api_url = (os.environ.get('OSPREY_GITHUB_API_URL') or DEFAULT_GITHUB_API).strip()
        repo = os.environ.get('OSPREY_RULES_REPO', '').strip()
        base = os.environ.get('OSPREY_RULES_BASE_BRANCH', 'main').strip() or 'main'
        rules_path = os.environ.get('OSPREY_RULES_PATH_IN_REPO', '').strip().strip('/')
        token = os.environ.get('OSPREY_GITHUB_TOKEN', '').strip()
        if not repo:
            raise RuleDraftBackendError(
                'OSPREY_RULES_REPO is not configured; set it to "owner/name" to enable PR submission.',
                status_code=503,
            )
        if not token:
            raise RuleDraftBackendError(
                'OSPREY_GITHUB_TOKEN is not configured; set a service-account PAT with repo write access.',
                status_code=503,
            )
        return cls(api_url=api_url, repo=repo, base_branch=base, rules_path=rules_path, token=token)


def _headers(cfg: GitHubConfig) -> dict[str, str]:
    return {
        'Authorization': f'Bearer {cfg.token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
    }


def _full_path(cfg: GitHubConfig, draft_path: str) -> str:
    draft_path = draft_path.lstrip('/')
    if cfg.rules_path:
        return f'{cfg.rules_path}/{draft_path}'
    return draft_path


def _get(cfg: GitHubConfig, url: str, **kwargs: Any) -> requests.Response:
    return git_common.request('GET', url, error_action='contacting GitHub', headers=_headers(cfg), **kwargs)


def _post(cfg: GitHubConfig, url: str, json: dict[str, Any]) -> requests.Response:
    return git_common.request('POST', url, error_action='contacting GitHub', headers=_headers(cfg), json=json)


def _put(cfg: GitHubConfig, url: str, json: dict[str, Any]) -> requests.Response:
    return git_common.request('PUT', url, error_action='contacting GitHub', headers=_headers(cfg), json=json)


def _raise_for_github(response: requests.Response, action: str) -> None:
    if response.ok:
        return
    body = response.text[:500]
    # 4xx from GitHub is still a backend failure from the caller's POV.
    raise RuleDraftBackendError(
        f'GitHub returned {response.status_code} while {action}: {body}',
        status_code=502,
    )


def _get_base_sha(cfg: GitHubConfig) -> str:
    res = _get(cfg, f'{cfg.repo_url}/git/ref/heads/{cfg.base_branch}')
    _raise_for_github(res, f'looking up base branch {cfg.base_branch!r}')
    return res.json()['object']['sha']


def _get_file_sha(cfg: GitHubConfig, path_in_repo: str, ref: str) -> str | None:
    res = _get(cfg, f'{cfg.repo_url}/contents/{path_in_repo}', params={'ref': ref})
    if res.status_code == 404:
        return None
    _raise_for_github(res, f'reading {path_in_repo!r} on {ref!r}')
    payload = res.json()
    if isinstance(payload, list):
        return None
    return payload.get('sha')


def _create_branch(cfg: GitHubConfig, branch: str, sha: str) -> None:
    res = _post(cfg, f'{cfg.repo_url}/git/refs', json={'ref': f'refs/heads/{branch}', 'sha': sha})
    if res.status_code == 422:
        raise RuleDraftBackendError(
            f'Branch {branch!r} already exists on {cfg.repo}. Pick a different name.',
            status_code=409,
        )
    _raise_for_github(res, f'creating branch {branch!r}')


def _commit_file(
    cfg: GitHubConfig,
    branch: str,
    path_in_repo: str,
    contents: str,
    message: str,
    existing_sha: str | None,
) -> None:
    payload: dict[str, Any] = {
        'message': message,
        'content': base64.b64encode(contents.encode('utf-8')).decode('ascii'),
        'branch': branch,
    }
    if existing_sha is not None:
        payload['sha'] = existing_sha
    res = _put(cfg, f'{cfg.repo_url}/contents/{path_in_repo}', json=payload)
    _raise_for_github(res, f'committing {path_in_repo!r} to {branch!r}')


def _open_pr(cfg: GitHubConfig, branch: str, title: str, body: str) -> dict[str, Any]:
    res = _post(
        cfg,
        f'{cfg.repo_url}/pulls',
        json={'title': title, 'head': branch, 'base': cfg.base_branch, 'body': body},
    )
    _raise_for_github(res, f'opening PR from {branch!r}')
    return res.json()


def _main_sml_path(cfg: GitHubConfig) -> str:
    return _full_path(cfg, 'main.sml')


def _fetch_file_on_ref(cfg: GitHubConfig, path_in_repo: str, ref: str) -> tuple[str, str] | None:
    res = _get(cfg, f'{cfg.repo_url}/contents/{path_in_repo}', params={'ref': ref})
    if res.status_code == 404:
        return None
    _raise_for_github(res, f'reading {path_in_repo!r} on {ref!r}')
    payload = res.json()
    if isinstance(payload, list):
        return None
    encoded = payload.get('content', '')
    sha = payload.get('sha')
    if sha is None:
        return None
    try:
        decoded = base64.b64decode(encoded).decode('utf-8')
    except Exception as exc:
        raise RuleDraftBackendError(f'could not decode {path_in_repo!r}: {exc}', status_code=502)
    return decoded, sha


class GitHubBackend:
    name = 'github'

    def __init__(self, cfg: GitHubConfig):
        self._cfg = cfg

    @classmethod
    def from_env(cls) -> 'GitHubBackend':
        return cls(GitHubConfig.from_env())

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
        base_sha = _get_base_sha(cfg)
        existing_sha = _get_file_sha(cfg, path_in_repo, cfg.base_branch)

        if is_new_rule and existing_sha is not None:
            raise RuleDraftBackendError(
                f'A file already exists at {path_in_repo!r} on {cfg.base_branch}. '
                'Pick a different filename, or edit the existing rule instead of creating a new one.',
                status_code=409,
            )

        branch = git_common.generate_branch_name(rule_name, author_email)
        _create_branch(cfg, branch, base_sha)

        verb = 'Add' if is_new_rule else 'Update'
        commit_message = f'{verb} rule {rule_name}\n\nAuthored via Osprey UI by {author_email}.'
        _commit_file(
            cfg,
            branch=branch,
            path_in_repo=path_in_repo,
            contents=sml_source,
            message=commit_message,
            existing_sha=existing_sha,
        )

        main_sml_updated = False
        if wire_into_main:
            main_path = _main_sml_path(cfg)
            fetched = _fetch_file_on_ref(cfg, main_path, cfg.base_branch)
            if fetched is None:
                raise RuleDraftBackendError(
                    f'wire_into_main requested but {main_path!r} does not exist on {cfg.base_branch}.',
                    status_code=409,
                )
            main_contents, main_sha = fetched
            if not git_common.require_already_present(main_contents, draft_path):
                new_main = git_common.append_require_to_main(main_contents, draft_path)
                _commit_file(
                    cfg,
                    branch=branch,
                    path_in_repo=main_path,
                    contents=new_main,
                    message=f'Wire {rule_name} into main.sml\n\nAuthored via Osprey UI by {author_email}.',
                    existing_sha=main_sha,
                )
                main_sml_updated = True

        title = f'{verb} rule {rule_name}'
        touched = f'`{path_in_repo}`'
        if main_sml_updated:
            touched += f', `{_main_sml_path(cfg)}`'
        body = (
            f'{summary.strip() or "_(no summary provided)_"}\n\n'
            f'---\n'
            f'Drafted in the Osprey rules UI by `{author_email}`.\n'
            f'Touches: {touched}.'
        )
        pr = _open_pr(cfg, branch=branch, title=title, body=body)
        pr_number = pr.get('number')
        pr_url = pr.get('html_url')
        return SubmissionResult(
            title=f'Pull request #{pr_number} opened',
            url=pr_url,
            main_sml_updated=main_sml_updated,
            extras={
                'pr_number': pr_number,
                'pr_url': pr_url,
                'branch': branch,
                'path_in_repo': path_in_repo,
            },
        )

    def list_pending_drafts(self) -> list[PendingDraft]:
        cfg = self._cfg
        res = _get(
            cfg,
            f'{cfg.repo_url}/pulls',
            params={'state': 'open', 'base': cfg.base_branch, 'per_page': 30},
        )
        _raise_for_github(res, 'listing open pull requests')
        open_prs = res.json()

        out: list[PendingDraft] = []
        for pr in open_prs:
            number = pr.get('number')
            if number is None:
                continue
            files_res = _get(cfg, f'{cfg.repo_url}/pulls/{number}/files', params={'per_page': 50})
            if not files_res.ok:
                continue
            files = files_res.json()
            touched = [
                f['filename']
                for f in files
                if isinstance(f.get('filename'), str)
                and (not cfg.rules_path or f['filename'].startswith(cfg.rules_path + '/'))
                and f['filename'].endswith('.sml')
            ]
            if not touched:
                continue
            out.append(
                PendingDraft(
                    title=pr.get('title', ''),
                    url=pr.get('html_url', ''),
                    author=(pr.get('user') or {}).get('login', ''),
                    created_at=pr.get('created_at', ''),
                    touched_files=touched,
                    extras={
                        'pr_number': number,
                        'branch': (pr.get('head') or {}).get('ref', ''),
                    },
                )
            )
        return out
