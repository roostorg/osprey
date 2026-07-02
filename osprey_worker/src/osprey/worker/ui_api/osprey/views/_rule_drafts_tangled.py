"""Tangled submission backend.

Tangled is an ATProto-native git host. Unlike GitHub/GitLab, there's no
`POST /pulls` REST endpoint on tangled.org itself. Pull requests are
`sh.tangled.repo.pull` records living on the submitter's PDS, carrying a
gzipped `git format-patch` blob as an ATProto blob reference. Auth is the
user's ATProto session (Bluesky app password), and the record is written
via `com.atproto.repo.createRecord` against the user's PDS.

Experimental feature. Feature parity with the git-host backends is narrower:
  * new rules only (`is_new_rule=True`); edits return 501
  * `wire_into_main` returns 501; the patch would need to touch two files
  * PDS is bsky.social by default; override with OSPREY_TANGLED_PDS_URL if
    the user's account lives on a different PDS

Both 501s document real limits of the current adapter, not runtime failures.
An adopter can extend either surface by implementing the two follow-ups in
the class body (see the RuleDraftBackendError messages for the specifics).
"""

from __future__ import annotations

import gzip
import hashlib
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any

from . import _rule_drafts_git_common as git_common
from ._rule_drafts_backend import (
    PendingDraft,
    RuleDraftBackendError,
    SubmissionResult,
)

DEFAULT_PDS_URL = 'https://bsky.social'
DEFAULT_TANGLED_URL = 'https://tangled.org'
PULL_COLLECTION = 'sh.tangled.repo.pull'


@dataclass(frozen=True)
class TangledConfig:
    handle: str
    app_password: str
    repo_owner: str
    repo_name: str
    repo_did: str
    base_branch: str
    rules_path: str
    pds_url: str
    tangled_url: str

    @classmethod
    def from_env(cls) -> 'TangledConfig':
        handle = os.environ.get('OSPREY_TANGLED_HANDLE', '').strip()
        app_password = os.environ.get('OSPREY_TANGLED_APP_PASSWORD', '').strip()
        # `repo` in the env is "owner/name" like the github/gitlab backends,
        # used to construct the viewable URL. Tangled's `sh.tangled.repo.pull`
        # record identifies the repo by DID though (v1.14.0+), so we also
        # require OSPREY_TANGLED_REPO_DID. Tangled surfaces this DID in the
        # empty-repo onboarding message ("Configure your remote to
        # git@tangled.org:did:plc:...").
        repo = os.environ.get('OSPREY_TANGLED_REPO', '').strip()
        repo_did = os.environ.get('OSPREY_TANGLED_REPO_DID', '').strip()
        base = os.environ.get('OSPREY_RULES_BASE_BRANCH', 'main').strip() or 'main'
        rules_path = os.environ.get('OSPREY_RULES_PATH_IN_REPO', '').strip().strip('/')
        pds_url = (os.environ.get('OSPREY_TANGLED_PDS_URL') or DEFAULT_PDS_URL).strip()
        tangled_url = (os.environ.get('OSPREY_TANGLED_URL') or DEFAULT_TANGLED_URL).strip()

        if not handle:
            raise RuleDraftBackendError(
                "OSPREY_TANGLED_HANDLE is not configured; set it to the user's ATProto handle "
                '(e.g. "alice.bsky.social").',
                status_code=503,
            )
        if not app_password:
            raise RuleDraftBackendError(
                'OSPREY_TANGLED_APP_PASSWORD is not configured; create an app password at '
                'https://bsky.app/settings/app-passwords and set it here.',
                status_code=503,
            )
        if not repo or '/' not in repo:
            raise RuleDraftBackendError(
                'OSPREY_TANGLED_REPO is not configured; set it to "owner-handle/repo-name" '
                '(e.g. "alice.bsky.social/osprey-rules").',
                status_code=503,
            )
        if not repo_did.startswith('did:'):
            raise RuleDraftBackendError(
                "OSPREY_TANGLED_REPO_DID is not configured; set it to the repo's DID "
                '(e.g. "did:plc:abcdefghijklmnopqrstuvwx"). Tangled shows this in the empty-repo '
                'onboarding message under "Configure your remote to git@tangled.org:<did>".',
                status_code=503,
            )
        owner, name = repo.split('/', 1)
        return cls(
            handle=handle,
            app_password=app_password,
            repo_owner=owner,
            repo_name=name,
            repo_did=repo_did,
            base_branch=base,
            rules_path=rules_path,
            pds_url=pds_url,
            tangled_url=tangled_url,
        )


@dataclass(frozen=True)
class _Session:
    did: str
    access_jwt: str


def _create_session(cfg: TangledConfig) -> _Session:
    res = git_common.request(
        'POST',
        f'{cfg.pds_url}/xrpc/com.atproto.server.createSession',
        error_action='authenticating with the PDS',
        json={'identifier': cfg.handle, 'password': cfg.app_password},
    )
    if res.status_code == 401:
        raise RuleDraftBackendError(
            'ATProto createSession returned 401; check that OSPREY_TANGLED_HANDLE and '
            'OSPREY_TANGLED_APP_PASSWORD match a real Bluesky app password.',
            status_code=502,
        )
    if not res.ok:
        raise RuleDraftBackendError(
            f'ATProto createSession returned {res.status_code}: {res.text[:400]}',
            status_code=502,
        )
    body = res.json()
    return _Session(did=body['did'], access_jwt=body['accessJwt'])


def _full_path(cfg: TangledConfig, draft_path: str) -> str:
    draft_path = draft_path.lstrip('/')
    if cfg.rules_path:
        return f'{cfg.rules_path}/{draft_path}'
    return draft_path


def _git_blob_sha1(contents: str) -> str:
    """Git's blob hash: sha1("blob <length>\\0<content>")."""
    encoded = contents.encode('utf-8')
    header = f'blob {len(encoded)}\0'.encode('utf-8')
    return hashlib.sha1(header + encoded).hexdigest()


def _rfc2822_now() -> str:
    return format_datetime(datetime.now(timezone.utc))


def _fake_commit_sha() -> str:
    # git format-patch's "From" header hash is informational; git-am doesn't
    # validate it. Any 40-char hex works.
    return hashlib.sha1(str(time.time_ns()).encode()).hexdigest()


_PATCH_BREAK_RE = re.compile(r'^(diff -|Index: |---|From )')


def _quote_patch_break_lines(body: str) -> str:
    """Neutralize body lines that git-am would read as patch structure.

    git mailinfo cuts the commit message at the first line that looks like a
    diff header or three-dash separator, and mailsplit treats "From " lines as
    mbox message boundaries. Left unescaped, a crafted summary could smuggle
    hunks for arbitrary files into the patch a maintainer applies. Quoting
    those lines keeps them visibly part of the user's text instead.
    """
    return '\n'.join(f'> {line}' if _PATCH_BREAK_RE.match(line) else line for line in body.split('\n'))


def _format_new_file_patch(
    *,
    path_in_repo: str,
    contents: str,
    author_email: str,
    subject: str,
    body: str,
) -> str:
    """Produce a git-format-patch string for adding a single new file.

    Structure follows the shape `git format-patch` emits so tangled's UI (and
    a maintainer running `git am`) accept it as-is. The blob sha and the
    "From" commit hash are informational and not validated by `git am`, so we
    generate stable-looking placeholders rather than pretending to be a real
    commit.
    """
    # The SML content is safe (every line is `+`-prefixed inside the hunk),
    # but the free-text body and the headers are not; keep every sanitization
    # at this single choke point.
    body = _quote_patch_break_lines(body)
    author_email = re.sub(r'[\r\n<>]', '', author_email)
    subject = re.sub(r'[\r\n]+', ' ', subject)
    if not contents.endswith('\n'):
        contents = contents + '\n'
    lines = contents.split('\n')
    if lines and lines[-1] == '':
        lines = lines[:-1]
    line_count = len(lines)
    blob_hash = _git_blob_sha1(contents)[:7]
    commit_sha = _fake_commit_sha()
    date = _rfc2822_now()

    added_lines = '\n'.join(f'+{line}' for line in lines)

    return (
        f'From {commit_sha} Mon Sep 17 00:00:00 2001\n'
        f'From: Osprey UI <{author_email}>\n'
        f'Date: {date}\n'
        f'Subject: [PATCH] {subject}\n'
        f'\n'
        f'{body}\n'
        f'---\n'
        f' {path_in_repo} | {line_count} {"+" * min(line_count, 30)}\n'
        f' 1 file changed, {line_count} insertions(+)\n'
        f' create mode 100644 {path_in_repo}\n'
        f'\n'
        f'diff --git a/{path_in_repo} b/{path_in_repo}\n'
        f'new file mode 100644\n'
        f'index 0000000..{blob_hash}\n'
        f'--- /dev/null\n'
        f'+++ b/{path_in_repo}\n'
        f'@@ -0,0 +1,{line_count} @@\n'
        f'{added_lines}\n'
        f'-- \n'
        f'2.42.0\n'
    )


def _upload_patch_blob(cfg: TangledConfig, session: _Session, patch: str) -> dict[str, Any]:
    """Upload the gzipped patch to the PDS and return the ATProto blob ref.

    Tangled's `sh.tangled.repo.pull` schema carries the patch as an
    `application/gzip` blob referenced from the record's `rounds[].patchBlob`
    field, not as an inline string. Confirmed against a pull record created by
    Tangled's own UI: patchBlob is a standard ATProto blob with a
    CID-linked ref, mimeType, and size.
    """
    gzipped = gzip.compress(patch.encode('utf-8'))
    res = git_common.request(
        'POST',
        f'{cfg.pds_url}/xrpc/com.atproto.repo.uploadBlob',
        error_action='uploading the patch blob',
        headers={
            'Authorization': f'Bearer {session.access_jwt}',
            'Content-Type': 'application/gzip',
        },
        data=gzipped,
    )
    if not res.ok:
        raise RuleDraftBackendError(
            f'ATProto uploadBlob returned {res.status_code}: {res.text[:400]}',
            status_code=502,
        )
    payload = res.json()
    blob = payload.get('blob')
    if not blob:
        raise RuleDraftBackendError(
            f'ATProto uploadBlob returned no blob ref: {res.text[:400]}',
            status_code=502,
        )
    return blob


def _create_pull_record(
    cfg: TangledConfig,
    session: _Session,
    *,
    title: str,
    body: str,
    patch: str,
    source_branch: str,
) -> dict[str, Any]:
    """Write the sh.tangled.repo.pull record.

    Schema mirrors what Tangled's own UI produces: a `source.branch` string,
    a `target` object holding {repo, branch, repoDid}, and a `rounds` array
    where each round has a gzipped-patch blob reference. Round 0 is the
    initial submission; revisions append a new round with an updated blob.
    """
    patch_blob = _upload_patch_blob(cfg, session, patch)
    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    record = {
        '$type': PULL_COLLECTION,
        'title': title,
        'body': body,
        'source': {'branch': source_branch},
        'target': {
            'repo': cfg.repo_did,
            'branch': cfg.base_branch,
            'repoDid': cfg.repo_did,
        },
        'rounds': [
            {
                'createdAt': now_iso,
                'patchBlob': patch_blob,
            }
        ],
        'createdAt': now_iso,
    }
    res = git_common.request(
        'POST',
        f'{cfg.pds_url}/xrpc/com.atproto.repo.createRecord',
        error_action='creating the pull record',
        headers={'Authorization': f'Bearer {session.access_jwt}'},
        json={
            'repo': session.did,
            'collection': PULL_COLLECTION,
            'record': record,
        },
    )
    if not res.ok:
        raise RuleDraftBackendError(
            f'ATProto createRecord returned {res.status_code}: {res.text[:400]}',
            status_code=502,
        )
    return res.json()


def _pull_url(cfg: TangledConfig, rkey: str) -> str:
    # Best-guess viewable URL for a tangled pull. If tangled's canonical URL
    # scheme differs the user can still navigate via at:// URI.
    return f'{cfg.tangled_url}/{cfg.repo_owner}/{cfg.repo_name}/pulls/{rkey}'


def _rkey_from_uri(uri: str) -> str:
    return uri.rsplit('/', 1)[-1]


class TangledBackend:
    name = 'tangled'

    def __init__(self, cfg: TangledConfig):
        self._cfg = cfg

    @classmethod
    def from_env(cls) -> 'TangledBackend':
        return cls(TangledConfig.from_env())

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
        if not is_new_rule:
            raise RuleDraftBackendError(
                'The Tangled backend does not yet support editing existing rules. Edits require '
                'rendering a diff against the existing file, which is a follow-up.',
                status_code=501,
            )
        if wire_into_main:
            raise RuleDraftBackendError(
                'The Tangled backend does not yet support wire_into_main. Uncheck '
                '"Turn this rule on once the review is approved" and add the Require line to main.sml '
                'in a follow-up patch.',
                status_code=501,
            )

        cfg = self._cfg
        session = _create_session(cfg)

        path_in_repo = _full_path(cfg, draft_path)
        subject = f'Add rule {rule_name}'
        body_text = (
            f'{summary.strip() or "_(no summary provided)_"}\n\n'
            f'Drafted in the Osprey rules UI by {author_email}.\n'
            f'Touches: {path_in_repo}.'
        )
        patch = _format_new_file_patch(
            path_in_repo=path_in_repo,
            contents=sml_source,
            author_email=author_email,
            subject=subject,
            body=body_text,
        )
        # Cosmetic source-branch label for the pull record; Tangled displays it
        # but doesn't require it to map to a real git ref on the knot.
        source_branch = git_common.generate_branch_name(rule_name, author_email, prefix='osprey-ui')
        created = _create_pull_record(
            cfg,
            session,
            title=subject,
            body=body_text,
            patch=patch,
            source_branch=source_branch,
        )
        uri = created.get('uri', '')
        rkey = _rkey_from_uri(uri) if uri else ''
        # Tangled assigns a numeric pull id (`/pulls/N/round/0`) when Bobbin
        # indexes the record, not at submit time. Link the user at the
        # pulls list; the new pull appears there once Bobbin ingests (usually
        # a few seconds).
        pulls_list_url = f'{cfg.tangled_url}/{cfg.repo_owner}/{cfg.repo_name}/pulls'
        return SubmissionResult(
            title='Tangled pull opened',
            url=pulls_list_url,
            main_sml_updated=False,
            extras={
                'at_uri': uri,
                'rkey': rkey,
                'path_in_repo': path_in_repo,
            },
        )

    def list_pending_drafts(self) -> list[PendingDraft]:
        # Reading pulls means either hitting the Bobbin appview (public XRPC
        # for sh.tangled.* reads) or listing records off the user's PDS.
        # This adapter surfaces an empty list rather than guess the appview
        # endpoint; the UI degrades gracefully on empty. A follow-up can wire
        # in the read side once Tangled's public read API surface stabilises.
        return []
