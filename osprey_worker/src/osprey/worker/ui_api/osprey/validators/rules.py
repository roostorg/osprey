"""Request models for the rules API.

Each mixes in a `FlaskRequestMarshaller` so views can use `@marshal_with(...)` instead
of unpacking `request.get_json()` by hand. The *response* shapes live in
`schemas/rules.py` -- these reject bad input, those describe output.
"""

import re
from typing import Any, Type

from flask import Request
from osprey.worker.ui_api.osprey.lib.marshal import FlaskRequestMarshaller, JsonBodyMarshaller, T
from pydantic import BaseModel, Field, validator

# Common regular expressions for various rule validators:
#
# Lowercase only. A path is a filename, and two filenames differing only in case are one
# file on a case-insensitive filesystem -- the macOS default -- while being two rows in a
# table Postgres compares case-sensitively. Rather than reconcile the two, the set of
# representable paths is narrowed until they cannot disagree. Every rule shipped in
# `example_rules` is already lowercase snake_case, so this writes down the convention
# rather than introducing one. Rule *names* are unaffected: those are SML identifiers,
# conventionally CamelCase, and `VALID_RULE_NAME` still allows both cases.
VALID_PATH = re.compile(r'^[a-z0-9_/-]+\.sml$')
VALID_RULE_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


# --------------------------------------------------------------------------- #
# Marshallers
# --------------------------------------------------------------------------- #


class ViewArgAndOptionalJsonBodyMarshaller(FlaskRequestMarshaller):
    """View args merged with a JSON body that may be absent.

    The stock `ViewArgAndJsonBodyMarshaller` uses `get_json(force=True)`, which
    aborts with a 400 when the request has no body at all. Deploy's body is
    optional — `wire_into_main` defaults to false — so an absent body has to
    marshal down to just the view args.

    View args are applied **last**, so the URL wins any collision. With the body last, a
    `POST /rules/drafts/1/deploy` carrying `{"draft_id": 2}` would deploy draft 2: the
    path segment is what a reviewer reads and what the audit log records, and it must
    not be able to disagree with the row actually acted on.

    A non-object body is discarded rather than merged. `get_json` happily returns a
    list, string or number, and spreading one of those into a dict is either a
    `TypeError` or nonsense.
    """

    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        body = flask_request.get_json(silent=True)
        if not isinstance(body, dict):
            body = {}
        return cls.parse_obj({**body, **(flask_request.view_args or {})})


class ArgMarshallerStripped(FlaskRequestMarshaller):
    """Query-string marshaller. `request.args` is a MultiDict; pydantic reads it fine."""

    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj(flask_request.args)


# --------------------------------------------------------------------------- #
# Request models
# --------------------------------------------------------------------------- #


class _HasDraftPath(BaseModel):
    """Shared `path` field for every endpoint addressed by SML source path.

    Three things make a path invalid, and each gets its own message so the editor
    can tell the user what to fix rather than just "invalid path".

    This is the *early* rejection, for a good error message. It is not the
    security boundary — `lib.rule_deployment._resolve_within` is, because it
    resolves symlinks and runs against the path read back from the rules table
    rather than the one in the request.
    """

    path: str

    # `cls`, not `self`: pydantic v1 wraps validators in `classmethod`, and rejects a
    # `self` first parameter with `ConfigError` at *import* time -- which takes the
    # whole app down, not just this model. Linters that don't model the decorator flag
    # this as "first parameter of a method is not named 'self'"; that advice is wrong
    # here, and ruff/mypy won't catch it because the signature is only invalid at
    # pydantic's runtime. Applies to every validator in this module.
    @validator('path')
    def _check_path(cls, value: str) -> str:
        value = value.strip()
        # Reported before the general check, because "it must be lowercase" is the
        # actionable answer and "it must contain only letters, numbers, underscores,
        # slashes and hyphens" reads as though `Spam.sml` already satisfied it.
        if value.lower() != value and VALID_PATH.match(value.lower()):
            raise ValueError(f'Path {value!r} must be lowercase. Try {value.lower()!r}.')
        if not VALID_PATH.match(value):
            raise ValueError(
                f'Path {value!r} is not a valid SML source path. It must end in .sml and contain only '
                'lowercase letters, numbers, underscores, slashes, and hyphens.'
            )
        if value.startswith('/'):
            # Rule paths are relative to the rules directory; an absolute path
            # would escape it.
            raise ValueError(f'Path {value!r} must be relative to the rules directory, not absolute.')
        if '..' in value.split('/'):
            raise ValueError(f'Path {value!r} contains a parent-directory segment.')
        return value


class RuleSourceQuery(_HasDraftPath, ArgMarshallerStripped):
    """`GET /rule/source?path=...`"""


class ValidateDraftRequest(_HasDraftPath, JsonBodyMarshaller):
    """`POST /rules/drafts/validate`"""

    source: str = ''


class ParseIntoBuilderRequest(_HasDraftPath, JsonBodyMarshaller):
    """`POST /rules/drafts/parse-into-builder`"""

    source: str = ''


class CreateDraftRequest(_HasDraftPath, JsonBodyMarshaller):
    """`POST /rules/drafts`

    `main.sml` is rejected at the view layer rather than here: it is a valid path
    for reading and validating against, and only invalid as a *draft target*.
    """

    source: str = Field(min_length=1)
    rule_name: str = Field(regex=VALID_RULE_NAME.pattern)
    summary: str = ''

    @validator('source')
    def _source_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('source must be a non-empty string.')
        return value

    @validator('rule_name', pre=True)
    def _strip_rule_name(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value


class DeployDraftRequest(BaseModel, ViewArgAndOptionalJsonBodyMarshaller):
    """`POST /rules/drafts/<int:draft_id>/deploy`"""

    draft_id: int
    wire_into_main: bool = False


class DeployPlanRequest(BaseModel, ViewArgAndOptionalJsonBodyMarshaller):
    """`GET /rules/drafts/<int:draft_id>/deploy-plan`

    Takes no wiring argument: the plan answers for both choices, so the dialog's
    checkbox needs no request per toggle.
    """

    draft_id: int


class RequestDeployRequest(BaseModel, ViewArgAndOptionalJsonBodyMarshaller):
    """`POST /rules/drafts/<int:draft_id>/request-deploy`

    No body: what is being requested is fully determined by which draft it is. The
    reason for the change already has a home in the draft's `summary`.
    """

    draft_id: int


class GetDraftRequest(BaseModel, ViewArgAndOptionalJsonBodyMarshaller):
    """`GET /rules/drafts/<int:draft_id>`"""

    draft_id: int
