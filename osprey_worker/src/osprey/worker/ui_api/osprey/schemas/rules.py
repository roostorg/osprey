"""Response shapes for the rules API.

The typed results the `lib.rule*` modules return and the views serialise. Separate
from `validators/`, which holds the *request* models -- those mix in a
`FlaskRequestMarshaller` and exist to reject bad input, while these exist to describe
output. Keeping them apart means `lib/` can depend on this without appearing to reach
into request validation.

Named `schemas` rather than `models` because `Model` already means a SQLAlchemy
declarative row in this codebase (`lib/storage/postgres.py`).

Everything serialises with `.dict()` (pydantic v1), snake_case, with no exceptions --
including the Rule Builder types, which once carried camelCase aliases. One casing rule
for every response; any conversion the UI wants is the client's to make.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, root_validator, validator

# --------------------------------------------------------------------------- #
# Result models — vocabulary
# --------------------------------------------------------------------------- #


class RuleSource(BaseModel):
    """`GET /rule-drafts/source` — an on-disk rule the engine has loaded."""

    path: str
    contents: str


class FeatureRef(BaseModel):
    """A top-level assignment a rule condition can reference."""

    name: str
    source_path: str
    source_line: int


class UdfArgument(BaseModel):
    name: str
    type_name: str


class UdfSignature(BaseModel):
    name: str
    return_type: str
    arguments: list[UdfArgument] = Field(default_factory=list)


class Vocabulary(BaseModel):
    """`GET /rule-drafts/vocabulary` — everything the builder dropdowns offer."""

    features: list[FeatureRef] = Field(default_factory=list)
    udfs: list[UdfSignature] = Field(default_factory=list)
    effects: list[str] = Field(default_factory=list)
    source_files: list[str] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Result models — Rule Builder
# --------------------------------------------------------------------------- #


class DraftSummary(BaseModel):
    """A row of the rules table without its SML: what `GET /rules/drafts` lists.

    Contrast `RuleCatalogEntry`, which is a `Rule(...)` parsed out of the engine's
    loaded sources: that one has no id and no author because nothing persisted it.
    This is the persisted record, and `status` says whether it is still a draft or has
    been deployed.

    `source` is deliberately absent. Only the editor needs a draft's SML and it opens
    one draft at a time, so putting it here would ship every rule's text on every list
    -- growing with the size of the table rather than with what is rendered. `cid`
    covers the case that motivates having it: a fixed 64 bytes that answers "is the
    copy I hold still current?" without transferring the source to find out.

    Built straight off the SQLAlchemy model via `from_orm(row)`. `author` is renamed on
    the way out, which the alias handles -- under `orm_mode` pydantic reads each field
    by its alias, so it is populated from `row.author_email`.

    `status` is a `RuleStatus`, a `StrEnum`, so it satisfies `str` unchanged. The
    timestamps are isoformatted rather than left as `datetime`, because Flask's JSON
    encoder renders a datetime as an RFC 822 string ("Tue, 01 Sep 2026 18:30:00 GMT")
    rather than ISO 8601.
    """

    # str, not int, matching `Query.serialize()` elsewhere in lib/storage. Osprey mints
    # ids as snowflakes in places, and a 64-bit id exceeds JavaScript's exact integer
    # range (2**53-1) -- so it would silently lose its low bits in `JSON.parse`. This
    # table's ids are a small serial today, which is exactly why the convention is worth
    # following now rather than after it starts mattering.
    id: str
    path: str
    rule_name: str
    # Content address of the SML (SHA-256 hex). Served so a client can tell whether the
    # draft it holds is still the one on the server, and -- because deploy writes the
    # source verbatim -- whether a deployed file still matches the draft that produced
    # it, without transferring the SML twice to compare it.
    cid: str
    author: str = Field(alias='author_email')
    status: str
    # The list is ordered by this, so it is served rather than left implicit.
    updated_at: str | None = None

    class Config:
        orm_mode = True
        # So the model can still be built by field name, not only by alias, for
        # anything constructing one without an ORM row.
        allow_population_by_field_name = True

    # `cls`, not `self`: pydantic v1 wraps validators in `classmethod`, and rejects a
    # `self` first parameter with `ConfigError` at *import* time -- which takes the
    # whole app down, not just this model. Linters that don't model the decorator flag
    # this as "first parameter of a method is not named 'self'"; that advice is wrong
    # here, and ruff/mypy won't catch it because the signature is only invalid at
    # pydantic's runtime.
    @validator('updated_at', pre=True)
    def _isoformat_updated_at(cls, value: object) -> object:
        return value.isoformat() if isinstance(value, datetime) else value


class RuleRecord(DraftSummary):
    """A full row of the rules table: a `DraftSummary` plus the SML itself.

    Served where one specific draft is the subject -- fetching it into the editor,
    and the responses to creating or deploying it -- as opposed to the list, where the
    source would be dead weight on every row but one.
    """

    source: str = Field(alias='sml_source')
    summary: str
    created_at: str | None = None
    deployed_at: str | None = None

    # See `_isoformat_updated_at` above for why this takes `cls`. `allow_reuse` because
    # the base class already registered a validator function of the same shape.
    @validator('created_at', 'deployed_at', pre=True, allow_reuse=True)
    def _isoformat_timestamps(cls, value: object) -> object:
        return value.isoformat() if isinstance(value, datetime) else value


class RuleDeployment(BaseModel):
    """The result of `lib.rule_deployment.deploy_rule()`.

    `path_on_disk` is relative to the rules directory; the absolute server path
    would leak the deployment's directory layout to the client.
    """

    rule: RuleRecord
    main_sml_updated: bool = False
    path_on_disk: str


class DraftList(BaseModel):
    """`GET /rules/drafts`

    Carries `DraftSummary`, not `RuleRecord`: the list is for choosing a draft, and the
    SML arrives when one is opened.
    """

    drafts: list[DraftSummary] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Result models — the engine rule catalog
# --------------------------------------------------------------------------- #


class RuleCatalogEntry(BaseModel):
    """One `Rule(...)` definition found in the engine's loaded sources.

    Distinct from `RuleRecord`: this is parsed out of the AST, not read from the
    rules table, so it has no id and no author.
    """

    name: str
    source_file: str
    description: str = ''
    when_all: list[str] = Field(default_factory=list)
    referenced_features: list[str] = Field(default_factory=list)
    # How many WhenRules blocks reference this rule. Zero means the rule is
    # defined but never fires — that's what `unused_total` counts.
    referenced_by_whenrules: int = 0


class RuleList(BaseModel):
    """The return type of `lib.rules.list_rules()`.

    `total` is derived from `rules` when omitted, so callers construct this with
    the three counts they actually compute during the walk.
    """

    rules: list[RuleCatalogEntry] = Field(default_factory=list)
    total: int | None = None
    when_rules_total: int = 0
    unused_total: int = 0

    # `cls` for the same reason as `_isoformat_timestamps` above: pydantic v1 root
    # validators are classmethods too, and `self` is a `ConfigError` at import.
    @root_validator
    def _default_total(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values.get('total') is None:
            values['total'] = len(values.get('rules') or [])
        return values
