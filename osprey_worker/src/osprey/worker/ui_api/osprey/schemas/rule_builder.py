"""Response shapes for Rule Builder round-tripping.

Plain snake_case like every other response in this API. The UI previously received
camelCase via `alias_generator`, which made this the single exception to the
convention -- case conversion is the client's business, and one casing rule beats a
per-endpoint one.

Split out of `schemas/rules.py` to mirror `lib/rule_builder.py`, so each library module has
a same-named schema module to import its result types from.
"""

from typing import Literal

from pydantic import BaseModel, Field

# `==`/`!=`/`>`/`<`/`>=`/`<=` come from a `BinaryComparison`; `includes`/`excludes`
# are synthesised from a `TextContains(...)` call and its negation.
BuilderOperator = Literal['==', '!=', '>', '<', '>=', '<=', 'includes', 'excludes']


class BuilderCondition(BaseModel):
    """One row of the builder's `when_all` list.

    `rhs` is always a string: a literal's text, or — when `rhs_is_feature` — the
    identifier of another feature. Numbers are stringified on the way out.
    """

    feature: str
    operator: BuilderOperator
    rhs: str
    rhs_is_feature: bool = False


class OutcomeArg(BaseModel):
    """One argument of an effect call. `name` is None for positional arguments."""

    name: str | None = None
    value: str
    is_feature: bool = False


class BuilderOutcome(BaseModel):
    effect: str
    args: list[OutcomeArg] = Field(default_factory=list)


class BuilderModel(BaseModel):
    rule_name: str
    description: str = ''
    conditions: list[BuilderCondition] = Field(default_factory=list)
    outcomes: list[BuilderOutcome] = Field(default_factory=list)


class BuilderParseResult(BaseModel):
    """`POST /rule-drafts/parse-into-builder`.

    Exactly one of `model` (when `supported`) or `reason` (when not) is set. The
    builder's expressible subset is deliberately narrow, so `reason` carries the
    specific thing that pushed the file out of it.
    """

    supported: bool
    reason: str | None = None
    model: BuilderModel | None = None
