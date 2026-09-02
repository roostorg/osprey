"""Response shapes for draft validation.

Split out of `schemas/rules.py` to mirror `lib/rule_validation.py`, so each library module has
a same-named schema module to import its result types from.
"""

from pydantic import BaseModel, Field


class ValidationMessage(BaseModel):
    """One `ValidationError` or `ValidationWarning`, flattened for the editor.

    `rendered` defaults to empty because the assemble-failure path synthesises a
    message that has no span to render from.
    """

    message: str
    hint: str | None = None
    source_path: str
    line: int = 0
    column: int = 0
    rendered: str = ''
    identifier: str | None = None
    defined_in_source_paths: list[str] = Field(default_factory=list)


class DraftValidation(BaseModel):
    """The outcome of validating a draft spliced into the loaded sources."""

    ok: bool
    errors: list[ValidationMessage] = Field(default_factory=list)
    warnings: list[ValidationMessage] = Field(default_factory=list)
    suggested_imports: list[str] = Field(default_factory=list)
    # Set when the sources couldn't even be assembled (e.g. broken main.sml),
    # which is distinct from the SML compiling but failing validation.
    assemble_error: str | None = None
