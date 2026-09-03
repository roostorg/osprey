"""Validating draft SML against the engine's currently loaded sources.

A draft is never validated in isolation: it is spliced into the sources the
engine already has loaded and the whole set is re-validated, so a draft that
references a feature defined elsewhere resolves the same way it will once
deployed.
"""

from __future__ import annotations

import logging

from osprey.engine.ast.error_utils import SpanWithHint
from osprey.engine.ast.grammar import Name
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validation_context import (
    ValidationError,
    ValidationFailed,
    ValidationWarning,
)
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.schemas.rule_validation import DraftValidation, ValidationMessage

logger = logging.getLogger(__name__)


def format_validation_message(msg: ValidationError | ValidationWarning) -> ValidationMessage:
    """Flatten an engine validation error/warning into the editor's message shape."""
    identifier: str | None = None
    try:
        # `ast_node` is a property that raises RuntimeError when the span has no node.
        node = msg.span.ast_node
    except (RuntimeError, AttributeError):
        node = None
    if isinstance(node, Name):
        identifier = node.identifier

    defined_in: list[str] = []
    for additional in msg.additional_spans:
        span = additional.span if isinstance(additional, SpanWithHint) else additional
        defined_in.append(span.source.path)

    return ValidationMessage(
        message=msg.message,
        hint=msg.hint,
        source_path=msg.source.path,
        line=msg.span.start_line,
        column=msg.span.start_pos,
        rendered=msg.rendered(),
        identifier=identifier,
        defined_in_source_paths=defined_in,
    )


def suggest_imports_from_errors(draft_path: str, errors: list[ValidationMessage]) -> list[str]:
    """Collect source paths the draft references but doesn't import.

    Pulled from each error's `defined_in_source_paths`. main.sml is the engine
    entry point and is never importable; the draft can't import itself either.
    """
    suggested: set[str] = set()
    for err in errors:
        for path in err.defined_in_source_paths:
            if path == 'main.sml' or path == draft_path:
                continue
            suggested.add(path)
    return sorted(suggested)


def current_sources_dict() -> dict[str, str]:
    """The engine's loaded sources as a `{path: contents}` mapping."""
    engine = ENGINE.instance()
    return engine.execution_graph.validated_sources.sources.to_dict()


def validate_draft_source(path: str, source_text: str) -> DraftValidation:
    """Splice the draft into the loaded sources and run AST validation.

    Shared by the validate endpoint (which reports the result) and the
    server-side re-validation on create/deploy (which rejects on failure), so the
    two paths can't drift apart.
    """
    spliced = current_sources_dict()
    spliced[path] = source_text
    try:
        sources = Sources.from_dict(spliced)
    except Exception:
        # Don't echo the raw exception to the client (it can leak internals).
        logger.exception('failed to assemble sources for draft at %r', path)
        return DraftValidation(ok=False, assemble_error='Could not assemble the rule sources for validation.')

    engine = ENGINE.instance()
    try:
        validated = validate_sources(
            sources,
            udf_registry=engine.udf_registry,
            validator_registry=engine.validator_registry,
        )
    except ValidationFailed as exc:
        errors = [format_validation_message(e) for e in exc.errors]
        return DraftValidation(
            ok=False,
            errors=errors,
            warnings=[format_validation_message(w) for w in exc.warnings],
            suggested_imports=suggest_imports_from_errors(path, errors),
        )

    return DraftValidation(
        ok=True,
        warnings=[format_validation_message(w) for w in validated.warnings],
    )
