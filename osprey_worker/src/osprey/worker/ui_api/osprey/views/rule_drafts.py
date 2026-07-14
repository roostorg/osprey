from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, request
from osprey.engine.ast.error_utils import SpanWithHint
from osprey.engine.ast.grammar import (
    Assign,
    BinaryComparison,
    Call,
    FormatString,
    Name,
    Not,
    Number,
    Source,
    String,
    UnaryOperation,
)
from osprey.engine.ast.grammar import (
    List as AstList,
)
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validation_context import (
    ValidationError,
    ValidationFailed,
    ValidationWarning,
)
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.lib.storage.rule_drafts import RuleDraft
from osprey.worker.ui_api.osprey.lib.abilities import CanEditRuleDrafts, require_ability
from osprey.worker.ui_api.osprey.lib.auth import get_current_user_email

from ._engine_ast_utils import get_func_identifier

logger = logging.getLogger(__name__)

blueprint = Blueprint('rule_drafts', __name__)

_VALID_PATH = re.compile(r'^[A-Za-z0-9_./-]+\.sml$')
_VALID_RULE_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _format_validation_message(msg: ValidationError | ValidationWarning) -> dict[str, Any]:
    identifier: str | None = None
    try:
        node = msg.span.ast_node
        if isinstance(node, Name):
            identifier = node.identifier
    except Exception:
        pass

    defined_in: list[str] = []
    for additional in msg.additional_spans:
        span = additional.span if isinstance(additional, SpanWithHint) else additional
        defined_in.append(span.source.path)

    return {
        'message': msg.message,
        'hint': msg.hint,
        'source_path': msg.source.path,
        'line': msg.span.start_line,
        'column': msg.span.start_pos,
        'rendered': msg.rendered(),
        'identifier': identifier,
        'defined_in_source_paths': defined_in,
    }


def _suggest_imports_from_errors(
    draft_path: str,
    errors: list[dict[str, Any]],
) -> list[str]:
    """Collect source paths the draft references but doesn't import.

    Pulled from each error's `defined_in_source_paths`. main.sml is the engine
    entry point and is never importable; the draft can't import itself either.
    """
    suggested: set[str] = set()
    for err in errors:
        for path in err.get('defined_in_source_paths') or []:
            if path == 'main.sml' or path == draft_path:
                continue
            suggested.add(path)
    return sorted(suggested)


def _validate_path(path: str) -> str | None:
    if not _VALID_PATH.match(path):
        return f'Path {path!r} is not a valid SML source path (must end .sml and contain only [A-Za-z0-9_./-]).'
    if '..' in path.split('/'):
        return f'Path {path!r} contains a parent-directory segment.'
    return None


def _current_sources_dict() -> dict[str, str]:
    engine = ENGINE.instance()
    return engine.execution_graph.validated_sources.sources.to_dict()


@blueprint.route('/rule-drafts/source', methods=['GET'])
@require_ability(CanEditRuleDrafts)
def get_source() -> Any:
    path = request.args.get('path', '').strip()
    err = _validate_path(path)
    if err:
        return jsonify({'error': err}), 400

    engine = ENGINE.instance()
    source: Source | None = engine.execution_graph.validated_sources.sources.get_by_path(path)
    if source is None:
        return jsonify({'error': f'No source found at {path!r}.'}), 404
    return jsonify({'path': source.path, 'contents': source.contents})


@blueprint.route('/rule-drafts/validate', methods=['POST'])
@require_ability(CanEditRuleDrafts)
def validate_draft() -> Any:
    """Splice the draft into the engine's sources and re-run AST validation.

    A 200 with `{ok: false, errors: [...]}` means the SML failed validation; the
    response is still JSON so the editor can render structured errors inline.
    A 400 means the request itself was malformed (bad path, missing source).
    """
    payload = request.get_json(silent=True) or {}
    path = (payload.get('path') or '').strip()
    source_text = payload.get('source', '')

    path_err = _validate_path(path)
    if path_err:
        return jsonify({'error': path_err}), 400
    if not isinstance(source_text, str):
        return jsonify({'error': 'source must be a string.'}), 400

    spliced = _current_sources_dict()
    spliced[path] = source_text

    try:
        sources = Sources.from_dict(spliced)
    except Exception as exc:
        # Sources.from_dict asserts on shape (e.g., missing main.sml). Surface as a structured error
        # so the editor can show "you broke main.sml" without crashing.
        return jsonify(
            {
                'ok': False,
                'errors': [{'message': str(exc), 'hint': '', 'source_path': path, 'line': 0, 'column': 0}],
                'warnings': [],
            }
        ), 400

    engine = ENGINE.instance()
    try:
        validated = validate_sources(
            sources,
            udf_registry=engine.udf_registry,
            validator_registry=engine.validator_registry,
        )
    except ValidationFailed as exc:
        formatted_errors = [_format_validation_message(e) for e in exc.errors]
        return jsonify(
            {
                'ok': False,
                'errors': formatted_errors,
                'warnings': [_format_validation_message(w) for w in exc.warnings],
                'suggested_imports': _suggest_imports_from_errors(path, formatted_errors),
            }
        )

    return jsonify(
        {
            'ok': True,
            'errors': [],
            'warnings': [_format_validation_message(w) for w in validated.warnings],
            'suggested_imports': [],
        }
    )


def _iter_top_level_assigns(sources: Iterable[Source]) -> Iterable[tuple[Source, Assign]]:
    for source in sources:
        for statement in source.ast_root.statements:
            if isinstance(statement, Assign):
                yield source, statement


def _is_rule_call(node: Any) -> bool:
    return isinstance(node, Call) and get_func_identifier(node) == 'Rule'


def _collect_features(sources: Iterable[Source]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source, assign in _iter_top_level_assigns(sources):
        name = assign.target.identifier
        if name in seen:
            continue
        # Skip `MyRule = Rule(...)` assigns; the builder dropdown is for values
        # a user can reference inside conditions, not for rule definitions.
        if _is_rule_call(assign.value):
            continue
        seen.add(name)
        out.append(
            {
                'name': name,
                'source_path': source.path,
                'source_line': assign.span.start_line,
            }
        )
    out.sort(key=lambda item: item['name'])
    return out


def _collect_udfs() -> list[dict[str, Any]]:
    engine = ENGINE.instance()
    udf_registry = engine.udf_registry
    out: list[dict[str, Any]] = []
    for func in sorted(udf_registry.iter_functions(), key=lambda f: f.__name__):
        try:
            args_type = func.get_arguments_type()
            rvalue_type = func.get_rvalue_type()
        except Exception:
            continue
        arguments: list[dict[str, Any]] = []
        try:
            items = args_type.items().items()
        except Exception:
            items = []
        for arg_name, arg_type in items:
            arguments.append(
                {
                    'name': arg_name,
                    'type_name': getattr(arg_type, '__name__', str(arg_type)),
                }
            )
        out.append(
            {
                'name': func.__name__,
                'return_type': getattr(rvalue_type, '__name__', str(rvalue_type)),
                'arguments': arguments,
            }
        )
    return out


def _collect_effects(sources: Iterable[Source]) -> list[str]:
    """Names of UDFs that appear inside a `WhenRules(then=[...])` block.

    Used as the effect dropdown. Sourced from real usage rather than from the
    UDF registry because the registry holds every UDF and we want a shortlist
    of things users actually use as actions.
    """
    seen: set[str] = set()

    def _walk(node: Any) -> None:
        if isinstance(node, Call):
            ident = get_func_identifier(node)
            if ident:
                seen.add(ident)
            for arg in node.arguments:
                _walk(arg.value)
        elif isinstance(node, AstList):
            for item in node.items:
                _walk(item)
        elif isinstance(node, Assign):
            _walk(node.value)

    for source in sources:
        for statement in source.ast_root.statements:
            call_node: Call | None = None
            if isinstance(statement, Call) and get_func_identifier(statement) == 'WhenRules':
                call_node = statement
            elif (
                isinstance(statement, Assign)
                and isinstance(statement.value, Call)
                and get_func_identifier(statement.value) == 'WhenRules'
            ):
                call_node = statement.value
            if call_node is None:
                continue
            then_arg = call_node.find_argument('then')
            if then_arg is None:
                continue
            _walk(then_arg.value)

    return sorted(seen)


@blueprint.route('/rule-drafts/vocabulary', methods=['GET'])
@require_ability(CanEditRuleDrafts)
def vocabulary() -> Any:
    engine = ENGINE.instance()
    sources = list(engine.execution_graph.validated_sources.sources)
    features = _collect_features(sources)
    udfs = _collect_udfs()
    effects = _collect_effects(sources)
    source_files = sorted(s.path for s in sources)
    return jsonify(
        {
            'features': features,
            'udfs': udfs,
            'effects': effects,
            'source_files': source_files,
        }
    )


def _revalidate(path: str, source_text: str) -> tuple[Any, int] | None:
    """Re-run the engine's AST validation with the draft spliced into the loaded
    sources. Returns a Flask error tuple on failure, or None if it validates."""
    spliced = _current_sources_dict()
    spliced[path] = source_text
    try:
        sources = Sources.from_dict(spliced)
        validate_sources(
            sources,
            udf_registry=ENGINE.instance().udf_registry,
            validator_registry=ENGINE.instance().validator_registry,
        )
    except ValidationFailed as exc:
        return jsonify(
            {
                'error': 'Validation failed; fix errors before submitting.',
                'errors': [_format_validation_message(e) for e in exc.errors],
            }
        ), 400
    except Exception as exc:
        return jsonify({'error': f'Could not assemble sources: {exc}'}), 400
    return None


@blueprint.route('/rule-drafts', methods=['POST'])
@require_ability(CanEditRuleDrafts)
def create_draft() -> Any:
    """Validate a draft and upsert it into the rule_drafts table (one row per path)."""
    payload = request.get_json(silent=True) or {}
    path = (payload.get('path') or '').strip()
    source_text = payload.get('source', '')
    rule_name = (payload.get('rule_name') or '').strip()
    summary = (payload.get('summary') or '').strip()

    path_err = _validate_path(path)
    if path_err:
        return jsonify({'error': path_err}), 400
    if path == 'main.sml':
        # main.sml is the engine entry point; a draft never replaces it wholesale.
        # Deploying a draft optionally wires it into main.sml with a single Require line.
        return jsonify(
            {
                'error': 'main.sml is the engine entry point and cannot be saved as a draft. '
                'Deploy a rule with wire_into_main to add a Require line instead.'
            }
        ), 400
    if not isinstance(source_text, str) or not source_text.strip():
        return jsonify({'error': 'source must be a non-empty string.'}), 400
    if not _VALID_RULE_NAME.match(rule_name):
        return jsonify({'error': 'rule_name must be a valid SML identifier ([A-Za-z_][A-Za-z0-9_]*).'}), 400

    # Re-validate server-side so a client that skips the validate step still cannot store uncompilable SML.
    error = _revalidate(path, source_text)
    if error is not None:
        return error

    draft = RuleDraft.upsert(
        path=path,
        rule_name=rule_name,
        sml_source=source_text,
        summary=summary,
        author_email=get_current_user_email(),
    )
    return jsonify(draft.to_json())


@blueprint.route('/rule-drafts', methods=['GET'])
@require_ability(CanEditRuleDrafts)
def list_drafts() -> Any:
    """The draft rules table: every staged draft, newest-edited first."""
    return jsonify({'drafts': [d.to_json() for d in RuleDraft.list_all()]})


@blueprint.route('/rule-drafts/<int:draft_id>', methods=['GET'])
@require_ability(CanEditRuleDrafts)
def get_draft(draft_id: int) -> Any:
    draft = RuleDraft.get_one(draft_id)
    if draft is None:
        return jsonify({'error': f'No draft with id {draft_id}.'}), 404
    return jsonify(draft.to_json())


def _rules_dir_or_error() -> tuple[Path | None, tuple[Any, int] | None]:
    """The directory deploy writes into (OSPREY_RULES_LOCAL_PATH), or an error tuple.

    Deploying into the engine's own rules source is a filesystem hand-off, the same
    contract the retired `local` backend used: whatever pipeline already syncs that
    directory (etcd push, file watcher) activates the rule. A DB-backed
    SourcesProvider that lets the engine read deployed drafts straight from this
    table would remove that dependency entirely; see the PR notes.
    """
    raw = os.environ.get('OSPREY_RULES_LOCAL_PATH', '').strip()
    if not raw:
        return None, (
            jsonify(
                {
                    'error': 'Deploy is not configured. Set OSPREY_RULES_LOCAL_PATH to the rules '
                    'directory the engine loads so deployed drafts are written there.'
                }
            ),
            503,
        )
    rules_dir = Path(raw)
    if not rules_dir.is_dir():
        return None, (jsonify({'error': f'OSPREY_RULES_LOCAL_PATH {raw!r} is not a directory.'}), 503)
    return rules_dir, None


def _resolve_within(rules_dir: Path, draft_path: str) -> Path | None:
    """Resolve draft_path inside rules_dir, or None if it would escape via `..`/symlink."""
    candidate = (rules_dir / draft_path).resolve()
    try:
        candidate.relative_to(rules_dir.resolve())
    except ValueError:
        return None
    return candidate


def _main_requires(main_sml: str, draft_path: str) -> bool:
    pattern = re.compile(r"Require\s*\(\s*rule\s*=\s*['\"]" + re.escape(draft_path) + r"['\"]\s*\)", re.MULTILINE)
    return bool(pattern.search(main_sml))


def _append_require(main_sml: str, draft_path: str) -> str:
    suffix = f"\nRequire(rule='{draft_path}')\n"
    if main_sml and not main_sml.endswith('\n'):
        suffix = '\n' + suffix
    return main_sml + suffix


@blueprint.route('/rule-drafts/<int:draft_id>/deploy', methods=['POST'])
@require_ability(CanEditRuleDrafts)
def deploy_draft(draft_id: int) -> Any:
    """Write a draft's SML into the configured rules directory and mark it deployed.

    With `wire_into_main`, also append a `Require(rule=...)` line to main.sml so the
    rule takes effect (the file on its own is inert until something requires it).
    """
    draft = RuleDraft.get_one(draft_id)
    if draft is None:
        return jsonify({'error': f'No draft with id {draft_id}.'}), 404

    payload = request.get_json(silent=True) or {}
    wire_into_main = bool(payload.get('wire_into_main', False))

    # Re-validate at deploy time: the loaded sources may have changed since the draft was saved.
    error = _revalidate(draft.path, draft.sml_source)
    if error is not None:
        return error

    rules_dir, dir_error = _rules_dir_or_error()
    if dir_error is not None:
        return dir_error
    assert rules_dir is not None

    target = _resolve_within(rules_dir, draft.path)
    if target is None:
        return jsonify({'error': f'Draft path {draft.path!r} escapes the rules directory.'}), 400
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(draft.sml_source, encoding='utf-8')

    main_sml_updated = False
    if wire_into_main:
        main_path = rules_dir / 'main.sml'
        if not main_path.exists():
            return jsonify({'error': f'wire_into_main requested but main.sml does not exist at {main_path}.'}), 409
        main_contents = main_path.read_text(encoding='utf-8')
        if not _main_requires(main_contents, draft.path):
            main_path.write_text(_append_require(main_contents, draft.path), encoding='utf-8')
            main_sml_updated = True

    deployed = RuleDraft.mark_deployed(draft_id)
    result = deployed.to_json() if deployed is not None else draft.to_json()
    result['main_sml_updated'] = main_sml_updated
    result['path_on_disk'] = str(target)
    return jsonify(result)


# The set of comparator strings the Rule Builder UI can render.
_BUILDER_COMPARATORS = {'==', '!=', '>', '<', '>=', '<='}


def _condition_from_value(node: Any, feature: str, operator: str) -> dict[str, Any] | None:
    """Build a builder Condition row from an RHS node, returning None if the node
    isn't a literal or a bare Name (the only RHS shapes the builder supports)."""
    if isinstance(node, Name):
        return {'feature': feature, 'operator': operator, 'rhs': node.identifier, 'rhsIsFeature': True}
    if isinstance(node, String):
        return {'feature': feature, 'operator': operator, 'rhs': node.value, 'rhsIsFeature': False}
    if isinstance(node, Number):
        return {'feature': feature, 'operator': operator, 'rhs': str(node.value), 'rhsIsFeature': False}
    return None


def _parse_text_contains_call(call: Call, operator: str) -> dict[str, Any] | None:
    """Convert a `TextContains(text=Name, phrase=...)` call to an includes/excludes row.

    Returns None if the call doesn't have the exact shape the builder emits.
    """
    if get_func_identifier(call) != 'TextContains':
        return None
    text_arg = call.find_argument('text')
    phrase_arg = call.find_argument('phrase')
    if text_arg is None or phrase_arg is None:
        return None
    if not isinstance(text_arg.value, Name):
        return None
    feature = text_arg.value.identifier
    return _condition_from_value(phrase_arg.value, feature, operator)


def _parse_condition(node: Any) -> dict[str, Any] | None:
    if isinstance(node, UnaryOperation) and isinstance(node.operator, Not) and isinstance(node.operand, Call):
        return _parse_text_contains_call(node.operand, 'excludes')
    if isinstance(node, Call):
        return _parse_text_contains_call(node, 'includes')
    if isinstance(node, BinaryComparison):
        if not isinstance(node.left, Name):
            return None
        operator = node.comparator.original_comparator
        if operator not in _BUILDER_COMPARATORS:
            return None
        return _condition_from_value(node.right, node.left.identifier, operator)
    return None


def _parse_outcome_arg(arg: Any) -> dict[str, Any] | None:
    """Convert one `Call.arguments[i]` into a builder OutcomeArg, or None for
    anything richer than a literal or bare Name reference."""
    val = arg.value
    if isinstance(val, Name):
        return {'name': arg.name, 'value': val.identifier, 'isFeature': True}
    if isinstance(val, String):
        return {'name': arg.name, 'value': val.value, 'isFeature': False}
    if isinstance(val, Number):
        return {'name': arg.name, 'value': str(val.value), 'isFeature': False}
    return None


def _parse_outcome(node: Any) -> dict[str, Any] | None:
    if not isinstance(node, Call):
        return None
    effect = get_func_identifier(node)
    if effect is None:
        return None
    args: list[dict[str, Any]] = []
    for arg in node.arguments:
        parsed = _parse_outcome_arg(arg)
        if parsed is None:
            return None
        args.append(parsed)
    return {'effect': effect, 'args': args}


def _parse_into_builder_model(source: Source) -> dict[str, Any]:
    """Walk the AST of a single draft Source and either return a populated
    builder model JSON or `{supported: False, reason: ...}`.

    The builder's expressible subset is deliberately narrow: optional Import
    and Require statements (ignored for the model), exactly one
    `RuleName = Rule(when_all=[...], description='...')`, and an optional
    `WhenRules(rules_any=[RuleName], then=[...])` whose `then` entries are
    UDF calls with literal or Name arguments. Anything richer means the file
    can't round-trip and the user must use Code Editor.
    """
    try:
        statements = source.ast_root.statements
    except Exception as exc:
        return {'supported': False, 'reason': f'could not parse SML: {exc}'}

    rule_assign: Assign | None = None
    when_rules_call: Call | None = None

    for stmt in statements:
        if isinstance(stmt, Call):
            ident = get_func_identifier(stmt)
            if ident in ('Import', 'Require'):
                continue
            if ident == 'WhenRules':
                if when_rules_call is not None:
                    return {
                        'supported': False,
                        'reason': 'multiple WhenRules blocks; Rule Builder edits one rule at a time',
                    }
                when_rules_call = stmt
                continue
            return {'supported': False, 'reason': f'top-level call to `{ident}` is not supported by Rule Builder'}
        if isinstance(stmt, Assign) and isinstance(stmt.value, Call) and get_func_identifier(stmt.value) == 'Rule':
            if rule_assign is not None:
                return {
                    'supported': False,
                    'reason': 'multiple Rule definitions in one file; Rule Builder edits one rule at a time',
                }
            rule_assign = stmt
            continue
        if isinstance(stmt, Assign):
            return {
                'supported': False,
                'reason': f'helper assignment `{stmt.target.identifier} = ...` is not supported by Rule Builder',
            }
        return {'supported': False, 'reason': f'unsupported top-level statement: {type(stmt).__name__}'}

    if rule_assign is None:
        return {'supported': False, 'reason': 'no Rule(...) definition found in this file'}

    rule_name = rule_assign.target.identifier
    rule_call = rule_assign.value
    assert isinstance(rule_call, Call)

    description = ''
    description_arg = rule_call.find_argument('description')
    if description_arg is not None:
        if isinstance(description_arg.value, String):
            description = description_arg.value.value
        elif isinstance(description_arg.value, FormatString):
            # Round-trip the raw template; the builder doesn't expose format-string editing.
            description = description_arg.value.format_string
        else:
            return {'supported': False, 'reason': 'rule description must be a string literal'}

    when_all_arg = rule_call.find_argument('when_all')
    if when_all_arg is None or not isinstance(when_all_arg.value, AstList):
        return {'supported': False, 'reason': 'Rule must have `when_all=[...]`'}

    conditions: list[dict[str, Any]] = []
    for item in when_all_arg.value.items:
        cond = _parse_condition(item)
        if cond is None:
            return {
                'supported': False,
                'reason': 'one or more conditions use expressions Rule Builder cannot represent',
            }
        conditions.append(cond)
    if not conditions:
        # Builder needs at least one row to render anything sensible; matching the EMPTY_BUILDER_MODEL default.
        conditions = [{'feature': '', 'operator': '==', 'rhs': '', 'rhsIsFeature': False}]

    outcomes: list[dict[str, Any]] = []
    if when_rules_call is not None:
        rules_any_arg = when_rules_call.find_argument('rules_any')
        if rules_any_arg is not None and isinstance(rules_any_arg.value, AstList):
            for item in rules_any_arg.value.items:
                if not isinstance(item, Name) or item.identifier != rule_name:
                    return {
                        'supported': False,
                        'reason': 'WhenRules.rules_any must reference only the rule being edited',
                    }
        then_arg = when_rules_call.find_argument('then')
        if then_arg is not None and isinstance(then_arg.value, AstList):
            for item in then_arg.value.items:
                outcome = _parse_outcome(item)
                if outcome is None:
                    return {
                        'supported': False,
                        'reason': 'one or more outcomes use expressions Rule Builder cannot represent',
                    }
                outcomes.append(outcome)
    if not outcomes:
        outcomes = [{'effect': '', 'args': []}]

    return {
        'supported': True,
        'model': {
            'ruleName': rule_name,
            'description': description,
            'conditions': conditions,
            'outcomes': outcomes,
        },
    }


@blueprint.route('/rule-drafts/parse-into-builder', methods=['POST'])
@require_ability(CanEditRuleDrafts)
def parse_into_builder() -> Any:
    """Attempt to render an existing SML file as a Rule Builder model.

    Returns `{supported: true, model: {...}}` if the file fits the builder's
    expressible subset, or `{supported: false, reason: "..."}` otherwise. The
    UI uses this to decide whether to enable the Rule Builder toggle when
    editing an existing rule.
    """
    payload = request.get_json(silent=True) or {}
    path = (payload.get('path') or '').strip()
    source_text = payload.get('source', '')

    path_err = _validate_path(path)
    if path_err:
        return jsonify({'error': path_err}), 400
    if not isinstance(source_text, str):
        return jsonify({'error': 'source must be a string.'}), 400

    source = Source(path=path, contents=source_text)
    return jsonify(_parse_into_builder_model(source))
