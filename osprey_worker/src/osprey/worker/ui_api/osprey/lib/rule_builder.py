"""Round-tripping SML into the Rule Builder's model.

The builder's expressible subset is deliberately narrow, so most of this module
is about detecting the shapes it *cannot* represent and saying why. When a file
falls outside the subset the user is sent to the Code Editor instead — a wrong
round-trip would silently rewrite a rule, which is worse than refusing.
"""

from __future__ import annotations

import logging
from typing import Any

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
from osprey.worker.ui_api.osprey.lib.ast_utils import get_func_identifier
from osprey.worker.ui_api.osprey.schemas.rule_builder import BuilderParseResult

logger = logging.getLogger(__name__)

# The set of comparator strings the Rule Builder UI can render.
_BUILDER_COMPARATORS = {'==', '!=', '>', '<', '>=', '<='}


def _condition_from_value(node: Any, feature: str, operator: str) -> dict[str, Any] | None:
    """Build a builder Condition row from an RHS node, returning None if the node
    isn't a literal or a bare Name (the only RHS shapes the builder supports)."""
    if isinstance(node, Name):
        return {'feature': feature, 'operator': operator, 'rhs': node.identifier, 'rhs_is_feature': True}
    if isinstance(node, String):
        return {'feature': feature, 'operator': operator, 'rhs': node.value, 'rhs_is_feature': False}
    if isinstance(node, Number):
        return {'feature': feature, 'operator': operator, 'rhs': str(node.value), 'rhs_is_feature': False}
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
        return {'name': arg.name, 'value': val.identifier, 'is_feature': True}
    if isinstance(val, String):
        return {'name': arg.name, 'value': val.value, 'is_feature': False}
    if isinstance(val, Number):
        return {'name': arg.name, 'value': str(val.value), 'is_feature': False}
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


def _parse_into_builder_dict(source: Source) -> dict[str, Any]:
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
    except Exception:
        # The reason goes to the caller, so it stays a fixed string rather than carrying
        # `str(exc)`: the bare `except` catches anything the parser raises, not only
        # syntax errors, and an internal failure's text is neither actionable nor ours
        # to publish. Authors get the real diagnosis from `POST /rules/drafts/validate`,
        # which returns structured errors with spans; this endpoint only has to answer
        # whether the Builder can represent the file.
        logger.warning('could not parse %r into a builder model', source.path, exc_info=True)
        return {'supported': False, 'reason': 'this file could not be parsed as SML'}

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
        conditions = [{'feature': '', 'operator': '==', 'rhs': '', 'rhs_is_feature': False}]

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
            'rule_name': rule_name,
            'description': description,
            'conditions': conditions,
            'outcomes': outcomes,
        },
    }


def parse_into_builder_model(source: Source) -> BuilderParseResult:
    """Typed wrapper over `_parse_into_builder_dict`.

    The inner walk builds plain snake_case dicts matching the model fields, so the
    model validates the shape without a second mapping layer. Responses are snake_case
    throughout this API; any camelCase the UI wants is the client's conversion to make.
    """
    return BuilderParseResult.parse_obj(_parse_into_builder_dict(source))
