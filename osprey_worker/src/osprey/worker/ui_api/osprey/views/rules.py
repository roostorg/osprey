import logging
from typing import Any, Dict, List, Optional, Set

from flask import Blueprint, jsonify
from osprey.engine.ast.grammar import (
    Assign,
    Call,
    FormatString,
    Name,
    String,
)
from osprey.engine.ast.grammar import (
    List as AstList,
)
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.lib.abilities import CanViewDocs, require_ability

from ._engine_ast_utils import ast_to_string, collect_name_references, get_func_identifier

logger = logging.getLogger(__name__)

blueprint = Blueprint('rules', __name__)


def _build_whenrules_ref_count(sources: Any) -> tuple[Dict[str, int], int]:
    """Sub-pass 1: count how many WhenRules blocks reference each rule name.

    Returns (whenrules_ref_count, when_rules_total). Walks every source
    looking for WhenRules(...) calls (bare-statement or assigned),
    increments the count for each rule named in rules_any, and counts
    every WhenRules block (regardless of whether rules_any is present).
    """
    whenrules_ref_count: Dict[str, int] = {}
    when_rules_total = 0

    for source in sources:
        for statement in source.ast_root.statements:
            call_node: Optional[Call] = None
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
            when_rules_total += 1
            rules_any_arg = call_node.find_argument('rules_any')
            if rules_any_arg is None or not isinstance(rules_any_arg.value, AstList):
                continue
            for item in rules_any_arg.value.items:
                if isinstance(item, Name):
                    whenrules_ref_count[item.identifier] = whenrules_ref_count.get(item.identifier, 0) + 1

    return whenrules_ref_count, when_rules_total


def _description_to_string(value: Any) -> str:
    """Render a Rule's description argument back to a string.

    SML lets description be either a String literal or a FormatString
    template. We return the raw template for FormatString — the registry
    is a static view, never substituted.
    """
    if isinstance(value, String):
        return value.value
    if isinstance(value, FormatString):
        return value.format_string
    return ast_to_string(value)


def _extract_rules_from_engine() -> tuple[List[Dict[str, Any]], int]:
    """Walk the engine and return (rules_list, when_rules_total).

    Two sub-passes:
      1. _build_whenrules_ref_count — builds whenrules_ref_count map +
         when_rules_total. Done first because a WhenRules can textually
         precede the Rule it references.
      2. Collect every Rule(...) Assign in source order, rendering
         when_all conditions, the description, referenced feature names,
         and the referenced_by_whenrules count from sub-pass 1.
    """
    engine = ENGINE.instance()
    sources = engine.execution_graph.validated_sources.sources

    whenrules_ref_count, when_rules_total = _build_whenrules_ref_count(sources)

    rules: List[Dict[str, Any]] = []
    for source in sources:
        for statement in source.ast_root.statements:
            if not (
                isinstance(statement, Assign)
                and isinstance(statement.value, Call)
                and get_func_identifier(statement.value) == 'Rule'
            ):
                continue

            rule_name = statement.target.identifier
            call = statement.value

            when_all: List[str] = []
            when_all_arg = call.find_argument('when_all')
            if when_all_arg is not None and isinstance(when_all_arg.value, AstList):
                for item in when_all_arg.value.items:
                    when_all.append(ast_to_string(item))
            elif when_all_arg is not None:
                when_all.append(ast_to_string(when_all_arg.value))

            description = ''
            description_arg = call.find_argument('description')
            if description_arg is not None:
                description = _description_to_string(description_arg.value)

            refs: Set[str] = set()
            if when_all_arg is not None:
                collect_name_references(when_all_arg.value, refs)
            if description_arg is not None:
                collect_name_references(description_arg.value, refs)
            referenced_features = sorted(refs)

            rules.append(
                {
                    'name': rule_name,
                    'source_file': source.path,
                    'description': description,
                    'when_all': when_all,
                    'referenced_features': referenced_features,
                    'referenced_by_whenrules': whenrules_ref_count.get(rule_name, 0),
                }
            )

    return rules, when_rules_total


@blueprint.route('/rules', methods=['GET'])
@require_ability(CanViewDocs)
def rules_list() -> Any:
    """Return the catalog of Rule(...) definitions across the rules engine."""
    rules, when_rules_total = _extract_rules_from_engine()
    unused_total = sum(1 for r in rules if r['referenced_by_whenrules'] == 0)
    return jsonify(
        {
            'rules': rules,
            'total': len(rules),
            'when_rules_total': when_rules_total,
            'unused_total': unused_total,
        }
    )
