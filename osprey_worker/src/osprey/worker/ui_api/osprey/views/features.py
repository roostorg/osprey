import logging
from pathlib import PurePosixPath
from typing import Any

from flask import Blueprint, jsonify
from osprey.engine.ast.grammar import (
    Annotation,
    AnnotationWithVariants,
    Assign,
    Attribute,
    BinaryComparison,
    BinaryOperation,
    BooleanOperation,
    Call,
    FormatString,
    UnaryOperation,
)
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.lib.abilities import CanViewDocs, require_ability

from ._engine_ast_utils import ast_to_string, collect_name_references, get_func_identifier

logger = logging.getLogger(__name__)

blueprint = Blueprint('features', __name__)

# Call identifiers that name things other than extracted data features. These
# have their own dedicated UI pages or aren't meaningful as queryable dimensions:
#   - Rule: has /rules-visualizer; not a feature
#   - Count family: distinct concept
#   - Experiment family: experiment bucket configs, not data features
#   - WhenRules: handled separately for reference counts
_NON_FEATURE_CALLS: frozenset[str] = frozenset(
    {
        'Rule',
        'Count',
        'ReadCount',
        'ReadCountUnique',
        'CountUnique',
        'BitCount',
        'Experiment',
        'ExperimentWhen',
        'WhenRules',
    }
)


def _format_annotation(annotation: Any) -> str | None:
    """Format a type annotation AST node into a readable string like 'str', 'Entity[int]', 'Optional[str]'."""
    if annotation is None:
        return None
    if isinstance(annotation, AnnotationWithVariants):
        variant_strs = [_format_annotation(v) or str(v) for v in annotation.variants]
        return f'{annotation.identifier}[{", ".join(variant_strs)}]'
    if isinstance(annotation, Annotation):
        return annotation.identifier
    return None


def _derive_category(file_path: str) -> str:
    """Derive the category from a source file path.

    models/user.sml -> models
    models/actions/guild_created.sml -> models/actions
    lib/user_history.sml -> lib
    counters/active_days.sml -> counters
    actions/dm_channel_created.sml -> actions
    Empty path -> 'other'.
    """
    parts = PurePosixPath(file_path).parts
    if not parts:
        return 'other'
    if parts[0] == 'models' and len(parts) > 2:
        return f'{parts[0]}/{parts[1]}'
    return parts[0]


def _find_assign_for_feature(sources: Any, feature_name: str, source_path: str, source_line: int) -> Assign | None:
    """Look up the Assign AST node for a feature in its source file.

    First try exact (file, line) match. Fall back to first Assign in the file
    with the right target identifier (handles multi-line Assigns where
    source_line is the comment/start line, not the AST node's start_line).
    """
    source = sources.get_by_path(source_path)
    if source is None:
        return None
    for statement in source.ast_root.statements:
        if (
            isinstance(statement, Assign)
            and statement.target.identifier == feature_name
            and statement.span.start_line == source_line
        ):
            return statement
    for statement in source.ast_root.statements:
        if isinstance(statement, Assign) and statement.target.identifier == feature_name:
            return statement
    return None


def _extraction_fn_from_value(value: Any) -> str:
    """Describe how a feature is extracted based on its Assign value.

    - Call -> the function name ('JsonData', 'Entity', 'SnowflakeAge')
    - FormatString -> 'FormatString'
    - BinaryOperation / BinaryComparison -> 'Expression'
    - BooleanOperation -> 'BooleanExpression'
    - UnaryOperation -> 'UnaryExpression'
    - Attribute -> 'Attribute'
    - Otherwise -> the AST node class name as a fallback.
    """
    if isinstance(value, Call):
        return get_func_identifier(value) or 'Call'
    if isinstance(value, FormatString):
        return 'FormatString'
    if isinstance(value, (BinaryOperation, BinaryComparison)):
        return 'Expression'
    if isinstance(value, BooleanOperation):
        return 'BooleanExpression'
    if isinstance(value, UnaryOperation):
        return 'UnaryExpression'
    if isinstance(value, Attribute):
        return 'Attribute'
    return type(value).__name__


def _extract_features_from_engine() -> list[dict[str, Any]]:
    """Walk the engine AST and return the catalog of extracted features with references."""
    engine = ENGINE.instance()
    sources = engine.execution_graph.validated_sources.sources
    locations = engine.get_known_feature_locations()

    features: dict[str, dict[str, Any]] = {}

    for loc in locations:
        assign = _find_assign_for_feature(sources, loc.name, loc.source_path, loc.source_line)
        if assign is None:
            logger.warning('Feature %s at %s:%d not found in AST', loc.name, loc.source_path, loc.source_line)
            continue
        extraction_fn = _extraction_fn_from_value(assign.value)
        if extraction_fn in _NON_FEATURE_CALLS:
            continue
        features[loc.name] = {
            'name': loc.name,
            'source_file': loc.source_path,
            'source_line': loc.source_line,
            'category': _derive_category(loc.source_path),
            'type_annotation': _format_annotation(assign.annotation),
            'extraction_fn': extraction_fn,
            'definition': ast_to_string(assign.value),
            'referenced_by_rules': [],
            'referenced_by_features': [],
            'referenced_by_whenrules': 0,
        }

    feature_names = set(features.keys())
    rule_refs: dict[str, set[str]] = {}
    feature_refs: dict[str, set[str]] = {}
    whenrules_refs: dict[str, int] = {}

    for source in sources:
        for statement in source.ast_root.statements:
            # Rule(...) definitions — collect Names in when_all.
            if (
                isinstance(statement, Assign)
                and isinstance(statement.value, Call)
                and get_func_identifier(statement.value) == 'Rule'
            ):
                rule_name = statement.target.identifier
                refs: set[str] = set()
                when_all_arg = statement.value.find_argument('when_all')
                if when_all_arg:
                    collect_name_references(when_all_arg.value, refs)
                for feat in refs & feature_names:
                    rule_refs.setdefault(feat, set()).add(rule_name)
                continue

            # WhenRules(...) calls — bare statement OR assigned.
            call_node: Call | None = None
            if isinstance(statement, Call) and get_func_identifier(statement) == 'WhenRules':
                call_node = statement
            elif (
                isinstance(statement, Assign)
                and isinstance(statement.value, Call)
                and get_func_identifier(statement.value) == 'WhenRules'
            ):
                call_node = statement.value
            if call_node is not None:
                refs = set()
                for arg in call_node.arguments:
                    if arg.name == 'rules_any':
                        continue
                    collect_name_references(arg.value, refs)
                for feat in refs & feature_names:
                    whenrules_refs[feat] = whenrules_refs.get(feat, 0) + 1
                continue

            # Feature-to-feature references — only for features in our catalog.
            if isinstance(statement, Assign):
                defining_name = statement.target.identifier
                if defining_name not in feature_names:
                    continue
                refs = set()
                collect_name_references(statement.value, refs)
                for feat in refs & feature_names:
                    if feat == defining_name:
                        continue
                    feature_refs.setdefault(feat, set()).add(defining_name)

    for feat_name, info in features.items():
        info['referenced_by_rules'] = sorted(rule_refs.get(feat_name, set()))
        info['referenced_by_features'] = sorted(feature_refs.get(feat_name, set()))
        info['referenced_by_whenrules'] = whenrules_refs.get(feat_name, 0)
        info['total_references'] = (
            len(info['referenced_by_rules']) + len(info['referenced_by_features']) + info['referenced_by_whenrules']
        )

    return list(features.values())


@blueprint.route('/features', methods=['GET'])
@require_ability(CanViewDocs)
def features_list() -> Any:
    """Return the catalog of extracted features across the rules engine."""
    features = _extract_features_from_engine()

    categories: dict[str, int] = {}
    extraction_fns: dict[str, int] = {}
    for f in features:
        categories[f['category']] = categories.get(f['category'], 0) + 1
        extraction_fns[f['extraction_fn']] = extraction_fns.get(f['extraction_fn'], 0) + 1

    return jsonify(
        {
            'features': features,
            'total': len(features),
            'categories': categories,
            'extraction_fns': extraction_fns,
        }
    )
