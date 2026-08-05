"""CollectJsonDataPaths — AST validator that walks the transitive Import + Require
closure for each action source and produces a per-action manifest of all
json-extracting UDF calls (JsonData, EntityJson, ActionData, and any future UDF
decorated with `extracts_json_path = True`).

Result shape: Dict[action_name, List[FieldDeclaration]]
where action_name is derived from `actions/<name>.sml` paths.
"""

from __future__ import annotations

import logging
import re
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from osprey.engine.ast import grammar
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Assign, Call, Source, Span
from osprey.engine.ast_validator.base_validator import BaseValidator, HasResult
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.validate_call_kwargs import UDFNodeMapping, ValidateCallKwargs
from osprey.engine.schema.schema_loader import ActionSchema, load_schema_for_action, resolve_schemas_dir
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.graph import Graph

try:
    import jsonpath_rw
    import jsonpath_rw.jsonpath as _jp

    _HAS_JSONPATH_RW = True
except ImportError:
    _HAS_JSONPATH_RW = False

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidationContext

log = logging.getLogger(__name__)

# Stable node identity: (source_path, span.start_line, span.start_pos, ast_node_class_name)
NodeKey = Tuple[str, int, int, str]


@dataclass(frozen=True)
class FieldDeclaration:
    """A single json-path extraction call found in an action's transitive closure."""

    path: str
    """The JSON path string (e.g. '$.user.id')."""

    rvalue_type: str
    """The declared rvalue type annotation from the SML source (e.g. 'str', 'Optional[int]')."""

    source_file: str
    """The source file path where this call appears."""

    udf_name: str
    """The UDF class name: 'JsonData', 'EntityJson', 'ActionData', etc."""

    required: bool
    """Whether the field is marked required (defaults to True in JsonData)."""

    coerce_type: bool
    """Whether the field uses coerce_type."""

    top_level_group: str
    """First path segment after '$': '$.target_user.ip' -> 'target_user'."""

    span_start_line: int
    """Line number in source for stable identity."""

    span_start_col: int
    """Column position in source for stable identity."""


ActionManifest = Dict[str, List[FieldDeclaration]]


def _annotation_to_str(annotation: Optional[grammar.ASTNode]) -> str:
    """Convert an AST annotation node to a human-readable type string."""
    if annotation is None:
        return 'Any'
    if isinstance(annotation, grammar.AnnotationWithVariants):
        inner = ', '.join(_annotation_to_str(v) for v in annotation.variants)
        return f'{annotation.identifier}[{inner}]'
    if isinstance(annotation, grammar.Annotation):
        return annotation.identifier
    return 'Any'


def _extract_top_level_group(path_str: str) -> str:
    """Parse a JSON path string and return the first named segment after the root '$'.

    Examples:
        '$.user.id' -> 'user'
        '$.target_user.ip' -> 'target_user'
        '$.captcha_response.score' -> 'captcha_response'
    """
    # Fast path: handle the common $.x.y... format directly without jsonpath_rw parse overhead
    if path_str.startswith('$.'):
        rest = path_str[2:]
        if rest:
            return rest.split('.')[0].split('[')[0]
    # Fallback: use jsonpath_rw for non-trivial paths
    if _HAS_JSONPATH_RW:
        try:
            parsed = jsonpath_rw.parse(path_str)
            cur = parsed
            while hasattr(cur, 'left'):
                if isinstance(cur.left, _jp.Root):
                    return str(cur.right)
                if hasattr(cur.left, 'left') and isinstance(cur.left.left, _jp.Root):
                    return str(cur.left.right)
                cur = cur.left
            if hasattr(cur, 'fields'):
                return cur.fields[0]
        except Exception:
            log.debug('jsonpath_rw failed to parse %r, falling back to string split', path_str)
    return path_str.lstrip('$').lstrip('.').split('.')[0]


class CollectJsonDataPaths(BaseValidator, HasResult[ActionManifest]):
    """Walks the transitive Import + Require closure for each action source and
    collects every json-extracting UDF call into a per-action manifest.

    The manifest is keyed by action_name (derived from 'actions/<name>.sml' paths).
    A schema loader integration point is provided via _load_schema_if_present() for
    the optional type-drift cross-check (§4.6).

    Registration: add to validator_regsiter.py in the stdlib plugin.
    """

    _manifest: ActionManifest
    _udf_node_mapping: UDFNodeMapping
    _import_graph: Graph[Source]

    def __init__(self, context: 'ValidationContext') -> None:
        super().__init__(context)
        self._manifest = {}
        self._udf_node_mapping = context.get_validator_result(ValidateCallKwargs)
        import_result = context.get_validator_result(ImportsMustNotHaveCycles)
        self._import_graph = import_result.import_graph

    def run(self) -> None:
        """For each action source, compute the reachable closure and collect fields.

        Advisory only (emits warnings); guarded so an unexpected AST shape can never
        fail rule compilation fleet-wide — degrade to whatever manifest we collected.
        """
        try:
            self._collect_fields_and_cross_check()
        except Exception:
            log.exception('CollectJsonDataPaths failed; skipping typed-contract cross-check')

    def _collect_fields_and_cross_check(self) -> None:
        # Action sources are those under actions/<name>.sml
        action_sources = [
            source
            for source in self.context.sources
            if source.path.startswith('actions/') and source.path.endswith('.sml')
        ]

        for action_source in action_sources:
            action_name = Path(action_source.path).stem
            reachable = self._reachable_sources(action_source)
            fields: List[FieldDeclaration] = []
            seen_keys: Set[NodeKey] = set()

            for source in reachable:
                for call_node in filter_nodes(source.ast_root, Call):
                    if id(call_node) not in self._udf_node_mapping:
                        continue
                    udf, arguments = self._udf_node_mapping[id(call_node)]
                    if not getattr(type(udf), 'extracts_json_path', False):
                        continue

                    key = _node_key(call_node)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    decl = self._build_field_declaration(call_node, udf, arguments, source)
                    if decl is not None:
                        fields.append(decl)

            self._manifest[action_name] = fields

        self._cross_check_types()

    def get_result(self) -> ActionManifest:
        return self._manifest

    def _reachable_sources(self, action_source: Source) -> List[Source]:
        """BFS from action_source following both Import (static) and Require edges."""
        visited: Set[str] = set()
        queue: deque[Source] = deque([action_source])
        result: List[Source] = []

        while queue:
            src = queue.popleft()
            if src.path in visited:
                continue
            visited.add(src.path)
            result.append(src)

            # Static Import edges from the import graph (uses iter_edges)
            for neighbor in self._import_graph.iter_edges(src):
                if neighbor.path not in visited:
                    queue.append(neighbor)

            # Require edges (runtime, but we walk both branches statically)
            for target in self._resolve_require_targets(src):
                if target.path not in visited:
                    queue.append(target)

        return result

    def _resolve_require_targets(self, source: Source) -> List[Source]:
        """Resolve Require(rule=...) edges from a source.

        Both literal string and format-string (glob) forms are resolved.
        require_if is NOT inspected — both branches are walked statically.
        """
        targets: List[Source] = []
        for call_node in filter_nodes(source.ast_root, Call):
            if id(call_node) not in self._udf_node_mapping:
                continue
            udf, _ = self._udf_node_mapping[id(call_node)]
            if not isinstance(udf, Require):
                continue

            keyword = call_node.find_argument('rule')
            if keyword is None:
                continue
            # find_argument returns a Keyword node; the actual AST value is keyword.value
            rule_ast_node = keyword.value

            if isinstance(rule_ast_node, grammar.String):
                target = self.context.sources.get_by_path(rule_ast_node.value)
                if target:
                    targets.append(target)
            elif isinstance(rule_ast_node, grammar.FormatString):
                # Convert f-string to glob pattern (same as require.py:36-44)
                names_as_wildcards = {name.identifier: '*' for name in rule_ast_node.names}
                glob_path = rule_ast_node.format_string.format(**names_as_wildcards)
                for matched in self.context.sources.glob(glob_path):
                    targets.append(matched)

        return targets

    def _build_field_declaration(
        self,
        call_node: Call,
        udf: UDFBase[Any, Any],
        arguments: ArgumentsBase,
        source: Source,
    ) -> Optional[FieldDeclaration]:
        """Build a FieldDeclaration from a UDF call node."""
        # Get the path argument (ConstExpr — stored in _arguments dict)
        path_str: Optional[str] = None
        required: bool = True
        coerce_type: bool = True

        try:
            path_const = getattr(arguments, 'path', None)
            if path_const is not None and hasattr(path_const, 'value'):
                path_str = path_const.value
        except Exception:
            log.debug("Failed to read 'path' argument from %r", call_node)

        if path_str is None:
            return None

        # required and coerce_type are plain bool args (not ConstExpr).
        # They are only available as AST nodes in _arguments_ast, not as
        # resolved Python values in _arguments. Read from the AST directly.
        try:
            if hasattr(arguments, 'has_argument_ast') and arguments.has_argument_ast('required'):
                req_ast = arguments.get_argument_ast('required')
                if isinstance(req_ast, grammar.Boolean):
                    required = req_ast.value
        except Exception:
            log.debug("Failed to read 'required' argument from %r", call_node)

        try:
            if hasattr(arguments, 'has_argument_ast') and arguments.has_argument_ast('coerce_type'):
                coerce_ast = arguments.get_argument_ast('coerce_type')
                if isinstance(coerce_ast, grammar.Boolean):
                    coerce_type = coerce_ast.value
        except Exception:
            log.debug("Failed to read 'coerce_type' argument from %r", call_node)

        top_level_group = _extract_top_level_group(path_str)

        # Derive rvalue_type from the Assign node's annotation (if this call is on the rhs of an assign)
        rvalue_type = 'Any'
        parent = call_node.parent
        if isinstance(parent, Assign) and parent.annotation is not None:
            rvalue_type = _annotation_to_str(parent.annotation)

        udf_name = type(udf).__name__

        span = call_node.span
        return FieldDeclaration(
            path=path_str,
            rvalue_type=rvalue_type,
            source_file=source.path,
            udf_name=udf_name,
            required=required,
            coerce_type=coerce_type,
            top_level_group=top_level_group,
            span_start_line=span.start_line,
            span_start_col=span.start_pos,
        )

    def _load_schema_if_present(self, action_name: str) -> Optional[ActionSchema]:
        """Load the schema for this action from the resolved schemas directory.

        Returns None if no schema directory is discoverable (neither
        ``OSPREY_SCHEMAS_DIR`` nor ``OSPREY_RULES_PATH/schemas`` resolves)
        or no schema file exists for the action. Called from
        :meth:`_cross_check_types`.
        """
        schemas_dir = resolve_schemas_dir()
        if schemas_dir is None:
            return None
        return load_schema_for_action(action_name, schemas_dir)

    def _cross_check_types(self) -> None:
        """Consumer-side contract validation (§4.6).

        If OSPREY_SCHEMAS_DIR is set and a schema exists for an action, emit
        warnings for three categories of drift:

        Check 1 — Absent-field usage: the rule extracts a path whose top-level
          group is declared absent in the schema.  The specializer will prune
          this node at runtime, so the extraction is dead code.

        Check 2 — Type mismatch: the schema declares a field with a different
          type than the rvalue annotation in the rule.

        Check 3 — Unknown field: the rule extracts a path from a group that
          the schema declares as provided, but the specific field is not listed
          in provides.  The schema may need updating.
        """
        for action_name, fields in self._manifest.items():
            schema = self._load_schema_if_present(action_name)
            if schema is None:
                continue
            for field in fields:
                source = self.context.sources.get_by_path(field.source_file)
                if source is None:
                    continue
                span = Span(
                    source=source,
                    start_line=field.span_start_line,
                    start_pos=field.span_start_col,
                )

                # Check 1: absent-field usage
                if field.top_level_group in schema.absent_groups:
                    self.context.add_warning(
                        message=(
                            f'{action_name}: extracts {field.path!r} from group '
                            f'{field.top_level_group!r} which the schema declares absent. '
                            f'This node will be pruned at runtime.'
                        ),
                        span=span,
                    )
                    continue  # No further checks needed for absent fields

                # Strip array indices ($.a[0].b -> a.b) so the lookup matches the
                # flattened dotted key in provides_field_types.
                path_key = re.sub(r'\[[^\]]*\]', '', field.path.removeprefix('$.'))

                # Check 2: type mismatch (only for fields in a provided group)
                declared = schema.provides_field_types.get(path_key)
                if declared is not None:
                    actual = _normalize_rvalue_type(field.rvalue_type)
                    if actual != declared:
                        self.context.add_warning(
                            message=(
                                f'{action_name}: {field.path} declared {declared!r} in schema but rule reads {actual!r}'
                            ),
                            span=span,
                            hint='Possible InvalidJsonType at runtime — update schema or rule.',
                        )
                    continue  # Field is known to the schema, no Check 3 needed

                # Check 3: unknown field — group is provided but this specific field is not listed
                if field.top_level_group in schema.provides_groups:
                    self.context.add_warning(
                        message=(
                            f'{action_name}: extracts {field.path!r} but schema does not '
                            f'declare it in provides for group {field.top_level_group!r}. '
                            f'Schema may need updating.'
                        ),
                        span=span,
                    )


def _normalize_rvalue_type(rvalue_type: str) -> str:
    """Normalize an rvalue type string for comparison with schema types.

    Strips Optional[], Entity[], and other wrappers to get the base type.
    """
    t = rvalue_type.strip()
    # Strip Optional[...]
    if t.startswith('Optional[') and t.endswith(']'):
        t = t[len('Optional[') : -1].strip()
    # Strip Entity[...]
    if t.startswith('Entity[') and t.endswith(']'):
        t = t[len('Entity[') : -1].strip()
    return t


def _node_key(node: Call) -> NodeKey:
    """Compute a stable node key from a Call AST node."""
    span = node.span
    return (span.source.path, span.start_line, span.start_pos, type(node).__name__)
