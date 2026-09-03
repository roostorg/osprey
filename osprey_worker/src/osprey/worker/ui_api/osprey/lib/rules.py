"""Reading the rules the engine currently has loaded.

Everything here is a pure read of `ENGINE.instance()` — the rule catalog and the
vocabulary the Rule Builder's dropdowns offer. Validation lives in
`rule_validation`, builder round-tripping in `rule_builder`, and writing to disk
in `rule_deployment`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import (
    Assign,
    Call,
    Name,
    Source,
)
from osprey.engine.ast.grammar import (
    List as AstList,
)
from osprey.engine.ast.printer import print_ast
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.lib.ast_utils import collect_name_references, get_func_identifier
from osprey.worker.ui_api.osprey.schemas.rules import (
    FeatureRef,
    RuleCatalogEntry,
    RuleList,
    UdfArgument,
    UdfSignature,
    Vocabulary,
)

logger = logging.getLogger(__name__)


def get_rule_source(path: str) -> Source | None:
    """The on-disk rule the engine loaded at `path`, if any.

    This serves rules the engine already knows about, not drafts — a draft's own
    SML comes from the rules table.
    """
    engine = ENGINE.instance()
    return engine.execution_graph.validated_sources.sources.get_by_path(path)


def _collect_features(sources: Iterable[Source]) -> list[FeatureRef]:
    out: list[FeatureRef] = []
    seen: set[str] = set()
    for source in sources:
        for assign in source.ast_root.statements:
            if not isinstance(assign, Assign):
                continue
            name = assign.target.identifier
            if name in seen:
                continue
            # Skip `MyRule = Rule(...)` assigns; the builder dropdown is for values
            # a user can reference inside conditions, not for rule definitions.
            if isinstance(assign.value, Call) and get_func_identifier(assign.value) == 'Rule':
                continue
            seen.add(name)
            out.append(FeatureRef(name=name, source_path=source.path, source_line=assign.span.start_line))
    out.sort(key=lambda item: item.name)
    return out


def _collect_udfs() -> list[UdfSignature]:
    engine = ENGINE.instance()
    udf_registry = engine.udf_registry
    out: list[UdfSignature] = []
    for func in sorted(udf_registry.iter_functions(), key=lambda f: f.__name__):
        try:
            args_type = func.get_arguments_type()
            rvalue_type = func.get_rvalue_type()
        except Exception:
            # Dropping the UDF is the right behaviour -- one that can't describe itself
            # can't be offered in a dropdown -- but doing it silently means the function
            # is simply absent from the Builder with nothing anywhere to say why.
            logger.warning('omitting UDF %r from the vocabulary: its types could not be read', func.__name__)
            continue
        arguments: list[UdfArgument] = []
        try:
            items = args_type.items().items()
        except Exception:
            # Same reasoning, one level down: the UDF is still listed, but with no
            # arguments, which would otherwise look like a genuinely nullary function.
            logger.warning('listing UDF %r with no arguments: its argument types could not be read', func.__name__)
            items = []
        for arg_name, arg_type in items:
            arguments.append(UdfArgument(name=arg_name, type_name=getattr(arg_type, '__name__', str(arg_type))))
        out.append(
            UdfSignature(
                name=func.__name__,
                return_type=getattr(rvalue_type, '__name__', str(rvalue_type)),
                arguments=arguments,
            )
        )
    return out


def _collect_effects(sources: Iterable[Source]) -> list[str]:
    """Names of UDFs that appear inside a `WhenRules(then=[...])` block.

    Used as the effect dropdown. Sourced from real usage rather than from the
    UDF registry because the registry holds every UDF and we want a shortlist
    of things users actually use as actions.
    """
    seen: set[str] = set()

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
            # filter_nodes yields the root too, so a bare `then=[Foo(...)]` entry is
            # covered as well as calls nested inside one. The hand-rolled walk this
            # replaces only descended through Call arguments and list items, so it
            # missed calls sitting inside an operation (e.g. `Concat(...) + 'x'`).
            for call in filter_nodes(then_arg.value, Call):
                ident = get_func_identifier(call)
                if ident:
                    seen.add(ident)

    return sorted(seen)


def get_vocabulary() -> Vocabulary:
    """Everything the Rule Builder's dropdowns offer: features, UDFs, effects, files."""
    engine = ENGINE.instance()
    sources = list(engine.execution_graph.validated_sources.sources)
    return Vocabulary(
        features=_collect_features(sources),
        udfs=_collect_udfs(),
        effects=_collect_effects(sources),
        source_files=sorted(s.path for s in sources),
    )


def list_rules() -> RuleList:
    """Walk the engine once, collecting Rule defs and WhenRules → Rule reference counts.

    WhenRules can appear in a source iterated before the Rule they reference
    (e.g., main.sml's WhenRules referencing a Rule in an imported file), so
    we accumulate counts into a name-keyed map during the walk and backfill
    each Rule's referenced_by_whenrules at the end.
    """
    engine = ENGINE.instance()
    sources = engine.execution_graph.validated_sources.sources
    # The engine already derives rule -> description (the RuleNameToDescriptionMapping
    # validator), and /config serves that same mapping. Read it rather than
    # re-rendering the description AST here, so the two can't drift apart.
    rule_descriptions = engine.get_rule_to_info_mapping()

    whenrules_ref_count: dict[str, int] = {}
    when_rules_total = 0
    rules: list[RuleCatalogEntry] = []

    for source in sources:
        for statement in source.ast_root.statements:
            # WhenRules(...) — bare statement or assigned
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
                when_rules_total += 1
                rules_any_arg = call_node.find_argument('rules_any')
                if rules_any_arg is not None and isinstance(rules_any_arg.value, AstList):
                    for item in rules_any_arg.value.items:
                        if isinstance(item, Name):
                            whenrules_ref_count[item.identifier] = whenrules_ref_count.get(item.identifier, 0) + 1
                continue

            # Rule(...) — must be an Assign
            if not (
                isinstance(statement, Assign)
                and isinstance(statement.value, Call)
                and get_func_identifier(statement.value) == 'Rule'
            ):
                continue

            rule_name = statement.target.identifier
            call = statement.value

            when_all: list[str] = []
            when_all_arg = call.find_argument('when_all')
            if when_all_arg is not None and isinstance(when_all_arg.value, AstList):
                for item in when_all_arg.value.items:
                    when_all.append(print_ast(item))
            elif when_all_arg is not None:
                when_all.append(print_ast(when_all_arg.value))

            # Rules with no description argument are absent from the mapping.
            description = rule_descriptions.get(rule_name, '')
            # Still needed below: referenced_features unions names appearing in the
            # description template, which the mapping's rendered string can't give us.
            description_arg = call.find_argument('description')

            refs: set[str] = set()
            if when_all_arg is not None:
                refs |= collect_name_references(when_all_arg.value)
            if description_arg is not None:
                refs |= collect_name_references(description_arg.value)
            referenced_features = sorted(refs)

            rules.append(
                RuleCatalogEntry(
                    name=rule_name,
                    source_file=source.path,
                    description=description,
                    when_all=when_all,
                    referenced_features=referenced_features,
                    referenced_by_whenrules=0,  # backfilled below
                )
            )

    for rule in rules:
        rule.referenced_by_whenrules = whenrules_ref_count.get(rule.name, 0)

    unused_total = sum(1 for r in rules if r.referenced_by_whenrules == 0)

    return RuleList(rules=rules, when_rules_total=when_rules_total, unused_total=unused_total)
