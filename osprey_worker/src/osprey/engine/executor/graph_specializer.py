"""Graph specializer for typed action contracts.

Given a full ExecutionGraph and an ActionSchema, produces a specialized graph
that skips nodes whose extracted json paths belong to absent top-level groups.

The specializer works by creating a SpecializedExecutionGraph subclass that
overrides get_sorted_dependency_chain() to filter out pruned DependencyChains.

Pruning rules (per §4.4 of the typed-action-contracts plan):
  1. Root absent extractors: any DependencyChain whose executor's UDF has
     `extracts_json_path = True` AND whose path top-level group is in
     schema.absent_groups is pruned.
  2. Propagation:
     (a) Conservative Rule pruning: a Rule chain is pruned if ANY non-constant
         dep in chain.dependent_on is pruned.  Rule.execute calls all(when_all)
         via ListExecutor which resolves each dep WITHOUT return_none_for_failed;
         a pruned dep that was never executed will raise KeyError at runtime.
     (b) ResolveOptional with non-None default_value: kept (not pruned) even if its
         optional_value dep is absent. optional_value is an Optional kwarg, so a folded/absent
         optional_value resolves to None without running its extractor, and execute() returns the
         default on None.
     (c) Default propagation: prune if ALL non-constant (non-IsConstant) deps
         are pruned. Literal constants (String, Number, Boolean) do not block
         pruning of their parent, since they are always computable.
  3. Surviving chains are assembled into a SpecializedExecutionGraph.

Constant-folding (replaces the old "verdict-critical rescue"): the absent-derived nodes inside
every WhenRules closure (rules_any -> Rules -> when_all -> extractors/UDFs, plus then -> effects)
are not pruned — they are CONSTANT-FOLDED. Their value is precomputed once at specialize-time by
replaying the engine's own executors against absent input (and, for UDFs that declare an
`absent_value` such as HasLabel, taking that declaration instead of running the body), then injected
at runtime so the node never executes — no MissingJsonPath throw, no backend call. Because these
nodes are folded rather than pruned, the conservative Rule-prune never cascades into an enforcement
closure, so enforcement is computed exactly as the full graph and the explicit rescue step is no
longer needed. Non-fold-safe enforcement nodes (Rules, effects, undeclared backend UDFs) still
execute, reading the folded values. The fold bakes in the "absent" assumption, so a MISCLASSIFIED
payload is caught at dispatch by `absent_groups_satisfied` (serve the full graph) — see
typed_contract_dispatch.resolve_dispatch.

Node identity: NodeKey = id(ast_node) — collision-free (see NodeKey definition).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set

from osprey.engine.ast.grammar import ASTNode, IsConstant, Source, String
from osprey.engine.ast.grammar import List as GrammarList
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_context import Action, ExecutionContext, NodeResult
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.udf.base import UDFBase
from result import Err, Ok

if TYPE_CHECKING:
    from osprey.engine.schema.schema_loader import ActionSchema

log = logging.getLogger(__name__)

# Node identity = id() of the AST node. A structural key (source/line/col/class) is
# NOT unique: `A and B or C` yields an Or and an And node sharing line+col (Span has
# no end position), so pruning the inner And would also drop the surviving Or. The
# specializer and runtime share the same full_graph node objects (rebuilt per
# recompile), so id() is stable for a specialization and collision-free.
NodeKey = int


def _node_key_from_node(node: ASTNode) -> NodeKey:
    """Collision-free node identity (id of the AST node object)."""
    return id(node)


def _node_key_from_chain(chain: DependencyChain) -> NodeKey:
    """Collision-free node identity from a DependencyChain's executor node."""
    return id(chain.executor.node)


def _chain_udf(chain: DependencyChain) -> Optional[UDFBase[Any, Any]]:
    """Return the UDF instance from a CallExecutor chain, or None."""
    if isinstance(chain.executor, CallExecutor):
        return chain.executor._udf
    return None


def _is_json_extractor(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF has extracts_json_path = True."""
    udf = _chain_udf(chain)
    if udf is None:
        return False
    return getattr(type(udf), "extracts_json_path", False)


def _get_extractor_path(chain: DependencyChain) -> Optional[str]:
    """Return the path argument from a json-extractor chain."""
    udf = _chain_udf(chain)
    if udf is None:
        return None
    # Path is stored on the UDF's arguments (already resolved ConstExpr)
    # Access via the executor's unresolved_arguments
    if isinstance(chain.executor, CallExecutor):
        try:
            path_arg = chain.executor.unresolved_arguments.get_argument_ast("path")
            if path_arg is not None:
                if isinstance(path_arg, String):
                    return path_arg.value
        except Exception:
            pass
    return None


def _get_top_level_group(path_str: str) -> str:
    """Extract top-level group from a json path string."""
    if path_str.startswith("$."):
        rest = path_str[2:]
        if rest:
            return rest.split(".")[0].split("[")[0]
    return path_str.lstrip("$").lstrip(".").split(".")[0]


def _is_resolve_optional_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is ResolveOptional."""
    udf = _chain_udf(chain)
    return isinstance(udf, ResolveOptional)


def _is_whenrules_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is WhenRules (binds rules to effects). A WhenRules
    chain's transitive closure is the entire enforcement output path (rules_any -> Rules ->
    when_all -> extractors, plus then -> effects), which the specializer must never prune."""
    return isinstance(_chain_udf(chain), WhenRules)


def _is_rule_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is Rule.

    Rule.execute calls all(when_all) via ListExecutor which resolves each dep
    without return_none_for_failed_values=True. A pruned dep that was never
    executed will cause a KeyError at runtime, so Rule chains must be pruned
    conservatively (rule (a): prune if ANY non-constant dep is pruned).
    """
    udf = _chain_udf(chain)
    return isinstance(udf, Rule)


def _resolve_optional_has_default(chain: DependencyChain) -> bool:
    """Return True if this ResolveOptional has a non-None default_value."""
    if not isinstance(chain.executor, CallExecutor):
        return False
    try:
        default_arg = chain.executor.unresolved_arguments.get_argument_ast("default_value")
        return default_arg is not None
    except Exception:
        return False


def _get_all_sorted_chains(graph: ExecutionGraph) -> List[DependencyChain]:
    """Gather all sorted dependency chains from all sources in the graph."""
    chains: List[DependencyChain] = []
    seen: Set[int] = set()

    for source in graph.validated_sources.sources:
        try:
            for chain in graph.get_sorted_dependency_chain(source):
                if id(chain) not in seen:
                    seen.add(id(chain))
                    chains.append(chain)
        except KeyError:
            pass
    return chains


def _collect_all_chains_recursive(chains: Sequence[DependencyChain]) -> List[DependencyChain]:
    """Recursively collect all chains including sub-chains."""
    result: List[DependencyChain] = []
    seen: Set[int] = set()

    def visit(chain: DependencyChain) -> None:
        if id(chain) in seen:
            return
        seen.add(id(chain))
        for dep in chain.dependent_on:
            visit(dep)
        result.append(chain)

    for chain in chains:
        visit(chain)
    return result


def _collect_chain_keys(chain: DependencyChain, out: "Set[NodeKey]") -> None:
    """Add `chain`'s node key and the keys of all its transitive deps to `out`."""
    key = _node_key_from_chain(chain)
    if key in out:
        return
    out.add(key)
    for dep in chain.dependent_on:
        _collect_chain_keys(dep, out)


def _is_constant_node(chain: DependencyChain) -> bool:
    node = chain.executor.node
    return isinstance(node, IsConstant) and node.is_constant


def _is_fold_safe(chain: DependencyChain) -> bool:
    """True if this chain's value can be recomputed at specialize-time:
      - structural ops (comparison/bool/unary/binary), literals, lists, names, assigns — by
        replaying their own executor (they only read resolved deps);
      - json extractors — by replaying (they only read action data);
      - a UDF that declares `is_fold_safe_when_absent()` — via its `absent_value()` (so its body
        and any backend IO are skipped).
    Everything else (Rule / WhenRules / ResolveOptional / effects / undeclared backend UDFs) is NOT
    fold-safe; it stays scheduled + executed, so a node depending on it is never folded
    (safe-by-default)."""
    if isinstance(chain.executor, CallExecutor):
        if _is_json_extractor(chain):
            return True
        udf = chain.executor._udf
        return bool(udf is not None and udf.is_fold_safe_when_absent())
    return True


def _compute_foldable_closure(
    all_chains: Sequence[DependencyChain],
    absent_groups: FrozenSet[str],
) -> Set[NodeKey]:
    """The set of nodes whose value is fully determined by absent-group inputs.

    Seeded at absent json-extractors and propagated up through fold-safe nodes whose every
    non-constant dependency is itself foldable. A present-group extractor has no non-constant
    deps, so it is never added; a non-fold-safe UDF (Rule/effect/backend) blocks its parent.
    """
    foldable: Set[NodeKey] = set()
    for chain in all_chains:
        if _is_json_extractor(chain):
            path = _get_extractor_path(chain)
            if path is not None and _get_top_level_group(path) in absent_groups:
                foldable.add(_node_key_from_chain(chain))

    changed = True
    while changed:
        changed = False
        for chain in all_chains:
            key = _node_key_from_chain(chain)
            if key in foldable or not _is_fold_safe(chain):
                continue
            non_const_deps = [dep for dep in chain.dependent_on if not _is_constant_node(dep)]
            if non_const_deps and all(_node_key_from_chain(dep) in foldable for dep in non_const_deps):
                foldable.add(key)
                changed = True
    return foldable


def _compute_fold_values(
    full_graph: ExecutionGraph,
    all_chains: Sequence[DependencyChain],
    foldable: Set[NodeKey],
    action_name: str,
) -> "Dict[NodeKey, NodeResult]":
    """Compute each foldable node's NodeResult by REPLAYING its executor against an empty
    (all-groups-absent) action — reusing the engine's own executors + ExecutionContext.resolved
    so the Ok/Err kind and value are byte-identical to what the rescue would execute (no
    hand-derived fold table; zero drift). Constants are executed too so foldable operands
    resolve, but they are not injected (they stay cheap to recompute at runtime).

    Keying: we store under id(chain.executor.node) — the same key set_resolved_value uses. A
    consumer resolving a Name redirects (ExecutionContext.get_name_node) to the assignment node's
    id, so seeding by executor.node id is what makes Name/Assign resolution line up at runtime. Any
    widening of _is_fold_safe to new node kinds must keep test_fold_matches_rescue_node_for_node
    green — it is the load-bearing check that this replay equals the rescue's executed values.
    """
    ctx = ExecutionContext(
        full_graph,
        Action(action_id=0, action_name=action_name, data={}, timestamp=datetime(2020, 1, 1)),
        UDFHelpers(),
    )
    fold_values: "Dict[NodeKey, NodeResult]" = {}
    # all_chains is post-order (deps before dependents), so a node's deps are resolved first.
    for chain in all_chains:
        key = _node_key_from_chain(chain)
        if key not in foldable and not _is_constant_node(chain):
            continue
        udf = _chain_udf(chain)
        result: "NodeResult"
        if udf is not None and not _is_json_extractor(chain) and udf.is_fold_safe_when_absent():
            # A UDF that declares itself fold-safe: resolve its arguments (so an absent *required*
            # input still fail-propagates exactly as execute would) then take the declared
            # absent_value INSTEAD of running the body — skipping its backend IO. resolve_arguments
            # makes no UDF call itself.
            try:
                resolved_args = udf.resolve_arguments(ctx, chain.executor)
            except Exception:
                # A required absent input fail-propagated -> shadowed, exactly as execute would.
                result = Err(None)
            else:
                try:
                    result = udf.absent_value(resolved_args)
                except Exception as e:
                    # absent_value must never raise (it's a pure declaration). Surface a buggy
                    # declaration LOUDLY rather than silently mis-folding to Err — but don't crash
                    # config-load for every action; fall back to Err for this node only.
                    result = Err(None)
                    log.warning("absent_value raised for %s on %s: %r", chain.executor.node, action_name, e)
        else:
            try:
                result = Ok(chain.executor.execute(execution_context=ctx))
            except Exception as e:
                # An absent extractor expectedly raises (MissingJsonPath/ExpectedUdfException) ->
                # Err, exactly as the rescue would. Debug-log so a genuinely-broken fold-safe node
                # stays diagnosable rather than indistinguishable from the expected absent failures.
                result = Err(None)
                log.debug("fold replay raised for %s on %s: %r", chain.executor.node, action_name, e)
        # Store directly rather than via set_resolved_value — the latter calls the topological
        # sorter's done(), which raises for chains never handed out by get_ready().
        ctx._resolved_node_values[key] = result
        if key in foldable:
            fold_values[key] = result
    return fold_values


def specialize_graph(
    full_graph: ExecutionGraph,
    schema: "ActionSchema",
) -> "SpecializedExecutionGraph":
    """Produce a specialized execution graph for the given action schema.

    Chains whose top-level json group is in schema.absent_groups are pruned,
    along with their dependents (using the propagation rules in §4.4).

    Returns a SpecializedExecutionGraph that delegates to full_graph for
    everything except pruned chains.
    """
    absent_groups: FrozenSet[str] = schema.absent_groups

    # Step 1 — collect all chains
    all_top_level_chains = _get_all_sorted_chains(full_graph)
    all_chains = _collect_all_chains_recursive(all_top_level_chains)

    # Step 2 — constant-fold the enforcement-feeding absent subtrees (this REPLACES the old
    # "rescue"). `protected` is the transitive closure of every WhenRules chain: the rules_any
    # List, its Rules, each Rule's when_all conditions + their extractors/UDFs, and the then
    # effects. The fold-safe nodes in it (json extractors, structural ops, and UDFs that declare an
    # absent_value such as HasLabel) are folded — their value is precomputed by replaying the
    # engine's own executors against absent input and injected at runtime, skipping execution (no
    # MissingJsonPath throw, no backend call). Non-fold-safe enforcement nodes (the Rules, effects,
    # undeclared backend UDFs) are NOT folded; they stay scheduled and execute, reading the folded
    # values — exactly as the rescue made them, but without the rescue step.
    protected: Set[NodeKey] = set()
    for chain in all_chains:
        if _is_whenrules_chain(chain):
            _collect_chain_keys(chain, protected)
    foldable = _compute_foldable_closure(all_chains, absent_groups) & protected
    fold_values = (
        _compute_fold_values(full_graph, all_chains, foldable, schema.action) if foldable else {}
    )

    # Step 3 — seed the pruned set with absent extractors that are NOT folded (i.e. analytics-only
    # absent reads, outside any WhenRules closure). Folded enforcement extractors are deliberately
    # EXCLUDED: folding them (rather than pruning them) is what keeps the conservative Rule-prune
    # below from cascading into the enforcement closure, so the old step-3b "rescue" is unnecessary
    # and has been removed. A genuinely-misclassified payload is still caught by the dispatch-time
    # `absent_groups_satisfied` guard (serve the full graph), preserving misclassification safety.
    pruned: Set[NodeKey] = set()
    for chain in all_chains:
        if _is_json_extractor(chain):
            path = _get_extractor_path(chain)
            if path is not None and _get_top_level_group(path) in absent_groups:
                key = _node_key_from_chain(chain)
                if key not in foldable:
                    pruned.add(key)

    # Step 4 — propagation loop (analytics-only pruning).
    changed = True
    while changed:
        changed = False
        for chain in all_chains:
            key = _node_key_from_chain(chain)
            if key in pruned or key in foldable:
                # Folded nodes are injected, not pruned — never propagate-prune them.
                continue

            deps = chain.dependent_on

            # (a) Rule chains — conservative pruning: prune if ANY non-constant dep
            # (or dep-of-dep via the when_all List node) is pruned.
            # Rule.execute resolves when_all via ListExecutor which calls
            # execution_context.resolved(n) without return_none_for_failed_values=True.
            # A pruned (never-executed) dep would cause a KeyError at runtime.
            #
            # The Rule chain's dependent_on is [when_all_List_chain, description_String_chain].
            # The individual when_all conditions are deps of the List chain (one level deeper).
            # We must check both the direct deps AND the deps of the when_all List node.
            if _is_rule_chain(chain):
                should_prune = False
                for dep in deps:
                    if isinstance(dep.executor.node, IsConstant) and dep.executor.node.is_constant:
                        continue
                    dep_key = _node_key_from_chain(dep)
                    if dep_key in pruned:
                        should_prune = True
                        break
                    # If this dep is the when_all List node, check its own deps too.
                    if isinstance(dep.executor.node, GrammarList):
                        for when_all_dep in dep.dependent_on:
                            if isinstance(when_all_dep.executor.node, IsConstant) and when_all_dep.executor.node.is_constant:
                                continue
                            when_all_dep_key = _node_key_from_chain(when_all_dep)
                            if when_all_dep_key in pruned:
                                should_prune = True
                                break
                    if should_prune:
                        break
                if should_prune:
                    pruned.add(key)
                    changed = True
                continue  # Rule chains skip rules (b) and (c)

            # (b) ResolveOptional-with-default: keep it (don't prune), and don't rescue
            # its optional_value subtree. optional_value is an Optional kwarg, so
            # resolve_arguments resolves a pruned dep to None WITHOUT running its
            # extractor (udf/base.py return_none_for_failed_values=True), and execute()
            # returns the default on None. Rescuing it (the old behavior) only made an
            # absent-group extractor run and raise ExpectedUdfException before
            # defaulting anyway — same result, wasted work, spurious expected error.
            # (Without a default it falls to rule (c): pruned -> None, also neutral.)
            if _is_resolve_optional_chain(chain) and _resolve_optional_has_default(chain):
                continue

            # (c) Default propagation: prune if ALL non-constant deps are pruned.
            # A dep whose AST node is an IsConstant (String, Number, Boolean,
            # etc.) can never be pruned and should not block pruning of its
            # parent.  A dep that is NOT a constant but is not yet in `pruned`
            # is a live computed value that keeps the current chain alive.
            non_const_surviving_dep_keys = []
            for dep in deps:
                if isinstance(dep.executor.node, IsConstant) and dep.executor.node.is_constant:
                    # Literal constant — skip; cannot be pruned
                    continue
                dep_key = _node_key_from_chain(dep)
                if dep_key not in pruned:
                    non_const_surviving_dep_keys.append(dep_key)

            # Only prune if there is at least one non-constant dep (otherwise
            # the chain itself is effectively constant and should remain).
            has_non_const_dep = any(
                not (isinstance(dep.executor.node, IsConstant) and dep.executor.node.is_constant)
                for dep in deps
            )
            if has_non_const_dep and not non_const_surviving_dep_keys:
                pruned.add(key)
                changed = True

    # (The old step-3b "rescue" — un-pruning the WhenRules closure — is gone: enforcement-feeding
    # absent nodes are now FOLDED in step 2, so the propagation above never prunes them.)

    log.debug(
        "specialize_graph: schema=%s absent_groups=%r pruned %d, folded %d of %d chains",
        schema.action,
        absent_groups,
        len(pruned),
        len(fold_values),
        len(all_chains),
    )

    return SpecializedExecutionGraph(
        full_graph=full_graph,
        pruned_keys=frozenset(pruned),
        schema=schema,
        fold_values=fold_values,
    )


# RuleT carries a `features={...}` dict used only to render the human-readable label
# description; it can embed time-variant values (e.g. _AccountAge = SnowflakeAge(now()))
# and never affects enforcement. Strip it before comparing effects so a ~ms drift
# between the full and specialized passes is not mistaken for a divergence.
_EFFECT_DESC_META = re.compile(r"features=\{[^}]*\}")
# The enforcement-determining decision outputs (engine-injected, `__`-prefixed).
_DECISION_KEYS = ("__verdicts", "__classifications", "__entity_label_mutations")


def shadow_divergences(full_result: object, spec_result: object) -> List[str]:
    """ENFORCEMENT-divergence reasons between a full and specialized ExecutionResult
    (empty == equivalent).

    The bar is *enforcement equivalence* — emitted effects plus the decision keys — NOT
    feature-key identity. A pruned absent-group feature legitimately disappears, and an
    absent-group `: bool` feature legitimately becomes None instead of a fabricated False,
    so a changed/absent non-decision feature value is EXPECTED, not a divergence (matching
    the in-process equivalence harness and the RFC's "Pruning Semantics & Validation").
    Two things are still real bugs: a spec-only feature (pruning must only REMOVE, never
    ADD) and any change to the effects or decision outputs. Time-variant effect description
    metadata is normalized out (see `_EFFECT_DESC_META`).
    """
    issues: List[str] = []
    ff = getattr(full_result, "extracted_features", {}) or {}
    sf = getattr(spec_result, "extracted_features", {}) or {}
    # Pruning must only ever REMOVE features, never add them.
    extra = sorted(k for k in (set(sf) - set(ff)) if not k.startswith("__"))
    if extra:
        issues.append(f"spec-only features: {extra[:10]}")
    # Enforcement decision outputs must be identical.
    for k in _DECISION_KEYS:
        if ff.get(k) != sf.get(k):
            issues.append(f"decision changed: {k} ({ff.get(k)!r} != {sf.get(k)!r})")

    def _effects(result: object) -> List[str]:
        out: List[str] = []
        for effect_type, seq in (getattr(result, "effects", {}) or {}).items():
            for effect in seq:
                rendered = f"{getattr(effect_type, '__name__', effect_type)}:{effect!r}"
                out.append(_EFFECT_DESC_META.sub("features=<desc>", rendered))
        return sorted(out)

    fe, se = _effects(full_result), _effects(spec_result)
    if fe != se:
        issues.append(f"effects differ: full={fe[:6]} spec={se[:6]}")
    return issues


class SpecializedExecutionGraph(ExecutionGraph):
    """A specialized ExecutionGraph that filters out pruned dependency chains.

    Constructed by specialize_graph(); delegates to the full_graph for all
    unmodified behavior and overrides get_sorted_dependency_chain() to skip
    absent-group chains.
    """

    __slots__ = (
        '_root_node_executor_mapping',
        '_assignment_executor_mapping',
        '_node_executor_registry',
        '_validated_sources',
        '_sorted_dependency_chains',
        '_nodes_to_unwrap',
        '_full_graph',
        '_pruned_keys',
        '_fold_values',
        '_filtered_sorted_chains',
        '_schema',
    )

    def __init__(
        self,
        full_graph: ExecutionGraph,
        pruned_keys: FrozenSet[NodeKey],
        schema: "ActionSchema",
        fold_values: "Optional[Mapping[NodeKey, NodeResult]]" = None,
    ) -> None:
        # Initialize the base ExecutionGraph with the full graph's registry and sources
        super().__init__(
            node_executor_registry=full_graph._node_executor_registry,
            sources=full_graph._validated_sources,
            nodes_to_unwrap=full_graph._nodes_to_unwrap,
        )
        # Copy existing mappings from the full graph
        self._root_node_executor_mapping = full_graph._root_node_executor_mapping
        self._assignment_executor_mapping = full_graph._assignment_executor_mapping
        self._sorted_dependency_chains = full_graph._sorted_dependency_chains
        self._full_graph = full_graph
        self._pruned_keys = pruned_keys
        # Folded nodes: value precomputed at specialize-time, injected at runtime, never executed.
        # Keyed by NodeKey (== id(node)), so it doubles as the runtime pre-seed map.
        self._fold_values: Mapping[NodeKey, NodeResult] = fold_values or {}
        self._schema = schema
        self._filtered_sorted_chains = self._build_filtered_sorted_chains(full_graph)

    def _build_filtered_sorted_chains(self, full_graph: ExecutionGraph) -> Dict[Source, Sequence[DependencyChain]]:
        """Precompute every source's scheduling list once, at construction.

        The request path asks for these lists once per enqueued source per action, and both the
        graph and its pruned/folded sets are immutable after construction, so filtering per call
        was pure repeated work (one full pass over the source's chains, ~0.17ms/action for a
        ~1k-chain source). Precomputing eagerly rather than caching lazily costs one extra O(chains)
        pass inside specialize_graph — which already walks every chain several times — and keeps the
        instance read-only after __init__, so there is no cache-population race to reason about.

        A source whose chains are all kept maps to the full graph's own list object (identity, no
        copy), so a specialization that prunes and folds nothing is indistinguishable from the full
        graph and the memo costs no extra memory for untouched sources.
        """
        excluded = frozenset(self._pruned_keys) | frozenset(self._fold_values)
        filtered: Dict[Source, Sequence[DependencyChain]] = {}
        for source in full_graph._sorted_dependency_chains:
            # Via the getter, so specializing a specialized graph keeps the inner filtering.
            original = full_graph.get_sorted_dependency_chain(source)
            if not excluded:
                filtered[source] = original
                continue
            kept = tuple(chain for chain in original if _node_key_from_chain(chain) not in excluded)
            # We only ever remove, so equal lengths means nothing was dropped.
            filtered[source] = original if len(kept) == len(original) else kept
        return filtered

    def get_sorted_dependency_chain(self, source: Source) -> Sequence[DependencyChain]:
        """Return the sorted dependency chain for a source, with pruned AND folded chains removed
        (both are excluded from scheduling — pruned resolve to None/failure, folded are pre-seeded).

        Precomputed in __init__; raises KeyError for an unknown source, as the base class does.
        """
        return self._filtered_sorted_chains[source]

    def is_pruned_node(self, node: ASTNode) -> bool:
        """True if this node's chain was removed from scheduling (pruned or folded). Folded nodes
        are also pre-seeded, so their resolution never reaches the KeyError-pruned path; including
        them here keeps the 'not scheduled' predicate consistent."""
        key = _node_key_from_node(node)
        return key in self._pruned_keys or key in self._fold_values

    def get_prefolded_node_values(self) -> "Mapping[int, NodeResult]":
        """Precomputed NodeResults to seed into the ExecutionContext before execution, so folded
        nodes resolve to their constant without running their executor."""
        return self._fold_values

    def absent_groups_satisfied(self, action_data: Mapping[str, object]) -> bool:
        """True iff every group this specialization assumed absent is genuinely missing from
        ``action_data`` — the precondition for the folded/pruned values to be valid. A
        misclassified payload (an 'absent' group actually present) returns False, so dispatch
        must serve the full graph instead (preserving the rescue's misclassification safety)."""
        return all(group not in action_data for group in self._schema.absent_groups)

    @property
    def pruned_count(self) -> int:
        """Number of chains pruned by this specialization."""
        return len(self._pruned_keys)

    @property
    def fold_count(self) -> int:
        """Number of chains constant-folded (precomputed + injected) by this specialization."""
        return len(self._fold_values)

    @property
    def schema(self) -> "ActionSchema":
        return self._schema
