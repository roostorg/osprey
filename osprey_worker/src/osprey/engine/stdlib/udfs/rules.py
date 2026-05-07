import re
from typing import List, Optional, cast

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_utils import add_must_assign_to_variable_error
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.executor.execution_context import WhenRulesAuditEntry
from osprey.engine.language_types.effects import EffectBase
from osprey.engine.language_types.rules import RuleT
from osprey.engine.udf.type_helpers import validate_kwarg_node_type
from osprey.worker.lib.instruments import metrics

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories

_HAS_FORMAT_STRING_RE = re.compile(r'\{([^\d\W]\w*)\}')


class RuleArguments(ArgumentsBase):
    # TODO - Should we require that this is a list literal? If so, should we require it be non-empty?
    when_all: List[bool]
    description: str


class Rule(UDFBase[RuleArguments, RuleT]):
    """Defines a named rule with conditions. Evaluates to true when all conditions in when_all are met."""

    name: Optional[str] = None
    category = UdfCategories.ENGINE

    def __init__(self, validation_context: ValidationContext, arguments: RuleArguments):
        super().__init__(validation_context, arguments)
        # Figure out what the rule name is, by traversing up the AST.
        # We want to make sure that our rule is in the form of
        # FeatureName = Rule(...).
        call_node = arguments.get_call_node()
        parent = call_node.parent
        if isinstance(parent, grammar.Assign):
            if parent.target.is_local:
                validation_context.add_error(
                    message='rules must be stored in non-local features',
                    span=parent.span,
                    hint=(
                        f'this rule is being stored in the local variable `{parent.target.identifier}`'
                        "\nrules must be in non-local features (eg that don't start with `_`)"
                    ),
                )
            else:
                self.name = parent.target.identifier

        else:
            add_must_assign_to_variable_error(
                validation_context,
                message='`Rule(...)` must be assigned to a variable',
                node=call_node,
                example_variable_name='SomeRule',
            )

        # We want to make sure that the rule description is a string or fstring literal.
        description_ast = validate_kwarg_node_type(
            context=validation_context,
            udf=self,
            arguments=arguments,
            kwarg='description',
            message_ending='requires either a string literal or an f-string',
            expected_type=(grammar.String, grammar.FormatString),
            expected_str='`String` or `FormatString` literal',
        )

        # If our description ast is a string, do a lint, and see if the author meant
        # to use an f-string, because variables were attempted to be interpolated.
        if isinstance(description_ast, grammar.String):
            matches = _HAS_FORMAT_STRING_RE.findall(description_ast.value)

            if matches:
                found_variables = ', '.join(f'`{match}`' for match in matches)
                validation_context.add_error(
                    message='variable interpolation attempted in non-format string',
                    span=description_ast.span,
                    hint=(
                        'this string contains what looks like variable interpolation, but is not an f-string'
                        f'\nfound: {found_variables}'
                        '\nconsider prefixing with `f`'
                    ),
                )

    def execute(self, execution_context: ExecutionContext, arguments: RuleArguments) -> RuleT:
        assert self.name, 'BUG: Rule execution started with un-assigned name.'

        description_ast = arguments.get_argument_ast('description')
        if isinstance(description_ast, grammar.String):
            description = description_ast.value
            features = {}
        elif isinstance(description_ast, grammar.FormatString):
            description = description_ast.format_string
            features = {name.identifier: str(execution_context.resolved(name)) for name in description_ast.names}
        else:
            raise TypeError(f'BUG: Unknown type {description_ast!r}, should have been caught by validator.')

        value_result = all(arguments.when_all)

        # Rules prefixed with "metric" are used by AD for monitoring, but is an anti-pattern.
        if self.name.lower().startswith('metric'):
            metrics.increment(
                'execute',
                tags=[
                    f'udf:{self.__class__.__name__}',
                    f'rule:{self.name}',
                    f'result:{value_result}',
                    f'action:{execution_context.get_action_name()}',
                ],
            )
        return RuleT(name=self.name, value=value_result, description=description, features=features)


class WhenRulesArguments(ArgumentsBase):
    # TODO - Should we require this be non-empty?
    rules_any: List[RuleT]
    # TODO - Should we require this be non-empty?
    then: List[EffectBase]


class WhenRules(UDFBase[WhenRulesArguments, None]):
    """Binds rules to effects. When any of the referenced rules fire, the then= effects are applied."""

    category = UdfCategories.ENGINE

    def resolve_arguments(self, execution_context: ExecutionContext, call_executor: CallExecutor) -> WhenRulesArguments:
        # WhenRules has some custom resolve logic, in order to tolerate a failure in the `rules_any` dependency chain.

        # 1. Grab the raw nodes, that correspond to each key in `WhenRulesArgument`.
        then_node = call_executor.dependent_node_dict['then']
        rules_any_node = call_executor.dependent_node_dict['rules_any']

        # 2. Perform special resolution of the `then` list. Essentially, we are semi-circumventing the
        #    `ListExecutor`, in order to peek inside, and grab each non-failed item within the list.
        #    This is important because if we have a list of LabelAdd() calls, we can end up invalidating
        #    them all if any of the calls fails.
        then_value = []
        then_failed = 0
        assert isinstance(then_node, grammar.List), 'BUG: `then` node is not a list!'
        for item in then_node.items:
            resolved = execution_context.resolved(item, return_none_for_failed_values=True)
            if resolved is not None:
                then_value.append(resolved)
            else:
                then_failed += 1

        # 3. Perform special resolution of the `rules_any` list. Essentially, we are semi-circumventing the
        #    `ListExecutor`, in order to peek inside, and grab each non-failed item within the list.
        rules_any_value = []
        rules_any_failed = 0
        failed_rule_names: List[str] = []
        assert isinstance(rules_any_node, grammar.List), 'BUG: `rules_any node is not a List!'
        for item in rules_any_node.items:
            resolved = execution_context.resolved(item, return_none_for_failed_values=True)
            if resolved is not None:
                rules_any_value.append(resolved)
            else:
                rules_any_failed += 1
                if isinstance(item, grammar.Name):
                    failed_rule_names.append(item.identifier)
                else:
                    failed_rule_names.append('<unknown>')

        # 4. Store audit state for execute() to consume.
        # Safe because WhenRules is synchronous (execute_async=False) and CallExecutor.execute()
        # calls resolve_arguments() then execute() without yielding to the event loop.
        self._failed_rule_names = failed_rule_names
        self._then_failed = then_failed

        # 5. Emit completeness metrics for this WhenRules block.
        action_name = execution_context.get_action_name()
        is_degraded = rules_any_failed > 0 or then_failed > 0
        metrics.increment(
            'osprey.whenrules_completeness',
            tags=[f'action:{action_name}', f'degraded:{is_degraded}'],
        )

        # 6. Construct the resolved arguments, based on our custom argument resolution.
        return cast(
            WhenRulesArguments,
            call_executor.unresolved_arguments.update_with_resolved({'then': then_value, 'rules_any': rules_any_value}),
        )

    def execute(self, execution_context: ExecutionContext, arguments: WhenRulesArguments) -> None:
        all_rule_names = [rule.name for rule in arguments.rules_any]
        passing_rules = [rule for rule in arguments.rules_any if rule.value]
        passing_names = [rule.name for rule in passing_rules]
        assert hasattr(self, '_failed_rule_names'), 'BUG: resolve_arguments() must be called before execute()'
        failed_rule_names: List[str] = self._failed_rule_names
        then_failed: int = self._then_failed

        effects_emitted: List[str] = []
        if passing_rules:
            effects_emitted = [type(o).__name__ for o in arguments.then if isinstance(o, EffectBase)]

        entry = WhenRulesAuditEntry(
            rules_evaluated=all_rule_names,
            rules_matched=passing_names,
            rules_failed=failed_rule_names,
            effects_emitted=effects_emitted,
            effects_failed=then_failed,
            is_degraded=len(failed_rule_names) > 0 or then_failed > 0,
        )
        execution_context.add_rule_audit_entry(entry)

        if entry.effects_failed > 0:
            has_gap = not entry.is_degraded
            metrics.increment(
                'osprey.enforcement_gap',
                tags=[f'action:{execution_context.get_action_name()}', f'gap:{has_gap}'],
            )

        if not passing_rules:
            return

        for outcome in arguments.then:
            if isinstance(outcome, EffectBase):
                outcome.add_rules(passing_rules)
                execution_context.add_effect(outcome)
            else:
                raise TypeError(f'BUG: Non-EffectBase type for WhenRules: {outcome!r} (type: {type(outcome)})')
