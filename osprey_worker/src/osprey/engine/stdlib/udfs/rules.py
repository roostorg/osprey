import re
from typing import List, Optional, cast

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_utils import add_must_assign_to_variable_error
from osprey.engine.executor.node_executor.call_executor import CallExecutor
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
        assert isinstance(then_node, grammar.List), 'BUG: `then` node is not a list!'
        for item in then_node.items:
            resolved = execution_context.resolved(item, return_none_for_failed_values=True)
            if resolved is not None:
                then_value.append(resolved)

        # 3. Perform special resolution of the `rules_any` list. Essentially, we are semi-circumventing the
        #    `ListExecutor`, in order to peek inside, and grab each non-failed item within the list.
        rules_any_value = []
        assert isinstance(rules_any_node, grammar.List), 'BUG: `rules_any node is not a List!'
        for item in rules_any_node.items:
            resolved = execution_context.resolved(item, return_none_for_failed_values=True)
            if resolved is not None:
                rules_any_value.append(resolved)

        # 4. Construct the resolved arguments, based on our custom argument resolution.
        return cast(
            WhenRulesArguments,
            call_executor.unresolved_arguments.update_with_resolved({'then': then_value, 'rules_any': rules_any_value}),
        )

    def execute(self, execution_context: ExecutionContext, arguments: WhenRulesArguments) -> None:
        passing_rules = [rule for rule in arguments.rules_any if rule.value]
        if not passing_rules:
            return

        for outcome in arguments.then:
            if isinstance(outcome, EffectBase):
                outcome.add_rules(passing_rules)
                execution_context.add_effect(outcome)
            else:
                raise TypeError(f'BUG: Non-EffectBase type for WhenRules: {outcome!r} (type: {type(outcome)})')
