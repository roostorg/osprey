import pytest
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Source, String
from osprey.engine.ast_validator.base_validator import SourceValidator
from osprey.engine.conftest import CheckFailureFunction, CheckOutputFunction, RunValidationFunction


class WarnOnStringLiteralsThatContainJake(SourceValidator):
    def validate_source(self, source: 'Source') -> None:
        for string_literal in filter_nodes(source.ast_root, ty=String):
            if 'jake' in string_literal.value.lower():
                self.context.add_warning(
                    message='this string contains jake - which is suspect. Jakes are never suspicious',
                    span=string_literal.span,
                )


pytestmark = [pytest.mark.use_validators([WarnOnStringLiteralsThatContainJake])]


def test_validation_fails_if_warning_is_emitted_and_warning_as_error_is_true(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Foo = 'jake is suspicious'", warning_as_error=True)


def test_validation_succeeds_if_warning_is_emitted_and_warning_as_error_is_false(
    run_validation: RunValidationFunction, check_output: CheckOutputFunction
) -> None:
    result = run_validation("Foo = 'jake is suspicious'", warning_as_error=False)
    assert len(result.warnings) == 1
    assert check_output(result.render_warnings())
