from typing import Union

import pytest
from osprey.engine.ast.grammar import Boolean, Number, Span, UnaryOperation, USub
from osprey.engine.ast.sources import Sources
from osprey.engine.utils.osprey_unary_executor import OspreyUnaryExecutor


@pytest.fixture
def sources() -> Sources:
    return Sources.from_dict({'main.sml': 'hello'})


@pytest.fixture
def span(sources: Sources) -> Span:
    return Span(source=sources.get_entry_point(), start_line=0, start_pos=0)


@pytest.mark.parametrize(
    'value, expected_value',
    [(-1, 1), (2, -2), (3.1, -3.1)],
)
def test_execution_golden_path(span: Span, value: Union[int, float], expected_value: Union[int, float]) -> None:
    node = UnaryOperation(span=span, operand=Number(value=value, span=span), operator=USub(span=span))
    osprey_unary_executor = OspreyUnaryExecutor(node)
    assert osprey_unary_executor.get_execution_value() == expected_value
    assert osprey_unary_executor.get_modified_node() == UnaryOperation(
        span=span, operand=Number(value=expected_value, span=span), operator=USub(span=span)
    )


@pytest.mark.parametrize(
    'value, expected_value',
    [(True, -1), (False, 0)],
)
def test_execution_fails_validation(span: Span, value: bool, expected_value: Union[int, float]) -> None:
    node = UnaryOperation(span=span, operand=Boolean(value=value, span=span), operator=USub(span=span))
    with pytest.raises(Exception):
        OspreyUnaryExecutor(node).get_execution_value() == expected_value
