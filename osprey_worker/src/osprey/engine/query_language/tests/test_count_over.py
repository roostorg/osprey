from typing import Any, Callable, List

import pytest
from osprey.engine.ast import grammar
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.query_language.udfs.count_over import (
    CountOver,
    OperatorMetadata,
    operator_metadata_for,
)
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(CountOver)),
]


def test_count_over_with_key(run_validation: RunValidationFunction) -> None:
    run_validation("CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId)")


def test_count_over_without_key(run_validation: RunValidationFunction) -> None:
    run_validation("CountOver(predicate=Endpoint == '/foo', window='1m')")


def test_count_over_to_druid_query_raises_not_implemented(run_validation: RunValidationFunction) -> None:
    validated_sources = run_validation(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId)"
    )

    udf_mapping = validated_sources.get_validator_result(ValidateCallKwargs)

    source = validated_sources.sources.get_entry_point()
    count_over_call = None
    for call_node in filter_nodes(source.ast_root, grammar.Call):
        if isinstance(call_node.func, grammar.Name) and call_node.func.identifier == 'CountOver':
            count_over_call = call_node
            break

    assert count_over_call is not None, "CountOver call node not found in AST"

    count_over_udf, _ = udf_mapping[id(count_over_call)]
    assert isinstance(count_over_udf, CountOver)

    with pytest.raises(NotImplementedError, match="DruidQueryTransformer"):
        count_over_udf.to_druid_query()


# Tests for operator_metadata_for() — RFC Appendix A table
class TestOperatorMetadataFor:
    """Tests for the per-operator metadata helper covering all six comparison operators."""

    def test_operator_metadata_greater_than_equals(self) -> None:
        """Test >= operator: LAG(N-1), check within window."""
        metadata = operator_metadata_for(grammar.GreaterThanEquals, 10)
        assert isinstance(metadata, OperatorMetadata)
        assert metadata.lag_offsets == [9]
        assert metadata.post_filter_template == "pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds}"

    def test_operator_metadata_greater_than(self) -> None:
        """Test > operator: LAG(N), check within window."""
        metadata = operator_metadata_for(grammar.GreaterThan, 10)
        assert isinstance(metadata, OperatorMetadata)
        assert metadata.lag_offsets == [10]
        assert metadata.post_filter_template == "pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds}"

    def test_operator_metadata_equals(self) -> None:
        """Test == operator: LAG(N-1) and LAG(N), window bracket check."""
        metadata = operator_metadata_for(grammar.Equals, 10)
        assert isinstance(metadata, OperatorMetadata)
        assert metadata.lag_offsets == [9, 10]
        expected_filter = (
            "pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds} AND "
            "(pt2 IS NULL OR TIMESTAMPDIFF(SECOND, pt2, __time) > {window_seconds})"
        )
        assert metadata.post_filter_template == expected_filter

    def test_operator_metadata_not_equals(self) -> None:
        """Test != operator: Inverse of ==."""
        metadata = operator_metadata_for(grammar.NotEquals, 10)
        assert isinstance(metadata, OperatorMetadata)
        assert metadata.lag_offsets == [9, 10]
        expected_filter = (
            "NOT (pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds} AND "
            "(pt2 IS NULL OR TIMESTAMPDIFF(SECOND, pt2, __time) > {window_seconds}))"
        )
        assert metadata.post_filter_template == expected_filter

    def test_operator_metadata_less_than_equals(self) -> None:
        """Test <= operator: LAG(N), check outside window."""
        metadata = operator_metadata_for(grammar.LessThanEquals, 10)
        assert isinstance(metadata, OperatorMetadata)
        assert metadata.lag_offsets == [10]
        assert metadata.post_filter_template == "pt1 IS NULL OR TIMESTAMPDIFF(SECOND, pt1, __time) > {window_seconds}"

    def test_operator_metadata_less_than(self) -> None:
        """Test < operator: equivalent to <=(N-1), so LAG(N-1)."""
        metadata = operator_metadata_for(grammar.LessThan, 10)
        assert isinstance(metadata, OperatorMetadata)
        assert metadata.lag_offsets == [9]
        assert metadata.post_filter_template == "pt1 IS NULL OR TIMESTAMPDIFF(SECOND, pt1, __time) > {window_seconds}"

    def test_operator_metadata_various_thresholds(self) -> None:
        """Test that different threshold values produce correct offsets."""
        for threshold in [1, 5, 10, 100]:
            # >= threshold
            gte_meta = operator_metadata_for(grammar.GreaterThanEquals, threshold)
            assert gte_meta.lag_offsets == [threshold - 1]

            # > threshold
            gt_meta = operator_metadata_for(grammar.GreaterThan, threshold)
            assert gt_meta.lag_offsets == [threshold]

            # == and != use both
            eq_meta = operator_metadata_for(grammar.Equals, threshold)
            assert eq_meta.lag_offsets == [threshold - 1, threshold]

    def test_operator_metadata_unsupported_operator(self) -> None:
        """Test that unsupported operators raise ValueError."""
        # Use a dummy class that's not a real comparator
        class DummyComparator:
            pass

        with pytest.raises(ValueError, match="Unsupported comparator type"):
            operator_metadata_for(DummyComparator, 10)
