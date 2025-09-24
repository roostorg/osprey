import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, List, Self, Sequence, Tuple
from unittest.mock import Mock, patch

from osprey.engine.executor.custom_extracted_features import (
    ActionIdExtractedFeature,
    CustomExtractedFeature,
    ErrorCountExtractedFeature,
    SampleRateExtractedFeature,
    TimestampExtractedFeature,
)
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import (
    EffectBase,
    EffectToCustomExtractedFeatureBase,
)


def test_default_custom_extracted_features():
    action_id = ActionIdExtractedFeature(action_id=1)
    action_id_packed = 1

    timestamp = TimestampExtractedFeature(timestamp=datetime.datetime.fromisoformat('2025-04-16T00:00:00'))
    timestamp_packed = '2025-04-16T00:00:00'

    error_count = ErrorCountExtractedFeature(error_count=5)
    error_count_packed = 5

    sample_rate = SampleRateExtractedFeature(sample_rate=10)
    sample_rate_packed = 10
    sample_rate_full = SampleRateExtractedFeature(sample_rate=100)
    sample_rate_full_packed = None

    cases: Sequence[Tuple[CustomExtractedFeature, Any]] = [
        (action_id, action_id_packed),
        (timestamp, timestamp_packed),
        (error_count, error_count_packed),
        (sample_rate, sample_rate_packed),
        (sample_rate_full, sample_rate_full_packed),
    ]
    for case, packed in cases:
        assert case.get_serializable_feature() == packed


@dataclass
class TestEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    value: str

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        # isinstance check just helps with typing. it shouldnt be possible for it to fail cuz Self type
        return TestEffectsExtractedFeature(values=[value for value in values if isinstance(value, TestEffect)])


@dataclass
class TestEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    values: List[TestEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'test_effects'

    def get_serializable_feature(self) -> List[str] | None:
        return [value.value for value in self.values]


def test_effect_to_custom_extracted_feature_base():
    effect_1 = TestEffect(value='test_value_1')
    effect_2 = TestEffect(value='test_value_2')
    effect_3 = TestEffect(value='test_value_3')

    effects = [effect_1, effect_2, effect_3]

    custom_extracted_feature = TestEffect.build_custom_extracted_feature_from_list(effects)

    assert custom_extracted_feature.feature_name() == 'test_effects'
    assert custom_extracted_feature.get_serializable_feature() == ['test_value_1', 'test_value_2', 'test_value_3']


def test_execution_context_effect_processing():
    """Test the execution context's effect processing logic in get_extracted_features()"""

    mock_action = Mock()
    mock_action.data = {}
    mock_action.encoding = 'utf-8'

    with patch.object(ExecutionContext, '__init__', lambda self, *args, **kwargs: None):
        with patch.object(ExecutionContext, 'get_outputs', Mock(return_value={})):
            with patch.object(ExecutionContext, 'add_custom_extracted_feature', Mock()):
                context = ExecutionContext.__new__(ExecutionContext)
                context._effects = defaultdict(list)
                context._custom_extracted_features = {}
                context._outputs = {}

                effect_1 = TestEffect(value='test_value_1')
                effect_2 = TestEffect(value='test_value_2')
                effect_3 = TestEffect(value='test_value_3')

                context._effects[TestEffect] = [effect_1, effect_2, effect_3]

                context.get_extracted_features()

                assert context.add_custom_extracted_feature.call_count == 1

                call_args = context.add_custom_extracted_feature.call_args
                custom_extracted_feature = call_args[0][0]
                assert isinstance(custom_extracted_feature, TestEffectsExtractedFeature)
                assert custom_extracted_feature.get_serializable_feature() == [
                    'test_value_1',
                    'test_value_2',
                    'test_value_3',
                ]

                assert not call_args[1]['error_on_duplicate_key']


def test_execution_context_effect_processing_with_invalid_types():
    """Test the execution context's effect processing with invalid types in the effect list"""

    with patch.object(ExecutionContext, '__init__', lambda self, *args, **kwargs: None):
        with patch.object(ExecutionContext, 'get_outputs', Mock(return_value={})):
            with patch.object(ExecutionContext, 'add_custom_extracted_feature', Mock()):
                context = ExecutionContext.__new__(ExecutionContext)
                context._effects = defaultdict(list)
                context._custom_extracted_features = {}
                context._outputs = {}

                valid_effect_1 = TestEffect(value='valid_1')
                valid_effect_2 = TestEffect(value='valid_2')

                class InvalidEffect(EffectBase):
                    def __init__(self, value: str):
                        self.value = value

                invalid_effect = InvalidEffect(value='invalid')

                effect_list = [valid_effect_1, invalid_effect, valid_effect_2]
                # we mismatch the type here (this should never happen but just in case)
                context._effects[TestEffect] = effect_list

                with patch('osprey.engine.executor.execution_context.logger') as mock_logger:
                    context.get_extracted_features()

                    mock_logger.error.assert_called_once()
                    error_message = mock_logger.error.call_args[0][0]
                    assert 'TestEffect effect list contained an invalid type!' in error_message

                    context.add_custom_extracted_feature.assert_not_called()


def test_execution_context_effect_processing_with_non_extractable_feature_base():
    """Test that only effects that implement EffectToCustomExtractedFeatureBase are processed"""

    with patch.object(ExecutionContext, '__init__', lambda self, *args, **kwargs: None):
        with patch.object(ExecutionContext, 'get_outputs', Mock(return_value={})):
            with patch.object(ExecutionContext, 'add_custom_extracted_feature', Mock()):
                context = ExecutionContext.__new__(ExecutionContext)
                context._effects = defaultdict(list)
                context._custom_extracted_features = {}
                context._outputs = {}

                class RegularEffect(EffectBase):
                    def __init__(self, value: str):
                        self.value = value

                regular_effect = RegularEffect(value='regular')
                context._effects[RegularEffect] = [regular_effect]

                valid_effect = TestEffect(value='valid')
                context._effects[TestEffect] = [valid_effect]

                with patch('osprey.engine.executor.execution_context.logger') as mock_logger:
                    context.get_extracted_features()

                    assert context.add_custom_extracted_feature.call_count == 1

                    call_args = context.add_custom_extracted_feature.call_args
                    custom_extracted_feature = call_args[0][0]
                    assert isinstance(custom_extracted_feature, TestEffectsExtractedFeature)
                    assert custom_extracted_feature.get_serializable_feature() == ['valid']

                    mock_logger.error.assert_not_called()
