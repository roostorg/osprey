from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, TypeVar, Union

SerializableT = TypeVar(
    'SerializableT', bound=Union[str, int, float, bool, None, list[Any], dict[str, Any]], covariant=True
)


class CustomExtractedFeature(ABC, Generic[SerializableT]):
    """
    An abstract class used to define custom extracted features, which can be serialized to JSON.

    Custom extracted features are used to add additional information to the `ExecutionResult.extracted_features` dict.
    They are defined by implementing the `feature_name` and `get_serializable_feature` methods.

    When added to the `ExecutionResult.extracted_features`, the `feature_name` will have a double underscore appended to it.
    This is to avoid conflicts with base extracted features (variables).
    """

    @classmethod
    @abstractmethod
    def feature_name(cls) -> str:
        """
        **THIS METHOD SHOULD RETURN A STATIC STRING**

        The name of this custom extracted feature.

        Custom extracted feature names will start with a double underscore (e.g. a feature named
        'verdicts' will have a key of '__verdicts' in the `ExecutionResult.extracted_features` dict)
        """
        ...

    @abstractmethod
    def get_serializable_feature(self) -> SerializableT | None:
        """
        This function should return a value that can be converted to JSON.
        It will be the value for this feature in the `ExecutionResult.extracted_features` dict.

        If the returned value is None, the K/V pair will not be included at all.
        """
        ...


# !! default custom extracted features !!


@dataclass
class ActionIdExtractedFeature(CustomExtractedFeature[int]):
    action_id: int

    @classmethod
    def feature_name(cls) -> str:
        return 'action_id'

    def get_serializable_feature(self) -> int | None:
        return self.action_id


@dataclass
class TimestampExtractedFeature(CustomExtractedFeature[str]):
    timestamp: datetime

    @classmethod
    def feature_name(cls) -> str:
        return 'timestamp'

    def get_serializable_feature(self) -> str | None:
        return self.timestamp.isoformat()


@dataclass
class ErrorCountExtractedFeature(CustomExtractedFeature[int]):
    error_count: int

    @classmethod
    def feature_name(cls) -> str:
        return 'error_count'

    def get_serializable_feature(self) -> int | None:
        return self.error_count


@dataclass
class SampleRateExtractedFeature(CustomExtractedFeature[int]):
    sample_rate: int

    @classmethod
    def feature_name(cls) -> str:
        return 'sample_rate'

    def get_serializable_feature(self) -> int | None:
        if self.sample_rate >= 100:
            return None

        return self.sample_rate
