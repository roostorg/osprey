from random import random

from pydantic import BaseModel, validator

from .._registry import register_config_subkey

FEATURE_FLAGS_CONFIG_SUBKEY = 'feature_flags'


### Feature Flags ###
WEBHOOKS_USE_PUBSUB = 'WEBHOOKS_USE_PUBSUB'


class PercentageFlagInfo(BaseModel):
    value: float
    description: str = ''

    @validator('value')
    def check_float_value(cls, value: float) -> float:
        if value < 0 or value > 1.0:
            raise ValueError('flag value must be a float between 0.0 and 1.0')
        return value


@register_config_subkey(FEATURE_FLAGS_CONFIG_SUBKEY)
class FeatureFlagsConfig(BaseModel):
    percentage_flags: dict[str, PercentageFlagInfo] = {}

    def is_percentage_enabled(self, flag: str) -> bool:
        if flag not in self.percentage_flags:
            return False

        threshold = self.percentage_flags[flag].value
        value = random()

        return bool(value < threshold)
