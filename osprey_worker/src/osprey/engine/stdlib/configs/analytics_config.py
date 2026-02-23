from pydantic import BaseModel

from .._registry import register_config_subkey

ANALYTICS_CONFIG_SUBKEY = 'analytics'


class CounterInfo(BaseModel):
    threshold: int


@register_config_subkey(ANALYTICS_CONFIG_SUBKEY)
class AnalyticsConfig(BaseModel):
    """
    hold the `analytics` config.

    Validates that the config is valid on being loaded

    Currently validation is done in osprey_ast_validator because we need access to `labels` config.
    """

    filtered_labels: set[str] = set()
    monitored_labels: set[str] = set()
    monitored_counters: dict[str, CounterInfo] = dict()
