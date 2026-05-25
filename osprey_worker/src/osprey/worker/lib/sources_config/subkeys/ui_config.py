from osprey.worker.lib.sources_config import register_config_subkey
from pydantic import BaseModel


class FeatureSummaryConfig(BaseModel):
    actions: list[str] = []
    features: list[str] = []


@register_config_subkey('ui_config')
class UIConfig(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    default_summary_features: list[FeatureSummaryConfig] = []
    external_links: dict[str, str] = {}
