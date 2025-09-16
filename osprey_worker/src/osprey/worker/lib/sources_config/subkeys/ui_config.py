from typing import Dict, List

from osprey.worker.lib.sources_config import register_config_subkey
from pydantic import BaseModel


class FeatureSummaryConfig(BaseModel):
    actions: List[str] = []
    features: List[str] = []


@register_config_subkey('ui_config')
class UIConfig(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    default_summary_features: List[FeatureSummaryConfig] = []
    external_links: Dict[str, str] = {}
