from typing import Any, Dict, Optional

from osprey.worker.lib.sources_config import register_config_subkey
from pydantic import BaseModel, Field, root_validator


class ActionConfig(BaseModel):
    sample_rate: int = Field(ge=0, le=100, default=100)


@register_config_subkey('actions')
class ActionConfigs(BaseModel):
    actions: Dict[str, ActionConfig]

    @root_validator(pre=True)
    def root_validator(cls, values: Any) -> Dict[str, Any]:
        return {'actions': values}

    def get_action_config(self, action_name: str) -> Optional[ActionConfig]:
        return self.actions.get(action_name)
