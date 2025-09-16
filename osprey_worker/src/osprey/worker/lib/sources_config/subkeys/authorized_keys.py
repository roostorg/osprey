from osprey.worker.lib.sources_config import register_config_subkey
from pydantic import BaseModel, Extra


@register_config_subkey('authorized_keys')
class AuthorizedKeys(BaseModel):
    """Dummy class to allow the top-level 'authorized_keys' config"""

    class Config:
        extra = Extra.ignore
