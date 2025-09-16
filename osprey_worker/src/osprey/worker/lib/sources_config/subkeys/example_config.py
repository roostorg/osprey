from osprey.worker.lib.sources_config import register_config_subkey
from pydantic import BaseModel


@register_config_subkey('example')
class ExampleConfig(BaseModel):
    some_str: str = 'abc'
    some_int: int = 3
