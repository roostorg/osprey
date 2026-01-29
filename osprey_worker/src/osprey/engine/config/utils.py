from typing import Any, Type, Union

from osprey.engine.ast.sources import SourcesConfig
from pydantic.fields import SHAPE_LIST, SHAPE_SET
from pydantic.main import BaseModel


def parse_config_with_auto_default(
    sources_config: SourcesConfig, subkey_name: str, model: Type[BaseModel]
) -> BaseModel:
    # Since our configs are based on YAML, each config object can be a mapping-like object
    # or a list-like object. Therefore, when the config is empty
    # or missing, the default value should match the model's expected shape.
    default_config_obj: Union[dict[str, Any], list[Any]] = {}
    if model.__custom_root_type__ and model.__fields__['__root__'].shape in [SHAPE_LIST, SHAPE_SET]:
        default_config_obj = []

    return model.parse_obj(sources_config.get(subkey_name, default_config_obj))
