from typing import Any

import typing_inspect
from flask import Blueprint
from osprey.engine.stdlib.configs.labels_config import LabelInfo, LabelsConfig
from osprey.engine.udf.type_helpers import to_display_str
from osprey.worker.lib.osprey_engine import FeatureLocation, OspreyEngine
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.lib.sources_config.subkeys.ui_config import UIConfig
from pydantic import BaseModel

from ..lib.auth import get_current_user_email

blueprint = Blueprint('config', __name__)


class User(BaseModel):
    email: str


class UIConfigMerged(UIConfig):
    """Contains everything in the base ui config, as well as additional fields that are relevant
    to configuring the UI"""

    feature_name_to_entity_type_mapping: dict[str, str]
    feature_name_to_value_type_mapping: dict[str, str]
    label_info_mapping: dict[str, LabelInfo]
    known_feature_locations: list[FeatureLocation]
    known_action_names: set[str]
    current_user: User
    rule_info_mapping: dict[str, str]


_SIMPLE_TYPE_CONVERTIBLE = {str, int, bool, float}


def _serialize_type_for_ui(t: type) -> str:
    """Converts a Python type into a string representation the UI knows how to interpret.

    NOTE: If you change this logic, make sure to update the `renderValueWithType` function in the UI!
    """
    if t in _SIMPLE_TYPE_CONVERTIBLE:
        return t.__name__

    # `Optional[...]` is a `Union` with `None` at runtime.
    if typing_inspect.is_union_type(t):
        args = typing_inspect.get_args(t)
        non_none_args = [a for a in args if a is not type(None)]  # noqa: E721
        if len(non_none_args) == 1:
            (inner,) = non_none_args
            return f'{_serialize_type_for_ui(inner)}?'

    if typing_inspect.get_origin(t) is list:
        args = typing_inspect.get_args(t)
        assert len(args) == 1, 'BUG: List should only ever have one arg'
        (inner,) = args
        return f'list<{_serialize_type_for_ui(inner)}>'

    raise AssertionError(f"Don't know how to serialize {to_display_str(t)}")


def _get_feature_name_to_value_type_mapping(engine: OspreyEngine) -> dict[str, str]:
    return {
        name: _serialize_type_for_ui(t)
        for name, t in engine.get_post_execution_feature_name_to_value_type_mapping().items()
    }


@blueprint.route('/config', methods=['GET'])
def get_config() -> Any:
    engine = ENGINE.instance()
    ui_config = engine.get_config_subkey(UIConfig)
    return UIConfigMerged(
        default_summary_features=ui_config.default_summary_features,
        external_links=ui_config.external_links,
        feature_name_to_entity_type_mapping=engine.get_feature_name_to_entity_type_mapping(),
        feature_name_to_value_type_mapping=_get_feature_name_to_value_type_mapping(engine),
        label_info_mapping=engine.get_config_subkey(LabelsConfig).labels,
        known_feature_locations=engine.get_known_feature_locations(),
        known_action_names=engine.get_known_action_names(),
        current_user=User(email=get_current_user_email()),
        rule_info_mapping=engine.get_rule_to_info_mapping(),
    )
