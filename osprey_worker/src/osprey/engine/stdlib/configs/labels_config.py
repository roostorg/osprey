from enum import Enum
from typing import Dict, List

from pydantic import BaseModel, root_validator

from .._registry import register_config_subkey


# If you change this also change osprey_worker/src/osprey/worker/shared/labels.py
class LabelConnotation(Enum):
    POSITIVE = 'positive'
    NEGATIVE = 'negative'
    NEUTRAL = 'neutral'

    def get_color_code(self) -> int:
        """Given a label connotation, return a color code that may be used to stylize places where we
        display a label that has the given connotation."""

        if self == LabelConnotation.POSITIVE:
            return 0x3E7025
        elif self == LabelConnotation.NEGATIVE:
            return 0xF5DEDC
        else:
            return 0xDBDCDF


class LabelInfo(BaseModel):
    valid_for: List[str] = []
    connotation: LabelConnotation = LabelConnotation.NEUTRAL
    description: str = ''


LABELS_CONFIG_SUBKEY = 'labels'


@register_config_subkey(LABELS_CONFIG_SUBKEY)
class LabelsConfig(BaseModel):
    labels: Dict[str, LabelInfo]

    @root_validator(pre=True)
    def root_validator(cls, values: object) -> Dict[str, object]:
        # Remove old values so we can be backward compatible during the deploy
        # TODO - Remove this once the new config handling is fully deployed
        assert isinstance(values, dict)
        values.pop('positive', None)
        values.pop('negative', None)
        return {'labels': values}
