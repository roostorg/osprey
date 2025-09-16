from typing import Any, Dict, List, Mapping, Optional, Set, Union

from pydantic import HttpUrl, validator
from pydantic.main import BaseModel

from .._registry import register_config_subkey


class BulkLabelWebhookConfig(BaseModel):
    """Config related to bulk label notifications."""

    osprey_webhook_url: Optional[HttpUrl] = None
    min_bulk_size: int = 100
    """The minimum size of a bulk label to notify for."""


# TODO: OutgoingLabelsConfig should live outside of osprey_stdlib
#  webhooks and are not open-sourceable.
class OutgoingLabelsConfig(BaseModel):
    url: Union[str, List[str]] = ''
    message_content: str = ''


@register_config_subkey('webhooks')
class WebhookConfig(BaseModel):
    osprey_webhooks_url: str = ''
    outgoing_labels: Dict[str, Union[str, List[str], OutgoingLabelsConfig]] = {}
    """
    This target receives a POST request when the label changes on a given entity.
    This can be an `https://` endpoint.
    """

    outgoing_labels_features_to_include: Mapping[str, Set[str]] = {}
    bulk_label: BulkLabelWebhookConfig = BulkLabelWebhookConfig()

    @validator('outgoing_labels_features_to_include', pre=False)
    def validate_outgoing_labels_features_to_include(
        cls, outgoing_labels_features_to_include: Mapping[str, Set[str]], values: Mapping[str, Any]
    ) -> Mapping[str, Set[str]]:
        outgoing_labels = values.get('outgoing_labels', {})
        outgoing_labels_set = set(outgoing_labels.keys())

        for label_name in outgoing_labels_features_to_include.keys():
            if label_name not in outgoing_labels_set:
                raise ValueError(f'`{label_name}` is not defined in the `outgoing_labels` webhook config')

        return outgoing_labels_features_to_include
