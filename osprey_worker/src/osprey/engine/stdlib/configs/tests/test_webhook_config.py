import pytest
from osprey.engine.stdlib.configs.webhook_config import (
    WebhookConfig,
)


def test_outgoing_labels_features_to_include_golden_path() -> None:
    WebhookConfig.parse_obj(
        {
            'outgoing_labels': {'some_label': 'url://'},
            'outgoing_labels_features_to_include': {'some_label': ['SomeFeature']},
        },
    )


def test_outgoing_labels_features_to_include_error() -> None:
    with pytest.raises(ValueError) as e:
        WebhookConfig.parse_obj(
            {
                'outgoing_labels': {'some_label': 'url://'},
                'outgoing_labels_features_to_include': {'some_label_that_doesnt_exist': ['SomeFeature']},
            },
        )
    assert e.match('`some_label_that_doesnt_exist` is not defined in the `outgoing_labels` webhook config')
