from osprey.engine.stdlib.configs.analytics_config import AnalyticsConfig


def test_filtered_labels_for_analytics() -> None:
    labels = ['label_1', 'label_2']
    config = AnalyticsConfig.parse_obj(
        {'filtered_labels': labels},
    )
    assert config.filtered_labels == set(labels)


def test_filtered_labels_for_analytics_format_error() -> None:
    config = AnalyticsConfig.parse_obj(
        {'wrong_field': ['label_1', 'label_2']},
    )
    assert config.filtered_labels == set()
