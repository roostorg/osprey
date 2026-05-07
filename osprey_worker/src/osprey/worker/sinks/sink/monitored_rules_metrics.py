"""Utility for emitting monitored_rules metrics after label mutations."""

from typing import Sequence, Set, Tuple

from osprey.worker.lib.instruments import metrics


def emit_monitored_rules_metrics(
    mutations: Sequence[Tuple[str, str, str]],
    monitored_labels: Set[str],
    action_name: str,
) -> None:
    """Emit 'monitored_rules' metric for mutations on monitored labels.

    Should be called after label mutations are applied and at least one
    label changed (added or removed). Each tuple is (label_name, reason_name, status_tag).

    Args:
        mutations: Sequence of (label_name, reason_name, status_tag) tuples.
        monitored_labels: Label names to emit metrics for (from AnalyticsConfig).
        action_name: The action name for the metric tag.
    """
    for label_name, reason_name, status_tag in mutations:
        if label_name in monitored_labels:
            metrics.increment(
                'monitored_rules',
                tags=[
                    f'rule:{reason_name}',
                    f'label:{label_name}',
                    f'status:{status_tag}',
                    f'action:{action_name}',
                ],
            )
