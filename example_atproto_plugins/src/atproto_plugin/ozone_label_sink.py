"""Forward benign Osprey labels to a Bluesky (Ozone) labeler.

This is the same seam production ATProto deployments use: `register_label_output_sink`
lets a plugin replace the default label sink. This one keeps the normal internal
labeling AND forwards a small allow-list of benign labels (bleep/bloop) to the
workshop's Ozone labeler, so a rule firing on a live post applies a real,
visible label on Bluesky.

Opt-in: only active when `OZONE_RELAY_URL` is set. It posts to the workshop
relay (which holds the labeler credentials and does the Ozone `emitEvent`), so
no Ozone secrets live in Osprey. Only posts get labeled (strong ref by at-uri),
never accounts, and only labels in the allow-list are ever forwarded.
"""

from __future__ import annotations

import requests
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.engine.language_types.labels import LabelEffect
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.labels import LabelsProvider
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, LabelOutputSink

logger = get_logger(__name__)

# Only these benign, meaningless labels are ever forwarded to the real labeler.
FORWARDED_LABELS = frozenset({'bleep', 'bloop'})
_POST_COLLECTION = 'app.bsky.feed.post'


class OzoneLabelOutputSink(BaseOutputSink):
    """Wraps the default label sink and additionally forwards allow-listed
    labels on created posts to the Ozone relay."""

    def __init__(
        self,
        labels_provider: LabelsProvider,
        relay_url: str,
        relay_token: str | None = None,
        forwarded_labels: frozenset[str] = FORWARDED_LABELS,
    ) -> None:
        self._inner = LabelOutputSink(labels_provider)
        self._relay_url = relay_url.rstrip('/')
        self._forwarded = forwarded_labels
        self._headers = {'Authorization': f'Bearer {relay_token}'} if relay_token else {}

    def set_monitored_labels(self, monitored_labels: set[str]) -> None:
        self._inner.set_monitored_labels(monitored_labels)

    def will_do_work(self, result: ExecutionResult) -> bool:
        return self._inner.will_do_work(result)

    def push(self, result: ExecutionResult) -> None:
        # Internal labeling is unchanged; forwarding is purely additive.
        self._inner.push(result)

        labels = {
            effect.name
            for effect in result.effects.get(LabelEffect, [])
            if isinstance(effect, LabelEffect) and not effect.suppressed and effect.name in self._forwarded
        }
        if not labels:
            return

        uri = self._post_uri(result)
        if uri is None:
            # Not a created post (e.g. a like/follow); nothing to label.
            return

        for val in labels:
            self._forward_label(uri, val)

    def _post_uri(self, result: ExecutionResult) -> str | None:
        features = result.extracted_features
        did = features.get('UserId')
        collection = features.get('Collection')
        rkey = features.get('Rkey')
        if not did or collection != _POST_COLLECTION or not rkey:
            return None
        return f'at://{did}/{collection}/{rkey}'

    def _forward_label(self, uri: str, val: str) -> None:
        try:
            resp = requests.post(
                f'{self._relay_url}/label',
                json={'item': {'id': uri}, 'custom': {'labelVal': val}, 'actorEmail': 'osprey-rule'},
                headers=self._headers,
                timeout=5,
            )
            if resp.status_code != 200:
                logger.warning('ozone relay rejected label %s on %s: %s', val, uri, resp.text[:200])
        except requests.RequestException as exc:
            # Best-effort: a labeler hiccup must never break event processing.
            logger.warning('ozone relay unreachable for label %s on %s: %s', val, uri, exc)

    def stop(self) -> None:
        self._inner.stop()
