from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional, Sequence, Union

import requests
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.labels import LabelState, LabelStatus
from osprey.worker.lib.osprey_shared.logging import get_logger
from pydantic import BaseModel
from typing_extensions import Protocol

logger = get_logger(__name__)


class EntityLabelUpdateNotification(BaseModel):
    entity_type: str
    entity_id: str
    label_name: str
    label_state: LabelState
    expires_at: Optional[datetime]
    time: datetime
    features: Dict[str, Union[int, str, float, bool, None]] = {}

    @property
    def is_label_remove(self) -> bool:
        return self.label_state.status.value in (
            LabelStatus.REMOVED,
            LabelStatus.MANUALLY_REMOVED,
        )

    @property
    def is_label_addition(self) -> bool:
        return self.label_state.status.value in (LabelStatus.ADDED, LabelStatus.MANUALLY_ADDED)

    @property
    def is_manual_mutation(self) -> bool:
        return self.label_state.status.value in (
            LabelStatus.MANUALLY_ADDED,
            LabelStatus.MANUALLY_REMOVED,
        )

    def formatted_reasons(self) -> str:
        return ', '.join(sorted(self.label_state.reasons.keys()))


_HandlerCallback = Callable[[EntityLabelUpdateNotification], None]


class _HandlerDispatch(Protocol):
    def __init__(self, callback: _HandlerCallback, labels: Sequence[str]):
        pass

    def dispatch(self, payload: EntityLabelUpdateNotification) -> None:
        pass


STATS_ROOT: str = 'osprey_shared.webhooks'


def tags_from_dict(tag_dict: Dict[str, str]) -> List[str]:
    return [f'{k}:{v}' for k, v in tag_dict.items()]


class _InstrumentedDispatch(_HandlerDispatch):
    def __init__(self, callback: _HandlerCallback, labels: Sequence[str]):
        self._callback = callback
        self._callback_name = getattr(callback, '__name__', 'Unknown')
        self._labels = labels

    def dispatch(self, payload: EntityLabelUpdateNotification) -> None:
        tags = tags_from_dict(
            {
                'label_name': payload.label_name,
                'entity_type': payload.entity_type,
                'callback_name': self._callback_name,
            }
        )
        if not self._labels or payload.label_name in self._labels:
            metrics.increment(f'{STATS_ROOT}.attempt', tags=tags)
            try:
                self._callback(payload)
            except Exception as e:
                logger.error(
                    f'Error processing webhook callback for {self._callback_name} for label operation '
                    f'{payload.label_state} {payload.label_name} on {payload.entity_type} {payload.entity_id}: {e}',
                    exc_info=True,
                )
                metrics.histogram(f'{STATS_ROOT}.availability', value=0, tags=tags)
                metrics.increment(f'{STATS_ROOT}.failure', tags=tags)
                raise e
            else:
                metrics.histogram(f'{STATS_ROOT}.availability', value=1, tags=tags)
                metrics.increment(f'{STATS_ROOT}.success', tags=tags)
        else:
            metrics.increment(f'{STATS_ROOT}.skip', tags=tags)


class OspreyCallbackRunWhen(str, Enum):
    INTERVENTIONS_CLIENT_ENABLED = 'INTERVENTIONS_CLIENT_ENABLED'
    INTERVENTIONS_CLIENT_DISABLED = 'INTERVENTIONS_CLIENT_DISABLED'
    ALWAYS = 'ALWAYS'


class OspreyWebhookRouter:
    def __init__(self, is_interventions_client_enabled: Callable[[], bool]) -> None:
        # List of legacy osprey callback handlers, pre interventions client
        self._handlers_by_entity_type: Dict[str, List[_HandlerDispatch]] = defaultdict(list)
        # Interventions client handlers, that will be used when we turn the interventions feature flag on
        # After migrating to the intervention client handlers, we go back to only a single list of handlers
        # TODO: After Feature flag dialup, cleanup extra handler list
        # https://app.asana.com/0/1202424124203663/1203913333388469/f
        self._interventions_client_handlers_by_entity_type: Dict[str, List[_HandlerDispatch]] = defaultdict(list)
        self._is_interventions_client_enabled = is_interventions_client_enabled

    def register(
        self,
        entity_type: str,
        labels: Sequence[str] = tuple(),
        run_when: OspreyCallbackRunWhen = OspreyCallbackRunWhen.ALWAYS,
    ) -> Callable[[_HandlerCallback], _HandlerCallback]:
        """Registers a handler to be called when a Osprey webhook is received.

        Can give a set of labels to limit to, or if no labels are given the handler is invoked for all webhooks for
        the given entity type.
        """

        def decorator(handler: _HandlerCallback) -> _HandlerCallback:
            dispatcher = _InstrumentedDispatch(handler, labels)

            if run_when == OspreyCallbackRunWhen.INTERVENTIONS_CLIENT_ENABLED:
                self._interventions_client_handlers_by_entity_type[entity_type].append(dispatcher)
            elif run_when == OspreyCallbackRunWhen.ALWAYS:
                # Some handlers will not be affected by the cut between interventions client
                # These handlers will be added to both maps so they get run regardless of which mode we are in
                self._interventions_client_handlers_by_entity_type[entity_type].append(dispatcher)
                self._handlers_by_entity_type[entity_type].append(dispatcher)
            else:
                self._handlers_by_entity_type[entity_type].append(dispatcher)

            return handler

        return decorator

    def call_handlers(self, payload: EntityLabelUpdateNotification) -> None:
        """Calls the handlers for a given incoming webhook."""

        if self._is_interventions_client_enabled():
            for handler in self._interventions_client_handlers_by_entity_type.get(payload.entity_type, []):
                handler.dispatch(payload)
        else:
            for handler in self._handlers_by_entity_type.get(payload.entity_type, []):
                handler.dispatch(payload)


def get_osprey_public_keys_by_id(endpoint: str) -> Dict[str, str]:
    response = requests.get(endpoint + 'keys')
    response.raise_for_status()
    return response.json()
