import os
from typing import Any, Dict

import requests
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from udfs.ban_nostr_event import BanEventEffect

logger = get_logger(__name__)

DEFAULT_RELAY_MANAGER_URL = 'http://relay-manager.default.svc:5000'


class RelayManagerSink(BaseOutputSink):
    """Output sink that consumes BanEventEffect and POSTs to the
    relay-manager NIP-86 banevent endpoint.

    Requires the ``DIVINE_RELAY_MANAGER_URL`` environment variable to be set
    to the relay-manager base URL (e.g. ``http://relay-manager.default.svc:5000``).
    If unset, falls back to DEFAULT_RELAY_MANAGER_URL which assumes in-cluster
    Kubernetes service DNS.
    """

    timeout: float = 5.0
    max_retries: int = 2

    def __init__(self, relay_manager_url: str | None = None) -> None:
        self._url = relay_manager_url or os.environ.get('DIVINE_RELAY_MANAGER_URL', DEFAULT_RELAY_MANAGER_URL)

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(result.effects.get(BanEventEffect, [])) > 0

    def push(self, result: ExecutionResult) -> None:
        effects = result.effects.get(BanEventEffect, [])
        for effect in effects:
            assert isinstance(effect, BanEventEffect)
            payload: Dict[str, Any] = {
                'method': 'banevent',
                'params': [effect.event_id, effect.reason],
            }
            try:
                resp = requests.post(
                    f'{self._url}/api/relay-rpc',
                    json=payload,
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                logger.info(f'Banned event {effect.event_id} via relay-manager')
            except Exception:
                logger.exception(f'Failed to ban event {effect.event_id} via relay-manager')
                raise

    def stop(self) -> None:
        pass
