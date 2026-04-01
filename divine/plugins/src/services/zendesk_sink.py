import os
from typing import Any, Dict

import requests
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink

logger = get_logger(__name__)

# Verdicts that should create Zendesk tickets for human follow-up.
# Auto-bans (ban, auto_hide) don't need tickets -- they're already enforced.
# Safe results definitely don't need tickets.
TICKETABLE_VERDICTS = {'flag_for_review', 'restrict'}

# Verdicts that should resolve existing open tickets.
RESOLUTION_VERDICTS = {'ban', 'ban_user', 'approve', 'dismiss'}


class ZendeskSink(BaseOutputSink):
    """Output sink that creates or resolves Zendesk tickets from Osprey verdicts.

    Only creates tickets for verdicts requiring human follow-up
    (flag_for_review, restrict). Resolves open tickets when a verdict
    closes the issue (ban, approve, dismiss).

    Configuration (environment variables):
      - ``DIVINE_ZENDESK_URL``: Zendesk subdomain URL
        (e.g. ``https://rabblelabs.zendesk.com``). Required.
      - ``DIVINE_ZENDESK_EMAIL``: Agent email for API auth. Required.
      - ``DIVINE_ZENDESK_TOKEN``: API token for auth. Required.
      - ``DIVINE_ZENDESK_ENABLED``: Set to ``true`` to enable API calls.
        Defaults to ``false`` (dry-run mode, logs only).

    Auth uses Zendesk's email/token scheme:
      ``{email}/token:{api_token}`` as HTTP Basic credentials.
    """

    timeout: float = 5.0

    def __init__(self) -> None:
        self._url = os.environ.get('DIVINE_ZENDESK_URL', '')
        self._token = os.environ.get('DIVINE_ZENDESK_TOKEN', '')
        self._email = os.environ.get('DIVINE_ZENDESK_EMAIL', '')
        self._enabled = os.environ.get('DIVINE_ZENDESK_ENABLED', 'false').lower() == 'true'

        if not self._url:
            logger.info('DIVINE_ZENDESK_URL not set. ZendeskSink disabled.')
        elif not self._enabled:
            logger.info('ZendeskSink in dry-run mode (set DIVINE_ZENDESK_ENABLED=true to activate).')

    def _auth(self) -> tuple[str, str]:
        return (f'{self._email}/token', self._token)

    def will_do_work(self, result: ExecutionResult) -> bool:
        if not self._url:
            return False
        for verdict in result.verdicts:
            v = verdict.verdict.lower()
            if v in TICKETABLE_VERDICTS or v in RESOLUTION_VERDICTS:
                return True
        return False

    def push(self, result: ExecutionResult) -> None:
        for verdict in result.verdicts:
            v = verdict.verdict.lower()
            if v in TICKETABLE_VERDICTS:
                self._create_ticket(v, result)
            elif v in RESOLUTION_VERDICTS:
                self._log_resolution(v, result)

    def _create_ticket(self, verdict: str, result: ExecutionResult) -> None:
        """Create a Zendesk Problem ticket for content needing human review."""
        features = result.extracted_features_json or '{}'
        action_name = result.action.action_name if result.action else 'unknown'

        ticket_data: Dict[str, Any] = {
            'ticket': {
                'subject': f'Osprey: {verdict} - content flagged for review',
                'comment': {
                    'body': (f'Automated moderation verdict: {verdict}\n\nAction: {action_name}\nFeatures: {features}'),
                },
                'type': 'problem',
                'priority': 'high' if verdict == 'restrict' else 'normal',
                'tags': ['divine-moderation', 'osprey', f'verdict-{verdict}'],
            }
        }

        if not self._enabled:
            logger.info(f'[dry-run] Would create ticket: verdict={verdict} action={action_name}')
            return

        try:
            resp = requests.post(
                f'{self._url}/api/v2/tickets.json',
                json=ticket_data,
                auth=self._auth(),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            ticket_id = resp.json().get('ticket', {}).get('id')
            logger.info(f'Created Zendesk ticket #{ticket_id} for verdict={verdict}')
        except Exception:
            # Zendesk ticket creation is non-critical. Log and continue.
            logger.exception(f'Failed to create Zendesk ticket for verdict={verdict}')

    def _log_resolution(self, verdict: str, result: ExecutionResult) -> None:
        """Log resolution verdicts. Resolving existing tickets requires
        searching by event ID, which needs the Zendesk search API and
        a tag/field convention for linking tickets to Nostr events.
        Not implemented yet -- would need to match the relay-manager
        pattern (zendesk_tickets D1 table maps event_id to ticket_id).
        """
        action_name = result.action.action_name if result.action else 'unknown'
        logger.info(f'Resolution verdict: {verdict} action={action_name} (ticket resolution not yet implemented)')

    def stop(self) -> None:
        pass
