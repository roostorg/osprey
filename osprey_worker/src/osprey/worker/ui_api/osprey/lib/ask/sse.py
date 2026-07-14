"""Versioned Server-Sent Events serialization for Ask events."""

from __future__ import annotations

import json

from osprey.worker.lib.ask import AskEvent
from osprey.worker.lib.ask.errors import SerializationError


def format_sse(event: AskEvent) -> str:
    """Serialize an :class:`AskEvent` to a single SSE frame.

    The event name appears on the ``event:`` line and again as ``type`` inside the
    JSON ``data`` payload, alongside the schema ``version``. A non-serializable payload
    raises :class:`SerializationError` so the caller can emit a terminal error frame
    (whose payload -- code + message strings -- is always serializable).
    """
    data = {
        'version': event.version,
        'type': event.type,
        'conversation_id': event.conversation_id,
        'turn_id': event.turn_id,
        'payload': event.payload,
    }
    try:
        body = json.dumps(data)
    except (TypeError, ValueError) as exc:
        raise SerializationError() from exc
    return f'event: {event.type}\ndata: {body}\n\n'
