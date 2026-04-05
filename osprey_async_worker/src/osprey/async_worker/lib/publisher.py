"""Async Pub/Sub publisher with batching for the async worker.

Uses asyncio.Queue for buffering and a background task for periodic
flushing. No threading locks, no gevent, no background threads.
"""

import asyncio
import logging
from typing import List, Optional

from google.cloud import pubsub_v1
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AsyncPubSubPublisher:
    """Publishes Pydantic models to a Pub/Sub topic with async batching.

    Messages are buffered in an asyncio.Queue and flushed either when
    the batch reaches max_messages or after max_latency_seconds.
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        max_messages: int = 250,
        max_latency_seconds: float = 1.0,
    ):
        self._topic_path = f'projects/{project_id}/topics/{topic_id}'
        self._client = pubsub_v1.PublisherClient(
            batch_settings=pubsub_v1.types.BatchSettings(max_messages=1),
        )
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._max_messages = max_messages
        self._max_latency = max_latency_seconds
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._started = False

    def _ensure_started(self) -> None:
        """Start the background flush task on first publish."""
        if not self._started:
            self._started = True
            try:
                loop = asyncio.get_running_loop()
                self._flush_task = loop.create_task(self._flush_loop())
            except RuntimeError:
                pass  # No event loop — sync mode, flush immediately

    async def _flush_loop(self) -> None:
        """Background task that flushes the buffer periodically."""
        while True:
            try:
                batch: List[bytes] = []

                # Wait for first message or timeout
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=self._max_latency)
                    batch.append(msg)
                except TimeoutError:
                    continue

                # Drain up to max_messages
                while len(batch) < self._max_messages:
                    try:
                        msg = self._queue.get_nowait()
                        batch.append(msg)
                    except asyncio.QueueEmpty:
                        break

                if batch:
                    await self._flush_batch(batch)

            except asyncio.CancelledError:
                # Flush remaining on shutdown
                remaining: List[bytes] = []
                while not self._queue.empty():
                    try:
                        remaining.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if remaining:
                    await self._flush_batch(remaining)
                return
            except Exception:
                logger.exception('Error in flush loop')

    async def _flush_batch(self, batch: List[bytes]) -> None:
        """Publish a batch of messages. Runs sync publishes in executor."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_flush, batch)

    def _sync_flush(self, batch: List[bytes]) -> None:
        """Synchronous batch publish."""
        futures = []
        for data in batch:
            futures.append(self._client.publish(self._topic_path, data))
        for future in futures:
            try:
                future.result(timeout=5)
            except Exception:
                logger.exception('Failed to publish message')

    def publish(self, data: BaseModel) -> None:
        """Queue a Pydantic model for async batched publishing."""
        self.publish_bytes(data.json(exclude_none=True).encode())

    def publish_bytes(self, data: bytes) -> None:
        """Queue raw bytes for async batched publishing."""
        self._ensure_started()
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning('Publisher queue full, dropping message')

    async def stop(self) -> None:
        """Flush remaining messages and stop."""
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
