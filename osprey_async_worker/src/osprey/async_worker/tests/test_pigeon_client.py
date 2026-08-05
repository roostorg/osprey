"""Tests for the async pigeon client."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import grpc
import pytest
from osprey.async_worker.lib.pigeon import client as pigeon_client
from osprey.async_worker.lib.pigeon.client import AsyncUnaryUnaryRpcCallable
from osprey.async_worker.lib.pigeon.exceptions import RPCException
from osprey.async_worker.lib.pigeon.skip_rate_limit import skip_rate_limit_context
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.instruments import _DogStatsd

# --- skip_rate_limit contextvars ---


def test_skip_rate_limit_default_false():
    assert skip_rate_limit_context.skip is False


def test_skip_rate_limit_set_and_get():
    skip_rate_limit_context.skip = True
    assert skip_rate_limit_context.skip is True
    skip_rate_limit_context.skip = False
    assert skip_rate_limit_context.skip is False


def test_skip_rate_limit_property_api():
    """Uses .skip property, matching the gevent.local API."""
    skip_rate_limit_context.skip = True
    assert skip_rate_limit_context.skip is True
    skip_rate_limit_context.skip = False


# --- RoutingType constants ---


def test_routing_type_values():
    from osprey.async_worker.lib.pigeon.client import RoutingType

    assert RoutingType.CHUNKED == 1
    assert RoutingType.SCALAR == 2
    assert RoutingType.ROUND_ROBIN == 3
    assert RoutingType.ENVOY == 4
    assert len(RoutingType.ALL) == 4


# --- GRPC HTTP code translation ---


def test_grpc_http_translations():
    from osprey.async_worker.lib.pigeon.client import _GRPC_HTTP_CODE_TRANSLATIONS

    assert _GRPC_HTTP_CODE_TRANSLATIONS[grpc.StatusCode.OK] == 200
    assert _GRPC_HTTP_CODE_TRANSLATIONS[grpc.StatusCode.NOT_FOUND] == 404
    assert _GRPC_HTTP_CODE_TRANSLATIONS[grpc.StatusCode.INTERNAL] == 500
    assert _GRPC_HTTP_CODE_TRANSLATIONS[grpc.StatusCode.UNAVAILABLE] == 503
    assert _GRPC_HTTP_CODE_TRANSLATIONS[grpc.StatusCode.DEADLINE_EXCEEDED] == 504


# --- RetryPolicy ---


def test_retry_policy_type():
    from osprey.async_worker.lib.pigeon.client import RetryPolicy

    policy: RetryPolicy = {
        'retryable_grpc_status_codes': {grpc.StatusCode.UNAVAILABLE},
        'max_secondaries_to_retry': 2,
    }
    assert grpc.StatusCode.UNAVAILABLE in policy['retryable_grpc_status_codes']
    assert policy['max_secondaries_to_retry'] == 2


@pytest.mark.asyncio
async def test_retries_record_bounded_attempt_elapsed_and_sleep_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Removing any per-retry metric must expose unobserved retry amplification."""
    inner_error = MagicMock()
    inner_error.code.return_value = grpc.StatusCode.UNAVAILABLE
    inner_error.details.return_value = 'temporarily unavailable'
    retryable_error = RPCException('profile-service', 'GetProfile', inner_error)
    request = AsyncMock(side_effect=[retryable_error, retryable_error, 'response'])
    sleep = AsyncMock()
    mock_metrics = MagicMock()
    monotonic_clock = iter((0, 100_000_000, 600_000_000, 700_000_000, 1_700_000_000))
    rpc: AsyncUnaryUnaryRpcCallable[object, object, str] = AsyncUnaryUnaryRpcCallable(
        service_name='profile-service',
        method_name='GetProfile',
        client=SimpleNamespace(_default_retry_policy=None),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(pigeon_client.asyncio, 'sleep', sleep)
    monkeypatch.setattr(pigeon_client, 'metrics', mock_metrics)
    monkeypatch.setattr(pigeon_client, 'monotonic_ns', lambda: next(monotonic_clock))

    result = await rpc(
        object(),
        retry_policy={
            'retryable_grpc_status_codes': {grpc.StatusCode.UNAVAILABLE},
            'max_secondaries_to_retry': 2,
        },
    )

    assert result == 'response'
    assert sleep.await_args_list == [call(0.5), call(1.0)]
    assert mock_metrics.increment.call_args_list == [
        call(
            'pigeon.retry_attempt',
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
        ),
        call(
            'pigeon.retry_attempt',
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
        ),
    ]
    assert mock_metrics.timing.call_args_list == [
        call(
            'pigeon.retry_sleep_duration',
            500.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
        ),
        call(
            'pigeon.retry_elapsed_duration',
            600.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
        ),
        call(
            'pigeon.retry_sleep_duration',
            1000.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
        ),
        call(
            'pigeon.retry_elapsed_duration',
            1700.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
        ),
    ]


@pytest.mark.asyncio
async def test_service_discovery_retry_uses_bounded_reason_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    request = AsyncMock(side_effect=[ServiceUnavailable('empty ring'), 'response'])
    sleep = AsyncMock()
    mock_metrics = MagicMock()
    monotonic_clock = iter((0, 50_000_000, 550_000_000))
    rpc: AsyncUnaryUnaryRpcCallable[object, object, str] = AsyncUnaryUnaryRpcCallable(
        service_name='guild-service',
        method_name='GetGuild',
        client=SimpleNamespace(_default_retry_policy=None),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(pigeon_client.asyncio, 'sleep', sleep)
    monkeypatch.setattr(pigeon_client, 'metrics', mock_metrics)
    monkeypatch.setattr(pigeon_client, 'monotonic_ns', lambda: next(monotonic_clock))

    result = await rpc(
        object(),
        retry_policy={
            'retryable_grpc_status_codes': set(),
            'max_secondaries_to_retry': 1,
        },
    )

    assert result == 'response'
    assert mock_metrics.increment.call_args_list == [
        call(
            'pigeon.retry_attempt',
            tags=['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable'],
        )
    ]
    assert mock_metrics.timing.call_args_list == [
        call(
            'pigeon.retry_sleep_duration',
            500.0,
            tags=['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable'],
        ),
        call(
            'pigeon.retry_elapsed_duration',
            550.0,
            tags=['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable'],
        ),
    ]


@pytest.mark.asyncio
async def test_cancelled_backoff_records_partial_sleep_without_retry_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retryable_error = ServiceUnavailable('empty ring')
    request = AsyncMock(side_effect=retryable_error)
    sleep_started = asyncio.Event()
    block_sleep = asyncio.Event()
    mock_metrics = MagicMock()
    monotonic_clock = iter((0, 100_000_000, 350_000_000))

    async def cancellable_sleep(_duration: float) -> None:
        sleep_started.set()
        await block_sleep.wait()

    rpc: AsyncUnaryUnaryRpcCallable[object, object, str] = AsyncUnaryUnaryRpcCallable(
        service_name='guild-service',
        method_name='GetGuild',
        client=SimpleNamespace(_default_retry_policy=None),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(pigeon_client.asyncio, 'sleep', cancellable_sleep)
    monkeypatch.setattr(pigeon_client, 'metrics', mock_metrics)
    monkeypatch.setattr(pigeon_client, 'monotonic_ns', lambda: next(monotonic_clock))
    task = asyncio.create_task(
        rpc(
            object(),
            retry_policy={
                'retryable_grpc_status_codes': set(),
                'max_secondaries_to_retry': 1,
            },
        )
    )

    await asyncio.wait_for(sleep_started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert request.await_count == 1
    mock_metrics.increment.assert_not_called()
    assert mock_metrics.timing.call_args_list == [
        call(
            'pigeon.retry_sleep_duration',
            250.0,
            tags=['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable'],
        )
    ]


@pytest.mark.asyncio
async def test_unsampled_retry_counter_survives_real_client_aggregation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _DogStatsd()
    monkeypatch.setattr(client, '_send_to_server', lambda _packet: None)
    monkeypatch.setattr(client, 'timing', MagicMock())
    tags = ['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable']
    request = AsyncMock(side_effect=[ServiceUnavailable('empty ring'), 'response'])
    rpc: AsyncUnaryUnaryRpcCallable[object, object, str] = AsyncUnaryUnaryRpcCallable(
        service_name='guild-service',
        method_name='GetGuild',
        client=SimpleNamespace(_default_retry_policy=None),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(pigeon_client.asyncio, 'sleep', AsyncMock())
    monkeypatch.setattr(pigeon_client, 'metrics', client)
    monotonic_clock = iter((0, 0, 500_000_000))
    monkeypatch.setattr(pigeon_client, 'monotonic_ns', lambda: next(monotonic_clock))

    try:
        result = await rpc(
            object(),
            retry_policy={
                'retryable_grpc_status_codes': set(),
                'max_secondaries_to_retry': 1,
            },
        )
        [counter] = client.aggregator.flush_aggregated_metrics()
    finally:
        client.stop()

    assert result == 'response'
    assert counter.name == 'pigeon.retry_attempt'
    assert counter.value == 1
    assert counter.tags == tags
    assert counter.rate is None


# --- ServiceDefinition ---


def test_service_definition_type():
    from osprey.async_worker.lib.pigeon.client import ServiceDefinition

    sd: ServiceDefinition = {'address': 'localhost', 'ip': '127.0.0.1', 'port': 5000}
    assert sd['address'] == 'localhost'
    assert sd['port'] == 5000
