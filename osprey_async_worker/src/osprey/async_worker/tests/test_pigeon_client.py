"""Tests for the async pigeon client."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import grpc
import pytest
from osprey.async_worker.lib.pigeon import client as pigeon_client
from osprey.async_worker.lib.pigeon.client import AsyncUnaryUnaryRpcCallable
from osprey.async_worker.lib.pigeon.exceptions import RPCException
from osprey.async_worker.lib.pigeon.skip_rate_limit import skip_rate_limit_context
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable

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
    clock = iter((0, 1_000_000_000, 3_000_000_000))
    rpc: AsyncUnaryUnaryRpcCallable[object, object, str] = AsyncUnaryUnaryRpcCallable(
        service_name='profile-service',
        method_name='GetProfile',
        client=SimpleNamespace(_default_retry_policy=None),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(pigeon_client.asyncio, 'sleep', sleep)
    monkeypatch.setattr(pigeon_client, 'metrics', mock_metrics)
    monkeypatch.setattr(pigeon_client, 'time_ns', lambda: next(clock))

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
            sample_rate=0.01,
        ),
        call(
            'pigeon.retry_attempt',
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
            sample_rate=0.01,
        ),
    ]
    assert mock_metrics.timing.call_args_list == [
        call(
            'pigeon.retry_elapsed_duration',
            1000.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
            sample_rate=0.01,
        ),
        call(
            'pigeon.retry_sleep_duration',
            500.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
            sample_rate=0.01,
        ),
        call(
            'pigeon.retry_elapsed_duration',
            3000.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
            sample_rate=0.01,
        ),
        call(
            'pigeon.retry_sleep_duration',
            1000.0,
            tags=['service:profile-service', 'resource_name:GetProfile', 'reason:grpc.unavailable'],
            sample_rate=0.01,
        ),
    ]


@pytest.mark.asyncio
async def test_service_discovery_retry_uses_bounded_reason_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    request = AsyncMock(side_effect=[ServiceUnavailable('empty ring'), 'response'])
    sleep = AsyncMock()
    mock_metrics = MagicMock()
    clock = iter((0, 250_000_000))
    rpc: AsyncUnaryUnaryRpcCallable[object, object, str] = AsyncUnaryUnaryRpcCallable(
        service_name='guild-service',
        method_name='GetGuild',
        client=SimpleNamespace(_default_retry_policy=None),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(pigeon_client.asyncio, 'sleep', sleep)
    monkeypatch.setattr(pigeon_client, 'metrics', mock_metrics)
    monkeypatch.setattr(pigeon_client, 'time_ns', lambda: next(clock))

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
            sample_rate=0.01,
        )
    ]
    assert mock_metrics.timing.call_args_list == [
        call(
            'pigeon.retry_elapsed_duration',
            250.0,
            tags=['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable'],
            sample_rate=0.01,
        ),
        call(
            'pigeon.retry_sleep_duration',
            500.0,
            tags=['service:guild-service', 'resource_name:GetGuild', 'reason:service_unavailable'],
            sample_rate=0.01,
        ),
    ]


# --- ServiceDefinition ---


def test_service_definition_type():
    from osprey.async_worker.lib.pigeon.client import ServiceDefinition

    sd: ServiceDefinition = {'address': 'localhost', 'ip': '127.0.0.1', 'port': 5000}
    assert sd['address'] == 'localhost'
    assert sd['port'] == 5000
