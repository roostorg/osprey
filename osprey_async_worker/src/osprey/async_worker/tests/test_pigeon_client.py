"""Tests for the async pigeon client."""

import asyncio
from contextvars import copy_context
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from osprey.async_worker.lib.pigeon.skip_rate_limit import skip_rate_limit_context


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


# --- ServiceDefinition ---


def test_service_definition_type():
    from osprey.async_worker.lib.pigeon.client import ServiceDefinition

    sd: ServiceDefinition = {'address': 'localhost', 'ip': '127.0.0.1', 'port': 5000}
    assert sd['address'] == 'localhost'
    assert sd['port'] == 5000
