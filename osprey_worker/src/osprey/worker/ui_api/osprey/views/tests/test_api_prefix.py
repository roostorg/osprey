from unittest import mock

import pytest
from ddtrace import Span
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.storage.access_audit_log import AccessAuditLog


@pytest.mark.parametrize('endpoint', ['config.get_config', 'api_config.get_config'])
def test_routes_served_with_and_without_api_prefix(client: 'FlaskClient[Response]', endpoint: str) -> None:
    res = client.get(url_for(endpoint))
    assert res.status_code == 200


def test_api_prefixed_routes_mirror_unprefixed_routes(app: Flask) -> None:
    rules = {rule.rule for rule in app.url_map.iter_rules()}
    for rule in app.url_map.iter_rules():
        if rule.endpoint.startswith('api_'):
            assert rule.rule.startswith('/api/')
            assert rule.rule[len('/api') :] in rules


@pytest.mark.parametrize('path', ['/config', '/api/config', '/_health'])
def test_audit_exempt_endpoints(client: 'FlaskClient[Response]', path: str) -> None:
    with mock.patch.object(AccessAuditLog, 'persist') as persist:
        res = client.get(path)
    assert res.status_code == 200
    persist.assert_not_called()


@pytest.mark.parametrize('path', ['/docs/udfs', '/api/docs/udfs'])
def test_audited_endpoints(client: 'FlaskClient[Response]', path: str) -> None:
    with mock.patch.object(AccessAuditLog, 'persist') as persist:
        client.get(path)
    persist.assert_called_once()


@pytest.mark.parametrize('path', ['/config', '/api/config'])
def test_trace_resource_ignores_api_prefix(client: 'FlaskClient[Response]', path: str) -> None:
    finished: list[Span] = []
    original_finish = Span.finish

    def record_finish(span: Span, *args: object, **kwargs: object) -> None:
        finished.append(span)
        original_finish(span, *args, **kwargs)

    with mock.patch.object(Span, 'finish', autospec=True, side_effect=record_finish):
        res = client.get(path)

    assert res.status_code == 200
    assert [span.resource for span in finished if span.name == 'flask.request'] == ['config.get_config']
