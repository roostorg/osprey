from unittest.mock import patch

from flask import Flask
from osprey.worker.ui_api.osprey.lib.audit import audit_request


def _make_app() -> Flask:
    app = Flask(__name__)

    @app.route('/echo', methods=['POST'])
    def echo() -> str:
        return 'ok'

    app.after_request(audit_request)
    return app


def test_audit_records_raw_json_body() -> None:
    with patch('osprey.worker.ui_api.osprey.lib.audit.AccessAuditLog') as audit_log:
        response = _make_app().test_client().post('/echo', json={'a': 1})

    assert response.status_code == 200
    audit_log.assert_called_once()
    assert audit_log.call_args.kwargs['request_body'] == b'{"a": 1}'


def test_audit_records_raw_form_body() -> None:
    with patch('osprey.worker.ui_api.osprey.lib.audit.AccessAuditLog') as audit_log:
        response = _make_app().test_client().post('/echo', data={'a': '1'})

    assert response.status_code == 200
    audit_log.assert_called_once()
    assert audit_log.call_args.kwargs['request_body'] == b'a=1'
