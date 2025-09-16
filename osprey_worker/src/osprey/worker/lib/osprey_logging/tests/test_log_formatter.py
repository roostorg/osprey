import logging

import pytest
from flask import Flask
from flask import g as flask_global
from osprey.worker.lib.osprey_logging import (
    DATADOG_ROUTE_METADATA_ATTR,
    DATADOG_ROUTE_NAME_TAG,
    DATADOG_ROUTE_OWNER_TAG,
    JsonLogFormatter,
    PrettyLogFormatter,
)


@pytest.fixture()
def app():
    app = Flask(__name__)
    return app


def test_pretty_log_formatter_timestamp():
    formatter = PrettyLogFormatter()
    record = logging.LogRecord('recordy', logging.INFO, 'nopath', 44, 'i am a message', [], None)
    timestamps = [
        (0, '700101 00:00:00.000000'),
        (1571689240, '191021 20:20:40.000000'),
        (1571689240.837571, '191021 20:20:40.837571'),
        (1571689240.02831, '191021 20:20:40.028310'),
        (1571689240.9991823, '191021 20:20:40.999182'),
    ]
    for timestamp in timestamps:
        record.created = timestamp[0]
        assert formatter.format(record) == f'[I {timestamp[1]} nopath:44] i am a message'


def test_pretty_log_formatter_ddtrace():
    formatter = PrettyLogFormatter()
    record = logging.LogRecord('recordy', logging.INFO, 'nopath', 44, 'i am a message', [], None)
    record.created = 0
    setattr(record, 'dd.service', 'osprey')
    assert formatter.format(record) == '[I 700101 00:00:00.000000 nopath:44] [dd.service=osprey] i am a message'


def test_json_log_formatter_dtrace():
    formatter = JsonLogFormatter('%(asctime)s,%(name)s,%(levelname)s,%(message)s')
    record = logging.LogRecord('recordy', logging.INFO, 'nopath', 44, 'i am a message', [], None)
    record.created = 0
    record.msecs = 481.781005859375
    setattr(record, 'dd.service', 'osprey')
    assert formatter.format(record) == (
        '{"asctime": "1970-01-01 00:00:00.481", "name": "recordy", "levelname": "INFO", '
        + '"message": "i am a message", "dd.service": "osprey"}'
    )


def test_pretty_log_formatter_route_metadata(app):
    formatter = PrettyLogFormatter()
    with app.test_request_context('/'):
        record = logging.LogRecord('recordy', logging.INFO, 'nopath', 44, 'i am a message', [], None)
        record.created = 0
        route_owner = 'fake_owner'
        route_name = 'fake_name'
        route_metadata = {
            DATADOG_ROUTE_OWNER_TAG: route_owner,
            DATADOG_ROUTE_NAME_TAG: route_name,
        }
        setattr(flask_global, DATADOG_ROUTE_METADATA_ATTR, route_metadata)
        assert (
            formatter.format(record)
            == '[I 700101 00:00:00.000000 nopath:44] [team=fake_owner route_name=fake_name] i am a message'
        )


def test_json_log_formatter_route_metadata(app):
    formatter = JsonLogFormatter('%(asctime)s,%(name)s,%(levelname)s,%(message)s')
    with app.test_request_context('/'):
        record = logging.LogRecord('recordy', logging.INFO, 'nopath', 44, 'i am a message', [], None)
        record.created = 0
        record.msecs = 481.781005859375
        route_owner = 'fake_owner'
        route_name = 'fake_name'
        route_metadata = {
            DATADOG_ROUTE_OWNER_TAG: route_owner,
            DATADOG_ROUTE_NAME_TAG: route_name,
        }
        setattr(flask_global, DATADOG_ROUTE_METADATA_ATTR, route_metadata)
        assert formatter.format(record) == (
            '{"asctime": "1970-01-01 00:00:00.481", "name": "recordy", "levelname": "INFO", '
            + '"message": "i am a message", "team": "fake_owner", "route_name": "fake_name"}'
        )
