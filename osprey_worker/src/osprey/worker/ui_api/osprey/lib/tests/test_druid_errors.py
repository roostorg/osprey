import io
import json
import urllib.error
from datetime import datetime
from unittest import mock

import pytest
from osprey.worker.ui_api.osprey.lib.druid import DruidQueryError, TimeseriesDruidQuery
from pydruid.client import PyDruid
from pydruid.query import Query


def pydruid_error(urlopen_side_effect: Exception) -> OSError:
    """The OSError that pydruid raises when Druid fails a query, built by pydruid itself."""
    client = PyDruid('http://druid-broker:8082', 'druid/v2')
    query = Query(query_dict={'queryType': 'timeseries'}, query_type='timeseries')  # type: ignore[no-untyped-call]
    with mock.patch('urllib.request.urlopen', side_effect=urlopen_side_effect):
        with pytest.raises(OSError) as exc_info:
            client._post(query)
    return exc_info.value


def http_error(code: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        'http://druid-broker:8082/druid/v2',
        code,
        'Server Error',
        hdrs=None,
        fp=io.BytesIO(body),  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    'urlopen_side_effect, expected_message',
    [
        (
            http_error(
                500,
                json.dumps(
                    {
                        'error': 'Unsupported operation',
                        'errorMessage': 'Predicate does not support ARRAY types',
                        'host': '172.19.0.10:8100',
                    }
                ).encode(),
            ),
            'Druid query failed: Predicate does not support ARRAY types',
        ),
        (
            http_error(400, json.dumps({'error': 'druidException', 'errorMessage': "Column 'x' not found"}).encode()),
            "Druid query failed: Column 'x' not found",
        ),
        (
            urllib.error.URLError(ConnectionRefusedError(111, 'Connection refused')),
            'Druid query failed: <urlopen error [Errno 111] Connection refused>',
        ),
    ],
)
def test_druid_query_error_message(urlopen_side_effect: Exception, expected_message: str) -> None:
    error = DruidQueryError.from_pydruid_error(pydruid_error(urlopen_side_effect))

    assert str(error) == expected_message


@pytest.mark.parametrize(
    'urlopen_side_effect, expected_message',
    [
        # An empty body, e.g. from a proxy in front of Druid.
        (http_error(504, b''), 'Druid query failed: HTTP Error 504'),
        # Don't show the whole error dict: it names internal hosts.
        (
            http_error(500, json.dumps({'errorClass': 'SomeException', 'host': '10.0.0.5:8100'}).encode()),
            'Druid query failed: SomeException',
        ),
    ],
)
def test_druid_query_error_message_fallbacks(urlopen_side_effect: Exception, expected_message: str) -> None:
    assert str(DruidQueryError.from_pydruid_error(pydruid_error(urlopen_side_effect))) == expected_message


@pytest.mark.parametrize('status_code', [400, 500])
def test_druid_query_error_keeps_the_status_code(status_code: int) -> None:
    error = pydruid_error(http_error(status_code, json.dumps({'errorMessage': 'boom'}).encode()))

    assert DruidQueryError.from_pydruid_error(error).status_code == status_code


def test_druid_query_error_without_http_response_has_no_status_code() -> None:
    error = pydruid_error(urllib.error.URLError(ConnectionRefusedError(111, 'Connection refused')))

    assert DruidQueryError.from_pydruid_error(error).status_code is None


def test_non_json_500_is_reported_as_a_druid_query_error() -> None:
    # pydruid 0.6.9 raises TypeError, not OSError, for a 500 whose body isn't JSON.
    druid = mock.MagicMock(datasource='osprey.execution_results')
    druid.client = PyDruid('http://druid-broker:8082', 'druid/v2')
    query = TimeseriesDruidQuery(
        start=datetime(2026, 1, 1), end=datetime(2026, 1, 2), query_filter='', entity=None, granularity='day'
    )
    with (
        mock.patch('osprey.worker.ui_api.osprey.lib.druid.DRUID') as druid_holder,
        mock.patch('urllib.request.urlopen', side_effect=http_error(500, b'<html>Bad Gateway</html>')),
    ):
        druid_holder.instance.return_value = druid
        with pytest.raises(DruidQueryError) as exc_info:
            query.execute()

    assert str(exc_info.value) == 'Druid query failed: HTTP Error 500'
    assert exc_info.value.status_code == 500


def test_druid_query_error_message_leaves_out_the_query() -> None:
    error = pydruid_error(http_error(500, json.dumps({'errorMessage': 'boom'}).encode()))
    assert 'Query is:' in str(error)

    assert str(DruidQueryError.from_pydruid_error(error)) == 'Druid query failed: boom'


def test_unexpected_error_text_is_passed_through() -> None:
    assert str(DruidQueryError.from_pydruid_error(OSError('something else'))) == 'Druid query failed: something else'
