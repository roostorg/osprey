from typing import Dict, List, Union

import pytest
from osprey.worker.lib.instruments import metrics
from pytest_mock import MockFixture


@pytest.mark.parametrize(
    'tags',
    (
        pytest.param(['test_tag_1:test_value_1', 'test_tag_2:test_value_2'], id='list'),
        pytest.param({'test_tag_1': 'test_value_1', 'test_tag_2': 'test_value_2'}, id='dict'),
    ),
)
def test_report_accepts_dict_or_list_tags(tags: Union[Dict[str, str], List[str]], mocker: MockFixture) -> None:
    mock = mocker.patch('datadog.dogstatsd.base.DogStatsd._report', autospec=True)

    metrics._report('test.metric', 'c', 1, tags, 1)
    mock.assert_called_once_with(
        metrics, 'test.metric', 'c', 1, ['test_tag_1:test_value_1', 'test_tag_2:test_value_2'], 1
    )
