import pytest
from osprey.worker.lib.utils.flask_utils import canonical_endpoint


@pytest.mark.parametrize(
    'endpoint, expected',
    [
        ('config.get_config', 'config.get_config'),
        ('api_config.get_config', 'config.get_config'),
        ('api_saved-queries.get_saved_query', 'saved-queries.get_saved_query'),
        ('health', 'health'),
        ('api_health', 'api_health'),
        (None, None),
    ],
)
def test_canonical_endpoint(endpoint: str | None, expected: str | None) -> None:
    assert canonical_endpoint(endpoint) == expected
