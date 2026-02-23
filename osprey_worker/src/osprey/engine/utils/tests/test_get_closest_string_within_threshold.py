import pytest
from osprey.engine.utils.get_closest_string_within_threshold import get_closest_string_within_threshold


@pytest.mark.parametrize(
    'string, candidates, expected',
    [
        # Prefer case insensitive match.
        ('USERNAME', ['UserName', 'USERNAMW'], 'UserName'),
        # Ensure that the scoring is proper
        ('UserNaem', ['UserName'], 'UserName'),
        # No close match.
        ('UserName', ['GuildId', 'GuildName'], None),
        # Too short.
        ('Foo', ['GuildId', 'Food'], None),
    ],
)
def test_get_closest_string_within_threshold(string: str, candidates: list[str], expected: str | None) -> None:
    assert get_closest_string_within_threshold(string, candidate_strings=candidates) == expected
