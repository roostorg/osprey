"""`_is_disposable_database`: which database the session teardown may drop.

Untested until now, and the one guard in the suite whose failure mode is destroying a
developer's data rather than reporting a failure. The fixture it protects used to drop
whatever `POSTGRES_HOSTS` pointed at, and the compose files pointed the tests and the dev
stack at the same `osprey` database -- so running the suite deleted the local development
database, and it surfaced later and elsewhere as `osprey-ui-api` refusing to start.

The function is pure, so these need no database. They still pay for one: the session
fixture in `lib/conftest.py` is autouse, so anything under `lib/` creates it regardless.
"""

import pytest
from osprey.worker.lib.tests.test_utils import _is_disposable_database

#: The two URLs that actually appear in this repo's compose files. Named rather than
#: inlined, because the whole point of the guard is telling these two apart.
DEV_URL = 'postgresql://osprey:FoolishPassword@postgres:5432/osprey'
TEST_URL = 'postgresql://osprey:FoolishPassword@postgres:5432/osprey_test'


@pytest.mark.parametrize(
    ('name', 'url', 'created_by_this_run', 'expected'),
    [
        ('created it, and it is named like a test database', TEST_URL, True, True),
        ('named like a test database, but was already there', TEST_URL, False, False),
        ('created it, but it is the development database', DEV_URL, True, False),
        ('the development database, already there', DEV_URL, False, False),
    ],
    ids=['created-test', 'existing-test', 'created-dev', 'existing-dev'],
)
def test_only_a_test_database_this_run_created_may_be_dropped(
    name: str, url: str, created_by_this_run: bool, expected: bool
) -> None:
    """Both conditions are required, and each covers a hole the other leaves.

    Dropping only what this run created still destroys a developer's database when the
    run happens to be the thing that created it -- a fresh Postgres volume, or tests run
    before the app has ever started. Trusting only the name still drops a `*_test`
    database somebody else was using.

    The `created-dev` row is the accident that motivated the guard: the suite created the
    database it was pointed at, which was the dev one, and then felt entitled to drop it.
    """
    assert _is_disposable_database(url, created_by_this_run) is expected, name


def test_a_query_string_is_not_part_of_the_database_name() -> None:
    """Connection URLs carry parameters in deployments, and the suffix check must not see them.

    `urlsplit` keeps the query out of `.path`, so this passes -- but the check reads as
    though it operates on the whole URL, and a later rewrite that used `url.endswith(...)`
    would refuse to drop a legitimate test database and leave it behind for the next run
    to silently reuse.
    """
    assert _is_disposable_database(f'{TEST_URL}?sslmode=require', True) is True


def test_a_database_named_only_test_is_still_a_test_database() -> None:
    """The suffix is matched, not a separator-plus-suffix, so a bare `_test` qualifies.

    Degenerate but worth pinning: it is the boundary of what the name check accepts, and
    it documents that the rule is "ends with `_test`" rather than "has a `_test` suffix
    after some other name".
    """
    assert _is_disposable_database('postgresql://u:p@h:5432/_test', True) is True


def test_a_name_merely_containing_test_is_not_enough() -> None:
    """`osprey_testing` is not a test database, and neither is `latest`.

    Guards against the suffix check being loosened to a substring one, which would make
    every database with `test` anywhere in its name a candidate for deletion.
    """
    assert _is_disposable_database('postgresql://u:p@h:5432/osprey_testing', True) is False
    assert _is_disposable_database('postgresql://u:p@h:5432/latest', True) is False
