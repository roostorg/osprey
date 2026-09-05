"""`_is_disposable_database`: which database the session fixture may destroy.

The one check in the suite whose failure mode is destroying a developer's data rather
than reporting a failure. The fixture it protects used to drop whatever `POSTGRES_HOSTS`
pointed at, and the compose files pointed the tests and the dev stack at the same
`osprey` database -- so running the suite deleted the local development database, and it
surfaced later and elsewhere as `osprey-ui-api` refusing to start.

The function is pure, so these need no database. They still pay for one: the session
fixture in the root conftest is autouse for the whole repository.
"""

import pytest
from osprey.worker.lib.storage import postgres
from osprey.worker.lib.tests.test_utils import _drop_database, _is_disposable_database
from psycopg2.errors import InvalidCatalogName, ObjectInUse
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils import create_database

#: The two URLs that actually appear in this repo's compose files. Named rather than
#: inlined, because the whole point of the guard is telling these two apart.
DEV_URL = 'postgresql://osprey:FoolishPassword@postgres:5432/osprey'
TEST_URL = 'postgresql://osprey:FoolishPassword@postgres:5432/osprey_test'


def _drop_if_present(url: str) -> None:
    """Drop the database at `url`, tolerating it not being there.

    Only that. A cleanup step that swallows everything hides the failure it should be
    reporting -- a refused connection or a missing permission would surface later as a
    confusing `DuplicateDatabase` from the next `create_database`, pointing at the wrong
    thing entirely.
    """
    try:
        _drop_database(url)
    except ProgrammingError as error:
        if not isinstance(error.orig, InvalidCatalogName):
            raise


@pytest.mark.parametrize(
    ('name', 'url', 'expected'),
    [
        ('named like a test database', TEST_URL, True),
        ('the development database', DEV_URL, False),
    ],
    ids=['test-database', 'dev-database'],
)
def test_only_a_test_database_may_be_destroyed(name: str, url: str, expected: bool) -> None:
    """The name decides it, and the dev URL is the accident this was written for.

    An earlier version also required that this run had created the database. That was
    standing in for "somebody else may be using it", and it stopped meaning anything
    once the fixture began recreating an existing database at setup -- which destroys a
    database in use just as thoroughly as dropping it at teardown would. The fixture
    asks `_sessions_on_database` instead, which checks that condition directly.
    """
    assert _is_disposable_database(url) is expected, name


def test_a_query_string_is_not_part_of_the_database_name() -> None:
    """Connection URLs carry parameters in deployments, and the suffix check must not see them.

    `urlsplit` keeps the query out of `.path`, so this passes -- but the check reads as
    though it operates on the whole URL, and a later rewrite using `url.endswith(...)`
    would refuse to recreate a legitimate test database, leaving the stale schema that
    this fixture exists to prevent.
    """
    assert _is_disposable_database(f'{TEST_URL}?sslmode=require') is True


def test_a_database_named_only_test_is_still_a_test_database() -> None:
    """The suffix is matched, not a separator-plus-suffix, so a bare `_test` qualifies.

    Degenerate but worth pinning: it is the boundary of what the name check accepts, and
    it documents that the rule is "ends with `_test`" rather than "has a `_test` suffix
    after some other name".
    """
    assert _is_disposable_database('postgresql://u:p@h:5432/_test') is True


def test_a_name_merely_containing_test_is_not_enough() -> None:
    """`osprey_testing` is not a test database, and neither is `latest`.

    Guards against the suffix check being loosened to a substring one, which would make
    every database with "test" anywhere in its name a candidate for destruction.
    """
    assert _is_disposable_database('postgresql://u:p@h:5432/osprey_testing') is False
    assert _is_disposable_database('postgresql://u:p@h:5432/latest') is False


def test_dropping_a_database_that_is_in_use_is_refused() -> None:
    """A database with a connection open is not destroyed, and the refusal is Postgres's.

    This is the assertion that would catch a regression to
    `sqlalchemy_utils.drop_database`, which does not refuse: for Postgres it runs
    `pg_terminate_backend` against every other session and drops the database anyway,
    so a concurrent test run would be killed and left failing with connection errors
    that point nowhere near the cause.

    Runs against a scratch database rather than the suite's own, for the obvious reason
    that the success path here destroys whatever it is pointed at.
    """
    bound_engine = postgres.sessions['osprey_db'].kw['bind']
    scratch_url = bound_engine.url.set(database='osprey_drop_guard_test').render_as_string(hide_password=False)

    # Dropped first and last: a failure part way through this test would otherwise leave
    # the scratch database behind, and the next run's `create_database` would collide
    # with it rather than reporting whatever actually went wrong.
    _drop_if_present(scratch_url)

    scratch_engine = None
    try:
        create_database(scratch_url)
        scratch_engine = create_engine(scratch_url)

        with scratch_engine.connect():
            with pytest.raises(OperationalError) as excinfo:
                _drop_database(scratch_url)
            assert isinstance(excinfo.value.orig, ObjectInUse)

        # And once nothing is attached it drops -- so the refusal is about the
        # connection, not about the drop being broken.
        scratch_engine.dispose()
        _drop_database(scratch_url)
    finally:
        if scratch_engine is not None:
            scratch_engine.dispose()
        _drop_if_present(scratch_url)
