import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.list import (
    LIST_CACHE,
    ListContains,
)
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(
            ListContains,
        )
    ),
]


@pytest.fixture(autouse=True)
def setup_tests():
    LIST_CACHE.instance()._add_list(
        list_name='words_one', case_sensitive=False, terms=['hello', 'cat', 'goodbye', 'dog']
    )
    LIST_CACHE.instance()._add_list(
        list_name='words_one', case_sensitive=True, terms=['hello', 'cat', 'goodbye', 'dog']
    )


class TestListContainsUDF:
    """Tests for the ListContains UDF."""

    def test_finds_included_words(self, execute: ExecuteFunction) -> None:
        data = execute("""
            ContainsDog = ListContains(list_name="words_one", terms=["dog"])
            ContainsGoodbye = ListContains(list_name="words_one", terms=["goodbye"])
            ContainsBye = ListContains(list_name="words_one", terms=["bye"])
        """)
        assert data['ContainsDog'] is True
        assert data['ContainsGoodbye'] is True
        assert data['ContainsBye'] is False

    def test_checks_casing(self, execute: ExecuteFunction) -> None:
        data = execute("""
            ContainsDog = ListContains(list_name="words_one", case_sensitive=True, terms=["cat"])
            ContainsCatUpper = ListContains(list_name="words_one", terms=["CAT"])
        """)
        assert data['ContainsDog'] is True
        assert data['ContainsCatUpper'] is False
