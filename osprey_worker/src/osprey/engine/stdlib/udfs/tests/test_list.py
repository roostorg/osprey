from typing import cast

import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.list import (
    LIST_CACHE,
    ListContains,
    ListContainsCount,
    ListContainsItems,
)
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(
            ListContains,
            ListContainsCount,
            ListContainsItems,
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
    """Tests for the ListContains* UDFs."""

    def test_finds_included_words(self, execute: ExecuteFunction):
        data = execute("""
            ContainsDog = ListContains(list_name="words_one", terms=["dog"])
            ContainsGoodbye = ListContains(list_name="words_one", terms=["goodbye"])
            ContainsBye = ListContains(list_name="words_one", terms=["bye"])
        """)
        assert data['ContainsDog'] is True
        assert data['ContainsGoodbye'] is True
        assert data['ContainsBye'] is False

    def test_checks_casing(self, execute: ExecuteFunction):
        data = execute("""
            ContainsCat = ListContains(list_name="words_one", case_sensitive=True, terms=["cat"])
            ContainsCatUpper = ListContains(list_name="words_one", case_sensitive=True, terms=["CAT"])
        """)
        assert data['ContainsCat'] is True
        assert data['ContainsCatUpper'] is False

    def test_applies_word_boundaries(self, execute: ExecuteFunction):
        data = execute("""
            ContainsDog = ListContains(list_name="words_one", substrings=True, terms=["dogma"])
            ContainsExactDog = ListContains(list_name="words_one", substrings=False, terms=["dogma"])
        """)
        assert data['ContainsDog'] is True
        assert data['ContainsExactDog'] is False

    def test_counts_contained_words(self, execute: ExecuteFunction):
        data = execute("""
            WordCount = ListContainsCount(list_name="words_one", terms=["hello", "goodbye", "giraffe", "penguin"])
            WordCount2 = ListContainsCount(list_name="words_one", terms=["one", "two", "three", "four"])
        """)
        assert data['WordCount'] == 2
        assert data['WordCount2'] == 0

    def test_counts_cotnained_words_casing(self, execute: ExecuteFunction):
        data = execute("""
            WordCount = ListContainsCount(list_name="words_one", case_sensitive=True, terms=["hello", "cat", "GOODBYE", "dOg"])
        """)
        assert data['WordCount'] == 2

    def test_counts_contained_words_boundaries(self, execute: ExecuteFunction):
        data = execute("""
            WordCount = ListContainsCount(list_name="words_one", substrings=True, terms=["hello", "concatenate", "goodbye", "dogma"])
            WordCount2 = ListContainsCount(list_name="words_one", substrings=False, terms=["hello", "concatenate", "goodbye", "dogma"])
        """)
        assert data['WordCount'] == 4
        assert data['WordCount2'] == 2

    def test_counts_contained_words_once(self, execute: ExecuteFunction):
        """
        Even if the same term is found more than once in the list, it should only be counted once.
        """
        data = execute("""
            WordCount = ListContainsCount(list_name="words_one", terms=["hello", "hello", "goodbye"])
            WordCount2 = ListContainsCount(list_name="words_one", substrings=True, terms=["cat", "concatenate", "dog", "dogma"])
        """)
        assert data['WordCount'] == 2
        assert data['WordCount2'] == 2

    def test_gets_contained_words(self, execute: ExecuteFunction):
        data = execute("""
            FoundItems = ListContainsItems(list_name="words_one", terms=["hello", "what", "is", "up"])
            FoundItems2 = ListContainsItems(list_name="words_one", terms=["cat", "dog", "zebra"])
            FoundItems3 = ListContainsItems(list_name="words_one", terms=["mouse", "pinecone"])
        """)
        assert set(cast(list[str], data['FoundItems'])) == set(['hello'])
        assert set(cast(list[str], data['FoundItems2'])) == set(['cat', 'dog'])
        assert data['FoundItems3'] == []

    def test_gets_contained_words_casing(self, execute: ExecuteFunction):
        data = execute("""
            FoundItems = ListContainsItems(list_name="words_one", case_sensitive=True, terms=["gooDBYE", "to", "YOu"])
            FoundItems2 = ListContainsItems(list_name="words_one", case_sensitive=False, terms=["gooDBYE", "to", "YOu"])
        """)
        assert data['FoundItems'] == []
        assert set(cast(list[str], data['FoundItems2'])) == set(['goodbye'])

    def test_gets_contained_words_boundaries(self, execute: ExecuteFunction):
        data = execute("""
            FoundItems = ListContainsItems(list_name="words_one", substrings=True, terms=["dogma", "dog", "frog", "concatenate"])
            FoundItems2 = ListContainsItems(list_name="words_one", substrings=False, terms=["dogma", "dog", "frog", "concatenate"])
        """)
        assert set(cast(list[str], data['FoundItems'])) == set(['dog', 'cat'])
        assert set(cast(list[str], data['FoundItems2'])) == set(['dog'])


class TestRegexListContainsUDF:
    """Tests for the RegexListContains* UDFs"""
