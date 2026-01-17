from typing import cast

import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.list import (
    LIST_CACHE,
    CensoredListContains,
    CensoredListContainsCount,
    CensoredListContainsItems,
    ListContains,
    ListContainsCount,
    ListContainsItems,
    RegexListContains,
    RegexListContainsCount,
    RegexListContainsItems,
)
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(
            ListContains,
            ListContainsCount,
            ListContainsItems,
            RegexListContains,
            RegexListContainsCount,
            RegexListContainsItems,
            CensoredListContains,
            CensoredListContainsCount,
            CensoredListContainsItems,
        )
    ),
]


@pytest.fixture(autouse=True)
def setup_tests():
    words = ['hello', 'cat', 'goodbye', 'dog']
    LIST_CACHE.instance()._dangerously_add_list(list_name='words_one', case_sensitive=False, terms=words)
    LIST_CACHE.instance()._dangerously_add_list(list_name='words_one', case_sensitive=True, terms=words)

    patterns = [r'fr[o0]g', r'c[a4]t', r'h[e3][l1][l1][o0]']
    LIST_CACHE.instance()._dangerously_add_regex_list(
        list_name='regex',
        case_sensitive=False,
        str_patterns=patterns,
    )
    LIST_CACHE.instance()._dangerously_add_regex_list(
        list_name='regex',
        case_sensitive=True,
        str_patterns=patterns,
    )

    words_to_censor = ['hello', 'cat', 'frog', 'boar']
    LIST_CACHE.instance()._dangerously_add_censored_list(
        list_name='censored', plurals=False, substrings=False, terms=words_to_censor
    )
    LIST_CACHE.instance()._dangerously_add_censored_list(
        list_name='censored', plurals=True, substrings=False, terms=words_to_censor
    )
    LIST_CACHE.instance()._dangerously_add_censored_list(
        list_name='censored', plurals=True, substrings=True, terms=words_to_censor
    )
    LIST_CACHE.instance()._dangerously_add_censored_list(
        list_name='censored', plurals=False, substrings=True, terms=words_to_censor
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

    def test_counts_contains_words_casing(self, execute: ExecuteFunction):
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
        assert set(cast(list[str], data['FoundItems'])) == {'hello'}
        assert set(cast(list[str], data['FoundItems2'])) == {'cat', 'dog'}
        assert data['FoundItems3'] == []

    def test_gets_contained_words_casing(self, execute: ExecuteFunction):
        data = execute("""
            FoundItems = ListContainsItems(list_name="words_one", case_sensitive=True, terms=["gooDBYE", "to", "YOu"])
            FoundItems2 = ListContainsItems(list_name="words_one", case_sensitive=False, terms=["gooDBYE", "to", "YOu"])
        """)
        assert data['FoundItems'] == []
        assert set(cast(list[str], data['FoundItems2'])) == {'goodbye'}

    def test_gets_contained_words_boundaries(self, execute: ExecuteFunction):
        data = execute("""
            FoundItems = ListContainsItems(list_name="words_one", substrings=True, terms=["dogma", "dog", "frog", "concatenate"])
            FoundItems2 = ListContainsItems(list_name="words_one", substrings=False, terms=["dogma", "dog", "frog", "concatenate"])
        """)
        assert set(cast(list[str], data['FoundItems'])) == {'dog', 'cat'}
        assert set(cast(list[str], data['FoundItems2'])) == {'dog'}

    def test_handles_none_in_terms(self, execute: ExecuteFunction):
        data = execute("""
            Contains = ListContains(list_name="words_one", terms=[None])
        """)
        assert data['Contains'] is False

    def test_handles_empty_terms(self, execute: ExecuteFunction):
        data = execute("""
            Result = ListContains(list_name="words_one", terms=[])
            WordCount = ListContainsCount(list_name="words_one", terms=[])
            FoundItems = ListContainsItems(list_name="words_one", terms=[])
        """)
        assert data['Result'] is False
        assert data['WordCount'] == 0
        assert data['FoundItems'] == []

    def test_combined_case_sensitive_and_substrings(self, execute: ExecuteFunction):
        data = execute("""
            Result = ListContains(list_name="words_one", case_sensitive=True, substrings=True, terms=["DOGMA"])
        """)
        assert data['Result'] is False

    def test_word_boundaries_with_punctuation(self, execute: ExecuteFunction):
        data = execute("""
            WithPunctuation = ListContains(list_name="words_one", substrings=False, terms=["hello!"])
            InMiddle = ListContains(list_name="words_one", substrings=False, terms=["say hello there"])
        """)
        assert data['WithPunctuation'] is False
        assert data['InMiddle'] is False

    def test_counts_multiple_matches_in_single_term(self, execute: ExecuteFunction):
        data = execute("""
            WordCount = ListContainsCount(list_name="words_one", substrings=True, terms=["catdog"])
            FoundItems = ListContainsItems(list_name="words_one", substrings=True, terms=["catdog"])
        """)
        assert data['WordCount'] == 2
        assert set(cast(list[str], data['FoundItems'])) == {'cat', 'dog'}


class TestRegexListContainsUDF:
    """Tests for the RegexListContains* UDFs"""

    def test_matches_basic_patterns(self, execute: ExecuteFunction):
        data = execute("""
            MatchesFrog = RegexListContains(list_name="regex", terms=["frog"])
            MatchesCat = RegexListContains(list_name="regex", terms=["cat"])
            MatchesHello = RegexListContains(list_name="regex", terms=["hello"])
            MatchesDog = RegexListContains(list_name="regex", terms=["dog"])
        """)
        assert data['MatchesFrog'] is True
        assert data['MatchesCat'] is True
        assert data['MatchesHello'] is True
        assert data['MatchesDog'] is False

    def test_matches_leetspeak_variants(self, execute: ExecuteFunction):
        data = execute("""
            MatchesFr0g = RegexListContains(list_name="regex", terms=["fr0g"])
            MatchesC4t = RegexListContains(list_name="regex", terms=["c4t"])
            MatchesH3llo = RegexListContains(list_name="regex", terms=["h3llo"])
            MatchesHe11o = RegexListContains(list_name="regex", terms=["he11o"])
            MatchesH3110 = RegexListContains(list_name="regex", terms=["h3110"])
        """)
        assert data['MatchesFr0g'] is True
        assert data['MatchesC4t'] is True
        assert data['MatchesH3llo'] is True
        assert data['MatchesHe11o'] is True
        assert data['MatchesH3110'] is True

    def test_matches_within_longer_string(self, execute: ExecuteFunction):
        """Regex patterns should match anywhere in the term."""
        data = execute("""
            MatchesInSentence = RegexListContains(list_name="regex", terms=["the frog jumped"])
            MatchesC4tInWord = RegexListContains(list_name="regex", terms=["c4tastrophe"])
        """)
        assert data['MatchesInSentence'] is True
        assert data['MatchesC4tInWord'] is True

    def test_no_match_returns_false(self, execute: ExecuteFunction):
        data = execute("""
            NoMatch = RegexListContains(list_name="regex", terms=["elephant", "giraffe", "zebra"])
        """)
        assert data['NoMatch'] is False

    def test_counts_matching_terms(self, execute: ExecuteFunction):
        data = execute("""
            Count = RegexListContainsCount(list_name="regex", terms=["frog", "cat", "dog", "elephant"])
            CountLeet = RegexListContainsCount(list_name="regex", terms=["fr0g", "c4t", "h3110"])
            CountNone = RegexListContainsCount(list_name="regex", terms=["dog", "elephant", "zebra"])
        """)
        assert data['Count'] == 2
        assert data['CountLeet'] == 3
        assert data['CountNone'] == 0

    def test_counts_each_term_once(self, execute: ExecuteFunction):
        """A term matching multiple patterns should only be counted once."""
        data = execute("""
            Count = RegexListContainsCount(list_name="regex", terms=["frog", "frog", "cat"])
        """)
        assert data['Count'] == 2

    def test_gets_matching_terms(self, execute: ExecuteFunction):
        data = execute("""
            Items = RegexListContainsItems(list_name="regex", terms=["frog", "dog", "cat", "elephant"])
            ItemsLeet = RegexListContainsItems(list_name="regex", terms=["fr0g", "c4t", "h3110", "zebra"])
            ItemsNone = RegexListContainsItems(list_name="regex", terms=["dog", "elephant"])
        """)
        assert set(cast(list[str], data['Items'])) == {'fr[o0]g', 'c[a4]t'}
        assert set(cast(list[str], data['ItemsLeet'])) == {'fr[o0]g', 'c[a4]t', 'h[e3][l1][l1][o0]'}
        assert data['ItemsNone'] == []

    def test_handles_empty_terms(self, execute: ExecuteFunction):
        data = execute("""
            Result = RegexListContains(list_name="regex", terms=[])
            Count = RegexListContainsCount(list_name="regex", terms=[])
            Items = RegexListContainsItems(list_name="regex", terms=[])
        """)
        assert data['Result'] is False
        assert data['Count'] == 0
        assert data['Items'] == []

    def test_case_insensitive_matching(self, execute: ExecuteFunction):
        data = execute("""
            MatchesUpper = RegexListContains(list_name="regex", terms=["FROG"])
            MatchesMixed = RegexListContains(list_name="regex", terms=["FrOg"])
            MatchesUpperCat = RegexListContains(list_name="regex", terms=["C4T"])
        """)
        assert data['MatchesUpper'] is True
        assert data['MatchesMixed'] is True
        assert data['MatchesUpperCat'] is True

    def test_case_sensitive_matching(self, execute: ExecuteFunction):
        data = execute("""
            MatchesLower = RegexListContains(list_name="regex", case_sensitive=True, terms=["frog"])
            MatchesUpper = RegexListContains(list_name="regex", case_sensitive=True, terms=["FROG"])
            MatchesMixed = RegexListContains(list_name="regex", case_sensitive=True, terms=["FrOg"])
        """)
        assert data['MatchesLower'] is True
        assert data['MatchesUpper'] is False
        assert data['MatchesMixed'] is False

    def test_case_sensitive_count(self, execute: ExecuteFunction):
        data = execute("""
            CountInsensitive = RegexListContainsCount(list_name="regex", terms=["FROG", "CAT", "HELLO"])
            CountSensitive = RegexListContainsCount(list_name="regex", case_sensitive=True, terms=["FROG", "CAT", "HELLO"])
        """)
        assert data['CountInsensitive'] == 3
        assert data['CountSensitive'] == 0

    def test_case_sensitive_items(self, execute: ExecuteFunction):
        data = execute("""
            ItemsInsensitive = RegexListContainsItems(list_name="regex", terms=["FROG", "CAT"])
            ItemsSensitive = RegexListContainsItems(list_name="regex", case_sensitive=True, terms=["FROG", "CAT"])
        """)
        assert set(cast(list[str], data['ItemsInsensitive'])) == {'fr[o0]g', 'c[a4]t'}
        assert data['ItemsSensitive'] == []


class TestCensoredListContainsUDF:
    """Tests for the CensoredListContains* UDFs"""

    def test_matches_plain_words(self, execute: ExecuteFunction):
        data = execute("""
            MatchesCat = CensoredListContains(list_name="censored", terms=["cat"])
            MatchesFrog = CensoredListContains(list_name="censored", terms=["frog"])
            MatchesHello = CensoredListContains(list_name="censored", terms=["hello"])
            MatchesDog = CensoredListContains(list_name="censored", terms=["dog"])
        """)
        assert data['MatchesCat'] is True
        assert data['MatchesFrog'] is True
        assert data['MatchesHello'] is True
        assert data['MatchesDog'] is False

    def test_matches_censored_variants(self, execute: ExecuteFunction):
        data = execute("""
            MatchesC4t = CensoredListContains(list_name="censored", terms=["c4t"])
            MatchesCAt = CensoredListContains(list_name="censored", terms=["c@t"])
            MatchesFr0g = CensoredListContains(list_name="censored", terms=["fr0g"])
            MatchesH3llo = CensoredListContains(list_name="censored", terms=["h3llo"])
            MatchesB0ar = CensoredListContains(list_name="censored", terms=["b0ar"])
        """)
        assert data['MatchesC4t'] is True
        assert data['MatchesCAt'] is True
        assert data['MatchesFr0g'] is True
        assert data['MatchesH3llo'] is True
        assert data['MatchesB0ar'] is True

    def test_matches_plurals(self, execute: ExecuteFunction):
        data = execute("""
            MatchesWithPlurals = CensoredListContains(list_name="censored", plurals=True, terms=["cats"])
            MatchesWithoutPlurals = CensoredListContains(list_name="censored", plurals=False, terms=["cats"])
            MatchesFrogs = CensoredListContains(list_name="censored", plurals=True, terms=["frogs"])
        """)
        assert data['MatchesWithPlurals'] is True
        assert data['MatchesWithoutPlurals'] is False
        assert data['MatchesFrogs'] is True

    def test_matches_substrings(self, execute: ExecuteFunction):
        data = execute("""
            MatchesWithSubstrings = CensoredListContains(list_name="censored", substrings=True, terms=["concatenate"])
            MatchesWithoutSubstrings = CensoredListContains(list_name="censored", substrings=False, terms=["concatenate"])
            MatchesFrogman = CensoredListContains(list_name="censored", substrings=True, terms=["frogman"])
        """)
        assert data['MatchesWithSubstrings'] is True
        assert data['MatchesWithoutSubstrings'] is False
        assert data['MatchesFrogman'] is True

    def test_must_be_censored(self, execute: ExecuteFunction):
        data = execute("""
            PlainCat = CensoredListContains(list_name="censored", must_be_censored=True, terms=["cat"])
            CensoredCat = CensoredListContains(list_name="censored", must_be_censored=True, terms=["c@t"])
            CensoredC4t = CensoredListContains(list_name="censored", must_be_censored=True, terms=["c4t"])
            PlainFrog = CensoredListContains(list_name="censored", must_be_censored=True, terms=["frog"])
            CensoredFr0g = CensoredListContains(list_name="censored", must_be_censored=True, terms=["fr0g"])
        """)
        assert data['PlainCat'] is False
        assert data['CensoredCat'] is True
        assert data['CensoredC4t'] is True
        assert data['PlainFrog'] is False
        assert data['CensoredFr0g'] is True

    def test_counts_matching_terms(self, execute: ExecuteFunction):
        data = execute("""
            Count = CensoredListContainsCount(list_name="censored", terms=["cat", "dog", "frog", "elephant"])
            CountCensored = CensoredListContainsCount(list_name="censored", terms=["c@t", "d0g", "fr0g", "elephant"])
            CountNone = CensoredListContainsCount(list_name="censored", terms=["dog", "elephant", "zebra"])
        """)
        assert data['Count'] == 2
        assert data['CountCensored'] == 2
        assert data['CountNone'] == 0

    def test_counts_with_plurals(self, execute: ExecuteFunction):
        data = execute("""
            CountWithPlurals = CensoredListContainsCount(list_name="censored", plurals=True, terms=["cats", "frogs", "dogs"])
            CountWithoutPlurals = CensoredListContainsCount(list_name="censored", plurals=False, terms=["cats", "frogs", "dogs"])
        """)
        assert data['CountWithPlurals'] == 2
        assert data['CountWithoutPlurals'] == 0

    def test_counts_with_must_be_censored(self, execute: ExecuteFunction):
        data = execute("""
            CountAll = CensoredListContainsCount(list_name="censored", terms=["cat", "c@t", "frog", "fr0g"])
            CountOnlyCensored = CensoredListContainsCount(list_name="censored", must_be_censored=True, terms=["cat", "c@t", "frog", "fr0g"])
        """)
        assert data['CountAll'] == 4
        assert data['CountOnlyCensored'] == 2

    def test_gets_matching_terms(self, execute: ExecuteFunction):
        data = execute("""
            Items = CensoredListContainsItems(list_name="censored", terms=["cat", "dog", "frog", "elephant"])
            ItemsCensored = CensoredListContainsItems(list_name="censored", terms=["c@t", "d0g", "fr0g"])
            ItemsNone = CensoredListContainsItems(list_name="censored", terms=["dog", "elephant"])
        """)
        assert set(cast(list[str], data['Items'])) == {'cat', 'frog'}
        assert set(cast(list[str], data['ItemsCensored'])) == {'c@t', 'fr0g'}
        assert data['ItemsNone'] == []

    def test_gets_items_with_plurals(self, execute: ExecuteFunction):
        data = execute("""
            ItemsWithPlurals = CensoredListContainsItems(list_name="censored", plurals=True, terms=["cats", "dogs", "frogs"])
            ItemsWithoutPlurals = CensoredListContainsItems(list_name="censored", plurals=False, terms=["cats", "dogs", "frogs"])
        """)
        assert set(cast(list[str], data['ItemsWithPlurals'])) == {'cats', 'frogs'}
        assert data['ItemsWithoutPlurals'] == []

    def test_gets_items_with_must_be_censored(self, execute: ExecuteFunction):
        data = execute("""
            ItemsAll = CensoredListContainsItems(list_name="censored", terms=["cat", "c@t", "frog", "fr0g"])
            ItemsOnlyCensored = CensoredListContainsItems(list_name="censored", must_be_censored=True, terms=["cat", "c@t", "frog", "fr0g"])
        """)
        assert set(cast(list[str], data['ItemsAll'])) == {'cat', 'c@t', 'frog', 'fr0g'}
        assert set(cast(list[str], data['ItemsOnlyCensored'])) == {'c@t', 'fr0g'}

    def test_handles_empty_terms(self, execute: ExecuteFunction):
        data = execute("""
            Result = CensoredListContains(list_name="censored", terms=[])
            Count = CensoredListContainsCount(list_name="censored", terms=[])
            Items = CensoredListContainsItems(list_name="censored", terms=[])
        """)
        assert data['Result'] is False
        assert data['Count'] == 0
        assert data['Items'] == []

    def test_combined_plurals_and_substrings(self, execute: ExecuteFunction):
        data = execute("""
            MatchesConcatenates = CensoredListContains(list_name="censored", plurals=True, substrings=True, terms=["concatenates"])
        """)
        assert data['MatchesConcatenates'] is True

    def test_combined_all_parameters(self, execute: ExecuteFunction):
        data = execute("""
            PlainSubstring = CensoredListContains(list_name="censored", plurals=True, substrings=True, must_be_censored=True, terms=["concatenates"])
            CensoredSubstring = CensoredListContains(list_name="censored", plurals=True, substrings=True, must_be_censored=True, terms=["conc4tenates"])
        """)
        # "concatenates" contains "cat" but it's not censored, so must_be_censored=True should reject it
        assert data['PlainSubstring'] is False
        assert data['CensoredSubstring'] is True
