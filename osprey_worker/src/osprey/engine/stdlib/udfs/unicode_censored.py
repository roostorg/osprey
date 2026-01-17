import re
import unicodedata
from typing import Dict

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.string import StringArguments
from osprey.engine.stdlib.udfs.unicode_censored_charset import UNICODOE_CENSORED_LOOKALIKES
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.singleton import Singleton


def create_censored_regex(
    token: str,
    include_plural: bool,
    include_substrings: bool,
) -> re.Pattern[str]:
    """
    Create a compiled regex pattern that matches all variations of a token based on character replacement set.

    Args:
        token: The token to create a censored regex for
        include_plural: Will allow for each pattern to be compatible with plurals, i.e. if 'cat' is passed, the pattern will be 'cat[sS$]?'
        include_substrings: Whether substrings of characters are allowed, i.e. if "concatenate" will match "cat" or not
        char_set: The set of lookalike characters you wish to use. Defaults to the provided lookalike charset. Should be string literals for values,
            not regex patterns
    """

    token = token.lower()

    regex = ''

    # if we're not including substrings, start with forcing word boundary at the beginning of the string
    if not include_substrings:
        regex += r'(?:^|\W)'

    # create a capture group for the word
    regex += '(?P<word>'

    # start by looping over each character in the token
    for index, char in enumerate(token):
        char_variations = UNICODOE_CENSORED_LOOKALIKES.get(char)
        if char_variations is None:
            char_variations = [char]

        # place each possible character in the pattern
        regex += '['
        for char_variation in char_variations:
            regex += re.escape(char_variation)
        regex += ']'

        # if this isn't the last character in the token, we want to allow for "space" characters that people
        # often use, i.e. 'c#a#t' or 'c_a_t', as well as zero-width unicode characters used to evade filters
        if index < len(token) - 1:
            regex += r'[,.+/|&%#!@_\u200b\u200c\u200d\u200e\u200f\ufeff]*'

    # once we are at the end of the string, add on any possible s character if we are checking for substrings
    if include_plural:
        # start by adding our space characters
        regex += r'[,.+/|&%#!@_\u200b\u200c\u200d\u200e\u200f\ufeff]*'

        # grab the s variations from the charset
        s_variations = UNICODOE_CENSORED_LOOKALIKES.get('s', ['s', 'S'])

        # append each of the s variations
        regex += '['
        for s_variation in s_variations:
            regex += re.escape(s_variation)
        regex += ']?'

    # close the word capture group
    regex += ')'

    # follow up with a word boundary if we are not checking substrings
    if not include_substrings:
        regex += r'(?:\W|$)'

    return re.compile(regex)


class StringCensorCache:
    def __init__(self) -> None:
        self._cache: Dict[tuple[str, bool, bool], re.Pattern[str]] = {}

    def get_censored_regex(self, term: str, plurals: bool, substrings: bool) -> re.Pattern[str]:
        """
        Gets a regex pattern from the regex cache or creates a new one if it is not already in the cache.
        """

        cache_key = (term, plurals, substrings)

        if cache_key not in self._cache:
            pattern = create_censored_regex(term, include_plural=plurals, include_substrings=substrings)
            self._cache[cache_key] = pattern

        return self._cache[cache_key]


def _init_censor_cache() -> StringCensorCache:
    return StringCensorCache()


STRING_CENSOR_CACHE = Singleton(_init_censor_cache)


class StringCheckCensoredArguments(StringArguments):
    pattern: str
    """
    The string to create a regex pattern for.
    """

    plurals: bool = False
    """
    Whether to check for plurals of the string as well. I.e. if the input is 'cat', match both 'cat' and 'cats'.

    Default: False
    """

    substrings: bool = False
    """
    Whether to check substrings of the input string. I.e. 'concatenate' would match the pattern created for 'cat'.

    Default: False
    """

    must_be_censored: bool = False
    """
    Whether a string must be censored to return True. For example, 'cat' itself would return false but 'c@t'
    would return true.

    Default: False
    """


class StringCheckCensored(UDFBase[StringCheckCensoredArguments, bool]):
    """
    Checks a given string against another string's censored regex.
    """

    def execute(self, execution_context: ExecutionContext, arguments: StringCheckCensoredArguments) -> bool:
        normalized = unicodedata.normalize('NFKC', arguments.s)

        pattern = STRING_CENSOR_CACHE.instance().get_censored_regex(
            arguments.pattern, plurals=arguments.plurals, substrings=arguments.substrings
        )

        match = pattern.search(normalized)
        if match is None:
            return False

        if arguments.must_be_censored:
            matched_word = match.group('word')
            if matched_word.lower() == arguments.pattern.lower():
                return False

        return True
