import re
from typing import cast

import yaml
from osprey.engine.ast.sources import Sources
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.unicode_censored import CENSOR_CACHE
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG

logger = get_logger('list')


class ListNotFoundException(Exception):
    pass


class InvalidListFormatException(Exception):
    pass


class InvalidRegexListException(Exception):
    pass


class ListCache:
    def __init__(self, config: Config) -> None:
        # a cache of lists loaded from yaml
        self._cache: dict[tuple[str, bool], set[str]] = {}
        # same as above, but for regex pattern lists
        self._regex_cache: dict[tuple[str, bool], list[re.Pattern[str]]] = {}
        # same as above, but for lists that get unicode censor pattern creation
        self._censored_cache: dict[tuple[str, bool, bool], list[tuple[str, re.Pattern[str]]]] = {}
        # optional Sources that allows for specifying etcd sources instead of the base rules directory
        self._sources: Sources | None = None
        self._rules_path = config.get_str('OSPREY_RULES_PATH', 'example_rules')

    def _set_sources(self, sources: Sources):
        self._sources = sources

        self._cache.clear()
        self._regex_cache.clear()
        self._censored_cache.clear()

        logger.info('ListCache sources updated, cache cleared')

    def _load_list_data(self, list_name: str):
        """Load list data from a YAML file"""

        if self._sources:
            # build the matching sources
            list_path = f'lists/{list_name}.sml'
            matching_sources = [s for s in self._sources if s.path.endswith(list_path)]

            if matching_sources:
                source = matching_sources[0]

                try:
                    data = yaml.safe_load(source.contents)
                    logger.info(f'Loaded {list_name} from etcd sources')
                    return data
                except Exception as e:
                    logger.warning(f'Failed to load {list_name} from etcd sources. Falling back to disk: {e}')
                    # don't return, we'll fall back to loading from disk below

        # if no etcd sources are available or we failed to load a given list from etcd, we'll try
        # to fallback to disk to load it
        file_path = f'{self._rules_path}/lists/{list_name}.yaml'
        try:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            logger.info(f'Loaded {list_name} from disk')
            return data
        except Exception as e:
            logger.error(f'Failed to load {list_name} from disk: {e}')
            raise ListNotFoundException(list_name)

    def _load_list(self, list_name: str) -> list[str]:
        """Load the data for a list then validate it is a valid list object"""

        data = self._load_list_data(list_name)

        if not isinstance(data, list):
            raise InvalidListFormatException(list_name)

        return cast(list[str], data)

    def get_list(self, list_name: str, case_sensitive: bool):
        """Attempt to find the given list inside of the cache, otherwise attempt to load it from sources"""

        cache_key = (list_name, case_sensitive)

        if cache_key not in self._cache:
            str_list = self._load_list(list_name)

            if case_sensitive:
                processed_set = set(str_list)
            else:
                processed_set: set[str] = set()
                for item in str_list:
                    processed_set.add(item.lower())

            self._cache[cache_key] = processed_set

            logger.info(f'Cached list {list_name}')

        return self._cache[cache_key]

    def get_regex_list(self, list_name: str, case_sensitive: bool):
        """Attempt to find the given regex list inside of the cache, otherwise attempt to load it from sources"""

        cache_key = (list_name, case_sensitive)

        if cache_key not in self._regex_cache:
            str_list = self._load_list(list_name)

            compiled_patterns: list[re.Pattern[str]] = []
            flags = 0 if case_sensitive else re.IGNORECASE

            for item in str_list:
                try:
                    item = item.strip()
                    pattern = re.compile(item, flags)
                    compiled_patterns.append(pattern)
                except re.error as e:
                    logger.error(f'Found invalid regex pattern in regex list {list_name}: {e}')
                    raise InvalidRegexListException(f'Found invalid regex pattern in regex list {list_name}: {e}')

            self._regex_cache[cache_key] = compiled_patterns

            logger.info(f'Loaded and compiled regex patterns from {list_name}')

        return self._regex_cache[cache_key]

    def get_censored_regex_list(self, list_name: str, plurals: bool, substrings: bool):
        """Attempt to find the given censor list inside of the cache, otherwise attempt to load it from sources"""

        cache_key = (list_name, plurals, substrings)

        if cache_key not in self._censored_cache:
            str_list = self._load_list(list_name)

            compiled_patterns: list[tuple[str, re.Pattern[str]]] = []

            for item in str_list:
                pattern = CENSOR_CACHE.instance().get_censored_regex(item, plurals=plurals, substrings=substrings)
                compiled_patterns.append((item, pattern))

            self._censored_cache[cache_key] = compiled_patterns

            logger.info(f'Loaded and created censored regex for {list_name}')

        return self._censored_cache[cache_key]


def _init_list_cache() -> ListCache:
    return ListCache(CONFIG.instance())


LIST_CACHE: Singleton[ListCache] = Singleton(_init_list_cache)


class ListArgumentsBase(ArgumentsBase):
    list_name: str
    """Name of the list, which is the name of the YAML file minus the extension. For example, toxic_words.yaml would be named toxic_words"""

    terms: list[str | None]
    """List of strings to check for in the list"""


class ListContainsArguments(ListArgumentsBase):
    case_sensitive = False
    """Whether the terms should be checked with exact casing or not"""

    word_boundaries = True
    """Whether to use word boundaries or not when checking phrases"""


class ListContains(UDFBase[ListContainsArguments, bool]):
    """
    Return whether the given list contains any of the given terms
    """

    def execute(self, execution_context: ExecutionContext, arguments: ListContainsArguments) -> bool:
        list_items = LIST_CACHE.instance().get_list(arguments.list_name, case_sensitive=arguments.case_sensitive)

        for term in arguments.terms:
            if term is None:
                continue

            if not arguments.case_sensitive:
                term = term.lower()

            if arguments.word_boundaries:
                for item in list_items:
                    if re.search(rf'\b{re.escape(item)}\b', term):
                        return True
            else:
                for item in list_items:
                    if item in term:
                        return True

        return False


class ListContainsCount(UDFBase[ListContainsArguments, int]):
    """
    Find the number of term "hits" found in the given list
    """

    def execute(self, execution_context: ExecutionContext, arguments: ListContainsArguments) -> int:
        list_items = LIST_CACHE.instance().get_list(arguments.list_name, case_sensitive=arguments.case_sensitive)
        count = 0

        for term in arguments.terms:
            if term is None:
                continue

            if not arguments.case_sensitive:
                term = term.lower()

            if arguments.word_boundaries:
                for item in list_items:
                    if re.search(rf'\b{re.escape(item)}\b', term):
                        count += 1
                        break
            else:
                for item in list_items:
                    if item in term:
                        count += 1
                        break

        return count


class ListContainsItems(UDFBase[ListContainsArguments, list[str]]):
    """
    Find and return a list of terms that are in the given list
    """

    def execute(self, execution_context: ExecutionContext, arguments: ListContainsArguments) -> list[str]:
        list_items = LIST_CACHE.instance().get_list(arguments.list_name, case_sensitive=arguments.case_sensitive)
        found: list[str] = []

        for term in arguments.terms:
            if term is None:
                continue

            check_term = term.lower() if not arguments.case_sensitive else term

            if arguments.word_boundaries:
                for item in list_items:
                    if re.search(rf'\b{re.escape(item)}\b', check_term):
                        found.append(term)
                        break
            else:
                for item in list_items:
                    if item in check_term:
                        found.append(term)
                        break

        return found


class RegexListContainsArguments(ListArgumentsBase):
    case_sensitive = False
    """Whether the terms should be checked with exact casing or not"""


class RegexListContains(UDFBase[RegexListContainsArguments, bool]):
    """
    Return whether any of the terms are found in the given regex list
    """

    def execute(self, execution_context: ExecutionContext, arguments: RegexListContainsArguments) -> bool:
        patterns = LIST_CACHE.instance().get_regex_list(arguments.list_name, case_sensitive=arguments.case_sensitive)

        for term in arguments.terms:
            if not arguments.case_sensitive:
                term = term.lower()
            if any(pattern.search(term or '') for pattern in patterns):
                return True

        return False


class RegexListContainsCount(UDFBase[RegexListContainsArguments, int]):
    """
    Find the number of term "hits" found in the given list of regex
    """

    def execute(self, execution_context: ExecutionContext, arguments: RegexListContainsArguments) -> int:
        patterns = LIST_CACHE.instance().get_regex_list(arguments.list_name, case_sensitive=arguments.case_sensitive)

        if not arguments.case_sensitive:
            terms = {item.lower() for item in arguments.terms if item is not None}
        else:
            terms = set(arguments.terms)

        count = 0
        for term in terms:
            if not term:
                continue

            for pattern in patterns:
                if pattern.search(term):
                    count += 1
                    # break out of the pattern loop so we only count once for this term
                    break

        return count


class RegexListContainsItems(UDFBase[RegexListContainsArguments, list[str]]):
    """
    Find and return a list of terms that are in the given regex list
    """

    def execute(self, execution_context: ExecutionContext, arguments: RegexListContainsArguments) -> list[str]:
        patterns = LIST_CACHE.instance().get_regex_list(arguments.list_name, case_sensitive=arguments.case_sensitive)

        if not arguments.case_sensitive:
            terms = {item.lower() for item in arguments.terms if item is not None}
        else:
            terms = set(arguments.terms)

        found: list[str] = []
        for term in terms:
            if not term:
                continue

            for pattern in patterns:
                if pattern.search(term or ''):
                    found.append(term)
                    # break out of the pattern loop so we only append once for this term
                    break

        return found


class CensoredListContainsArguments(ListArgumentsBase):
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


class CensoredListContains(UDFBase[CensoredListContainsArguments, bool]):
    """
    Return whether any of the terms are found in the given censored regex list
    """

    def execute(self, execution_context: ExecutionContext, arguments: CensoredListContainsArguments) -> bool:
        patterns = LIST_CACHE.instance().get_censored_regex_list(
            arguments.list_name, plurals=arguments.plurals, substrings=arguments.substrings
        )

        for term in arguments.terms:
            if term is None:
                continue

            for plain, pattern in patterns:
                match = pattern.search(term)
                if not match:
                    continue

                if arguments.must_be_censored:
                    matched_word = match.group('word')
                    if matched_word.lower() == plain.lower():
                        continue

                return True

        return False


class CensoredListContainsCount(UDFBase[CensoredListContainsArguments, int]):
    """
    Find the number of term "hits" found in the given list of censored regex
    """

    def execute(self, execution_context: ExecutionContext, arguments: CensoredListContainsArguments) -> int:
        patterns = LIST_CACHE.instance().get_censored_regex_list(
            arguments.list_name, plurals=arguments.plurals, substrings=arguments.substrings
        )

        count = 0
        for term in arguments.terms:
            if not term:
                continue

            for plain, pattern in patterns:
                match = pattern.search(term)
                if not match:
                    continue

                if arguments.must_be_censored:
                    matched_word = match.group('word')
                    if matched_word.lower() == plain.lower():
                        continue

                count += 1
                # break out of the pattern loop so we only count once for this term
                break

        return count


class CensoredListContainsItems(UDFBase[CensoredListContainsArguments, list[str]]):
    """
    Find and return a list of terms that are in the given censored regex list
    """

    def execute(self, execution_context: ExecutionContext, arguments: CensoredListContainsArguments) -> list[str]:
        patterns = LIST_CACHE.instance().get_censored_regex_list(
            arguments.list_name, plurals=arguments.plurals, substrings=arguments.substrings
        )

        found: list[str] = []
        for term in arguments.terms:
            if not term:
                continue

            for plain, pattern in patterns:
                match = pattern.search(term)
                if not match:
                    continue

                if arguments.must_be_censored:
                    matched_word = match.group('word')
                    if matched_word.lower() == plain.lower():
                        continue

                found.append(term)
                # break out of the pattern loop so we only count once for this term
                break

        return found
