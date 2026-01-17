import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.unicode_censored import (
    StringCheckCensored,
    create_censored_regex,
)
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(
            StringCheckCensored,
        )
    ),
]


class TestCreateCensorizeRegex:
    """Tests for the create_censored_regex() helper function."""

    def test_basic_match(self) -> None:
        pattern = create_censored_regex('cat', include_plural=False, include_substrings=True)
        assert pattern.search('cat') is not None
        assert pattern.search('c@t') is not None
        assert pattern.search('C4T') is not None

    def test_plural_matching(self) -> None:
        pattern = create_censored_regex('cat', include_plural=True, include_substrings=True)
        assert pattern.search('cats') is not None
        assert pattern.search('c@ts') is not None
        assert pattern.search('cat') is not None  # should still match singular

    def test_no_plural_matching(self) -> None:
        pattern = create_censored_regex('cat', include_plural=False, include_substrings=False)
        match = pattern.search('cats')
        assert match is None

    def test_substring_matching(self) -> None:
        pattern = create_censored_regex('cat', include_plural=False, include_substrings=True)
        assert pattern.search('concatenate') is not None

    def test_no_substring_matching(self) -> None:
        pattern = create_censored_regex('cat', include_plural=False, include_substrings=False)
        assert pattern.search('concatenate') is None

    def test_word_boundary_matching(self) -> None:
        pattern = create_censored_regex('cat', include_plural=False, include_substrings=False)
        assert pattern.search('the cat sat') is not None
        assert pattern.search('cat') is not None
        assert pattern.search(' cat ') is not None

    def test_space_character_insertion(self) -> None:
        # the regex allows space-like chars between letters
        pattern = create_censored_regex('cat', include_plural=False, include_substrings=True)
        assert pattern.search('c___a__t') is not None
        assert pattern.search('c.a....t') is not None
        assert pattern.search('c@_a@#t') is not None


class TestCheckCensoredUDF:
    """Tests for the StringCheckCensored UDF."""

    def test_basic_match(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Result = StringCheckCensored(s="cat", pattern="cat")
        """)
        assert data['Result'] is True

    def test_censored_match(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Result = StringCheckCensored(s="c@t", pattern="cat")
        """)
        assert data['Result'] is True

    def test_no_match(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Result = StringCheckCensored(s="dog", pattern="cat")
        """)
        assert data['Result'] is False

    def test_unicode_lookalike_match(self, execute: ExecuteFunction) -> None:
        # using cyrillic 'а' which looks like latin 'a'
        data = execute("""
            Result = StringCheckCensored(s="cаt", pattern="cat")
        """)
        assert data['Result'] is True

        data = execute("""
            Result = StringCheckCensored(s="𝒞𝞪𝔗", pattern="cat")
        """)
        assert data['Result'] is True

    def test_plural_option_enabled(self, execute: ExecuteFunction) -> None:
        data = execute("""
            WithPlural = StringCheckCensored(s="cats", pattern="cat", plurals=True)
            WithoutPlural = StringCheckCensored(s="cats", pattern="cat", plurals=False)
        """)
        assert data['WithPlural'] is True
        assert data['WithoutPlural'] is False

    def test_substring_option(self, execute: ExecuteFunction) -> None:
        data = execute("""
            WithSubstring = StringCheckCensored(s="concatenate", pattern="cat", substrings=True)
            WithoutSubstring = StringCheckCensored(s="concatenate", pattern="cat", substrings=False)
        """)
        assert data['WithSubstring'] is True
        assert data['WithoutSubstring'] is False

    def test_must_be_censored_option(self, execute: ExecuteFunction) -> None:
        data = execute("""
            PlainText = StringCheckCensored(s="cat", pattern="cat", must_be_censored=True)
            CensoredText = StringCheckCensored(s="c@t", pattern="cat", must_be_censored=True)
        """)
        assert data['PlainText'] is False
        assert data['CensoredText'] is True

    def test_must_be_censored_with_surrounding_test(self, execute: ExecuteFunction) -> None:
        data = execute("""
            PlainText = StringCheckCensored(s="the cat sat", pattern="cat", must_be_censored=True)
            CensoredText = StringCheckCensored(s="the c@t sat", pattern="cat", must_be_censored=True)
        """)
        assert data['PlainText'] is False
        assert data['CensoredText'] is True

    def test_case_insensitive(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Upper = StringCheckCensored(s="CAT", pattern="cat")
            Mixed = StringCheckCensored(s="CaT", pattern="cat")
        """)
        assert data['Upper'] is True
        assert data['Mixed'] is True

    def test_with_special_chars_in_pattern(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Result = StringCheckCensored(s="a.b", pattern="a.b")
        """)
        assert data['Result'] is True

    def test_empty_string(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Result = StringCheckCensored(s="", pattern="cat")
        """)
        assert data['Result'] is False

    @pytest.mark.parametrize(
        'input_str,pattern,expected',
        [
            ('hello world', 'hello', True),
            ('h3ll0', 'hello', True),
            ('h e l l o', 'hello', False),
            ('HELLO', 'hello', True),
            ('dog', 'cat', False),
            ('', 'test', False),
            ('c@t', 'cat', True),
            ('h4ck3r', 'hacker', True),
            ('p@$$w0rd', 'password', True),
            ('t35t', 'test', True),
            ('1337', 'leet', True),
            ('n00b', 'noob', True),
            ('ph1sh', 'phish', True),
            ('саt', 'cat', True),
            ('руthоn', 'python', True),
            ('НЕLLО', 'hello', True),
            ('Ηello', 'hello', True),
            ('Αpple', 'apple', True),
            ('Βank', 'bank', True),
            ('Κing', 'king', True),
            ('Νice', 'nice', True),
            ('Οpen', 'open', True),
            ('Ρython', 'python', True),
            ('Τest', 'test', True),
            ('Χmas', 'xmas', True),
            ('Υes', 'yes', True),
            ('Ζero', 'zero', True),
            ('𝐜𝐚𝐭', 'cat', True),
            ('𝑐𝑎𝑡', 'cat', True),
            ('𝒄𝒂𝒕', 'cat', True),
            ('𝓬𝓪𝓽', 'cat', True),
            ('𝔠𝔞𝔱', 'cat', True),
            ('𝕔𝕒𝕥', 'cat', True),
            ('𝖈𝖆𝖙', 'cat', True),
            ('𝗰𝗮𝘁', 'cat', True),
            ('𝘤𝘢𝘵', 'cat', True),
            ('𝙘𝙖𝙩', 'cat', True),
            ('𝚌𝚊𝚝', 'cat', True),
            ('ｃａｔ', 'cat', True),
            ('ｈｅｌｌｏ', 'hello', True),
            ('ＨＥＬＬＯ', 'hello', True),
            ('ᑕᗩT', 'cat', True),
            ('ꓚꓮT', 'cat', True),
            ('ⲤⲀT', 'cat', True),
            ('ԁоg', 'dog', True),
            ('bаnk', 'bank', True),
            ('pаypаl', 'paypal', True),
            ('аmаzоn', 'amazon', True),
            ('s3cur1ty', 'security', True),
            ('4dm1n', 'admin', True),
            ('r00t', 'root', True),
            ('z3r0', 'zero', True),
            ('0n3', 'one', True),
            ('tw0', 'two', True),
            ('с@т', 'cat', True),
            ('ρ@$$ωθrd', 'password', True),
            ('𝕙𝕒𝕔𝕜', 'hack', True),
            ('հello', 'hello', True),
            ('ոice', 'nice', True),
            ('քhone', 'phone', True),
            ('ցame', 'game', True),
            ('(at', 'cat', True),
            ('<at', 'cat', True),
            ('ca+', 'cat', True),
            ('ca7', 'cat', True),
            ('he!!o', 'hello', True),
            ('he||o', 'hello', True),
            ('c.a.t', 'cat', True),
            ('c_a_t', 'cat', True),
            ('c+a+t', 'cat', True),
            ('c/a/t', 'cat', True),
            ('c@a@t', 'cat', True),
            ('c#a#t', 'cat', True),
            # zero-width and invisible chars as separators
            ('c\u200bat', 'cat', True),
            ('c\u200cat', 'cat', True),
            ('c\u200dat', 'cat', True),
            ('ca\u200et', 'cat', True),
            ('ca\u200ft', 'cat', True),
            ('c\ufeffat', 'cat', True),
            # multiple zero-width chars
            ('c\u200b\u200bat', 'cat', True),
            ('c\u200b\u200c\u200dat', 'cat', True),
            # zero-width between every letter
            ('c\u200ba\u200bt', 'cat', True),
            ('h\u200be\u200bl\u200bl\u200bo', 'hello', True),
            ('c\u200b_a\u200c_t', 'cat', True),
            ('c.\u200ba.\u200bt', 'cat', True),
        ],
    )
    def test_various_inputs(self, execute: ExecuteFunction, input_str: str, pattern: str, expected: bool) -> None:
        data = execute(f"""
            Result = StringCheckCensored(s="{input_str}", pattern="{pattern}")
        """)
        assert data['Result'] is expected

    def test_leet_speak_variations(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Leet1 = StringCheckCensored(s="h3ll0", pattern="hello")
            Leet2 = StringCheckCensored(s="t35t", pattern="test")
        """)
        assert data['Leet1'] is True
        assert data['Leet2'] is True

    def test_with_separator_characters(self, execute: ExecuteFunction) -> None:
        data = execute("""
            Dots = StringCheckCensored(s="c.a.t", pattern="cat", substrings=True)
            Underscores = StringCheckCensored(s="c_a_t", pattern="cat", substrings=True)
            Mixed = StringCheckCensored(s="c.a_t", pattern="cat", substrings=True)
        """)
        assert data['Dots'] is True
        assert data['Underscores'] is True
        assert data['Mixed'] is True
