import string
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, cast

import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.string import (
    StringClean,
    StringEndsWith,
    StringExtractDomains,
    StringExtractURLs,
    StringJoin,
    StringLength,
    StringLStrip,
    StringReplace,
    StringRStrip,
    StringSplit,
    StringStartsWith,
    StringStrip,
    StringTokenize,
    StringToLower,
    StringToUpper,
)
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(
            StringEndsWith,
            StringJoin,
            StringLength,
            StringLStrip,
            StringClean,
            StringReplace,
            StringRStrip,
            StringSplit,
            StringStartsWith,
            StringStrip,
            StringToLower,
            StringToUpper,
            StringExtractDomains,
            StringExtractURLs,
            StringTokenize,
        )
    ),
]

THE_QUICK_BROWN_FOX = 'The Quick Brown Fox'
THE_QUICK_BROWN_FOX_LIST = THE_QUICK_BROWN_FOX.split()
QUICK_BROWN_FOX_DOMAIN_1 = 'the.quick.brown'
QUICK_BROWN_FOX_DOMAIN_2 = 'fox.jumps.over.the'
QUICK_BROWN_FOX_URL_1 = f'https://{QUICK_BROWN_FOX_DOMAIN_1}'
QUICK_BROWN_FOX_URL_2 = f'https://{QUICK_BROWN_FOX_DOMAIN_2}'


def test_string_join(execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        Result = StringJoin(s=',', iterable={THE_QUICK_BROWN_FOX_LIST})
        """
    )
    assert data == {'Result': ','.join(THE_QUICK_BROWN_FOX_LIST)}


@pytest.mark.parametrize('sep,maxsplit', [(None, -1), (None, 2), ('o', -1), ('o', 2)])
def test_string_split(execute: ExecuteFunction, sep: Optional[str], maxsplit: int) -> None:
    sep_arg = '' if sep is None else f', sep="{sep}"'
    maxsplit_arg = '' if maxsplit == -1 else f', maxsplit={maxsplit}'

    data = execute(
        f"""
        Result = StringSplit(s="{THE_QUICK_BROWN_FOX}"{sep_arg}{maxsplit_arg})
        """
    )
    assert data == {'Result': THE_QUICK_BROWN_FOX.split(sep, maxsplit)}


@pytest.mark.parametrize(
    'input_str,length',
    [(THE_QUICK_BROWN_FOX, len(THE_QUICK_BROWN_FOX)), ('', 0)],
)
def test_string_length(execute: ExecuteFunction, input_str: str, length: int) -> None:
    result = execute(f'InputLength = StringLength(s="{input_str}")')
    assert result == {'InputLength': length}


def test_string_replace(execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        Result1 = StringReplace(s="{THE_QUICK_BROWN_FOX}", old="Quick", new="Slow")
        Result2 = StringReplace(s="{THE_QUICK_BROWN_FOX}", old="Fast", new="Slow")
        """
    )
    assert data == {'Result1': 'The Slow Brown Fox', 'Result2': THE_QUICK_BROWN_FOX}


@pytest.mark.parametrize(
    'UDF,func', [('StringStrip', str.strip), ('StringRStrip', str.rstrip), ('StringLStrip', str.lstrip)]
)
@pytest.mark.parametrize(
    'input,chars',
    [
        (THE_QUICK_BROWN_FOX, None),
        (f'   {THE_QUICK_BROWN_FOX}', None),
        (f'{THE_QUICK_BROWN_FOX}   ', None),
        (THE_QUICK_BROWN_FOX, 'The'),
        (THE_QUICK_BROWN_FOX, 'Fox'),
        (THE_QUICK_BROWN_FOX, 'T he'),
        (THE_QUICK_BROWN_FOX, 'Fo x'),
    ],
)
def test_string_strip(
    execute: ExecuteFunction,
    UDF: str,
    func: Callable[..., str],
    input: str,
    chars: Optional[str],
) -> None:
    chars_arg = '' if chars is None else f', chars="{chars}"'
    data = execute(
        f"""
        Result = {UDF}(s="{input}"{chars_arg})
        """
    )
    assert data == {'Result': func(input, chars)}


def test_string_starts_with(execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        YesResult = StringStartsWith(s="{THE_QUICK_BROWN_FOX}", start="The Quick")
        NoResult = StringStartsWith(s="{THE_QUICK_BROWN_FOX}", start="Brown Fox")
        """
    )
    assert data == {'YesResult': True, 'NoResult': False}


def test_string_ends_with(execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        YesResult = StringEndsWith(s="{THE_QUICK_BROWN_FOX}", end="Brown Fox")
        NoResult = StringEndsWith(s="{THE_QUICK_BROWN_FOX}", end="The Quick")
        """
    )
    assert data == {'YesResult': True, 'NoResult': False}


def test_string_to_lower(execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        English = StringToLower(s="{THE_QUICK_BROWN_FOX}")
        Greek = StringToLower(s="ΑΒΓ")
        """
    )
    assert data == {
        'English': THE_QUICK_BROWN_FOX.lower(),
        'Greek': 'αβγ',
    }


def test_string_to_upper(execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        English = StringToUpper(s="{THE_QUICK_BROWN_FOX}")
        Greek = StringToUpper(s="αβγ")
        """
    )
    assert data == {
        'English': THE_QUICK_BROWN_FOX.upper(),
        'Greek': 'ΑΒΓ',
    }


@dataclass
class Scenario:
    s: str
    expects: Union[str, Iterable[str]] = ('text',)
    not_expects: Iterable[str] = ('|',)


@pytest.mark.parametrize(
    's',
    [
        Scenario('🔞t3xt'),
        Scenario('ΠOPQ 100-', ['nop', '100']),
        Scenario('Te.xt'),
        Scenario('∏opq', 'nopq'),
        Scenario('dat@ 2334', ['data', '2334']),
        Scenario('t3xt |00-', ['100']),
        Scenario('𝐓𝐄𝐗𝐓'),
        Scenario('𝚃𝙴𝚇𝚃'),
        Scenario('tex刀', 'texn'),
        Scenario('te×ts'),
        Scenario('∏opq', 'nopq'),
        Scenario('nopq', 'nopq'),
        Scenario('t3x∩', 'texn'),
        Scenario('texη', 'texn'),
        Scenario('Ç', ['c']),
        Scenario('⒜,⒝,⒞,⒟,⒠,⒡,⒢,⒣,⒤,⒥,⒦,⒧,⒨,⒩,⒪,⒫,⒬,⒭,⒮,⒯,⒰,⒱,⒲,⒳,⒴,⒵', string.ascii_lowercase),
        Scenario('Ⓐ,Ⓑ,Ⓒ,Ⓓ,Ⓔ,Ⓕ,Ⓖ,Ⓗ,Ⓘ,Ⓙ,Ⓚ,Ⓛ,Ⓜ,Ⓝ,Ⓞ,Ⓟ,Ⓠ,Ⓡ,Ⓢ,Ⓣ,Ⓤ,Ⓥ,Ⓦ,Ⓧ,Ⓨ,Ⓩ', string.ascii_lowercase),
        Scenario('🄐,🄑,🄒,🄓,🄔,🄕,🄖,🄗,🄘,🄙,🄚,🄛,🄜,🄝,🄞,🄟,🄠,🄡,🄢,🄣,🄤,🄥,🄦,🄧,🄨,🄩', string.ascii_lowercase),
        Scenario('🄰,🄱,🄲,🄳,🄴,🄵,🄶,🄷,🄸,🄹,🄺,🄻,🄼,🄽,🄾,🄿,🅀,🅁,🅂,🅃,🅄,🅅,🅆,🅇,🅈,🅉', string.ascii_lowercase),
        Scenario('🅐,🅑,🅒,🅓,🅔,🅕,🅖,🅗,🅘,🅙,🅚,🅛,🅜,🅝,🅞,🅟,🅠,🅡,🅢,🅣,🅤,🅥,🅦,🅧,🅨,🅩', string.ascii_lowercase),
        Scenario('🅰,🅱,🅲,🅳,🅴,🅵,🅶,🅷,🅸,🅹,🅺,🅻,🅼,🅽,🅾,🅿,🆀,🆁,🆂,🆃,🆄,🆅,🆆,🆇,🆈,🆉', string.ascii_lowercase),
        Scenario('🇦,🇧,🇨,🇩,🇪,🇫,🇬,🇭,🇮,🇯,🇰,🇱,🇲,🇳,🇴,🇵,🇶,🇷,🇸,🇹,🇺,🇻,🇼,🇽,🇾,🇿', string.ascii_lowercase),
        Scenario('plaϲeholder', 'placeholder'),  # the 'ϲ' is irregular
        Scenario('EXAMΡLE', 'example'),  # the 'P' is irregular
        Scenario('ｄｒｏｉｄ', 'droid'),  # the whole thing is irregular
        Scenario('🅳🆁🅾🅸🅳', 'droid'),
        # todo: handle this garbage
        # Scenario('\u032f\u034e\u0330\u032a\u032b', 'noidea'),
    ],
)
def test_string_normalization(s: Scenario, execute: ExecuteFunction) -> None:
    data = execute(
        f"""
        S = StringClean(s="{s.s}",
            l33t=True,
            homoglyph=True,
            space=True,
            unicode_normalize=True,
            remove_emoji=True,
            remove_punctuation=True,
            lower=True,
        )
        """
    )
    result = cast(str, data['S'])

    for e in [s.expects] if isinstance(s.expects, str) else s.expects:
        assert e in result

    for e in s.not_expects:
        assert e not in result


@pytest.mark.parametrize(
    'text,expected_result',
    [
        (f'https://{QUICK_BROWN_FOX_DOMAIN_1}', [QUICK_BROWN_FOX_DOMAIN_1]),  # simple domain
        (f'http://{QUICK_BROWN_FOX_DOMAIN_1}', [QUICK_BROWN_FOX_DOMAIN_1]),  # simple domain
        (
            f'http://{QUICK_BROWN_FOX_DOMAIN_1}/for/device?six=picturesque#locations',
            [QUICK_BROWN_FOX_DOMAIN_1],
        ),  # complex domain
        (
            f'http://{QUICK_BROWN_FOX_DOMAIN_1}/for/device?six=picturesque#locations and then a markdown [hello.com](http://{QUICK_BROWN_FOX_DOMAIN_2}/for/this).',
            [QUICK_BROWN_FOX_DOMAIN_1, QUICK_BROWN_FOX_DOMAIN_2],
        ),  # complex domain and markdown domain
        (
            f'this message contains whitespace https://{QUICK_BROWN_FOX_DOMAIN_1}',
            [QUICK_BROWN_FOX_DOMAIN_1],
        ),  # characters and whitespace before link
        (f'a domain without an http prefix is considered invalid {QUICK_BROWN_FOX_DOMAIN_1}', []),  # no http prefix
        (
            f'https://{QUICK_BROWN_FOX_DOMAIN_1} multiple links should be handled https://{QUICK_BROWN_FOX_DOMAIN_2}',
            [QUICK_BROWN_FOX_DOMAIN_1, QUICK_BROWN_FOX_DOMAIN_2],
        ),
        ('', []),  # empty string should not be considered a link
        (
            f'https://{QUICK_BROWN_FOX_DOMAIN_1} same domain should reduce to a set https://{QUICK_BROWN_FOX_DOMAIN_1}',
            [QUICK_BROWN_FOX_DOMAIN_1],
        ),
        (f'https:/{QUICK_BROWN_FOX_DOMAIN_1}', []),  # invalid url
        (f'https:///{QUICK_BROWN_FOX_DOMAIN_1}', []),  # invalid url
        ('https://[::1]:8080/path', ['[::1]']),  # valid IPv6 URL
        ('https://[invalid', []),  # invalid IPv6 URL (unclosed bracket) - should not raise ValueError
        ('check this out https://[bad::ipv6 click here', []),  # malformed IPv6 in text
    ],
)
def test_extract_domains(execute: ExecuteFunction, text: str, expected_result: List[str]) -> None:
    data: Dict[str, Any] = execute(
        f"""
        Result = StringExtractDomains(s="{text}")
        """
    )

    result: List[str] = data['Result']
    assert len(expected_result) == len(result)
    assert set(expected_result) == set(result)


@pytest.mark.parametrize(
    'text,expected_result',
    [
        (QUICK_BROWN_FOX_URL_1, [QUICK_BROWN_FOX_URL_1]),  # simple url
        (f'http://{QUICK_BROWN_FOX_DOMAIN_1}', [f'http://{QUICK_BROWN_FOX_DOMAIN_1}']),  # simple url
        (
            f'http://{QUICK_BROWN_FOX_DOMAIN_1}/for/device?six=picturesque#locations',
            [f'http://{QUICK_BROWN_FOX_DOMAIN_1}/for/device?six=picturesque#locations'],
        ),  # complex url
        (
            f'http://{QUICK_BROWN_FOX_DOMAIN_1}/for/device?six=picturesque#locations and then a markdown [hello.com](http://{QUICK_BROWN_FOX_DOMAIN_2}/for/this).',
            [
                f'http://{QUICK_BROWN_FOX_DOMAIN_1}/for/device?six=picturesque#locations',
                f'http://{QUICK_BROWN_FOX_DOMAIN_2}/for/this',
            ],
        ),  # complex domain and markdown domain
        (
            f'this message contains whitespace {QUICK_BROWN_FOX_URL_1}',
            [QUICK_BROWN_FOX_URL_1],
        ),  # characters and whitespace before link
        (f'an url without an http prefix is considered invalid {QUICK_BROWN_FOX_DOMAIN_1}', []),  # no http prefix
        (
            f'{QUICK_BROWN_FOX_URL_1} multiple links should be handled {QUICK_BROWN_FOX_URL_2}',
            [QUICK_BROWN_FOX_URL_1, QUICK_BROWN_FOX_URL_2],
        ),
        ('', []),  # empty string should not be considered a link
        (
            f'{QUICK_BROWN_FOX_URL_1} same url should reduce to a set {QUICK_BROWN_FOX_URL_1}',
            [QUICK_BROWN_FOX_URL_1],
        ),
        (f'https:/{QUICK_BROWN_FOX_DOMAIN_1}', []),  # invalid url
        (f'https:///{QUICK_BROWN_FOX_DOMAIN_1}', []),  # invalid url
        ('https://[::1]:8080/path', ['https://[::1]:8080/path']),  # valid IPv6 URL
        ('https://[invalid', []),  # invalid IPv6 URL (unclosed bracket) - should not raise ValueError
        ('check this out https://[bad::ipv6 click here', []),  # malformed IPv6 in text
    ],
)
def test_extract_urls(execute: ExecuteFunction, text: str, expected_result: List[str]) -> None:
    data: Dict[str, Any] = execute(
        f"""
        Result = StringExtractURLs(s="{text}")
        """
    )

    result: List[str] = data['Result']
    assert len(expected_result) == len(result)
    assert set(expected_result) == set(result)


@pytest.mark.parametrize(
    'text,expected_result',
    [
        ('the cat in the box', ['the', 'cat', 'in', 'the', 'box']),
        ('the Cat in the bOx', ['the', 'cat', 'in', 'the', 'box']),
        ("i'm going to the store", ["i'm", 'going', 'to', 'the', 'store']),
        ('hello. where are you going? over here!', ['hello', 'where', 'are', 'you', 'going', 'over', 'here']),
        ('hello123world', ['hello123world']),
        ('test 456 test', ['test', '456', 'test']),
        ('the   cat', ['the', 'cat']),
        ('hello\\tworld\\ntest', ['hello', 'world', 'test']),
        ('hello, world!', ['hello', 'world']),
        ('end. start', ['end', 'start']),
        ('café résumé', ['café', 'résumé']),
        ('donʼt', ["don't"]),  # curly apostrophe (u02bc)
        ('don’t', ["don't"]),  # curly apostrophe (u2019)
        ("cat's", ["cat's"]),
        ("''hello", ['hello']),
        ("test''test", ['test', 'test']),
    ],
)
def test_tokenize(execute: ExecuteFunction, text: str, expected_result: List[str]) -> None:
    data: Dict[str, Any] = execute(f"""
        Result = StringTokenize(s="{text}")
    """)

    result: List[str] = data['Result']
    assert len(expected_result) == len(result)
    assert expected_result == result
