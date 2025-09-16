from __future__ import annotations

import re
import string
import unicodedata
from itertools import chain
from typing import Dict, Iterator, List, Literal, Optional, Set, cast
from urllib.parse import ParseResult, urlparse, urlunparse

from osprey.engine.stdlib.udfs._prelude import (
    ArgumentsBase,
    ExecutionContext,
    UDFBase,
    ValidationContext,
)
from unidecode import unidecode

from .categories import UdfCategories


class StringArguments(ArgumentsBase):
    s: str


class StringLength(UDFBase[StringArguments, int]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringArguments) -> int:
        return len(arguments.s)


class StringToLower(UDFBase[StringArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringArguments) -> str:
        return arguments.s.lower()


class StringToUpper(UDFBase[StringArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringArguments) -> str:
        return arguments.s.upper()


class StringStartsWithArgument(StringArguments):
    s: str
    start: str


class StringStartsWith(UDFBase[StringStartsWithArgument, bool]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringStartsWithArgument) -> bool:
        return arguments.s.startswith(arguments.start)


class StringEndsWithArgument(StringArguments):
    end: str


class StringEndsWith(UDFBase[StringEndsWithArgument, bool]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringEndsWithArgument) -> bool:
        return arguments.s.endswith(arguments.end)


class StringStripArguments(StringArguments):
    chars: Optional[str] = None


class StringStrip(UDFBase[StringStripArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringStripArguments) -> str:
        return arguments.s.strip(arguments.chars)


class StringRStrip(UDFBase[StringStripArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringStripArguments) -> str:
        return arguments.s.rstrip(arguments.chars)


class StringLStrip(UDFBase[StringStripArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringStripArguments) -> str:
        return arguments.s.lstrip(arguments.chars)


class StringReplaceArguments(StringArguments):
    old: str
    new: str


class StringReplace(UDFBase[StringReplaceArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringReplaceArguments) -> str:
        return arguments.s.replace(arguments.old, arguments.new)


class StringJoinArguments(StringArguments):
    iterable: List[str]


class StringJoin(UDFBase[StringJoinArguments, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringJoinArguments) -> str:
        return arguments.s.join(arguments.iterable)


class StringSplitArguments(StringArguments):
    sep: Optional[str] = None
    maxsplit: int = -1


class StringSplit(UDFBase[StringSplitArguments, List[str]]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringSplitArguments) -> List[str]:
        return arguments.s.split(arguments.sep, arguments.maxsplit)


class StringCleaningArguments(StringArguments):
    form: str = 'NFKC'

    # normalizations happen in this order, so lower will win over upper
    remove_emoji: bool = False

    # reduce any repeated space to a single to a single normal space (U+0020)
    space: bool = True

    # remove any l33t code, these are homoglyphs that are less direct and may have contextual meaning
    l33t: bool = False

    # remove clear homoglyphs that are stylization of letters
    homoglyph: bool = True

    # deconstruct any combined unicode and keep the first char ('Ç' -> 'C')
    unicode_normalize: bool = True

    # replace any unicode with english transliteration (bad for l33t), good for roman-ish script 'Κνωσός' -> 'Knosos'
    unidecode: bool = False

    # uppercase the string (lower takes precedence over upper)
    upper: bool = False

    # lower case the string
    lower: bool = False

    # remove all spaces from the string
    remove_space: bool = False

    # remove all punctuation from the string (using unicodedata.category SP)
    remove_punctuation: bool = False


TranslationT = Dict[int, Optional[int]]


_SPACE_PATTERN: re.Pattern[str] = re.compile(r'\s+')

_EMOJI_PATTERN: re.Pattern[str] = re.compile(
    r'['
    r'\U0001F600-\U0001F64F'  # emoticons
    r'\U0001F300-\U0001F5FF'  # symbols & pictographs
    r'\U0001F680-\U0001F6FF'  # transport & map symbols
    r'\U0001F1E0-\U0001F1FF'  # flags (iOS)
    r']+',
    flags=re.UNICODE,
)

# sub for l33t -> leet
_L33T_THREES_SUB_PATTERN: re.Pattern[str] = re.compile(r'([A-z]?)(3+)([A-z]?)', flags=re.IGNORECASE)

# sub for |7 -> 17
_L33T_PIPE_NUMBER_SUB_PATTERN: re.Pattern[str] = re.compile(r'\|(\d)')

# homoglyphs for ascii letters from the homoglyphs lib
_HOMOGLYPHS = {
    'a': '⍺𝐀𝐚𝐴𝑎𝑨𝒂𝒜𝒶𝓐𝓪𝔄𝔞𝔸𝕒𝕬𝖆𝖠𝖺𝗔𝗮𝘈𝘢𝘼𝙖𝙰𝚊𝚨𝛂𝛢𝛼𝜜𝜶𝝖𝝰𝞐𝞪',
    'b': 'ℬ𝐁𝐛𝐵𝑏𝑩𝒃𝒷𝓑𝓫𝔅𝔟𝔹𝕓𝕭𝖇𝖡𝖻𝗕𝗯𝘉𝘣𝘽𝙗𝙱𝚋𝚩𝛣𝜝𝝗𝞑',
    'c': 'ℂℭ𝐂𝐜𝐶𝑐𝑪𝒄𝒞𝒸𝓒𝓬𝔠𝕔𝕮𝖈𝖢𝖼𝗖𝗰𝘊𝘤𝘾𝙘𝙲𝚌🝌',
    'd': 'ⅅⅆ𝐃𝐝𝐷𝑑𝑫𝒅𝒟𝒹𝓓𝓭𝔇𝔡𝔻𝕕𝕯𝖉𝖣𝖽𝗗𝗱𝘋𝘥𝘿𝙙𝙳𝚍',
    'e': '℮ℯℰⅇ⋿𝐄𝐞𝐸𝑒𝑬𝒆𝓔𝓮𝔈𝔢𝔼𝕖𝕰𝖊𝖤𝖾𝗘𝗲𝘌𝘦𝙀𝙚𝙴𝚎𝚬𝛦𝜠𝝚𝞔',
    'f': 'ℱ𝐅𝐟𝐹𝑓𝑭𝒇𝒻𝓕𝓯𝔉𝔣𝔽𝕗𝕱𝖋𝖥𝖿𝗙𝗳𝘍𝘧𝙁𝙛𝙵𝚏𝟊',
    'g': 'ℊ𝐆𝐠𝐺𝑔𝑮𝒈𝒢𝓖𝓰𝔊𝔤𝔾𝕘𝕲𝖌𝖦𝗀𝗚𝗴𝘎𝘨𝙂𝙜𝙶𝚐',
    'h': 'ℋℌℍℎ𝐇𝐡𝐻𝑯𝒉𝒽𝓗𝓱𝔥𝕙𝕳𝖍𝖧𝗁𝗛𝗵𝘏𝘩𝙃𝙝𝙷𝚑𝚮𝛨𝜢𝝜𝞖',
    'i': 'l˛ℹⅈ⍳𝐢𝑖𝒊𝒾𝓲𝔦𝕚𝖎𝗂𝗶𝘪𝙞𝚒𝚤𝛊𝜄𝜾𝝸𝞲',
    'j': 'ⅉ𝐉𝐣𝐽𝑗𝑱𝒋𝒥𝒿𝓙𝓳𝔍𝔧𝕁𝕛𝕵𝖏𝖩𝗃𝗝𝗷𝘑𝘫𝙅𝙟𝙹𝚓',
    'k': '𝐊𝐤𝐾𝑘𝑲𝒌𝒦𝓀𝓚𝓴𝔎𝔨𝕂𝕜𝕶𝖐𝖪𝗄𝗞𝗸𝘒𝘬𝙆𝙠𝙺𝚔𝚱𝛫𝜥𝝟𝞙',
    'l': '1I|ℐℑℒℓ∣⏽￨𝐈𝐋𝐥𝐼𝐿𝑙𝑰𝑳𝒍𝓁𝓘𝓛𝓵𝔏𝔩𝕀𝕃𝕝𝕴𝕷𝖑𝖨𝖫𝗅𝗜𝗟𝗹𝘐𝘓𝘭𝙄𝙇𝙡𝙸𝙻𝚕𝚰𝛪𝜤𝝞𝞘𝟏𝟙𝟣𝟭𝟷',
    'm': 'ℳ𝐌𝑀𝑴𝓜𝔐𝕄𝕸𝖬𝗠𝘔𝙈𝙼𝚳𝛭𝜧𝝡𝞛',
    'n': 'ℕ𝐍𝐧𝑁𝑛𝑵𝒏𝒩𝓃𝓝𝓷𝔑𝔫𝕟𝕹𝖓𝖭𝗇𝗡𝗻𝘕𝘯𝙉𝙣𝙽𝚗𝚴𝛮𝜨𝝢𝞜',
    'o': '0ℴ𝐎𝐨𝑂𝑜𝑶𝒐𝒪𝓞𝓸𝔒𝔬𝕆𝕠𝕺𝖔𝖮𝗈𝗢𝗼𝘖𝘰𝙊𝙤𝙾𝚘𝚶𝛐𝛔𝛰𝜊𝜎𝜪𝝄𝝈𝝤𝝾𝞂𝞞𝞸𝞼𝟎𝟘𝟢𝟬𝟶',
    'p': 'ℙ⍴𝐏𝐩𝑃𝑝𝑷𝒑𝒫𝓅𝓟𝓹𝔓𝔭𝕡𝕻𝖕𝖯𝗉𝗣𝗽𝘗𝘱𝙋𝙥𝙿𝚙𝚸𝛒𝛠𝛲𝜌𝜚𝜬𝝆𝝔𝝦𝞀𝞎𝞠𝞺𝟈',
    'q': 'ℚ𝐐𝐪𝑄𝑞𝑸𝒒𝒬𝓆𝓠𝓺𝔔𝔮𝕢𝕼𝖖𝖰𝗊𝗤𝗾𝘘𝘲𝙌𝙦𝚀𝚚',
    'r': 'ℛℜℝ𝐑𝐫𝑅𝑟𝑹𝒓𝓇𝓡𝓻𝔯𝕣𝕽𝖗𝖱𝗋𝗥𝗿𝘙𝘳𝙍𝙧𝚁𝚛',
    's': '𝐒𝐬𝑆𝑠𝑺𝒔𝒮𝓈𝓢𝓼𝔖𝔰𝕊𝕤𝕾𝖘𝖲𝗌𝗦𝘀𝘚𝘴𝙎𝙨𝚂𝚜',
    't': '⊤⟙𝐓𝐭𝑇𝑡𝑻𝒕𝒯𝓉𝓣𝓽𝔗𝔱𝕋𝕥𝕿𝖙𝖳𝗍𝗧𝘁𝘛𝘵𝙏𝙩𝚃𝚝𝚻𝛵𝜯𝝩𝞣🝨',
    'u': '∪⋃𝐔𝐮𝑈𝑢𝑼𝒖𝒰𝓊𝓤𝓾𝔘𝔲𝕌𝕦𝖀𝖚𝖴𝗎𝗨𝘂𝘜𝘶𝙐𝙪𝚄𝚞𝛖𝜐𝝊𝞄𝞾',
    'v': '∨⋁𝐕𝐯𝑉𝑣𝑽𝒗𝒱𝓋𝓥𝓿𝔙𝔳𝕍𝕧𝖁𝖛𝖵𝗏𝗩𝘃𝘝𝘷𝙑𝙫𝚅𝚟𝛎𝜈𝝂𝝼𝞶',
    'w': '𝐖𝐰𝑊𝑤𝑾𝒘𝒲𝓌𝓦𝔀𝔚𝔴𝕎𝕨𝖂𝖜𝖶𝗐𝗪𝘄𝘞𝘸𝙒𝙬𝚆𝚠',
    'x': '×╳⤫⤬⨯𝐗𝐱𝑋𝑥𝑿𝒙𝒳𝓍𝓧𝔁𝔛𝔵𝕏𝕩𝖃𝖝𝖷𝗑𝗫𝘅𝘟𝘹𝙓𝙭𝚇𝚡𝚾𝛸𝜲𝝬𝞦',
    'y': 'ℽ𝐘𝐲𝑌𝑦𝒀𝒚𝒴𝓎𝓨𝔂𝔜𝔶𝕐𝕪𝖄𝖞𝖸𝗒𝗬𝘆𝘠𝘺𝙔𝙮𝚈𝚢𝚼𝛄𝛶𝛾𝜰𝜸𝝪𝝲𝞤𝞬',
    'z': 'ℤℨ𐋵𝐙𝐳𝑍𝑧𝒁𝒛𝒵𝓏𝓩𝔃𝔷𝕫𝖅𝖟𝖹𝗓𝗭𝘇𝘡𝘻𝙕𝙯𝚉𝚣𝚭𝛧𝜡𝝛𝞕',
}

# extra homoglyphs that we have found useful
_HOMOGLYPHS_EXTRA = {
    'a': '@ªα∀⟑',
    'b': 'βВь฿',
    'c': '¢©Çç∁⊂ϲ',
    'd': 'ძ∂⫒',
    'e': 'ϱ€ℇ℮∃∈∑⋿',
    'f': 'ϝ៛⨍⨗⫭𐅿',
    'g': 'Ԍց₲',
    'h': '₶ℏ⫲⫳',
    'i': 'ιї⫯',
    'j': 'ϳј⌡',
    'k': 'κϏ₭',
    'l': '|լ₤∟',
    'm': '₥≞⋔⨇⩋⫙',
    'n': 'Πηπ∏∩刀',
    'o': 'ºοօ☉⦿',
    'p': 'Ρρբ₱℗♇',
    'q': 'ҩԛգզ৭',
    'r': '®ЯՒ𐅾',
    's': '$ѕ∫',
    't': 'τէ⊺♰♱⟙',
    'u': 'µ∐∪⨃',
    'v': '√∨⩔',
    'w': 'ω₩⨈⩊⫝',
    'x': '×☓✗⨯',
    'y': '¥ӱჄ⑂',
    'z': 'ՀჀꙀ',
}

_HOMOGLYPHS_ENCLOSED = {
    a: {chr(ord(enclosure) + (ord(a) - ord('a'))) for enclosure in ['⒜', 'Ⓐ', '🄐', '🄰', '🅐', '🅰', '🇦']}
    for a in string.ascii_lowercase
}

# translate emoji range lookalikes to ascii
_HOMOGLYPHS_EMOJI_TRANSLATION_TABLE: TranslationT = str.maketrans(
    {  # type: ignore[arg-type]
        glyph: ord(alpha)
        for alpha, glyphs in chain(
            _HOMOGLYPHS.items(),
            _HOMOGLYPHS_EXTRA.items(),
            _HOMOGLYPHS_ENCLOSED.items(),
        )
        for glyph in glyphs
        if _EMOJI_PATTERN.match(glyph)
    }
)

# translate unicode lookalikes to ascii
_HOMOGLYPHS_TRANSLATION_TABLE: TranslationT = str.maketrans(
    {  # type: ignore[arg-type]
        glyph: ord(alpha)
        for alpha, glyphs in chain(
            _HOMOGLYPHS.items(),
            _HOMOGLYPHS_EXTRA.items(),
            _HOMOGLYPHS_ENCLOSED.items(),
        )
        for glyph in glyphs
        if not (
            # digits that get in here somehow should be handled by 'l33t'
            glyph.isdigit()
            # a regular letter isn't a homoglyph for this, handle that in 'l33t'
            # e.g. 'I' != 'l' for homoglyphs, but might in the right context
            or glyph in string.ascii_letters
        )
    }
)


class StringClean(UDFBase[StringCleaningArguments, str]):
    """
    String cleaning swiss army knife
    """

    category = UdfCategories.STRING

    def __init__(self, validation_context: ValidationContext, arguments: StringCleaningArguments):
        super().__init__(validation_context, arguments)
        if arguments.form not in ['NFC', 'NFKC', 'NFD', 'NFKD']:
            call_node = arguments.get_call_node()
            validation_context.add_error(
                message='invalid value for `form`',
                span=call_node.span,
                hint=(f'`form` must be one of `NFC`, `NFKC`, `NFD`, or `NFKD`, not `{arguments.form}`'),
            )

    @staticmethod
    def _sub_l33t_3_to_e_helper(m: re.Match[str]) -> str:
        # TODO: there is probably a much better solution for this
        return f'{m[1]}{"e" * len(m[2])}{m[3]}' if m[1] or m[3] else m[2]

    def execute(self, execution_context: ExecutionContext, arguments: StringCleaningArguments) -> str:
        s = arguments.s

        if arguments.remove_emoji:
            if arguments.homoglyph:
                # the intent is probably not to remove these
                s = s.translate(_HOMOGLYPHS_EMOJI_TRANSLATION_TABLE)
            s = _EMOJI_PATTERN.sub(r' ', s)

        if arguments.space:
            s = _SPACE_PATTERN.sub(r' ', s)

        if arguments.l33t:
            s = _L33T_PIPE_NUMBER_SUB_PATTERN.sub(r'1\1', s)
            s = _L33T_THREES_SUB_PATTERN.sub(self._sub_l33t_3_to_e_helper, s)

        if arguments.homoglyph:
            s = s.replace('ℹ︎', 'i')  # ℹ︎ is multi byte and is incompatible with str.translate
            s = s.translate(_HOMOGLYPHS_TRANSLATION_TABLE)  # needs to go after l33t regex work

        if arguments.unicode_normalize:
            # We know that arguments.form has type Literal[...] because of the validation in __init__.
            # Ideally we could type this in StringCleaningArguments but Osprey's type evaluator
            # doesn't support Literals so we keep it as str and cast it here
            arguments.form = cast(Literal['NFC', 'NFKC', 'NFD', 'NFKD'], arguments.form)
            new_s = unicodedata.normalize(arguments.form, s)

            if len(s) != len(new_s):
                # the new string had multi-byte chars in it, remove them individually
                new_s = ''.join(unicodedata.normalize(arguments.form, _)[0] for _ in s)

            s = new_s

        if arguments.unidecode:
            s = unidecode(s)

        if arguments.upper and not arguments.lower:
            s = s.upper()

        if arguments.lower:
            s = s.lower()

        if arguments.remove_space:
            s = _SPACE_PATTERN.sub(r'', s)

        if arguments.remove_punctuation:
            s = ''.join(ch for ch in s if unicodedata.category(ch)[0] not in 'SP')

        return s


class StringExtractDomains(UDFBase[StringArguments, List[str]]):
    """
    Used to extract a list of potential URL domains from a string of tokens. Returns a list
    of candidate domains encountered in the input string. Should be used in conjunction with
    other UDFs that expect a domain as an input
    """

    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringArguments) -> List[str]:
        # split the message into individual tokens as based on a modified URL regex from messages_common.
        # should capture space based links and markdown based links without duplication.
        potential_urls: Iterator[ParseResult] = (
            urlparse(token) for token in re.findall('(https?:\/\/[^\/\s][^\s\)>]+)', arguments.s)
        )

        # filter out any tokens that do not have a scheme or a domain
        valid_domains: Set[str] = set(url.netloc.split(':')[0] for url in potential_urls if url.scheme and url.netloc)

        # return any valid domains encountered in the message
        return list(valid_domains)


class StringExtractURLs(UDFBase[StringArguments, List[str]]):
    """
    Used to extract a list of potential URLs from a string of tokens. Returns a list
    of candidate URLs encountered in the input string. Should be used in conjunction with
    other UDFs that expect an url as an input
    """

    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: StringArguments) -> List[str]:
        # split the message into individual tokens as based on a modified URL regex from messages_common.
        # should capture space based links and markdown based links without duplication.
        potential_urls: Iterator[ParseResult] = (
            urlparse(token) for token in re.findall('(https?:\/\/[^\/\s][^\s\)>]+)', arguments.s)
        )

        # filter out any tokens that do not have a scheme or a domain
        valid_urls: Set[str] = set(
            urlunparse(parsed_url) for parsed_url in potential_urls if parsed_url.scheme and parsed_url.netloc
        )

        # return any valid urls encountered in the message
        return list(valid_urls)
