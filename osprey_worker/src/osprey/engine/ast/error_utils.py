import math
from collections.abc import Sequence
from dataclasses import dataclass

from osprey.engine.utils.types import add_slots

from .grammar import Span


@add_slots
@dataclass(frozen=True)
class SpanWithHint:
    """Holds a pair of (span, hint)"""

    span: Span
    hint: str


def _assert_valid_message(msg: str) -> str:
    """Some basic assertions about the format of our error strings."""
    if msg == '':
        return msg

    # We're using snippets, not sentences, so the first letter should not be upper cased and it should not end in a
    # period.
    starts_with_upper = msg[0].isupper()
    ends_with_period = msg[-1] == '.'
    # Separately, we're using backticks for symbols, rather than double quotes, (though this assumes that if you have a
    # backtick in the string then any double quotes are examples of strings in the language and are therefore okay).
    has_unescaped_double_quote = '"' in msg and '`' not in msg

    assert not starts_with_upper and not ends_with_period and not has_unescaped_double_quote, msg
    return msg


def render_span_context_with_message(
    message: str,
    span: Span,
    hint: str = '',
    additional_spans_message: str = '',
    additional_spans: Sequence[Span | SpanWithHint] = tuple(),
    message_type: str = 'error',
) -> str:
    """Given a span, a message, and a hint, print out a human readable error
    message pointing to the location of the span in the given source file,
    annotated with the message and hint.

    An error message looks like:
    ```
    error: the error message here
     --> src/test.py:1:6
       |
     1 | Foo = Bar(Baz, Qux)
       |       ^ hint
               | hint line 2
    ```
    """

    # Convert to span-with-hint pairs.
    additional_spans_with_hints = [
        SpanWithHint(additional, hint='') if isinstance(additional, Span) else additional
        for additional in additional_spans
    ]

    all_spans = [span, *(span_with_hint.span for span_with_hint in additional_spans_with_hints)]

    max_line_no = max(s.start_line for s in all_spans)
    line_char_length = max(math.ceil(math.log10(max_line_no + 1)), 3)
    left_padding = ' ' * (line_char_length + 2)

    # TODO: Colors?!
    parts = [f'{message_type}: {_assert_valid_message(message)}']

    def append_span(span_: Span, hint_: str, num_prefix: int | None = None) -> None:
        if num_prefix:
            num_prefix_str = f'({num_prefix}) '
        else:
            num_prefix_str = ''

        justified_line = str(span_.start_line).rjust(line_char_length, ' ')
        offset_padding = ' ' * (span_.start_pos + 1)
        source_lines = span_.source.lines
        source_line = source_lines[span_.start_line - 1]

        parts.extend(
            [
                f'--> {num_prefix_str}{span_.source.path}:{span_.start_line}:{span_.start_pos}',
                f'{left_padding}|',
                f' {justified_line} | {source_line}',
            ]
        )

        if not hint_:
            parts.append(f'{left_padding}|{offset_padding}^')
        else:
            for i, line in enumerate(hint_.splitlines(keepends=False)):
                if i == 0:
                    marker = '^'
                    gutter = '|'
                else:
                    marker = '|'
                    gutter = ' '

                parts.append(f'{left_padding}{gutter}{offset_padding}{marker} {_assert_valid_message(line)}')

    append_span(span, hint)

    if additional_spans_with_hints:
        if additional_spans_message:
            parts.append(f'~~~ {_assert_valid_message(additional_spans_message)}')

        for i, additional_span_with_hint in enumerate(additional_spans_with_hints, 1):
            append_span(additional_span_with_hint.span, additional_span_with_hint.hint, num_prefix=i)

    return '\n'.join(parts)
