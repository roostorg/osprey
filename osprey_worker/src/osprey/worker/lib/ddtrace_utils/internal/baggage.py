from typing import Optional, Union

from ddtrace.filters import TraceFilter
from ddtrace.span import Span

from ..constants import BaggagePrefix

Baggage = dict[str, str]

DEFAULT_BAGGAGE_PREFIX = 'baggage.'


class BaggageManager:
    """
    The BaggageManager provides support for managing tracing "baggage" between
    spans and their descendants, both within and across services; as well as for
    ensuring that baggage is properly exported to Datadog.

    The BaggageManager is not directly responsible for propagating baggage across
    service boundaries in either direction. Instead, the middleware (or equivalent)
    for the relevant libraries should propagate it using the appropriate manner,
    such as baggage headers, using methods from the BaggageManager to extract and
    inject it from or into spans.

    For more info, see:
    https://opentelemetry.io/docs/reference/specification/baggage/api/
    """

    # Ideally, we could instead implement baggage via something that is capable
    # of directly handling both span start and finish, like a
    # `ddtrace.internal.processor.SpanProcessor`. However, Datadog doesn't
    # actually let us set our own `SpanProcessor`s.
    # We could still reach in and create a custom `SpanProcessor` plus override
    # the tracer's `_span_processors`, but that seems a bit flakier than the
    # two-step `BaggageFilter` + `on_start_span` hook ¯\_(ツ)_/¯
    def __init__(self, baggage_prefix: str = DEFAULT_BAGGAGE_PREFIX):
        self._baggage_prefix = baggage_prefix

    @property
    def baggage_prefix(self) -> str:
        return self._baggage_prefix

    def trace_filter(self) -> TraceFilter:
        return _BaggageFilter(self.baggage_prefix)

    def start_span_callback(self, span: Span) -> None:
        if span._parent:  # Yeah, `_parent` is internal, c'est la vie
            for k, v in span._parent.get_tags().items():
                if isinstance(k, bytes):
                    k = k.decode('utf-8')
                if k.startswith(self.baggage_prefix):
                    span.set_tag(k, v)

    def store_baggage_item(
        self, span: Span, key: str, value: str, set_prefixes: bool = True, override_root: bool = True
    ) -> None:
        """
        Adds a single baggage key-value pair to a span.

        If the key already exists in the span's baggage, it will be overridden with
        the new value.

        If `set_prefixes` is `True`, then:
        - keys will be prefixed with the "local" prefix
        - keys will separately be prefixed with the "root" prefix if not already set

        Typically, `set_prefixes` should be `True` when setting values defined locally
        and `False` when propagating values extracted from external request.

        When `override_root` is `True`, the root value will be overridden with
        the new value if it already set and the current local value is the same.
        """

        if set_prefixes:
            root_key = f'{self.baggage_prefix}{BaggagePrefix.ROOT.prepend(key)}'
            local_key = f'{self.baggage_prefix}{BaggagePrefix.LOCAL.prepend(key)}'

            current_root_value = span.get_tag(root_key)
            current_local_value = span.get_tag(local_key)

            # Assumes that the current span, or at least service, is responsible for setting
            # the `root` tag value and that it is being deliberately updated here.
            # An example use case is overriding a value set in middleware or other standard
            # instrumentation.
            if override_root and (not current_root_value or current_root_value == current_local_value):
                span.set_tag(root_key, value)

            span.set_tag(local_key, value)
        else:
            span.set_tag(f'{self.baggage_prefix}{key}', value)

    def store_baggage_items(self, span: Span, baggage: Baggage, set_prefixes: bool = True) -> None:
        """
        Adds multiple baggage key-value pairs to a span.

        Any keys that already exist in the span's baggage will be overridden with
        the new values.
        """
        for k, v in baggage.items():
            self.store_baggage_item(span, k, v, set_prefixes)

    def get_baggage(self, span: Span) -> Baggage:
        """
        Returns the baggage from the span without removing it from the span.

        Useful when propagating baggage to downstream services.
        """
        baggage = {}

        for k, v in span.get_tags().items():
            if isinstance(k, bytes):
                k = k.decode('utf-8')
            if k.startswith(self.baggage_prefix):
                baggage[k[len(self.baggage_prefix) :]] = v

        return baggage


class _BaggageFilter(TraceFilter):
    def __init__(self, baggage_prefix: str = DEFAULT_BAGGAGE_PREFIX):
        self._baggage_prefix = baggage_prefix

    def process_trace(self, trace: list[Span]) -> Optional[list[Span]]:
        # Export the baggage to Datadog under the non-prefixed key
        for span in trace:
            # We need a new dictionary because you can't safely mutate a dict
            # while iterating over it
            tags: dict[Union[str, bytes], str] = {}

            for k, v in span.get_tags().items():
                if isinstance(k, bytes):
                    k = k.decode('utf-8')
                if k.startswith(self._baggage_prefix):
                    tags[k[len(self._baggage_prefix) :]] = v
                else:
                    tags[k] = v

            # Use the internal field rather than `set_tags` because we want
            # to actually replace the prefixed keys with the unprefixed ones,
            # whereas `set_tags` would append the unprefixed keys without removing
            # the unprefixed ones.
            # This can significantly affect the number of bytes we emit to Datadog.
            span._meta = tags

        return trace
