from re import compile, split, sub
from urllib.parse import quote_plus, unquote_plus

from osprey.worker.lib.ddtrace_utils.constants import BaggagePrefix
from osprey.worker.lib.ddtrace_utils.internal.baggage import Baggage

DEFAULT_BAGGAGE_HEADER = 'baggage'

_LOCAL_PREFIX_PATTERN = compile(rf'^{BaggagePrefix.LOCAL.value}')
_DELIMITER_PATTERN = compile(r'[ \t]*,[ \t]*')


class HTTPBaggagePropagator(object):
    """
    Propagates baggage via HTTP headers or an equivalent, such as gRPC metadata.

    See https://www.w3.org/TR/baggage/ for the W3C baggage specification.

    Note: The HTTPBaggagePropagator is not intended as a full span propagator!
    It is expected to be used in conjunction with a ddtrace context propagator,
    such as ddtrace's `HTTPPropagator`. This is to prevent awkward conflicts in
    expectations built in anywhere, since ddtrace is unaware of baggage.

    Additionally, the HTTPBaggagePropagator implements only a simplified version
    of the full W3C spec, e.g., it doesn't limit the number of baggage pairs
    potentially propagated.
    """

    def __init__(self, baggage_header: str = DEFAULT_BAGGAGE_HEADER):
        self._baggage_header = baggage_header
        self._possible_headers = [baggage_header]
        if not baggage_header.lower().startswith('x-'):
            self._possible_headers.append(f'x-{baggage_header}')

    def inject(self, baggage: Baggage, headers: dict[str, str]) -> None:
        # Extract any existing baggage first so it's not overwridden
        existing_baggage = self.extract(headers)
        existing_baggage.update(baggage)

        baggage = {}

        for k, v in existing_baggage.items():
            # Don't continue propagating `peer`-prefixed values
            if not k.startswith(BaggagePrefix.PEER.value):  # Skip `peer`-prefixed values
                # Propagate `local` values as `peer`
                k = sub(_LOCAL_PREFIX_PATTERN, BaggagePrefix.PEER.value, k)

                baggage[k] = v

        headers[self._baggage_header] = ','.join([f'{quote_plus(k)}={quote_plus(v)}' for k, v in baggage.items()])

    def extract(self, headers: dict[str, str]) -> Baggage:
        normalized_headers = {name.lower(): v for name, v in headers.items()}
        header = _extract_header_value(normalized_headers, self._possible_headers)
        entries = split(_DELIMITER_PATTERN, header)
        baggage = {}

        for entry in entries:
            try:
                key, value = entry.split('=', 1)
            except Exception:
                # Invalid baggage pair
                continue

            key = unquote_plus(key).strip().lower()
            value = unquote_plus(value).strip()

            baggage[key] = value

        return baggage


def _extract_header_value(headers: dict[str, str], possible_headers: list[str]) -> str:
    for header in possible_headers:
        try:
            return headers[header]
        except KeyError:
            pass

    return ''
