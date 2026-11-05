"""Redaction of configuration values for `osprey-cli config dump`.

Everything here is pure: no I/O, no logging. Rules, applied fail-closed:

- Key names are split into tokens (on `_`, `-`, `.` and camelCase) and redacted when a token is sensitive
  (`SECRET`, `TOKEN`, `PASSWORD`, `KEY`, `AUTH`, `DSN`, ...), or when the joined key contains a compound such as
  `password`, `apikey` or `serviceaccount`. Matching whole tokens keeps `KEYSPACE`, `KEEPALIVE` or `AUTHOR` visible.
- Values are redacted by shape: PEM blocks, JWTs and service-account JSON are replaced entirely; URLs keep
  scheme/host/port/path but lose their password (or the whole userinfo when it has no password) and the values of
  sensitive query parameters; long high-entropy tokens redact the whole value.
- Nested dicts and lists (JSON config values) are walked, with key rules applied to their keys.

The same rules are used by the coop config dump, so the two outputs look alike.
"""

import json
import math
import re
from collections import Counter
from typing import Any

REDACTED = '[REDACTED]'

_SENSITIVE_TOKENS = frozenset(
    {
        'apikey',
        'auth',
        'authentication',
        'authorization',
        'bearer',
        'cert',
        'certificate',
        'certs',
        'cookie',
        'cookies',
        'credential',
        'credentials',
        'creds',
        'dsn',
        'header',
        'headers',
        'jwt',
        'kek',
        'key',
        'keys',
        'passphrase',
        'passwd',
        'password',
        'passwords',
        'private',
        'pwd',
        'salt',
        'secret',
        'secrets',
        'session',
        'sessions',
        'signing',
        'token',
        'tokens',
        'webhook',
        'webhooks',
    }
)

# Matched against the key with separators removed, so `DBPASSWORD`, `ApiKey` and `SERVICE_ACCOUNT` are caught.
_SENSITIVE_COMPOUNDS = (
    'accesskey',
    'accesstoken',
    'apikey',
    'authtoken',
    'passwd',
    'password',
    'privatekey',
    'secretkey',
    'serviceaccount',
)

_CAMEL_BOUNDARY_RE = re.compile(r'(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])')
_TOKEN_SPLIT_RE = re.compile(r'[^A-Za-z0-9]+')

_PEM_RE = re.compile(r'-----BEGIN [A-Z0-9 ]+-----')
_JWT_RE = re.compile(r'eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*')
# userinfo runs from `://` to the last `@` before any `/`, `?`, `#` or whitespace, so a password containing `@`
# doesn't leak its tail.
_URL_USERINFO_RE = re.compile(r'(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)(?P<userinfo>[^\s/?#]*)@')
# Anything still shaped like `scheme://...@` after masking means we couldn't parse it safely.
_UNPARSED_USERINFO_RE = re.compile(r'://[^\s@]*@')
# `name=value` pairs in query strings, `;`-separated connection strings and `,`-separated lists
# (OTEL_RESOURCE_ATTRIBUTES).
_QUERY_PARAM_RE = re.compile(r'(?P<prefix>^|[?&;,\s])(?P<name>[A-Za-z0-9_.-]+)=(?P<value>[^&;,#\s]*)')

_ENTROPY_TOKEN_SPLIT_RE = re.compile(r'[\s,;&=]+')
_SEGMENT_SPLIT_RE = re.compile(r'[./?#:@]+')
_HEX_RE = re.compile(r'[0-9a-fA-F]+')
_ALNUM_RE = re.compile(r'[A-Za-z0-9]+')
_BASE64ISH_RE = re.compile(r'[A-Za-z0-9+/=_-]+')
_HIGH_ENTROPY_MIN_LENGTH = 32


def key_tokens(key: str) -> list[str]:
    """Splits a config key into lower-case tokens: `fooBar_BAZ-qux.KEY` -> `['foo', 'bar', 'baz', 'qux', 'key']`."""
    return [t.lower() for t in _TOKEN_SPLIT_RE.split(_CAMEL_BOUNDARY_RE.sub('_', key)) if t]


def is_sensitive_key(key: str) -> bool:
    """Whether a config key's name suggests its value is a secret."""
    tokens = key_tokens(key)
    if any(t in _SENSITIVE_TOKENS for t in tokens):
        return True
    joined = ''.join(tokens)
    if any(c in joined for c in _SENSITIVE_COMPOUNDS):
        return True
    # GCP_SA_JSON, GOOGLE_CREDENTIALS_JSON and friends.
    return bool({'gcp', 'google'} & set(tokens)) and bool({'json', 'sa'} & set(tokens))


def shannon_entropy(s: str) -> float:
    """Bits per character."""
    if not s:
        return 0.0
    n = len(s)
    return -sum(c / n * math.log2(c / n) for c in Counter(s).values())


def looks_high_entropy(token: str) -> bool:
    """Whether a token looks like a random secret rather than a name. All branches need 32+ characters and a digit:

    - hex with entropy >= 3.0 bits/char (hex digests and keys);
    - letters and digits only, with entropy >= 4.0 (single-case random tokens);
    - base64/url-safe characters with upper- and lower-case letters and entropy >= 3.5. Requiring mixed case keeps
      lower-case slugs such as `my-service-name-for-the-k8s-cluster-2024` visible.
    """
    if len(token) < _HIGH_ENTROPY_MIN_LENGTH or not any(c.isdigit() for c in token):
        return False
    entropy = shannon_entropy(token)
    if _HEX_RE.fullmatch(token):
        return entropy >= 3.0
    if _ALNUM_RE.fullmatch(token):
        return entropy >= 4.0
    if _BASE64ISH_RE.fullmatch(token):
        has_mixed_case = any(c.isupper() for c in token) and any(c.islower() for c in token)
        return has_mixed_case and entropy >= 3.5
    return False


def _contains_high_entropy_token(s: str) -> bool:
    # Each token is checked whole and split into segments, so secrets inside URLs or dotted keys
    # (`SG.<id>.<secret>`) are caught.
    for token in _ENTROPY_TOKEN_SPLIT_RE.split(s):
        if looks_high_entropy(token) or any(looks_high_entropy(seg) for seg in _SEGMENT_SPLIT_RE.split(token)):
            return True
    return False


def _mask_userinfo(match: re.Match[str]) -> str:
    userinfo = match.group('userinfo')
    if ':' in userinfo:
        masked = userinfo.split(':', 1)[0] + ':' + REDACTED
    else:
        masked = REDACTED
    return f'{match.group("scheme")}{masked}@'


def _mask_query_param(match: re.Match[str]) -> str:
    if not is_sensitive_key(match.group('name')) or not match.group('value'):
        return match.group(0)
    return f'{match.group("prefix")}{match.group("name")}={REDACTED}'


def redact_string(value: str) -> tuple[Any, bool]:
    """Redacts a string by its shape. Returns `(redacted, changed)`.

    A string holding a JSON object or array is parsed and redacted as a structure; the structure is returned.
    """
    if _PEM_RE.search(value) or _JWT_RE.search(value):
        return REDACTED, True

    stripped = value.strip()
    if stripped.startswith(('{', '[')):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            pass
        else:
            redacted, changed = redact_structure(parsed)
            return (redacted, True) if changed else (value, False)

    masked = _URL_USERINFO_RE.sub(_mask_userinfo, value)
    masked = _QUERY_PARAM_RE.sub(_mask_query_param, masked)
    if _UNPARSED_USERINFO_RE.search(masked.replace(f':{REDACTED}@', '').replace(f'{REDACTED}@', '')):
        return REDACTED, True
    if _contains_high_entropy_token(masked):
        return REDACTED, True
    return masked, masked != value


def _is_service_account_blob(value: dict[Any, Any]) -> bool:
    return 'private_key' in value or value.get('type') == 'service_account'


def redact_structure(value: Any) -> tuple[Any, bool]:
    """Redacts a value of any shape (scalar, string, list, dict) without regard to the key it is stored under.
    Returns `(redacted, changed)`."""
    if isinstance(value, str):
        return redact_string(value)
    if isinstance(value, dict):
        if _is_service_account_blob(value):
            return REDACTED, True
        out: dict[Any, Any] = {}
        changed = False
        for k, v in value.items():
            out[k], item_changed = redact_entry(str(k), v)
            changed = changed or item_changed
        return out, changed
    if isinstance(value, (list, tuple)):
        items = [redact_structure(v) for v in value]
        return [v for v, _ in items], any(c for _, c in items)
    return value, False


def redact_entry(key: str, value: Any, sensitive: bool | None = None) -> tuple[Any, bool]:
    """Redacts `value` stored under `key`. Returns `(redacted, changed)`.

    `sensitive` overrides the key-name rules: `True` always redacts, `False` marks a key whose name merely looks
    sensitive (e.g. an etcd key path) as safe. Value-shape rules apply either way. Missing (`None`) and empty
    values are returned as-is: they hide nothing, and "set but empty" is worth seeing.
    """
    if value is None or value == '':
        return value, False
    if sensitive is None:
        sensitive = is_sensitive_key(key)
    if sensitive:
        return REDACTED, True
    return redact_structure(value)
