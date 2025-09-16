import datetime
from collections.abc import Set

import pytz

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore # https://github.com/python/mypy/issues/1153


class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, serializable_classes=(), serialize_patches=None, dictable_classes=None, *args, **kwargs):
        if serialize_patches is None:
            serialize_patches = {}
        if dictable_classes is None:
            dictable_classes = {}
        self.serializable_classes = serializable_classes
        self.serialize_patches = serialize_patches
        self.dictable_classes = dictable_classes
        super().__init__(*args, **kwargs)

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.replace(tzinfo=pytz.UTC).isoformat()
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, Set):
            return list(o)
        elif type(o) in self.dictable_classes:
            return dict(o)
        elif type(o) in self.serialize_patches:
            return self.serialize_patches[type(o)](o)
        elif isinstance(o, self.serializable_classes):
            return o.serialize()
        else:
            return super().default(o)


def parse_int_length(json_str: str, max_length: int = 1000) -> int:
    """
    Custom `parse_int` function to prevent strings of integers causing Denial-of-Service.

    For instance, without length check the following will timeout and fully utilize a worker:

        import json
        json.loads('9' * 10**8)

    Note: floating point values get translated into InF, so there is no DoS for floats.
    """
    if len(json_str) > max_length:
        raise ValueError(f'JSON string exceeds {max_length} characters for int conversion')

    return int(json_str)


class CustomJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs) -> None:
        if 'parse_int' not in kwargs:
            kwargs['parse_int'] = parse_int_length
        super().__init__(*args, **kwargs)
