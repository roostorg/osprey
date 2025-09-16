from enum import Enum


class BaggagePrefix(Enum):
    LOCAL = 'local.'
    PEER = 'peer.'
    ROOT = 'root.'

    def prepend(self, key):
        return f'{self.value}{key}'


class SpanAttributes:
    METHOD = 'method'
    OPERATION = 'operation'
    RESOURCE = 'resource'
    SERVICE = 'service'
    URL = 'url'
    API_VERSION = 'api.version'
