class BaseEvent(object):
    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, ' '.join('%s=%r' % it for it in self.__dict__.items()))


class FullSyncOne(BaseEvent):
    """A key is being set to a given value."""

    @classmethod
    def from_node(cls, node):
        return cls(key=node.key, value=node.value)

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.key == other.key and self.value == other.value


class FullSyncOneNoKey(BaseEvent):
    """The key was deleted or does not exist."""

    def __init__(self, key):
        self.key = key

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.key == other.key


class FullSyncRecursive(BaseEvent):
    """Fully synchronize all values within a recursive watch."""

    def __init__(self, items):
        self.items = items

    def values(self):
        return [n.value for n in self.items]

    def as_dict(self):
        return {n.key: n.value for n in self.items}


class IncrementalSyncUpsert(BaseEvent):
    """A key within a recursive watch was created or updated."""

    @classmethod
    def from_node(cls, node):
        return cls(key=node.key, value=node.value)

    def __init__(self, key, value):
        self.key = key
        self.value = value


class IncrementalSyncDelete(BaseEvent):
    """A key within a recursive watch was deleted."""

    @classmethod
    def from_node(cls, node, prev_node):
        return cls(key=node.key, prev_value=prev_node.value)

    def __init__(self, key, prev_value):
        self.key = key
        self.prev_value = prev_value
