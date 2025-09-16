import fnmatch
from functools import reduce
from hashlib import sha256
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Union

import deepmerge
import yaml

from .grammar import Source, Span
from .yaml import SafeDumper, SafeLineLoader, WithLineAndCol

SOURCE_ENTRY_POINT_PATH = 'main.sml'
CONFIG_PATH = 'config.yaml'


class Sources:
    """A collection of sources, and an arbitrary configuration which describes a set of imported rules and perhaps
    additional configuration that will be executed by the engine."""

    def __init__(self, sources: Dict[str, Source], config: Optional['SourcesConfig'] = None):
        assert SOURCE_ENTRY_POINT_PATH in sources, (
            "Sources requires a file with the `path` 'main.sml' to be present as the entry-point"
        )
        self._sources = sources
        self._config = config if config is not None else SourcesConfig(Source(path=CONFIG_PATH, contents=''))
        self._hash: Optional[str] = None

    @property
    def config(self) -> 'SourcesConfig':
        return self._config

    def get_by_path(self, path: str) -> Optional[Source]:
        """Gets a source that belongs to a given path, if it exists."""
        return self._sources.get(path)

    def get_entry_point(self) -> Source:
        """Gets the entry-point (or the first file to execute) within this source collection."""
        return self._sources[SOURCE_ENTRY_POINT_PATH]

    def __iter__(self) -> Iterator[Source]:
        return iter(self._sources.values())

    def __len__(self) -> int:
        return len(self._sources)

    def paths(self) -> Set[str]:
        """Returns the set of paths within this sources collection."""
        return set(self._sources.keys())

    def glob(self, pattern: str) -> List[Source]:
        """Returns a list of sources that match the given pattern."""
        matches = fnmatch.filter(self._sources.keys(), pattern)
        return [self._sources[k] for k in matches]

    def to_dict(self) -> Dict[str, str]:
        """The inverse of from_dict, serializes this Sources collection to a dictionary."""
        sources = list(self)
        if self._config.source.contents:
            sources.append(self._config.source)

        return {source.path: source.contents for source in sources}

    @staticmethod
    def from_dict(sources_dict: Dict[str, str]) -> 'Sources':
        """Creates a Sources object from a dict of path -> contents."""
        builder = SourcesBuilder()

        for path, contents in sources_dict.items():
            source = Source(path=path, contents=contents)
            if source.path == CONFIG_PATH:
                builder.add_config(source)
            else:
                builder.add_source(source)

        return builder.build()

    @staticmethod
    def from_path(root: Path) -> 'Sources':
        """Creates a Sources object, by using a given Path as the sources root."""
        builder = SourcesBuilder()

        for path in root.rglob('*.sml'):
            contents = path.read_text()
            path_str = '/'.join(path.relative_to(root).parts)
            builder.add_source(Source(path=path_str, contents=contents, actual_path=path.resolve()))

        config_paths = chain([root.joinpath(CONFIG_PATH)], root.rglob('config/*.yaml'))
        config_sources = [
            Source(path='/'.join(path.relative_to(root).parts), contents=path.read_text(), actual_path=path.resolve())
            for path in config_paths
            if path.exists()
        ]
        builder.add_config(*config_sources)

        return builder.build()

    def hash(self) -> str:
        """Returns the hash of the sources - a good way to quickly identify what sources are being executed."""
        if self._hash is None:
            hasher = sha256()
            # Sort sources by key for stable hashing.
            sources = list(self)
            sources.sort(key=lambda source: source.path)

            if any(_.contents for _ in self._config.sources):
                # Prepend this to maintain hash values with historical implementations
                sources = self._config.sources + sources

            for source in sources:
                hasher.update(source.path.encode('utf-8'))
                hasher.update(b'|')
                hasher.update(source.contents.encode('utf-8'))

            self._hash = hasher.hexdigest()

        return self._hash


class SourcesBuilder:
    """Takes care of building a Sources object. Once built, Sources are immutable, so the builder is the only phase
    at which you can mutate sources."""

    def __init__(self) -> None:
        self._sources: Dict[str, Source] = {}
        self._config: Optional['SourcesConfig'] = None
        self._config_sources: Dict[str, Source] = {}

    def add_source(self, source: Source) -> 'SourcesBuilder':
        """Adds a source to the sources collection."""
        assert source.path not in self._sources, 'A source with that path already exists.'
        assert source.path.endswith('.sml'), f'Source {source!r} does not end with .sml'
        self._sources[source.path] = source
        return self

    def add_config(self, *sources: Source) -> 'SourcesBuilder':
        if self._config is not None:
            raise ValueError('A configuration already exists for this builder.')

        for source in sources:
            assert source.path not in self._config_sources, 'A source with that path already exists.'
            assert source.path.endswith('.yaml'), f'Config {source!r} does not end with .yaml'
            self._config_sources[source.path] = source

        self._config = SourcesConfig(*self._config_sources.values())
        return self

    def build(self) -> Sources:
        return Sources(self._sources, config=self._config)


class SourcesConfig(Dict[str, Any]):
    """Wraps a configuration provided by a source, performing preliminary validation of its parsed
    contents, but not interpreting the content. This lets us side-load a "config" within the sources, that
    will generally have additional meaning outside of the rules engine, for example, doing event sampling
    or dropping within the sink. However, the configuration of the sinks is (generally) not a concern here,
    so we're just providing a container that contains the data we want to eventually want to interpret elsewhere.


    A config is a yaml encoded file, that consists of string keys, to arbitrary values. This class performs
    that validation."""

    def __init__(self, *sources: Source):
        assert sources, 'sources can not be empty'
        self._sources = sorted(sources, key=lambda source: source.path)

        if not all(_.contents for _ in self._sources):
            super().__init__()
            return

        configs = [yaml.load(_.contents, Loader=SafeLineLoader.with_source(_)) for _ in self._sources]

        if not all(isinstance(config, dict) for config in configs):
            raise TypeError('Config is not a yaml serialized dictionary.')

        config: Dict[str, Any] = reduce(deepmerge.merge_or_raise.merge, configs, {})

        if not isinstance(config, dict):
            raise TypeError('Config is not a yaml serialized dictionary.')

        for k in config:
            if not isinstance(k, str):
                raise TypeError(f'Config key {k!r} is not a string.')

        compiled_src = yaml.dump(config, Dumper=SafeDumper, indent=4, default_flow_style=False, sort_keys=False)
        self._source = Source(path='config.yaml', contents=compiled_src)

        super().__init__(config)

    @property
    def source(self) -> Source:
        return self._source

    @property
    def sources(self) -> List[Source]:
        return self._sources

    def closest_span_for_location(self, location: Sequence[Union[int, str]], key_only: bool) -> Span:
        """Given a location in the config, returns the span for the beginning of that location in the original source,
        or if we can't get the span for that element returns the span for the nearest possible parent.

        If key_only is True, will only try to return the location of the closest key, not of any values.
        """
        parents = []
        current_node: Any = self
        for location_part in location:
            try:
                next_node = current_node[location_part]
            except (IndexError, KeyError, TypeError):
                # If we hit an invalid location, just stop traversing and use what we *were* able to get.
                break

            if isinstance(location_part, str):
                # Get the str key from the current node and append that. This allows us to point to the key for this
                # node in case the node itself doesn't have line/column.
                (key,) = (k for k in current_node if k == location_part)
                parents.append(key)
            if not key_only:
                parents.append(next_node)

            current_node = next_node

        for node in reversed(parents):
            if isinstance(node, WithLineAndCol):
                return Span(node.source, start_line=node.line_num, start_pos=node.column_num)

        return Span(self._source, start_line=1, start_pos=0)
