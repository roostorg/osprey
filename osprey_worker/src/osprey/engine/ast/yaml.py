from __future__ import annotations

from collections.abc import Hashable, Iterator
from typing import TYPE_CHECKING, Any, ClassVar, Type, TypeVar

import yaml
from osprey.engine.ast.grammar import Source
from yaml.dumper import SafeDumper as _SafeDumper
from yaml.representer import SafeRepresenter

_T = TypeVar('_T', bound='WithLineAndCol')


class WithLineAndCol:
    """A mixin for an element that has the original line and column from a block of yaml text stored on it."""

    if TYPE_CHECKING:
        line_num: int
        column_num: int
        source: Source
    else:
        # NOTE: You can't put the real slots here, since otherwise you get
        #
        #   `TypeError: multiple bases have instance lay-out conflict`
        #
        # when trying to define subclasses that use this mixin with builtin classes (which is all of them). The
        # solution is to put the extra slots in the leaf class. We add an empty slots here to ensure we don't
        # accidentally add a `__dict__` to the final class.
        __slots__ = ()

    @classmethod
    def from_node(cls: Type[_T], orig: Any, node: yaml.Node, source: Source) -> '_T':
        instance = cls(orig)  # type: ignore[call-arg] # Mypy is confused here
        # Add 1 so line numbering starts at 1
        instance.line_num = node.start_mark.line + 1
        instance.column_num = node.start_mark.column
        instance.source = source
        return instance


class _ListWithLineAndCol(list[object], WithLineAndCol):
    __slots__ = ('line_num', 'column_num', 'source')


class _DictWithLineAndCol(dict[Hashable, Any], WithLineAndCol):
    __slots__ = ('line_num', 'column_num', 'source')


class _StrWithLineAndCol(str, WithLineAndCol):
    __slots__ = ('line_num', 'column_num', 'source')


class SafeLineLoader(yaml.SafeLoader):
    """Safely parses yaml, but where all dict and list instances have WithLineAndCol mixed in to them so you can
    recover the original line and column for any of them."""

    _source: ClassVar[Source]

    @classmethod
    def with_source(cls, source: Source) -> Type[SafeLineLoader]:
        """
        Return a SafeLineLoader with the current source installed.
        This will allow for any given loaded node to know which source file it came from
        """

        class SourcedSafeLineLoader(SafeLineLoader):
            _source: ClassVar[Source] = source

        return SourcedSafeLineLoader

    def construct_yaml_seq(self, node: yaml.SequenceNode) -> Iterator[_ListWithLineAndCol]:
        data = _ListWithLineAndCol.from_node([], node, source=self._source)
        yield data
        data.extend(self.construct_sequence(node))

    def construct_yaml_map(self, node: yaml.MappingNode) -> Iterator[_DictWithLineAndCol]:
        data = _DictWithLineAndCol.from_node({}, node, source=self._source)
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_yaml_str(self, node: yaml.Node) -> str:
        value = super().construct_yaml_str(node)
        return _StrWithLineAndCol.from_node(value, node, source=self._source)


SafeLineLoader.add_constructor('tag:yaml.org,2002:seq', SafeLineLoader.construct_yaml_seq)
SafeLineLoader.add_constructor('tag:yaml.org,2002:map', SafeLineLoader.construct_yaml_map)
SafeLineLoader.add_constructor('tag:yaml.org,2002:str', SafeLineLoader.construct_yaml_str)


class SafeDumper(_SafeDumper):
    """dumper to fix list indentation issues"""

    def increase_indent(self, flow: bool = False, *args: Any, **kwargs: Any) -> None:
        super().increase_indent(flow=flow, indentless=False)


SafeDumper.add_representer(_StrWithLineAndCol, SafeRepresenter.represent_str)
SafeDumper.add_representer(_DictWithLineAndCol, SafeRepresenter.represent_dict)
SafeDumper.add_representer(_ListWithLineAndCol, SafeRepresenter.represent_list)
