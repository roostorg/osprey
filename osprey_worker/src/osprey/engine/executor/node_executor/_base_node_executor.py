from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, Sequence, Type, TypeVar

from ddtrace.span import Span
from osprey.engine.ast.grammar import ASTNode

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from ..execution_context import ExecutionContext

T = TypeVar('T', bound=ASTNode)
V = TypeVar('V')


class BaseNodeExecutor(ABC, Generic[T, V]):
    """Subclass this to implement the execution of a given AST node."""

    # NOTE: mypy does not permit Type variables in ClassVars...but apparently hasn't
    # since 2018? https://github.com/python/mypy/issues/5144
    node_type: ClassVar[Type[T]]  # type: ignore[misc]

    def __init__(self, node: T, sources: 'ValidatedSources'):
        assert node.__class__ == self.node_type, 'Attempted to construct a node executor with the wrong type.'
        self._node = node

    def get_dependent_nodes(self) -> Sequence[ASTNode]:
        """Returns the nodes that this node depends on before it may execute. This is only called during the
        execution graph building phase, and not the execution phase. Thus, it's not worth caching the return
        value of this function anywhere."""
        return []

    def set_tracing_tags(self, span: 'Span') -> None:
        pass

    @abstractmethod
    def execute(self, execution_context: 'ExecutionContext') -> V:
        """Called when the node is being executed by the executor."""
        raise NotImplementedError

    @property
    def node(self) -> T:
        return self._node

    @property
    def execute_async(self) -> bool:
        """Whether this node has expensive/slow IO that would benefit from running asynchronously. Note that
        this is not a guarantee that it *will* run asynchronously in the executor, just a hint that it's a
        good candidate for asynchronous execution.
        """
        return False
