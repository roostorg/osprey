from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type

from osprey.engine.ast.grammar import ASTNode

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from .node_executor._base_node_executor import BaseNodeExecutor


class NodeExecutorRegistry:
    """Holds a mapping of ASTNode -> BaseNodeExecutor, which will be used by the execution graph to build the
    dependency chains."""

    _instance: ClassVar[Optional['NodeExecutorRegistry']] = None

    @classmethod
    def get_instance(cls) -> 'NodeExecutorRegistry':
        """Gets the global singleton node executor registry."""
        if cls._instance is None:
            cls._instance = cls()

        return cls._instance

    @classmethod
    def register_globally(cls, node_executor: Type['BaseNodeExecutor[Any, Any]']) -> Type['BaseNodeExecutor[Any, Any]']:
        """Registers a node executor with the global node executor registry."""
        return cls.get_instance().register(node_executor)

    def __init__(self) -> None:
        self._registered_executors: Dict[Type[ASTNode], Type['BaseNodeExecutor[Any, Any]']] = {}

    def register(self, node_executor: Type['BaseNodeExecutor[Any, Any]']) -> Type['BaseNodeExecutor[Any, Any]']:
        """Registers a node executor with this node executor registry, throwing an error if an executor has already
        been registered for a given node-type."""
        node_type = node_executor.node_type
        if node_type in self._registered_executors:
            raise Exception(
                f'An executor for that node type has already been registered: {self._registered_executors[node_type]!r}'
            )

        self._registered_executors[node_type] = node_executor
        return node_executor

    def construct_executor_for(
        self, node: ASTNode, validated_sources: 'ValidatedSources'
    ) -> 'BaseNodeExecutor[Any, Any]':
        """Constructs an executor for a AST node using the registered executor for that node type."""
        return self._registered_executors[node.__class__](node, validated_sources)
