from typing import Mapping, Type, Union

from osprey.engine.ast.grammar import Number, UnaryOperation, UnaryOperator, USub
from osprey.engine.executor.node_executor.unary_operation_executor import _UNARY_OPERATORS


class OspreyUnaryExecutor:
    """There exists a need to execute certain Unary operations outside of the scope of the Osprey rule executor.

    This provides an interface to do that by running simple validations and executing the UnaryOperations.
    """

    # We only allow USub unary operations on number types at the moment. ie. Support negative numbers
    # There exists the possibility of extending what types we allow to be executed on in the future, if needed
    _valid_unary_ops_to_type: Mapping[Type[UnaryOperator], Type[Number]] = {USub: Number}

    def __init__(self, node: UnaryOperation):
        self._node = node
        self._executed_value = self._execute_unary_operation()

    def get_execution_value(self) -> Union[int, float]:
        return self._executed_value

    def get_modified_node(self) -> UnaryOperation:
        return self._node

    def _execute_unary_operation(self) -> Union[int, float]:
        if self._validate_unary_operation():
            assert isinstance(self._node.operand, self._valid_unary_ops_to_type[self._node.operator.__class__])
            executed_value: Union[int, float] = _UNARY_OPERATORS[self._node.operator.__class__](
                self._node.operand.value
            )
            self._node.operand.value = executed_value
            return executed_value
        else:
            raise Exception('Validation error when executing UnaryOperation.')

    def _validate_unary_operation(self) -> bool:
        if self._node.operator.__class__ in self._valid_unary_ops_to_type and isinstance(
            self._node.operand, self._valid_unary_ops_to_type[self._node.operator.__class__]
        ):
            return True
        return False
