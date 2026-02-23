import inspect
import textwrap
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Type, TypeVar, cast

import typing_inspect
from osprey.engine.executor.execution_context import ExecutionContext
from pydantic import BaseModel
from result import Result

from .arguments import ArgumentsBase, ArgumentSpec
from .rvalue_type_checker import RValueTypeChecker
from .type_helpers import is_typevar, to_display_str

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidationContext
    from osprey.engine.executor.node_executor.call_executor import CallExecutor


Arguments = TypeVar('Arguments', bound=ArgumentsBase)
BatchableArguments = TypeVar('BatchableArguments')
RValue = TypeVar('RValue')
NoneType = type(None)


class MethodSpec(BaseModel):
    name: str
    doc: str | None
    argument_specs: Sequence[ArgumentSpec]
    return_type: str
    category: str | None

    def stub(self) -> str:
        arg_declarations = ', '.join(arg.declaration() for arg in self.argument_specs)
        arg_param_docstrings = '\n'.join(filter(None, (arg.param_docstring() for arg in self.argument_specs)))

        docstring = (self.doc or '').strip()
        if arg_param_docstrings:
            if docstring:
                docstring += '\n'
            docstring += '\n' + arg_param_docstrings
        if '\n' in docstring:
            docstring += '\n'

        if not docstring:
            docstring = '    pass'
        else:
            docstring = textwrap.indent(f'"""{docstring}"""', '    ')

        return f'def {self.name}({arg_declarations}) -> {self.return_type}:\n{docstring}'


class UDFBase(Generic[Arguments, RValue], ABC):
    """Represents a user defined function (UDF) in the Osprey language.

    A minimal UDF is defined by setting the `Arguments` and `RValue` generic type variables when inheriting from this
    class and overriding the `execute` method to implement the actual behavior.

    Additional configuration and functionality is available via:

    - Overriding the constructor and performing additional validation, adding errors to the given `ValidationContext`
      instance if needed. The constructor can also get access to the AST if needed via methods on the given
      `Arguments` instance. Any `ConstExpr`s on the `Arguments` class will be resolved to their literal value by the
      time the constructor is called.

    - Setting the `execute_async` class variable (see the variable's declaration for more details on what this does)

    - Additionally inheriting from `OspreyInvariantGeneric` to create a generic function. There are two forms of
      generic functions:
        - **With a *generic* arguments class.** In this case, the arguments class must have at least one item
          (eg one kwarg) that is generic, and that item's type must be just the TypeVar or a simple wrapper around
          the TypeVar that has one parameter (i.e. T | None and list[T] are okay, but not dict[T, T]). We infer the
          value of the TypeVar to determine the return type of the function statically (more on returns below).
        - **With a *non-generic* arguments class.** In this case, we have no way to statically determine the return
          type, so we treat it as a dynamic return type. The function must be assigned to a type-annotated variable,
          and that type annotation has to be valid within our `RValueTypeChecker` system.

        In either case, all generic functions must have a generic return type (otherwise why is it generic?). Both the
        UDF classes and Arguments classes must inherit from `OspreyInvariantGeneric`, not the plain `typing.Generic`,
        though the return type may inherit from `typing.Generic` (eg `T | None`) or even just be a plain `TypeVar`
        (eg `T`).

        These restrictions are verified by a combination of getters on `UDFBase` and `ArgumentsBase` and extra
        assertions made when registering the UDF (see the `_assert_valid_generic` method in `registry.py`).
    """

    execute_async: ClassVar[bool] = False
    """Whether this function has expensive/slow IO that would benefit from running asynchronously. For more details see
    the `execute_async` field in `BaseNodeExecutor`.
    """

    category: ClassVar[str | None] = None
    """If present, a string name for what category this UDF falls into. Can be used for, say, grouping UDFs
    in documentation."""

    def __init__(self, validation_context: 'ValidationContext', arguments: Arguments):
        self._rvalue_type_checker: RValueTypeChecker | None = None

    def resolve_arguments(self, execution_context: ExecutionContext, call_executor: 'CallExecutor') -> Arguments:
        """Resolves the arguments once all dependent nodes have completed execution. The return value of this function
        is then passed to `execute`, and can be overridden to perform specialized argument resolution behavior within
        a given UDF."""
        resolved = {
            k: execution_context.resolved(
                v, return_none_for_failed_values=call_executor.unresolved_arguments.kwarg_can_be_none(k)
            )
            for k, v in call_executor.dependent_node_dict.items()
        }
        return cast(Arguments, call_executor.unresolved_arguments.update_with_resolved(resolved))

    @abstractmethod
    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        """Override this method to implement the UDF's execution logic. The arguments provided have been fully
        resolved."""
        raise NotImplementedError

    @classmethod
    def _get_udf_base_args(cls) -> tuple[type, ...]:
        for base in cls.__mro__:
            for generic_base in typing_inspect.get_generic_bases(base):
                if typing_inspect.get_origin(generic_base) in (UDFBase, BatchableUDFBase, QueryUdfBase):
                    return typing_inspect.get_args(generic_base)

        raise TypeError(f'Class does not inherit from generic UDFBase: {cls!r}')

    @classmethod
    def get_arguments_type(cls) -> Type[Arguments]:
        arguments_type = cls._get_udf_base_args()[0]
        # It's hard to have the typing_inspection stubs be good enough here.
        return cast(Type[Arguments], arguments_type)

    @classmethod
    def get_rvalue_type(cls) -> Type[RValue]:
        rvalue_type = cls._get_udf_base_args()[1]
        if rvalue_type is type(None):  # noqa E721
            rvalue_type = None  # type: ignore

        # It's hard to have the typing_inspection stubs be good enough here.
        return cast(Type[RValue], rvalue_type)

    @classmethod
    def has_result(cls) -> bool:
        return cls.get_rvalue_type() is not None

    @classmethod
    def is_generic(cls) -> bool:
        return len(typing_inspect.get_parameters(cls)) > 0

    @classmethod
    def has_dynamic_result(cls) -> bool:
        return cls.get_rvalue_type() is Any or (cls.is_generic() and not cls.get_arguments_type().is_generic())

    @classmethod
    def get_substituted_generic_rvalue_type(cls, resolved_typevar_type: type) -> type:
        """Get the concrete return type, using the given resolved type to fill in the generic return.

        If the UDF returns a `TypeVar` (eg `T`) then this will just return the given resolved type. If the UDF
        returns a generic type (eg `list[T | None]`) then this will return that type with the given type inserted
        into if (eg `list[str | None] if `resolved_generic_type` were `str`).

        Asserts that the UDF is generic.
        """
        assert cls.is_generic()
        assert resolved_typevar_type is not None

        # If we're generic, substitute this into our return type
        if is_typevar(cls.get_rvalue_type()):
            # The return type is just the bare typevar, so return the resolved typevar
            return resolved_typevar_type
        else:
            # If the type var is nested within another type (e.g. list[T]), then we perform typevar substitution
            return cls.get_rvalue_type()[resolved_typevar_type]  # type: ignore # Runtime typing

    def get_resolved_rvalue_type(self, resolved_typevar_type: type | None) -> type:
        if self.has_dynamic_result():
            assert resolved_typevar_type is None, 'Should not have resolved generic type with dynamic result'
            assert self._rvalue_type_checker is not None, (
                'Should have set rvalue type checker if we have a dynamic result.'
            )
            return self._rvalue_type_checker.to_typing_type()

        if resolved_typevar_type:
            return self.get_substituted_generic_rvalue_type(resolved_typevar_type)
        else:
            assert not self.is_generic()
            return self.get_rvalue_type()

    def set_rvalue_type_checker(self, rvalue_type_checker: RValueTypeChecker) -> None:
        assert self.has_dynamic_result(), 'Can only set RValueTypeChecker on Dynamic Result UDF'
        self._rvalue_type_checker = rvalue_type_checker

    def check_result_type(self, result: object) -> RValue:
        if self._rvalue_type_checker and not self._rvalue_type_checker.check(result):
            raise InvalidDynamicReturnType(
                self, expected_type=self._rvalue_type_checker.to_typing_type(), actual_type=type(result)
            )

        return cast(RValue, result)

    @classmethod
    def get_method_spec(cls) -> MethodSpec:
        doc = inspect.getdoc(cls)
        if doc in [inspect.getdoc(parent_class) for parent_class in cls.__mro__[1:]]:
            doc = None

        return MethodSpec(
            name=cls.__name__,
            doc=doc,
            argument_specs=cls.get_arguments_type().get_argument_specs(),
            return_type=to_display_str(cls.get_rvalue_type(), include_quotes=False),
            category=cls.category,
        )


class BatchableUDFBase(Generic[Arguments, RValue, BatchableArguments], UDFBase[Arguments, RValue]):
    """A base class for UDFs that can support batching"""

    execute_async = True

    @classmethod
    def get_batchable_arguments_type(cls) -> Type[BatchableArguments]:
        arguments_type = cls._get_udf_base_args()[2]
        # It's hard to have the typing_inspection stubs be good enough here.
        return cast(Type[BatchableArguments], arguments_type)

    @abstractmethod
    def get_batchable_arguments(self, arguments: Arguments) -> BatchableArguments:
        """Given a single argument, return the arguments that will be used in the batch endpoint."""
        raise NotImplementedError

    @abstractmethod
    def execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[BatchableArguments],
    ) -> Sequence[Result[RValue, Exception]]:
        """Given a batch of arguments, execute the UDF for all of them and return the results."""
        raise NotImplementedError

    def get_batch_routing_key(self, arguments: BatchableArguments) -> str:
        """Given `BatchableArguments`, return a key that will be used to route the batched arguments to the correct endpoint.
        This is important if you are using hash ring routing so that only the UDFs that share the same routing key will be batched.
        For example, if you want to ensure that all `Count` UDFs that share the same user are batched together, you would return the user id here.

        By default, this will return a string literal so that all executions of a given batchable UDF are able to be grouped ^^"""
        return 'lovely'


class QueryUdfBase(Generic[Arguments, RValue], UDFBase[Arguments, RValue]):
    """A base class for all UDFs intended to be used in queries."""

    # This is never actually executed in our rules engine, it's converted to a Druid query. Re-define it here so that
    # subclasses don't have to define it to be non-abstract. We take it on honor system that this returns the right
    # type.
    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        raise NotImplementedError

    @abstractmethod
    def to_druid_query(self) -> dict[str, object]:
        raise NotImplementedError


class InvalidDynamicReturnType(Exception):
    def __init__(self, udf: UDFBase[Any, Any], expected_type: type, actual_type: type) -> None:
        self.udf = udf
        self.expected_type = expected_type
        self.actual_type = actual_type

    def __str__(self) -> str:
        return (
            f'Function {to_display_str(type(self.udf))} with dynamic return type returned'
            f' {to_display_str(self.actual_type)} but was expected to return {to_display_str(self.expected_type)}.'
        )
