import ast
import inspect
import textwrap
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Dict, Generic, Iterator, List, Optional, Sequence, Type, TypeVar, Union, cast, get_type_hints

import typing_inspect
from osprey.engine.ast import grammar
from pydantic import BaseModel

from .type_helpers import UnsupportedTypeError, get_osprey_generic_param, is_typevar, to_display_str

T = TypeVar('T')
DefaultT = TypeVar('DefaultT', None, bool, int, float, str)
_dummy_span = grammar.Span(source=grammar.Source(path='<NOT A REAL PATH>', contents=''), start_line=1, start_pos=0)

# If an Arguments class has this attribute as a dict then extra arguments (otherwise unspecified on the arguments class)
# will be collected in it. The arguments will be typechecked against the value type, e.g. extra_args: Dict[str, int]
# would require all extra arguments to be ints
EXTRA_ARGS_ATTR = 'extra_arguments'


class ConstExpr(Generic[T]):
    """A constant expression value."""

    def __init__(self, name: str, value: T, literal: grammar.Literal, argument_span: grammar.Span):
        self.name = name
        self.value = value
        self.literal = literal
        self.argument_span = argument_span

    @classmethod
    def for_default(cls, name: str, value: DefaultT) -> 'ConstExpr[DefaultT]':
        literal: grammar.Literal
        if value is None:
            literal = grammar.None_(span=_dummy_span)
        elif isinstance(value, bool):
            # Need to check bool before int, to be more specific
            literal = grammar.Boolean(value=value, span=_dummy_span)
        elif isinstance(value, (int, float)):
            literal = grammar.Number(value=value, span=_dummy_span)
        else:
            literal = grammar.String(value=value, span=_dummy_span)

        instance: ConstExpr[DefaultT] = ConstExpr(name, value, literal, _dummy_span)
        return instance

    @staticmethod
    def get_type_class(cls: 'Type[ConstExpr[T]]') -> Type[T]:
        type_class = typing_inspect.get_args(cls)[0]
        origin = typing_inspect.get_origin(type_class)
        # It's hard to have the typing_inspection stubs be good enough here.
        return cast(Type[T], origin or type_class)

    @staticmethod
    def get_item_type_class(cls: 'Type[ConstExpr[T]]') -> Type[Union[str, float, int, bool, None]]:
        type_class = ConstExpr.get_type_class(cls)
        assert type_class is list
        list_ty = typing_inspect.get_args(cls)[0]
        item_ty = typing_inspect.get_args(list_ty)[0]
        assert item_ty in (str, float, int, bool, type(None))
        # Mypy isn't smart enough to downcast with the above assert.
        return cast(Type[Union[str, float, int, bool, None]], item_ty)

    @staticmethod
    def get_item_literal_node_type(cls: 'Type[ConstExpr[T]]') -> Type[grammar.Literal]:
        item_ty = ConstExpr.get_item_type_class(cls)
        return _RUNTIME_TO_LITERAL_TYPE_TRANSLATIONS[item_ty]

    @staticmethod
    def get_literal_node_type(cls: 'Type[ConstExpr[T]]') -> Type[grammar.Literal]:
        return _RUNTIME_TO_LITERAL_TYPE_TRANSLATIONS[ConstExpr.get_type_class(cls)]

    @staticmethod
    def resolve(cls: 'Type[ConstExpr[T]]', name: str, node: grammar.ASTNode) -> 'ConstExpr[T]':
        # for now, only support resolution of direct literals
        literal = type_invariant(node, ConstExpr.get_literal_node_type(cls))
        if isinstance(literal, grammar.List):
            item_literal_type = ConstExpr.get_item_literal_node_type(cls)
            items = []
            for item_node in literal.items:
                item_literal = type_invariant(item_node, item_literal_type)
                items.append(literal_value(item_literal))

            ret = ConstExpr(name=name, value=items, literal=literal, argument_span=literal.span)
            return ret  # type: ignore

        value = literal_value(literal)
        ret = ConstExpr(
            # Comment here to force args to own lines, for more specific type ignore below
            name=name,
            value=value,  # type: ignore
            literal=literal,
            argument_span=literal.span,
        )
        return ret  # type: ignore

    def raise_for(self, exception: Exception, message: Optional[str] = None) -> None:
        """
        Raises an exception on behalf of an argument. Use this if you wish to implement more bespoke
        error handling than that provided by `attribute_errors`.
        """
        raise ConstExprArgumentException(const_expr=self, wrapped_exception=exception, message_override=message)

    @contextmanager
    def attribute_errors(self, message: Optional[str] = None) -> Iterator[None]:
        """
        A context manager wraps something that might throw during the compilation/validation phase, allowing
        the error to point to the specific argument, with a specific exception.

        For example:
        ```
        value = arguments.get_value('foo')
        with value.attribute_errors():
            bar = parse(value.value)
        ```

        If `parse` throws an exception, we will be able to properly attribute the error to the argument, and given
        the contained AST info, point to the argument in the source code!
        """
        try:
            yield
        except Exception as e:
            self.raise_for(e, message)


class ConstExprArgumentException(Exception):
    """An exception that is thrown by `ConstExpr.{raise_for,attribute_errors} when an argument has an invalid
    value during the validation phase.

    NOTE: This is not thrown when the type does not match, but simply when the value is invalid, e.g. a JSON
    string is expected, but it cannot be parsed."""

    def __init__(
        self, const_expr: ConstExpr[Any], wrapped_exception: Exception, message_override: Optional[str] = None
    ):
        self.const_expr = const_expr
        self.wrapped_exception = wrapped_exception
        self.message_override = message_override


# TODO: Remove
_LITERAL_TO_RUNTIME_TYPE_TRANSLATIONS = {
    grammar.String: str,
    grammar.Number: (int, float),
    grammar.List: list,
    grammar.Boolean: bool,
    grammar.None_: type(None),
}

_RUNTIME_TO_LITERAL_TYPE_TRANSLATIONS: Dict[Any, Type[grammar.Literal]] = {}
for k, v in _LITERAL_TO_RUNTIME_TYPE_TRANSLATIONS.items():
    if isinstance(v, tuple):
        for it in v:
            _RUNTIME_TO_LITERAL_TYPE_TRANSLATIONS[it] = k

    else:
        _RUNTIME_TO_LITERAL_TYPE_TRANSLATIONS[v] = k


T_arguments = TypeVar('T_arguments', bound='ArgumentsBase')


class ArgumentSpec(BaseModel):
    name: str
    type: str
    default: Optional[str]
    doc: Optional[str]

    def declaration(self) -> str:
        if self.default is None:
            default = ''
        else:
            default = f' = {self.default}'

        return f'{self.name}: {self.type}{default}'

    def param_docstring(self) -> Optional[str]:
        if not self.doc:
            return None
        # Clean it up a bit
        doc = self.doc.strip().replace('\n\n', '\n').replace('\n', '\n    ')
        return f':param {self.name}: {doc}'


class ArgumentsBase:
    """The container object that holds all all arguments a call node is called with. There are 2 phases of arguments,
    the first, being when the function is parsed statically. We'll only have access to literal values - these can be
    used to pre-compute values while building the execution graph, for example, parsing/validating JSONpath expressions
    or the likes. And the second, with fully resolved values from the execution graph.

    During the call execution construction/validation phase, it is guaranteed that the constructed node will have the
    access to resolved values of any literals, and during execution phase, will have access to all resolved attributes.
    """

    __slots__ = ('_call_node', '_arguments', '_arguments_ast', '_resolved')

    def __init__(
        self,
        call_node: grammar.Call,
        arguments: Dict[str, Any],
        resolved: bool = False,
    ):
        self._call_node = call_node
        self._arguments_ast = call_node.argument_dict()
        self._arguments = arguments
        self._resolved = resolved

        # If kwargs are allowed by the base class extract them into self.kwargs
        if self.is_extra_arguments_allowed():
            expected_items = self.items().keys()
            setattr(self, EXTRA_ARGS_ATTR, {k: arguments.pop(k) for k in list(arguments) if k not in expected_items})

        # Assign our values to this instance. If we have any options that have defaults that aren't set, accesses to
        # them will fall back to accessing the attr set on the class (which is the default value). If we did override
        # a default, we'll set it on the instance here and be able to use it moving forward.
        for argument_name, argument_value in arguments.items():
            # The subclass should have defined any arguments in its slots (or more likely doesn't have slots and we
            # can assign anything to it).
            setattr(self, argument_name, argument_value)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, ArgumentsBase) and self.__class__ == o.__class__ and self._arguments == o._arguments

    def __hash__(self) -> int:
        # raises TypeError('unhashable type: yada yada yada') if an argument is not hashable
        assert self._resolved, 'arguments are not comparable until resolved'
        return hash(tuple(v for _, v in sorted(self._arguments.items())))

    def get_call_node(self) -> grammar.Call:
        return self._call_node

    def get_argument_ast(self, key: str) -> grammar.Expression:
        return self._arguments_ast[key]

    def get_extra_arguments_ast(self) -> Dict[str, grammar.Expression]:
        """returns a dict of keys that are unexpected kwargs and their expressions"""
        keys = set(self._arguments_ast.keys() - self.items().keys())
        return {k: v for k, v in self._arguments_ast.items() if k in keys}

    def has_argument_ast(self, key: str) -> bool:
        return key in self._arguments_ast

    def update_with_resolved(self: T_arguments, resolved: Dict[str, Any]) -> T_arguments:
        assert not self._resolved
        return self.__class__(call_node=self._call_node, arguments={**self._arguments, **resolved}, resolved=True)

    @classmethod
    def _traverse_mro(cls) -> Sequence[type]:
        # As of c-python 3.6, dict is guaranteed to be insertion-ordered. We can use this,
        # to make a simple ordered set.
        ordered_mro = {}

        def traverse_mro(klass: Any) -> None:
            if hasattr(klass, '__mro__'):
                for it in klass.__mro__[1:-1]:
                    traverse_mro(it)

            ordered_mro[klass] = True

        traverse_mro(cls)

        return list(ordered_mro)

    @classmethod
    @lru_cache(1)
    def items(cls) -> Dict[str, type]:
        fields: Dict[str, type] = {}

        for klass in cls._traverse_mro():
            for field, value in get_type_hints(klass).items():
                if field not in fields and not field.startswith('_') and not typing_inspect.is_classvar(value):
                    fields[field] = value
        return fields

    @classmethod
    def get_generic_param(cls) -> Optional[type]:
        """Returns the `TypeVar` used if this class is generic, otherwise `None`.

        If it *is* generic, asserts that it inherits from `OspreyInvariantGeneric`.
        """
        return get_osprey_generic_param(cls, kind='arguments')

    @classmethod
    def get_generic_item_names(cls, func_name: str) -> List[str]:
        """
        Get the list of generic argument names.

        Asserts that we only have one type variable. For each generic item, also asserts that it only has one type
        parameter (so Optional[T] is okay, but Dict[T, T] is not).
        """
        generic_args = []
        for arg_name, arg_type in cls.items().items():
            if is_typevar(arg_type):
                generic_args.append(arg_name)

            type_params = typing_inspect.get_parameters(arg_type)
            if len(type_params) > 1:
                raise UnsupportedTypeError(
                    message='generic arguments item can only have one type param',
                    hint=(
                        f'in generic UDF `{func_name}` arguments type {to_display_str(cls)}'
                        f' had params: {type_params}, expected only one param'
                    ),
                )
            elif len(type_params) == 1:
                generic_args.append(arg_name)

        return generic_args

    @classmethod
    def is_generic(cls) -> bool:
        return cls.get_generic_param() is not None

    @classmethod
    @lru_cache(1)
    def is_extra_arguments_allowed(cls) -> bool:
        return EXTRA_ARGS_ATTR in cls.items()

    @classmethod
    @lru_cache(1)
    def get_extra_arguments_values_type(cls) -> type:
        """returns the type allowed by unexpected kwargs"""
        assert cls.is_extra_arguments_allowed(), 'check if is_extra_arguments_allowed() first'

        extra_args_type = cls.items()[EXTRA_ARGS_ATTR]
        key_type, val_type = typing_inspect.get_args(extra_args_type)

        assert key_type is str, f'{EXTRA_ARGS_ATTR} keys must be of type string'
        return val_type

    @classmethod
    def kwarg_can_be_none(cls, name: str) -> bool:
        """Whether or not the kwarg can accept None as an input.

        The name must be a valid kwarg, otherwise will raise an exception."""

        items = cls.items()
        if cls.is_extra_arguments_allowed() and name not in items:
            return False

        kwarg_type = items[name]
        # None and object can still accept None values.
        return kwarg_type in (None, object) or typing_inspect.is_optional_type(kwarg_type)

    @classmethod
    def kwarg_has_default(cls, name: str) -> bool:
        """Whether or not the kwarg must be passed a value, or has a default

        The name must be a valid kwarg, otherwise will raise an exception."""
        # If this value is set on the class, then that's the default.
        return hasattr(cls, name)

    def get_dependent_node_dict(self) -> Dict[str, grammar.ASTNode]:
        assert not self._resolved

        items = self.items()
        return {
            k: v
            for k, v in self._arguments_ast.items()
            # Nodes that inherit from LiteralBase are already in the unresolved arguments, as they
            # are resolved statically in the validation phase. This means we don't need to
            # add them to the dependent node map, as we already have their values.
            if not is_const_expr(items[k] if k in items else self.get_extra_arguments_values_type())
        }

    @classmethod
    def get_argument_specs(cls) -> Sequence[ArgumentSpec]:
        specs = []
        seen_specs = set()
        items = cls.items()

        def add_argument(name: str, doc: Optional[str]) -> None:
            default: Optional[str]
            if hasattr(cls, name):
                default_value = getattr(cls, name)
                if isinstance(default_value, ConstExpr):
                    default_value = default_value.value
                default = repr(default_value)
            else:
                default = None

            specs.append(
                ArgumentSpec(
                    name=name, type=to_display_str(items[name], include_quotes=False), default=default, doc=doc
                )
            )
            seen_specs.add(name)

        for klass in cls._traverse_mro():
            if klass == ArgumentsBase:
                continue

            # Some hacks to access the docstrings after attributes. These aren't available on objects, so we have to
            # re-parse it and look at the AST to get them.
            src = inspect.getsource(klass)
            parsed = ast.parse(textwrap.dedent(src))
            klass_ast = parsed.body[0]
            assert isinstance(klass_ast, ast.ClassDef)

            previous_variable: Optional[str] = None
            for statement in klass_ast.body:
                current_variable: Optional[str]
                if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                    current_variable = statement.target.id
                else:
                    current_variable = None

                if previous_variable is not None and previous_variable not in seen_specs and previous_variable in items:
                    doc: Optional[str]
                    if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Str):
                        doc = inspect.cleandoc(statement.value.s)
                    else:
                        doc = None
                    add_argument(previous_variable, doc)

                previous_variable = current_variable

            if previous_variable is not None:
                add_argument(previous_variable, doc=None)

        assert seen_specs == set(items), (seen_specs, set(items))

        return specs


class ConstExprTypeException(Exception):
    """An exception that is thrown if a literal type does not match the type that was expected"""

    def __init__(self, node: grammar.ASTNode, expected: Type[grammar.Literal]):
        super().__init__()
        self.node = node
        self.expected = expected


LiteralT = TypeVar('LiteralT', bound=grammar.Literal)


def type_invariant(node: grammar.ASTNode, expected: Type[LiteralT]) -> LiteralT:
    """Checks the type of literal, and throws ArgumentLiteralTypeException if the type does not match
    the expected type."""
    if not isinstance(node, expected):
        raise ConstExprTypeException(node=node, expected=expected)

    return node


def literal_value(literal: grammar.Literal) -> Union[str, int, float, bool, None]:
    if isinstance(literal, (grammar.String, grammar.Number, grammar.Boolean)):
        return literal.value

    if isinstance(literal, grammar.None_):
        return None

    raise TypeError(f'Unexpected literal type: {literal.__class__.__name__}')


def is_const_expr(argument: Any) -> bool:
    return typing_inspect.get_origin(argument) is ConstExpr
