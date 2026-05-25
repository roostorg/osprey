from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import ClassVar, TypeVar

from gevent.lock import Semaphore

# TODO: Uncomment logging when we have a logging system
# from osprey.worker.ui_api.lib.osprey_shared.logging import get_logger
from osprey.engine.utils.types import add_slots, cached_property

# Keep these outside `Source` so we don't break pickling/hashing/eq
# Will this leak memory? Maybe
# TODO(old): put this stuff back in cached_property
parsed_ast_root_cache: dict['Source', 'Root'] = {}
ast_root_lock_cache: dict['Source', Semaphore] = defaultdict(lambda: Semaphore())

# logger = get_logger()


@add_slots
@dataclass(frozen=True)
class Source:
    """Source represents a source file that this package has parsed."""

    path: str
    """The location that the source should be considered located at. This is used in error messages to provide a
    human-readable location to the end-user - and also would be used as where the source is located by the rest of the
    engine."""

    contents: str = field(repr=False)
    """The contents of the source file.
    """

    actual_path: Path | None = field(default=None, compare=False)
    """If the source file was loaded from disk, rather than a virtual source, this will be Some path, where
    the file exists. Useful if we want to provide a jump to source functionality.
    """

    @cached_property
    def lines(self) -> Sequence[str]:
        return self.contents.splitlines(keepends=False)

    @property
    def ast_root(self) -> 'Root':
        """Returns the ast of this source."""
        ast_root_lock = ast_root_lock_cache[self]
        # If there is lock contention (which there should never be, print out some debug info).
        if ast_root_lock.locked():
            import traceback

            # logger.error('CONTENTION FOR AST ROOT LOCK')
            traceback.print_stack()

        with ast_root_lock:
            if self in parsed_ast_root_cache:
                return parsed_ast_root_cache[self]

            from .py_ast import transform

            # logger.debug(f'transforming {self.path}')
            parsed_ast_root = transform(self)
            parsed_ast_root_cache[self] = parsed_ast_root  # hack to get around frozen dataclass for now
            # logger.debug(f'finished transforming {self.path}')
            return parsed_ast_root


@add_slots
@dataclass(frozen=True)
class Span:
    """A span represents the location in which an AST node exists within a
    source file.

    When code is transformed into AST nodes, that transformation
    preserves the source file location, for printing out compiler
    warnings, errors, and other debug messages.
    """

    source: Source
    """The source that contains this span."""

    start_line: int
    """The line at which this span starts."""

    start_pos: int
    """The position in the start-line that the span starts."""

    _ast_node: 'ASTNode' | None = None

    def __repr__(self) -> str:
        return f'<Span source={self.source.path} start_line={self.start_line} start_pos={self.start_pos}>'

    @property
    def ast_node(self) -> 'ASTNode':
        if self._ast_node is None:
            raise RuntimeError('ast_node must be set')
        return self._ast_node

    def copy(self, ast_node: 'ASTNode') -> 'Span':
        return replace(self, _ast_node=ast_node)

    def parent_ast_node(self, n: int = 1) -> 'ASTNode' | None:
        """Helper for node traversal, gets the parent value off the n^th generation ast_node.

        >>> self.get_parent_ast_node(n=0) == self.ast_node
        >>> self.get_parent_ast_node(n=1) == self.ast_node.parent
        >>> self.get_parent_ast_node(n=2) == self.ast_node.parent.parent
        """
        node: ASTNode | None = self.ast_node
        while n > 0 and node is not None:
            n -= 1
            node = node.parent

        return node


class IsConstant:
    """
    Provide the is_constant interface for ASTNodes.

    The is_constant property is True if the current node itself is a constant
    or if the current node is an assembly of constants.

    Nodes can either override is_constant value as a ClassVar or cached_property.
    """

    @property
    def is_constant(self) -> bool:
        """is this node actually a single or assembly of constants"""
        raise NotImplementedError(f"'is_constant' must be implemented by '{self.__class__.__name__}'")


class IsExtractable:
    """
    Provide the can_extract interface for ASTNodes.

    The can_extract property is True if the current node itself can be extracted.
    Note, this may rely on parental information.
    """

    @property
    def can_extract(self) -> bool:
        """
        Can the target node be extracted?

        A false value for can_extract will percolate through dependent features like fstrings.
        is this node actually a single or assembly of secret nodes
        """
        raise NotImplementedError(f"'can_extract' must be implemented by '{self.__class__.__name__}'")


@add_slots
@dataclass
class ASTNode:
    """This is the base-class of all AST nodes."""

    parent: 'ASTNode' | None = field(default=None, init=False, repr=False, compare=False)
    span: Span = field(repr=False, compare=False)
    """The location of this AST node, in a given source file."""

    def __post_init__(self) -> None:
        # We can't give this a default in the constructor since we want subclasses to be able to have constructor
        # args without defaults.
        if not hasattr(self, 'parent'):
            self.parent = None

        # fix the span passed in to have this node as its associated ast_node
        self.span = replace(self.span, _ast_node=self)

    def __repr__(self) -> str:
        from .ast_utils import iter_fields

        pairs = []
        for attr in iter_fields(self):
            if not hasattr(self, attr):
                continue

            field = getattr(self, attr)
            pairs.append(f'{attr}={field!r}')

        pairs_str = ', '.join(pairs)
        return f'{self.__class__.__name__}({pairs_str})'


@add_slots
@dataclass
class Root(ASTNode):
    """The root AST node contains a list of statements, or the "body" of the
    source file."""

    statements: Sequence['Statement']


class Expression(ASTNode, IsExtractable):
    """An expression represents a value."""

    @property
    def can_extract(self) -> bool:
        raise NotImplementedError('expressions must implement can_extract')


@add_slots
@dataclass
class Context:
    """The context in which a `Name` is used, which is to say, is the variable
    `Store` (it's an assignment),

    or `Load` - it's being loaded from a global.
    """

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'


class Store(Context):
    """The Name is being stored, generally meaning it's the target of an
    `Assign`.

    X = Y
    ^ Name(identifier='X', context=Store())
    """


class Load(Context):
    """The Name is being loaded, generally meaning it is NOT the target of an
    assign, and may be used as a value in a general expression.

    The Name is being stored, generally meaning it's the target of an `Assign`.

    X = Y
        ^ Name(identifier='Y', context=Load())
    """


# Sentinel for _source_annotation
class _Sentinel(object): ...


_SOURCE_ANNOTATION_UNSET = _Sentinel()


@add_slots
@dataclass
class Name(Expression, IsExtractable):
    """
    Represents a variable referenced by its `name.`

    ```
    X = 1
    ^ This is a Name(identifier='X')
    ```
    """

    identifier: str
    context: Context
    _source_annotation: None | _Sentinel | 'Annotation' | 'AnnotationWithVariants' = field(
        default=_SOURCE_ANNOTATION_UNSET,
        repr=False,
        compare=False,
    )

    @cached_property
    def is_local(self) -> bool:
        """Returns whether this identifier is for a local variable."""
        return self.identifier.startswith('_')

    @cached_property
    def identifier_key(self) -> str:
        """Returns an identifier key that is globally unique, even if the node is a local."""
        if self.is_local:
            return f'{self.span.source.path}/{self.identifier}'
        else:
            return self.identifier

    def set_source_annotation(self, annotation: None | 'Annotation' | 'AnnotationWithVariants') -> None:
        self._source_annotation = annotation

    @property
    def can_extract(self) -> bool:
        if isinstance(self._source_annotation, _Sentinel):
            raise RuntimeError('_source_annotation accessed before being set')

        if self._source_annotation:
            return self._source_annotation.can_extract

        return True


class Literal(Expression, IsConstant):
    """A literal is a hard-coded value, e.g. a "string", number (11234.45), or
    boolean (True/False)"""

    @property
    def is_constant(self) -> bool:
        return True

    @property
    def can_extract(self) -> bool:
        return True


@add_slots
@dataclass
class String(Literal):
    """Represents a String from parsed AST."""

    value: str


@add_slots
@dataclass
class Number(Literal):
    """Represents a number parsed from ast."""

    value: int | float


@add_slots
@dataclass
class Boolean(Literal):
    """Represents a boolean as parsed from the AST"""

    value: bool


class None_(Literal):
    """Represents a None as parsed from the AST."""


class Statement(ASTNode):
    pass


@add_slots
@dataclass
class Assign(Statement, IsConstant, IsExtractable):
    """
    Represents an assignment of an Expression to a given Name - with an optional annotation.

    Foo: str = 1 + 1
     ^    ^    ^
     |    |    value
     |    annotation
     target
    """

    target: Name
    value: Expression
    annotation: None | 'Annotation' | 'AnnotationWithVariants' = None

    @cached_property
    def should_extract(self) -> bool:
        """
        Should the target node should be extracted?

        A false value for should_extract only applies to the current feature. An fstring using a feature with
        `should_extract == False` would be extractable.
        """
        if self.annotation and self.annotation.identifier == Annotations.ExtractLiteral:
            # always extract if the node is wrapped in `ExtractLiteral'
            return True

        elif self.target.is_local or self.is_constant:
            # don't extract if its local or constant
            return False

        elif self.annotation and self.annotation.identifier == Annotations.ExtractSecret:
            return True

        elif not self.can_extract:
            # don't extract anything that depends on secret information
            return False

        # extract everything else!
        return True

    @cached_property
    def is_constant(self) -> bool:  # type: ignore[override]
        # conditionally extract if the node supports the should_extract interface
        if isinstance(self.value, IsConstant):
            return self.value.is_constant

        return False

    @property
    def can_extract(self) -> bool:
        if self.annotation and self.annotation.can_extract is False:
            return False
        if self.annotation and self.annotation.identifier == Annotations.ExtractSecret:
            return True
        return self.value.can_extract


@add_slots
@dataclass
class Call(Expression, Statement):
    """Represents a named function call.

    A call is special, it can be used as an expression, but also as a statement, e.g.:

    ```
    Result = Fun(foo='bar') # <-- Expression
    Fun(foo='bar') # <-- Statement
    ```
    """

    func: Name | 'Attribute'
    arguments: Sequence['Keyword']

    def find_argument(self, name: str) -> None | 'Keyword':
        for argument in self.arguments:
            if argument.name == name:
                return argument

        return None

    def argument_dict(self) -> dict[str, Expression]:
        return {arg.name: arg.value for arg in self.arguments}

    @property
    def can_extract(self) -> bool:
        return not any(argument.can_extract is False for argument in self.arguments)


@add_slots
@dataclass
class Keyword(ASTNode, IsExtractable):
    """Represents a keyword argument in a Call.

    For example:
    ```
    Foo(key=value)
        ^    ^
        |    the value.
        - the name of the keyword
    ```
    """

    name: str
    value: Expression

    @property
    def can_extract(self) -> bool:
        return self.value.can_extract


class Operator(ASTNode):
    """An operator represents the operation which is performed on two arguments
    of a BinaryOperation`,

    E.g.
    ```
    a + b
      ^ this is the operator
    ```
    """

    original_operator: ClassVar[str]

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'


class Add(Operator):
    """left + right."""

    original_operator = '+'


class Subtract(Operator):
    """left - right"""

    original_operator = '-'


class Multiply(Operator):
    """left * right"""

    original_operator = '*'


class Divide(Operator):
    """left / right"""

    original_operator = '/'


class FloorDivide(Operator):
    """left // right"""

    original_operator = '//'


class Modulo(Operator):
    """left % right."""

    original_operator = '%'


class Pow(Operator):
    """left ** right."""

    original_operator = '**'


class LeftShift(Operator):
    """left << right."""

    original_operator = '<<'


class RightShift(Operator):
    """left >> right."""

    original_operator = '>>'


class BitwiseOr(Operator):
    """left | right."""

    original_operator = '|'


class BitwiseXor(Operator):
    """left ^ right."""

    original_operator = '^'


class BitwiseAnd(Operator):
    """left & right."""

    original_operator = '&'


OperatorT = TypeVar('OperatorT', bound=Operator)


@add_slots
@dataclass
class BinaryOperation(Expression):
    """A binary operation represents an expression that has a left, right, and
    operator,

    left (operator) right

    Eg:
    ```
    a + b
    ```
    Where: a is the left, b is the right, and `Add` is the operator.
    """

    left: Expression
    right: Expression
    operator: Operator

    @property
    def can_extract(self) -> bool:
        return True


class UnaryOperator(ASTNode):
    """An operator represents the operation which is performed on a single argument
    of a UnaryOperation`,

    E.g. ``` not x ```
              ^ this is the operator
    """

    original_operator: ClassVar[str]

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'


class Not(UnaryOperator):
    """not x"""

    original_operator = 'not'


class USub(UnaryOperator):
    """(-)x"""

    original_operator = '-'


@add_slots
@dataclass
class UnaryOperation(Expression):
    """A unary operation represents an expression that has a operand and an
    operator,

    (operator) operand

    Eg:
    ```
    not x
    ```
    Where: x is the operand, and `not` is the operator.
    """

    operand: Expression
    operator: UnaryOperator

    # booleans are ok to extract
    @property
    def can_extract(self) -> bool:
        return True


class Comparator(ASTNode):
    """A comparator compares two nodes (left and right), and returns a
    boolean."""

    original_comparator: ClassVar[str]


class Equals(Comparator):
    """left == right."""

    original_comparator = '=='


class NotEquals(Comparator):
    """left != right."""

    original_comparator = '!='


class LessThan(Comparator):
    """left < right."""

    original_comparator = '<'


class LessThanEquals(Comparator):
    """left <= right."""

    original_comparator = '<='


class GreaterThan(Comparator):
    """left > right."""

    original_comparator = '>'


class GreaterThanEquals(Comparator):
    """left >= right."""

    original_comparator = '>='


class In(Comparator):
    """left in right."""

    original_comparator = 'in'


class NotIn(Comparator):
    """left not in right."""

    original_comparator = 'not in'


@add_slots
@dataclass
class BinaryComparison(Expression):
    """A binary comparison represents an expression that has a left, right, and
    a comparator.

    Eg:
    ```
    a == b
    ```
    Where: a is the left, b is the right, and `Equals` is the operator.
    """

    left: Expression
    right: Expression
    comparator: Comparator

    @property
    def can_extract(self) -> bool:
        return True


class BooleanOperand(ASTNode):
    """Represents a boolean `And` or `Or` operand."""

    original_operand: ClassVar[str]


class And(BooleanOperand):
    """left and right."""

    original_operand = 'and'


class Or(BooleanOperand):
    """left or right."""

    original_operand = 'or'


@add_slots
@dataclass
class BooleanOperation(Expression):
    """A boolean operation represents a boolean and/or operation across
    multiple values.

    Eg:
    ```
    a and b and c
    ```
    Where: a, b, c = the values, and `And` is the operand.
    """

    values: Sequence[Expression]
    operand: BooleanOperand

    @property
    def can_extract(self) -> bool:
        return True


@add_slots
@dataclass
class Attribute(Expression):
    """
    Allows for a single level of attribute access, e.g.

    ```
    Foo.Bar
    ```

    Where the name is a Name(identifier='Foo'), and the attribute is 'Bar'.
    """

    name: Name
    attribute: str
    context: Context

    @property
    def identifier(self) -> str:
        return self.name.identifier

    @property
    def can_extract(self) -> bool:
        return self.name.can_extract


@add_slots
@dataclass
class List(Literal, IsExtractable):
    """
    A list is simply a list of expressions, e.g.

    ```
    [a, b, c]
    ```
    """

    items: Sequence[Expression]

    @cached_property
    def is_constant(self) -> bool:  # type: ignore[override]
        # list is constant unless a single element is...
        for item in self.items:
            if (
                # not a literal
                not isinstance(item, Literal)
                # and the item is not a constant
                and not (isinstance(item, IsConstant) and item.is_constant)
            ):
                return False
        return True

    def __len__(self) -> int:
        return len(self.items)

    @property
    def can_extract(self) -> bool:
        return not any(item.can_extract is False for item in self.items)


@add_slots
@dataclass
class FormatString(Expression, IsConstant):
    """A format string, or f-string, is a string that has Name's interpolated
    within it.

    It's like a python format string, but much, much simpler.
    """

    format_string: str
    names: Sequence[Name]

    @property
    def is_constant(self) -> bool:
        # TODO: Figure out how to correctly calculate this
        return False

    @property
    def can_extract(self) -> bool:
        return not any(name.can_extract is False for name in self.names)


@add_slots
@dataclass
class Annotation(Expression):
    """Represents an annotation on an assign operator.

    For example,

    X: Y = 1
       ^ this is the annotation.
    """

    identifier: str

    @property
    def can_extract(self) -> bool:
        return True


@add_slots
@dataclass
class AnnotationWithVariants(Expression):
    """Represents an annotation on an assign operator.

    For example,

    X: Union[X, Y] = 1
       ^     ^  ^
       |     |  |
       |    these are the variants,
       '- this is the annotation.
    """

    identifier: str
    variants: Sequence[Annotation | 'AnnotationWithVariants']

    @property
    def can_extract(self) -> bool:
        if self.identifier == Annotations.Secret:
            return False

        if self.identifier == Annotations.ExtractSecret:
            return True

        return not any(variant.can_extract is False for variant in self.variants)


class Annotations(str, Enum):
    """Annotations that can be used for AnnotationWithVariants"""

    # Marks a feature as secret and should not be stored anywhere
    Secret = 'Secret'

    # Allows for the extraction of something marked as Secret
    ExtractSecret = 'ExtractSecret'

    # Marks a literal value for extraction. Without ExtractLiteral Literals will be filtered from the output
    ExtractLiteral = 'ExtractLiteral'

    # Optional type annotation
    Optional = 'Optional'

    # Union type annotation
    Union = 'Union'


__all__ = [
    'Source',
    'Span',
    'ASTNode',
    'Root',
    'Expression',
    'Context',
    'Store',
    'Load',
    'Name',
    'Literal',
    'String',
    'Number',
    'Boolean',
    'None_',
    'Statement',
    'Assign',
    'Call',
    'Keyword',
    'Operator',
    'Add',
    'Subtract',
    'Multiply',
    'Divide',
    'FloorDivide',
    'Modulo',
    'Pow',
    'LeftShift',
    'RightShift',
    'BitwiseOr',
    'BitwiseXor',
    'BitwiseAnd',
    'BinaryOperation',
    'UnaryOperation',
    'UnaryOperator',
    'Not',
    'USub',
    'Comparator',
    'Equals',
    'NotEquals',
    'LessThan',
    'LessThanEquals',
    'GreaterThan',
    'GreaterThanEquals',
    'In',
    'NotIn',
    'BinaryComparison',
    'BooleanOperand',
    'And',
    'Or',
    'BooleanOperation',
    'Attribute',
    'List',
    'FormatString',
    'Annotation',
    'AnnotationWithVariants',
]
