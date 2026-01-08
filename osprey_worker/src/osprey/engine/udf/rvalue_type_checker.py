from abc import ABC, abstractmethod
from typing import Callable, ClassVar, Optional, Sequence, Tuple, Type, TypeVar, Union

from osprey.engine.ast.grammar import Annotation, Annotations, AnnotationWithVariants, Assign, Span
from osprey.engine.language_types.entities import EntityT

from .type_helpers import to_display_str

_Self = TypeVar('_Self')


class RValueTypeChecker(ABC):
    """Holds information about a type that can be dynamically checked at runtime."""

    @abstractmethod
    def check(self, obj: object) -> bool:
        raise NotImplementedError

    @abstractmethod
    def to_typing_type(self) -> type:
        raise NotImplementedError

    def coerce(self, obj: object) -> object:
        if self.check(obj):
            return obj

        raise TypeError(f'Cannot coerce `{obj!r}` to {to_display_str(self.to_typing_type())}')


_AnnotationMaybeWithVariantT = Union[Annotation, AnnotationWithVariants]
TypeConstructorT = Callable[[_AnnotationMaybeWithVariantT, str], RValueTypeChecker]


class GenericTypeChecker(RValueTypeChecker, ABC):
    """An extension for runtime checkable types that are generic."""

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def parse(cls: Type[_Self], node: AnnotationWithVariants, type_constructor: TypeConstructorT) -> _Self:
        pass


_GenericType = TypeVar('_GenericType', bound=Type[GenericTypeChecker])


class TypeRegistry:
    """Holds information about generic and non-generic types that can be checked at runtime."""

    def __init__(self) -> None:
        self._non_generic_types: dict[str, RValueTypeChecker] = {}
        self._generic_types: dict[str, Type[GenericTypeChecker]] = {}

    def register_non_generic(self, name: str, type_checker: RValueTypeChecker) -> None:
        self._non_generic_types[name] = type_checker

    def register_generic(self, type_checker: _GenericType) -> _GenericType:
        self._generic_types[type_checker.name()] = type_checker
        return type_checker

    def get_non_generic(self, node: Annotation) -> Optional[RValueTypeChecker]:
        return self._non_generic_types.get(node.identifier)

    def get_generic(self, node: AnnotationWithVariants) -> Optional[Type[GenericTypeChecker]]:
        return self._generic_types.get(node.identifier)

    def generic_names(self) -> list[str]:
        return list(self._generic_types.keys())


REGISTRY = TypeRegistry()


class InstanceTypeChecker(RValueTypeChecker):
    def __init__(self, valid_type: type):
        self.valid_type = valid_type

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(valid_type={self.valid_type.__name__})'

    def check(self, obj: object) -> bool:
        # *Don't* allow using a bool where an int is required
        is_bool_for_num = self.valid_type in (int, float) and isinstance(obj, bool)
        # *Do* allow using an int where a float is required
        is_int_for_float = self.valid_type is float and isinstance(obj, int)
        return not is_bool_for_num and (is_int_for_float or isinstance(obj, self.valid_type))

    def coerce(self, obj: object) -> object:
        if self.check(obj):
            return obj

        if self.valid_type is type(None):  # noqa: E721
            raise TypeError(f'Cannot coerce non-None `{obj!r}` to `None`')

        if obj is None and self.valid_type is not bool:
            raise TypeError(f'Cannot coerce `{obj!r}` to {to_display_str(self.valid_type)}')

        if self.valid_type in (int, float) and isinstance(obj, bool):
            raise TypeError(f'Cannot coerce `{obj!r}` to {to_display_str(self.valid_type)}')

        return self.valid_type(obj)

    def to_typing_type(self) -> type:
        return self.valid_type


for scalar_type in (str, bool, int, float):
    REGISTRY.register_non_generic(scalar_type.__name__, InstanceTypeChecker(scalar_type))
REGISTRY.register_non_generic('None', InstanceTypeChecker(type(None)))


@REGISTRY.register_generic
class UnionTypeChecker(GenericTypeChecker):
    @classmethod
    def name(cls) -> str:
        return Annotations.Union.value

    @classmethod
    def parse(cls, node: AnnotationWithVariants, type_constructor: TypeConstructorT) -> 'UnionTypeChecker':
        # Union of 1 item is the same as a simple annotation type-checker.
        if len(node.variants) == 1:
            variant = node.variants[0]
            parent = node.parent
            if isinstance(parent, Assign):
                # Tell the user that using `Foo: Union[x] = ...` is the same as just using `Foo: x = ...`.
                hint = f'assign it to a variable as `{parent.target.identifier}: {variant.identifier} = ...` instead'
            else:
                hint = f'use `{variant.identifier}` instead'
            raise AnnotationConversionError(
                message='a `Union` of one item should be represented as the item by itself',
                span=node.span,
                hint=hint,
            )

        seen_types: dict[str, Span] = {}
        inner_types = []
        for variant in node.variants:
            # Check if the variant was already seen for this Union.
            if variant.identifier in seen_types:
                raise AnnotationConversionError(
                    message=f'duplicated type `{variant.identifier}` seen in `Union[...]`',
                    span=variant.span,
                    additional_spans_message='union variant already specified here:',
                    additional_spans=[seen_types[variant.identifier]],
                )

            seen_types[variant.identifier] = variant.span
            inner_types.append(type_constructor(variant, 'expected simple type in `Union`'))

        return UnionTypeChecker(tuple(inner_types))

    def __init__(self, inner_types: Tuple[RValueTypeChecker, ...]):
        assert len(inner_types) > 1, f'Tried to create union with {len(inner_types)} inner types: {inner_types}'
        self.inner_types = inner_types

    def _find_working_inner_type(self, obj: object) -> Optional[RValueTypeChecker]:
        for inner_type in self.inner_types:
            if inner_type.check(obj):
                return inner_type
        return None

    def check(self, obj: object) -> bool:
        return bool(self._find_working_inner_type(obj))

    def coerce(self, obj: object) -> object:
        # first check for a "best match"
        working_inner_type = self._find_working_inner_type(obj)
        if working_inner_type:
            return working_inner_type.coerce(obj)

        # next coerce to anything that will take it
        for inner in self.inner_types:
            try:
                return inner.coerce(obj)
            except TypeError:
                pass

        raise TypeError(f'Cannot coerce `{obj!r}` to {to_display_str(self.to_typing_type())}')

    def to_typing_type(self) -> type:
        # Mypy doesn't like runtime types like this
        # noinspection PyTypeChecker
        return Union[tuple(inner.to_typing_type() for inner in self.inner_types)]  # type: ignore


@REGISTRY.register_generic
class OptionalTypeChecker(UnionTypeChecker):
    @classmethod
    def name(cls) -> str:
        return Annotations.Optional.value

    @classmethod
    def parse(cls, node: AnnotationWithVariants, type_constructor: TypeConstructorT) -> 'OptionalTypeChecker':
        if len(node.variants) != 1:
            raise AnnotationConversionError(
                message='`Optional` must have exactly one argument',
                span=node.span,
                hint=f'got {len(node.variants)} arguments',
            )

        variant = node.variants[0]
        inner_type = type_constructor(variant, 'expected simple type in `Optional`')
        return OptionalTypeChecker((inner_type, InstanceTypeChecker(type(None))))


class BaseWrapperTypeChecker(GenericTypeChecker):
    wrapped_type: ClassVar[Optional[Annotations]] = None

    @classmethod
    def name(cls) -> str:
        assert cls.wrapped_type is not None
        return cls.wrapped_type.value

    @classmethod
    def parse(cls, node: AnnotationWithVariants, type_constructor: TypeConstructorT) -> 'BaseWrapperTypeChecker':
        if len(node.variants) != 1:
            raise AnnotationConversionError(
                message=f'`{cls.name()}` must have exactly one argument',
                span=node.span,
                hint=f'got {len(node.variants)} arguments',
            )

        variant, *_ = node.variants
        if isinstance(variant, AnnotationWithVariants):
            inner_type = _convert_annotation_with_variants_to_typechecker(variant)
        else:
            inner_type = type_constructor(variant, f'expected simple type in `{cls.name()}`')
        return cls((inner_type,))

    def __init__(self, inner_types: Tuple[RValueTypeChecker, ...]):
        assert len(inner_types) == 1, (
            f'Tried to create `{self.name()}` with {len(inner_types)} inner types: {inner_types}'
        )
        self.inner_type = inner_types[0]

    def check(self, obj: object) -> bool:
        return self.inner_type.check(obj)

    def to_typing_type(self) -> type:
        # noinspection PyTypeChecker
        return self.inner_type.to_typing_type()


@REGISTRY.register_generic
class ExtractLiteralTypeChecker(BaseWrapperTypeChecker):
    wrapped_type: ClassVar[Annotations] = Annotations.ExtractLiteral


@REGISTRY.register_generic
class SecretTypeChecker(BaseWrapperTypeChecker):
    wrapped_type: ClassVar[Annotations] = Annotations.Secret


@REGISTRY.register_generic
class ExtractSecretTypeChecker(BaseWrapperTypeChecker):
    wrapped_type: ClassVar[Annotations] = Annotations.ExtractSecret


@REGISTRY.register_generic
class ListTypeChecker(GenericTypeChecker):
    @classmethod
    def name(cls) -> str:
        return 'List'

    @classmethod
    def parse(cls, node: AnnotationWithVariants, type_constructor: TypeConstructorT) -> 'ListTypeChecker':
        [first_variant, *unexpected_variants] = node.variants
        if unexpected_variants:
            [first, *rest] = [n.span for n in unexpected_variants]
            raise AnnotationConversionError(
                message='unexpected additional variants to `list[...]`',
                span=first,
                additional_spans_message='also:' if rest else '',
                additional_spans=rest,
            )

        item_checker = type_constructor(first_variant, 'expected simple type in `List`')
        return ListTypeChecker(item_checker)

    def __init__(self, item_checker: RValueTypeChecker):
        self.item_checker = item_checker

    def check(self, obj: object) -> bool:
        if not isinstance(obj, list):
            return False

        return all(self.item_checker.check(it) for it in obj)

    def coerce(self, obj: object) -> object:
        if self.check(obj):
            return obj

        # Don't treat strings as sequences for this purpose
        if not isinstance(obj, Sequence) or isinstance(obj, (str, bytes)):
            raise TypeError(f'Cannot coerce non-Sequence `{obj!r}` to `List`')

        return [self.item_checker.coerce(item) for item in obj]

    def to_typing_type(self) -> type:
        # noinspection PyTypeChecker
        return list[self.item_checker.to_typing_type()]  # type: ignore # Doesn't like runtime types like this


@REGISTRY.register_generic
class EntityTypeChecker(GenericTypeChecker):
    @classmethod
    def name(cls) -> str:
        return EntityT.__name__

    @classmethod
    def parse(cls, node: AnnotationWithVariants, type_constructor: TypeConstructorT) -> 'EntityTypeChecker':
        if len(node.variants) != 1:
            raise AnnotationConversionError(
                message=f'`{cls.name()}` must have exactly one argument',
                span=node.span,
                hint=f'got {len(node.variants)} arguments',
            )

        variant = node.variants[0]
        if variant.identifier not in ('str', 'int'):
            raise AnnotationConversionError(
                message=f'`{cls.name()}` only supports `str` and `int` IDs currently',
                span=variant.span,
                hint=f'got `{variant.identifier}` ID type',
            )

        inner_type = type_constructor(variant, f'expected simple type in `{cls.name()}`')
        return EntityTypeChecker(inner_type)

    def __init__(self, inner_type: RValueTypeChecker) -> None:
        self.inner_type = inner_type

    def check(self, obj: object) -> bool:
        return isinstance(obj, EntityT) and self.inner_type.check(obj.id)

    def to_typing_type(self) -> type:
        return EntityT[self.inner_type.to_typing_type()]  # type: ignore # Doesn't like runtime types like this


def convert_ast_annotation_to_type_checker(node: _AnnotationMaybeWithVariantT) -> RValueTypeChecker:
    """Given an annotation ast node, convert it to an RValueTypeChecker. If conversion is not possible, throws an
    `AnnotationConversionError` which explains why."""
    if isinstance(node, Annotation):
        return _convert_simple_annotation_to_typechecker(node)
    else:
        return _convert_annotation_with_variants_to_typechecker(node)


def _get_default_error_message() -> str:
    allowed_generics = ', '.join(f'`{name}`' for name in sorted(REGISTRY.generic_names()))
    return f'can only check dynamic return values of simple types or {allowed_generics} generics'


def _convert_simple_annotation_to_typechecker(
    node: _AnnotationMaybeWithVariantT, error_message: Optional[str] = None
) -> RValueTypeChecker:
    assert not isinstance(node, AnnotationWithVariants), 'expected simple annotation'
    checker = REGISTRY.get_non_generic(node)
    if checker is None:
        if node.identifier in REGISTRY.generic_names():
            hint = f'`{node.identifier}` is generic, try `{node.identifier}[T]` where `T` is a type like `str` or `int`'
        else:
            hint = f'got unknown non-generic type `{node.identifier}`'

        raise AnnotationConversionError(
            message=error_message or _get_default_error_message(),
            span=node.span,
            hint=hint,
        )

    return checker


def _convert_annotation_with_variants_to_typechecker(node: AnnotationWithVariants) -> RValueTypeChecker:
    checker_class = REGISTRY.get_generic(node)
    if checker_class is None:
        raise AnnotationConversionError(
            message=_get_default_error_message(),
            span=node.span,
            hint=f'got unknown generic type `{node.identifier}[...]`',
        )

    return checker_class.parse(node, _convert_simple_annotation_to_typechecker)


class AnnotationConversionError(Exception):
    """Thrown when we cannot convert an annotation to a RValueTypeChecker."""

    def __init__(
        self,
        span: Span,
        message: str,
        hint: str = '',
        additional_spans: Sequence[Span] = tuple(),
        additional_spans_message: str = '',
    ):
        super().__init__(message)
        self.span = span
        self.message = message
        self.hint = hint
        self.additional_spans = additional_spans
        self.additional_spans_message = additional_spans_message
