from enum import Enum
from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union

import click
from click import Context, Parameter

T = TypeVar('T', bound=Enum)


class EnumChoice(click.Choice, Generic[T]):
    """For use with `@click.option(...)` in order to support for choosing a single value of an `Enum`.

    Example usage:
    ```
    class MyChoice(Enum):
        OPT_A = auto()
        OPT_B = auto()

    @click.option('--choice', type=EnumChoice(MyChoice), default=MyChoice.OPT_B)
    def func(choice):
        click.echo(f"Your choice was: {choice}")
    ```
    """

    choice_map: Dict[str, T]
    enum: Type[T]

    def __init__(self, enum: Type[T]):
        self.choice_map = {_to_kebab_case(variant.name): variant for variant in enum}
        self.enum = enum
        super().__init__(choices=list(self.choice_map.keys()))

    def convert(self, value: Union[str, T], param: Optional[Parameter], ctx: Optional[Context]) -> Optional[T]:
        # This is to support when click is given a default that is a variant. If we have a value that is our variant,
        # we can just return it without trying to coerce.
        if isinstance(value, self.enum):
            return value

        assert isinstance(value, str)
        try:
            return self.choice_map[_to_kebab_case(value)]
        except KeyError:
            self.fail('invalid choice: %s. (choose from %s)' % (value, ', '.join(self.choices)), param, ctx)

        return None


class EnumChoicePb2(click.Choice):
    """For use with `@click.option(...)` in order to support for choosing a single value of a protobuf enum.

    Example usage:
    ```
    from some_pb2 import SomeEnum

    @click.option('--choice', type=EnumChoicePb2(SomeEnum), default=SomeEnum.SOME_VALUE)
    def func(choice):
        click.echo(f"Your choice was: {choice}")
    ```
    """

    choice_map: Dict[str, int]
    enum_class: Any

    def __init__(self, enum_class: Any):
        self.enum_class = enum_class
        # Build choice map from enum descriptor
        self.choice_map = {}

        # Get the enum descriptor and build choices from value names
        for value_descriptor in enum_class.DESCRIPTOR.values:
            kebab_name = _to_kebab_case(value_descriptor.name)
            self.choice_map[kebab_name] = value_descriptor.number

        super().__init__(choices=list(self.choice_map.keys()))

    def convert(self, value: Union[str, int], param: Optional[Parameter], ctx: Optional[Context]) -> Optional[int]:
        # If we already have an int (protobuf enum value), return it
        if isinstance(value, int):
            return value

        assert isinstance(value, str)
        try:
            return self.choice_map[_to_kebab_case(value)]
        except KeyError:
            self.fail('invalid choice: %s. (choose from %s)' % (value, ', '.join(self.choices)), param, ctx)

        return None


def _to_kebab_case(value: str) -> str:
    return value.lower().replace('_', '-')
