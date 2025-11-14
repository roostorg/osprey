from enum import Enum
from typing import Dict, Generic, Optional, Type, TypeVar, Union

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


def _to_kebab_case(value: str) -> str:
    return value.lower().replace('_', '-')
