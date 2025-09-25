from typing import Any, Mapping, Sequence, TypeVar

import pytest
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric


@pytest.mark.parametrize(
    'args, kwargs',
    (
        # Bound
        ((), {'bound': object}),
        # Constraints
        ((int, float), {}),
        # Non-invariant
        ((), {'covariant': True}),
        ((), {'contravariant': True}),
    ),
)
def test_type_var_must_be_unbounded_invariant(args: Sequence[Any], kwargs: Mapping[str, Any]) -> None:
    # noinspection PyTypeHints
    t = TypeVar('t', *args, **kwargs)  # type: ignore # Doesn't like runtime construction
    with pytest.raises(TypeError):
        _ = OspreyInvariantGeneric[t]  # type: ignore # Since the above TypeVar is ignored


def test_must_have_one_parameter() -> None:
    a = TypeVar('a')
    b = TypeVar('b')
    with pytest.raises(TypeError):
        _ = OspreyInvariantGeneric[a, b]  # type: ignore # This is an intentional mypy error
