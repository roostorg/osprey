import pytest

from ..entities import EntityT


def test_allowed_types() -> None:
    EntityT[str]
    EntityT[int]
    EntityT('MyType', 'hello')
    EntityT('MyType', 1234)

    with pytest.raises(TypeError):
        EntityT[bool]
    with pytest.raises(TypeError):
        EntityT[float]
    with pytest.raises(TypeError):
        EntityT('MyType', True)
    with pytest.raises(TypeError):
        EntityT('MyType', 12.34)
