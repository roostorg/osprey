import builtins
from collections.abc import Sequence
from types import GeneratorType
from typing import Protocol, cast

import osprey.engine.utils.types as types_module


class _StatefulValue(Protocol):
    value: object

    def __getstate__(self) -> Sequence[object]:
        pass

    def __setstate__(self, state: Sequence[object]) -> None:
        pass


def _record_tuple_inputs(monkeypatch) -> list[object]:
    tuple_inputs: list[object] = []

    def recording_tuple(values):
        tuple_inputs.append(values)
        return builtins.tuple(values)

    monkeypatch.setattr(types_module, 'tuple', recording_tuple, raising=False)
    return tuple_inputs


def test_add_state_functions_does_not_pass_a_generator_to_tuple(monkeypatch) -> None:
    tuple_inputs = _record_tuple_inputs(monkeypatch)

    cls_dict: dict[str, object] = {}

    types_module._add_state_functions(cls_dict, ('value',))

    assert tuple_inputs
    assert all(not isinstance(values, GeneratorType) for values in tuple_inputs)


def test_slots_getstate_does_not_pass_a_generator_to_tuple(monkeypatch) -> None:
    tuple_inputs = _record_tuple_inputs(monkeypatch)
    cls_dict: dict[str, object] = {}
    types_module._add_state_functions(cls_dict, ('value',))
    getstate = cls_dict['__getstate__']

    class Instance:
        value = object()

    assert callable(getstate)
    instance = Instance()
    state = getstate(instance)
    assert state == (instance.value,)

    restored = Instance()
    restored.value = object()
    setstate = cls_dict['__setstate__']
    assert callable(setstate)
    setstate(restored, state)
    assert restored.value is instance.value
    assert tuple_inputs
    assert all(not isinstance(values, GeneratorType) for values in tuple_inputs)


def test_add_slots_does_not_pass_a_generator_to_tuple(monkeypatch) -> None:
    tuple_inputs = _record_tuple_inputs(monkeypatch)

    class Field:
        name = 'value'

    class Fields:
        def __iter__(self):
            yield Field()

    @types_module.dataclasses.dataclass
    class Value:
        value: object

    monkeypatch.setattr(types_module.dataclasses, 'fields', lambda _: Fields())

    slotted_value_cls = types_module.add_slots(Value)
    instance = cast(_StatefulValue, slotted_value_cls(value=object()))
    state = instance.__getstate__()
    restored = cast(_StatefulValue, slotted_value_cls(value=object()))
    restored.__setstate__(state)

    assert 'value' in getattr(slotted_value_cls, '__slots__')
    assert state == (instance.value,)
    assert restored.value is instance.value
    assert tuple_inputs
    assert all(not isinstance(values, GeneratorType) for values in tuple_inputs)
