import builtins
from types import GeneratorType

import osprey.engine.utils.types as types_module


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

    assert all(not isinstance(values, GeneratorType) for values in tuple_inputs)


def test_slots_getstate_does_not_pass_a_generator_to_tuple(monkeypatch) -> None:
    tuple_inputs = _record_tuple_inputs(monkeypatch)
    cls_dict: dict[str, object] = {}
    types_module._add_state_functions(cls_dict, ('value',))
    getstate = cls_dict['__getstate__']

    class Instance:
        value = object()

    assert callable(getstate)
    assert len(getstate(Instance())) == 1
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

    types_module.add_slots(Value)

    assert all(not isinstance(values, GeneratorType) for values in tuple_inputs)
