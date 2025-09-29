import abc
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Type

import gevent
import gevent.event
import gevent.pool
import pytest
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.conftest import ExecuteFunction, ExecuteWithResultFunction
from osprey.engine.executor.execution_context import ExecutionContext, ExpectedUdfException
from osprey.engine.language_types.post_execution_convertible import PostExecutionConvertible
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import BatchableUDFBase, UDFBase
from osprey.engine.udf.registry import UDFRegistry
from result import Err, Ok, Result


class RecordingArguments(ArgumentsBase):
    id: str
    routing_key: Optional[str] = None


class RecordingUdf(UDFBase[RecordingArguments, str], metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def order_called(cls) -> List[str]:
        pass

    def execute(self, execution_context: ExecutionContext, arguments: RecordingArguments) -> str:
        self.order_called().append(arguments.id)
        return arguments.id


@pytest.fixture()
def recording_udf(udf_registry: UDFRegistry) -> Type[RecordingUdf]:
    class RecordingUdfImpl(RecordingUdf):
        _order_called: List[str] = []

        @classmethod
        def order_called(cls) -> List[str]:
            return cls._order_called

    RecordingUdfImpl.__name__ = 'RecordingUdf'
    udf_registry.register(RecordingUdfImpl)
    return RecordingUdfImpl


class BatchRecordingUdf(BatchableUDFBase[RecordingArguments, str, RecordingArguments], metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def order_called(cls) -> List[List[str]]:
        pass

    def execute(self, execution_context: ExecutionContext, arguments: RecordingArguments) -> str:
        self.order_called().append([arguments.id])
        return arguments.id

    def get_batchable_arguments(self, arguments):
        return arguments

    def get_batch_routing_key(self, arguments):
        if arguments.routing_key:
            return arguments.routing_key
        return super().get_batch_routing_key(arguments)

    def execute_batch(
        self, execution_context: ExecutionContext, udfs: Sequence[UDFBase], arguments: Sequence[RecordingArguments]
    ) -> Sequence[Result[str, Exception]]:
        self.order_called().append([arg.id for arg in arguments])
        return [Ok(arg.id) for arg in arguments]


@pytest.fixture()
def batch_recording_udf(udf_registry: UDFRegistry) -> Type[BatchRecordingUdf]:
    class BatchRecordingUdfImpl(BatchRecordingUdf):
        _order_called: List[List[str]] = []

        @classmethod
        def order_called(cls) -> List[List[str]]:
            return cls._order_called

    BatchRecordingUdfImpl.__name__ = 'BatchRecordingUdf'
    udf_registry.register(BatchRecordingUdfImpl)
    return BatchRecordingUdfImpl


class BlockingArguments(ArgumentsBase):
    id: str
    block: bool


class BlockingUdf(UDFBase[BlockingArguments, str]):
    @classmethod
    @abc.abstractmethod
    def order_called(cls) -> List[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def blocking(cls) -> Dict['BlockingUdf', 'gevent.Greenlet[object]']:
        pass

    @classmethod
    def unblock_any(cls) -> None:
        first_blocked = next(iter(cls.blocking().keys()))
        first_blocked.unblock()

    def __init__(self, validation_context: ValidationContext, arguments: BlockingArguments):
        super().__init__(validation_context, arguments)
        self._arguments = arguments
        self._event = gevent.event.Event()

    def unblock(self) -> None:
        self._event.set()

    def execute(self, execution_context: ExecutionContext, arguments: BlockingArguments) -> str:
        self.order_called().append(arguments.id)
        if arguments.block:
            self.blocking()[self] = gevent.getcurrent()
            self._event.wait()
            self.blocking().pop(self)
        return arguments.id


@pytest.fixture()
def blocking_udf(udf_registry: UDFRegistry) -> Type[BlockingUdf]:
    class BlockingUdfImpl(BlockingUdf):
        _order_called: List[str] = []
        _blocking: Dict['BlockingUdf', 'gevent.Greenlet[object]'] = {}

        # Put this here (not in base) so it's reset for each test.
        execute_async = True

        @classmethod
        def order_called(cls) -> List[str]:
            return cls._order_called

        @classmethod
        def blocking(cls) -> Dict['BlockingUdf', 'gevent.Greenlet[object]']:
            return cls._blocking

    BlockingUdfImpl.__name__ = 'BlockingUdf'
    udf_registry.register(BlockingUdfImpl)
    return BlockingUdfImpl


class FailingUdf(UDFBase[ArgumentsBase, int]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> int:
        raise ValueError('intentional failure')


class FailingStrUdf(UDFBase[ArgumentsBase, str]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        raise ValueError('intentional failure')


class BatchFailingUdfArgs(ArgumentsBase):
    id: int


class BatchFailingUdf(BatchableUDFBase[BatchFailingUdfArgs, int, BatchFailingUdfArgs], metaclass=abc.ABCMeta):
    """
    This UDF will fail any odd ID executions within a batch. All even ID (and non-batched) executions will succeed.
    """

    @classmethod
    @abc.abstractmethod
    def order_called(cls) -> List[List[int]]:
        pass

    def execute(self, execution_context: ExecutionContext, arguments: BatchFailingUdfArgs) -> int:
        self.order_called().append([arguments.id])
        return arguments.id

    def get_batchable_arguments(self, arguments):
        return arguments

    def execute_batch(
        self, execution_context: ExecutionContext, udfs: Sequence[UDFBase], arguments: Sequence[BatchFailingUdfArgs]
    ) -> Sequence[Result[int, Exception]]:
        self.order_called().append([arg.id for arg in arguments])
        return [Ok(arg.id) if arg.id % 2 == 0 else Err(ValueError('intentional failure')) for arg in arguments]


@pytest.fixture()
def batch_failing_udf(udf_registry: UDFRegistry) -> Type[BatchFailingUdf]:
    class BatchFailingUdfImpl(BatchFailingUdf):
        _order_called: List[List[int]] = []

        @classmethod
        def order_called(cls) -> List[List[int]]:
            return cls._order_called

    BatchFailingUdfImpl.__name__ = 'BatchFailingUdf'
    udf_registry.register(BatchFailingUdfImpl)
    return BatchFailingUdfImpl


def wait_idle(main_greenlet: 'gevent.Greenlet[object]') -> None:
    gevent.sleep(1.0)
    gevent.idle()
    if main_greenlet.exception is not None:
        raise main_greenlet.exception


def test_execute_basic(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Foo = 1 + 2
        Bar = Foo + 2
        """
    )

    assert data == {'Foo': 3, 'Bar': 5}


def test_only_run_nodes_once(recording_udf: Type[RecordingUdf], execute: ExecuteFunction) -> None:
    data = execute(
        """
        Foo = "a" + RecordingUdf(id="t")
        Bar = "c" + Foo
        """
    )

    assert data == {'Foo': 'at', 'Bar': 'cat'}
    assert recording_udf.order_called() == ['t']


def test_only_run_nodes_once_with_batching(
    batch_recording_udf: Type[BatchRecordingUdf], execute: ExecuteFunction
) -> None:
    data = execute(
        """
        Foo = "a" + BatchRecordingUdf(id="t")
        Bar = "c" + Foo
        """,
        async_pool=gevent.pool.Pool(10),
    )

    assert data == {'Foo': 'at', 'Bar': 'cat'}
    assert batch_recording_udf.order_called() == [['t']]


def test_executes_leaf_nodes_first(recording_udf: Type[RecordingUdf], execute: ExecuteFunction) -> None:
    data = execute(
        """
        A = RecordingUdf(id=("a3" + RecordingUdf(id=("a2" + RecordingUdf(id="a1")))))
        B = RecordingUdf(id=("b3" + RecordingUdf(id=("b2" + RecordingUdf(id="b1")))))
        """
    )

    assert data == {'A': 'a3a2a1', 'B': 'b3b2b1'}
    assert set(recording_udf.order_called()) == {'a1', 'b1', 'a2a1', 'b2b1', 'a3a2a1', 'b3b2b1'}
    # The order within these groups doesn't matter
    assert set(recording_udf.order_called()[:2]) == {'a1', 'b1'}
    assert set(recording_udf.order_called()[2:4]) == {'a2a1', 'b2b1'}
    assert set(recording_udf.order_called()[4:6]) == {'a3a2a1', 'b3b2b1'}


def test_executes_leaf_nodes_first_with_batching(
    batch_recording_udf: Type[BatchRecordingUdf], execute: ExecuteFunction
) -> None:
    data = execute(
        """
        A = BatchRecordingUdf(id=("a3" + BatchRecordingUdf(id=("a2" + BatchRecordingUdf(id="a1")))))
        B = BatchRecordingUdf(id=("b3" + BatchRecordingUdf(id=("b2" + BatchRecordingUdf(id="b1")))))
        C = BatchRecordingUdf(id="c1")
        D = BatchRecordingUdf(id="d1")
        """,
        async_pool=gevent.pool.Pool(10),
    )

    assert data == {'A': 'a3a2a1', 'B': 'b3b2b1', 'C': 'c1', 'D': 'd1'}
    assert batch_recording_udf.order_called() == [['a1', 'b1', 'c1', 'd1'], ['a2a1', 'b2b1'], ['a3a2a1', 'b3b2b1']]


def test_batch_grouping_with_routing_keys(
    batch_recording_udf: Type[BatchRecordingUdf], execute: ExecuteFunction
) -> None:
    data = execute(
        """
        A = BatchRecordingUdf(id=("a3" + BatchRecordingUdf(id=("a2" + BatchRecordingUdf(id="a1", routing_key='1')))))
        B = BatchRecordingUdf(id=("b3" + BatchRecordingUdf(id=("b2" + BatchRecordingUdf(id="b1", routing_key='2')))))
        C = BatchRecordingUdf(id="c1", routing_key='1')
        D = BatchRecordingUdf(id="d1", routing_key='2')
        E = BatchRecordingUdf(id="e1", routing_key='3')
        F = BatchRecordingUdf(id="f1", routing_key='3')
        G = BatchRecordingUdf(id="g1", routing_key='3')
        """,
        async_pool=gevent.pool.Pool(1),
    )

    assert data == {'A': 'a3a2a1', 'B': 'b3b2b1', 'C': 'c1', 'D': 'd1', 'E': 'e1', 'F': 'f1', 'G': 'g1'}
    assert batch_recording_udf.order_called() == [
        ['a1', 'c1'],
        ['b1', 'd1'],
        ['e1', 'f1', 'g1'],
        ['a2a1'],
        ['a3a2a1'],
        ['b2b1'],
        ['b3b2b1'],
    ]


def test_can_make_progress_while_blocked_on_async(blocking_udf: Type[BlockingUdf], execute: ExecuteFunction) -> None:
    # Run and wait for things to settle down
    main = gevent.spawn(
        execute,
        # Only blocks on b1
        """
        A1 = BlockingUdf(block=False, id="a1")
        A2 = BlockingUdf(block=False, id=A1 + "a2")
        A3 = BlockingUdf(block=False, id=A2 + "a3")
        B1 = BlockingUdf(block=True, id="b1")
        B2 = BlockingUdf(block=False, id=B1 + "b2")
        B3 = BlockingUdf(block=False, id=B2 + "b3")
        """,
        async_pool=gevent.pool.Pool(10),
    )
    wait_idle(main)

    assert set(blocking_udf.order_called()) == {'a1', 'a1a2', 'a1a2a3', 'b1'}
    assert len(blocking_udf.blocking()) == 1

    blocking_udf.unblock_any()
    wait_idle(main)

    data = main.get(block=False)
    assert data == {'A1': 'a1', 'A2': 'a1a2', 'A3': 'a1a2a3', 'B1': 'b1', 'B2': 'b1b2', 'B3': 'b1b2b3'}


def test_limits_max_async(blocking_udf: Type[BlockingUdf], execute: ExecuteFunction) -> None:
    main = gevent.spawn(
        execute,
        """
        A = BlockingUdf(block=True, id="a")
        B = BlockingUdf(block=True, id="b")
        C = BlockingUdf(block=True, id="c")
        D = BlockingUdf(block=True, id="d")
        E = BlockingUdf(block=True, id="e")
        """,
        async_pool=gevent.pool.Pool(3),
    )
    wait_idle(main)

    assert len(blocking_udf.order_called()) == 3
    assert len(blocking_udf.blocking()) == 3

    blocking_udf.unblock_any()
    wait_idle(main)

    assert len(blocking_udf.order_called()) == 4
    assert len(blocking_udf.blocking()) == 3

    blocking_udf.unblock_any()
    wait_idle(main)

    assert len(blocking_udf.order_called()) == 5
    assert len(blocking_udf.blocking()) == 3

    blocking_udf.unblock_any()
    wait_idle(main)

    assert len(blocking_udf.order_called()) == 5
    assert len(blocking_udf.blocking()) == 2

    blocking_udf.unblock_any()
    wait_idle(main)

    assert len(blocking_udf.order_called()) == 5
    assert len(blocking_udf.blocking()) == 1

    blocking_udf.unblock_any()
    wait_idle(main)

    assert len(blocking_udf.order_called()) == 5
    assert len(blocking_udf.blocking()) == 0

    data = main.get(block=False)
    assert data == {'A': 'a', 'B': 'b', 'C': 'c', 'D': 'd', 'E': 'e'}


def test_can_run_fully_synchronously(blocking_udf: Type[BlockingUdf], execute: ExecuteFunction) -> None:
    main = gevent.spawn(execute, 'A = BlockingUdf(block=True, id="a")', async_pool=None)
    wait_idle(main)

    assert len(blocking_udf.blocking()) == 1
    # We were run/blocked on the main thread
    assert next(iter(blocking_udf.blocking().values())) == main

    blocking_udf.unblock_any()
    wait_idle(main)
    data = main.get(block=False)

    assert data == {'A': 'a'}


@pytest.mark.parametrize('execute_async', (True, False))
def test_only_runs_async_if_requested(
    execute_async: bool, blocking_udf: Type[BlockingUdf], execute: ExecuteFunction
) -> None:
    blocking_udf.execute_async = execute_async
    main = gevent.spawn(execute, 'A = BlockingUdf(block=True, id="a")', async_pool=gevent.pool.Pool(1))
    wait_idle(main)

    assert len(blocking_udf.blocking()) == 1
    if execute_async:
        # We were run/blocked *off* the main thread
        assert next(iter(blocking_udf.blocking().values())) != main
    else:
        # We were run/blocked *on* the main thread
        assert next(iter(blocking_udf.blocking().values())) == main

    # Let this finish
    blocking_udf.unblock_any()
    wait_idle(main)
    main.get(block=False)


def test_detects_invalid_pool(execute: ExecuteFunction) -> None:
    with pytest.raises(AssertionError):
        execute('A = 1', async_pool=gevent.pool.Pool(0))


@pytest.mark.skip(reason='errors temporarily off')
def test_records_root_cause_errors(udf_registry: UDFRegistry, execute_with_result: ExecuteWithResultFunction) -> None:
    udf_registry.register(FailingUdf)
    result = execute_with_result(
        """
        A = 1 + FailingUdf()
        B = 2 + FailingUdf()
        """
    )

    # These should be the two original errors
    assert [type(e.error) for e in result.error_infos] == [ValueError, ValueError]
    # We should set the Druid metadata correctly
    assert result.extracted_features['__error_count'] == 2

    parsed_errors_json = json.loads(result.error_traces_json)
    assert len(parsed_errors_json) == 2
    # These should look like tracebacks with the right errors
    assert parsed_errors_json[0]['traceback'].startswith('Traceback')
    assert 'ValueError: intentional failure' in parsed_errors_json[0]['traceback']
    assert parsed_errors_json[1]['traceback'].startswith('Traceback')
    assert 'ValueError: intentional failure' in parsed_errors_json[1]['traceback']
    # They should also include the source location
    rules_source_locations = sorted(parsed_error['rules_source_location'] for parsed_error in parsed_errors_json)
    assert rules_source_locations == ['main.sml:2:8 - FailingUdf()', 'main.sml:3:8 - FailingUdf()']


def test_errors_propagate_to_dependencies_only(udf_registry: UDFRegistry, execute: ExecuteFunction) -> None:
    udf_registry.register(FailingUdf)
    data = execute(
        """
        A = 1 + FailingUdf()
        B = 2 + 3
        C = 4 + FailingUdf()
        D = 5 + 6
        """,
        allow_errors=True,
    )
    assert data == {'A': None, 'B': 5, 'C': None, 'D': 11}


def test_errors_propagate_to_dependencies_only_with_batching(
    udf_registry: UDFRegistry, batch_failing_udf: Type[BatchFailingUdf], execute: ExecuteFunction
) -> None:
    udf_registry.register(FailingUdf)
    data = execute(
        """
        A = 1 + FailingUdf()
        # This will fail because it is ODD
        B = 2 + BatchFailingUdf(id=1)
        C = 4 + FailingUdf()
        # This will succeed because it is EVEN
        D = 5 + BatchFailingUdf(id=2)
        # This should remain unaffected by the failing nodes
        E = 6 + 7
        """,
        allow_errors=True,
        async_pool=gevent.pool.Pool(10),
    )
    assert data == {'A': None, 'B': None, 'C': None, 'D': 7, 'E': 13}
    # Make sure that everything was actually batched properly
    assert batch_failing_udf.order_called() == [[1, 2]]


def test_errors_propagate_through_features(
    udf_registry: UDFRegistry, execute_with_result: ExecuteWithResultFunction
) -> None:
    udf_registry.register(FailingUdf)
    result = execute_with_result(
        """
        A = FailingUdf()
        B = 1 + A
        """
    )

    # We shouldn't get an error from the addition
    assert len(result.error_infos) == 1
    assert result.extracted_features['A'] is None
    assert result.extracted_features['B'] is None


def test_errors_propagate_through_features_with_batching(
    batch_failing_udf: Type[BatchFailingUdf], execute_with_result: ExecuteWithResultFunction
) -> None:
    result = execute_with_result(
        """
        A = BatchFailingUdf(id=1)
        B = 1 + BatchFailingUdf(id=2)
        C = 1 + BatchFailingUdf(id=3)
        # Will fail because of A
        D = 1 + A
        # Will succeed with 5
        E = 2 + B
        # Will fail because of C
        F = 3 + C
        """,
        async_pool=gevent.pool.Pool(10),
    )

    # We shouldn't get any extra error from downstream node failures.
    # Only 2 errors should be recorded.
    assert len(result.error_infos) == 2
    assert result.extracted_features['A'] is None
    assert result.extracted_features['B'] == 3
    assert result.extracted_features['C'] is None
    assert result.extracted_features['D'] is None
    assert result.extracted_features['E'] == 5
    assert result.extracted_features['F'] is None
    # Ensure batching actually happened
    assert batch_failing_udf.order_called() == [[1, 2, 3]]


# TODO: if we're enforcing strict optionals then this won't work anymore. We might need to come up with a way to
# explicitly handle errors (rather than implicitly treating every value as Nullable)
def test_errors_can_conditionally_be_consumed_as_none(udf_registry: UDFRegistry, execute: ExecuteFunction) -> None:
    class HadValueUdfArguments(ArgumentsBase):
        value: Optional[object]

    class HadValueUdf(UDFBase[HadValueUdfArguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: HadValueUdfArguments) -> str:
            if arguments.value is None:
                return 'got None'
            else:
                return 'got non-None'

    class FailingDynamicUdf(UDFBase[ArgumentsBase, Any]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> Any:
            raise ValueError('intentional failure')

    udf_registry.register(HadValueUdf)
    udf_registry.register(FailingDynamicUdf)
    udf_registry.register(FailingUdf)
    udf_registry.register(FailingStrUdf)

    data = execute(
        """
        FailedBool: bool = FailingDynamicUdf()
        Or = FailedBool or True
        And = FailedBool and True

        ### In

        # These still execute:
        In_False1 = FailingUdf() in [1, 2, 3]
        InStr_False1 = FailingStrUdf() in "abc"
        InStr_False2 = FailingStrUdf() in ["a", "b", "c"]

        # These propogate failures:
        In_None1 = FailingUdf() in [FailingUdf(), 2, 3]
        In_None2 = FailingUdf() in [FailingUdf()]
        InStr_None1 = FailingStrUdf() in [FailingStrUdf(), "b", "c"]
        InStr_None2 = FailingStrUdf() in FailingStrUdf()
        InStr_None3 = "a" in FailingStrUdf()

        #

        ### NotIn

        # These still execute:
        NotIn_True1 = FailingUdf() not in [1, 2, 3]
        NotInStr_True1 = FailingStrUdf() not in "abc"
        NotInStr_True2 = FailingStrUdf() not in ["a", "b", "c"]

        # These propogate failures:
        NotIn_None1 = FailingUdf() not in [FailingUdf(), 2, 3]
        NotIn_None2 = FailingUdf() not in [FailingUdf()]
        NotInStr_None1 = FailingStrUdf() not in [FailingStrUdf(), "b", "c"]
        NotInStr_None2 = FailingStrUdf() not in FailingStrUdf()
        NotInStr_None3 = "a" not in FailingStrUdf()

        #

        EqualTrue = FailingUdf() == None
        EqualFalse = 1 == FailingUdf()
        NotEqualTrue = FailingUdf() != 1
        NotEqualFalse = None != FailingUdf()
        Udf = HadValueUdf(value=FailingUdf())
        """,
        allow_errors=True,
    )
    assert data == {
        'FailedBool': None,
        'Or': True,
        'And': False,
        # In
        'In_False1': False,
        'InStr_False1': False,
        'InStr_False2': False,
        'In_None1': None,
        'In_None2': None,
        'InStr_None1': None,
        'InStr_None2': None,
        'InStr_None3': None,
        # NotIn
        'NotIn_True1': True,
        'NotInStr_True1': True,
        'NotInStr_True2': True,
        'NotIn_None1': None,
        'NotIn_None2': None,
        'NotInStr_None1': None,
        'NotInStr_None2': None,
        'NotInStr_None3': None,
        #
        'EqualTrue': True,
        'EqualFalse': False,
        'NotEqualTrue': True,
        'NotEqualFalse': False,
        'Udf': 'got None',
    }


def test_errors_that_are_expected_are_filtered_out(udf_registry: UDFRegistry, execute: ExecuteFunction) -> None:
    class FailingExpectedUdf(UDFBase[ArgumentsBase, str]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
            raise ExpectedUdfException()

    udf_registry.register(FailingExpectedUdf)

    data = execute('A = FailingExpectedUdf()')

    assert data == {'A': None}


def test_converts_convertible_types_after_execution(udf_registry: UDFRegistry, execute: ExecuteFunction) -> None:
    @dataclass
    class MyConvertibleType(PostExecutionConvertible[str]):
        value: str

        def to_post_execution_value(self) -> str:
            return self.value

    class ReturnsConvertible(UDFBase[ArgumentsBase, MyConvertibleType]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> MyConvertibleType:
            return MyConvertibleType(value='hello')

    udf_registry.register(ReturnsConvertible)

    data = execute('A = ReturnsConvertible()')

    assert data == {'A': 'hello'}


def test_unwraps_values_during_execution_as_needed(udf_registry: UDFRegistry, execute: ExecuteFunction) -> None:
    @dataclass
    class MyConvertibleType(PostExecutionConvertible[str]):
        value: str

        def to_post_execution_value(self) -> str:
            return self.value

    class ReturnsConvertible(UDFBase[ArgumentsBase, MyConvertibleType]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> MyConvertibleType:
            return MyConvertibleType(value='hello')

    class TakesConvertibleArguments(ArgumentsBase):
        convertible: MyConvertibleType

    class TakesConvertible(UDFBase[TakesConvertibleArguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: TakesConvertibleArguments) -> str:
            return type(arguments.convertible).__name__

    class TakesStrArguments(ArgumentsBase):
        s: str

    class TakesStr(UDFBase[TakesStrArguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: TakesStrArguments) -> str:
            return type(arguments.s).__name__

    udf_registry.register(ReturnsConvertible)
    udf_registry.register(TakesConvertible)
    udf_registry.register(TakesStr)

    data = execute(
        """
        Convertible = ReturnsConvertible()
        OpConvert1 = Convertible + " there"
        OpConvert2 = ReturnsConvertible() + " there"
        ArgConvert1 = TakesStr(s=Convertible)
        ArgConvert2 = TakesStr(s=ReturnsConvertible())
        ArgNoConvert1 = TakesConvertible(convertible=Convertible)
        ArgNoConvert2 = TakesConvertible(convertible=ReturnsConvertible())
        Assigned1: str = Convertible
        Assigned2: str = ReturnsConvertible()
        FString: ExtractLiteral[str] = f'hey {Convertible}'
        EqLeftTrue = Convertible == 'hello'
        EqLeftFalse = Convertible == 'there'
        EqRightTrue = 'hello' == Convertible
        EqRightFalse = 'there' == Convertible
        InLeftTrue = Convertible in ['hello', 'there']
        InLeftFalse = Convertible in ['hey', 'hi']
        InRightTrue = 'ello' in Convertible
        InRightFalse = 'here' in Convertible
        """
    )

    assert data == {
        'Convertible': 'hello',
        'OpConvert1': 'hello there',
        'OpConvert2': 'hello there',
        'ArgConvert1': 'str',
        'ArgConvert2': 'str',
        'ArgNoConvert1': 'MyConvertibleType',
        'ArgNoConvert2': 'MyConvertibleType',
        'Assigned1': 'hello',
        'Assigned2': 'hello',
        'FString': 'hey hello',
        'EqLeftTrue': True,
        'EqLeftFalse': False,
        'EqRightTrue': True,
        'EqRightFalse': False,
        'InLeftTrue': True,
        'InLeftFalse': False,
        'InRightTrue': True,
        'InRightFalse': False,
    }


def test_local_values_are_sandboxed_to_file_and_ignored_in_output(
    udf_registry: UDFRegistry, execute: ExecuteFunction
) -> None:
    udf_registry.register(Import)

    data = execute(
        {
            'main.sml': """
                _Foo = 1
                Import(rules=['bar.sml'])
                Main = _Foo
            """,
            'bar.sml': """
                _Foo = 2
                Bar = _Foo
            """,
        }
    )
    assert data == {'Main': 1, 'Bar': 2}


def test_dependent_node_imported_after_parent_node_executed(
    udf_registry: UDFRegistry, execute: ExecuteFunction
) -> None:
    udf_registry.register(Import)

    data = execute(
        {
            # This rule structure gives the executor enough iterations to execute `Parent = 4` before `bar.sml` can be
            # imported. We want to test that the dependency relationship between `Child` and `Parent` will be maintained
            # even if `Parent` is done executing before we add `Child` to the execution DAG. If this were not true,
            # then the `Child` node would not get executed.
            'main.sml': """
                Import(rules=['baz.sml'])
                Import(rules=['foo.sml'])
            """,
            'foo.sml': """
                Import(rules=['bar.sml'])
            """,
            'bar.sml': """
                Import(rules=['baz.sml'])
                Child = Parent + 4
            """,
            'baz.sml': """
                Parent = 4
            """,
        }
    )
    assert data == {'Child': 8}
