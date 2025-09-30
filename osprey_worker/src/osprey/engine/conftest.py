import itertools
import json
import pathlib
import sys
from contextlib import contextmanager
from datetime import datetime
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, ContextManager, Dict, Iterator, Optional, Set, Type, TypeVar, Union

import gevent.pool
import pytest
from osprey.engine import shared_constants
from osprey.engine.ast.sources import SOURCE_ENTRY_POINT_PATH, Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.base_validator import BaseValidator
from osprey.engine.ast_validator.validation_context import ValidatedSources, ValidationContext, ValidationFailed
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.execution_graph import compile_execution_graph
from osprey.engine.executor.executor import execute as osprey_execute
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.stdlib import get_config_registry
from osprey.engine.udf.registry import UDFRegistry
from typing_extensions import Protocol

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.fixtures import FixtureRequest

SourcesDict = Union[Sources, str, Dict[str, str]]
CheckOutputFunction = Callable[[str], bool]

_WINDOWS_FRIENDLY_SUBSTITUTIONS = {
    '\\': '[bslash]',
    '/': '[fslash]',
    ':': '[colon]',
    '*': '[star]',
    '?': '[question]',
    '"': "'",
    '<': '[lt]',
    '>': '[gt]',
    '|': '[or]',
}


@pytest.fixture()
def check_output(request: 'FixtureRequest') -> CheckOutputFunction:
    """
    This checks a string against the output of a pre-saved string. This is good to test raw validator/rendered
    output.

    To use:
    ```
    def test_some_foo(check_output):
        output = some_function_that_returns_a_string()
        assert check_output(output)
    ```

    This function will check that the string `output` does not change. If it does change, tests will fail! However,
    the output may have deliberately changed, in which you can re-run the tests with `py.test --write-outputs`, which
    will re-snapshot the results. You can then compare the diffs using git, etc...

    You can instruct this fixture to record different outputs for different versions of Python with the
    `vary_output_by_py_version` marker.
    """
    should_include_version = request.node.get_closest_marker('vary_output_by_py_version')
    did_call = False

    def get_output_file() -> pathlib.Path:
        test_dir = pathlib.Path(request.node.fspath.dirname)
        output_dir = test_dir / request.node.fspath.purebasename
        sanitized_name = request.node.name
        for orig, replacement in _WINDOWS_FRIENDLY_SUBSTITUTIONS.items():
            sanitized_name = sanitized_name.replace(orig, replacement)
        if should_include_version:
            file_name = f'{sanitized_name}-py-{sys.version_info.major}{sys.version_info.minor}.txt'
        else:
            file_name = f'{sanitized_name}.txt'
        output_file = output_dir / file_name
        return output_file

    def get_file_contents() -> str:
        output_file = get_output_file()
        if not output_file.exists():
            raise Exception('file does not exist, re-run tests with --write-outputs')

        return output_file.read_text().rstrip('\n')

    def put_file_contents(rendered: str) -> None:
        output_file = get_output_file()
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open('w') as fp:
            fp.write(rendered)

    def check_output(output: str) -> bool:
        nonlocal did_call
        if did_call:
            raise Exception('Cannot call check_output more than once per test run.')

        did_call = True
        if getattr(request.config.option, 'write_outputs', False):
            put_file_contents(output)
            return True
        else:
            try:
                assert output.rstrip('\n') == get_file_contents(), (
                    'Expected output has changed. Re-run with --write-outputs if change is intended.'
                    f'\n\tExpected test output: {get_output_file()}'
                )
            except AssertionError as e:
                if getattr(request.config.option, 'write_first_failed_output', False):
                    request.config.option.write_first_failed_output = False
                    put_file_contents(output)
                    raise AssertionError(
                        e,
                        'Expected output has changed. New output was written.'
                        f'\n\tExpected test output: {get_output_file()}',
                    )
                raise
            return True

    return check_output


CheckJsonOutputFunction = Callable[[SourcesDict], bool]


@pytest.fixture()
def check_json_output(check_output: CheckOutputFunction) -> CheckJsonOutputFunction:
    """
    Calls `check_output` after converting request to JSON
    """

    def check_json_output(source_dict: SourcesDict) -> bool:
        jsonified_dict = json.dumps(source_dict, sort_keys=True, indent=4, separators=(',', ': '))
        return check_output(jsonified_dict)

    return check_json_output


_T = TypeVar('_T')


def assert_isinstance(val: object, typ: Type[_T]) -> _T:
    assert isinstance(val, typ)
    return val


def assert_issubclass(val: object, typ: Type[_T]) -> Type[_T]:
    assert isinstance(val, type) and issubclass(val, typ)
    return val


class RunValidationFunction(Protocol):
    def __call__(
        self,
        sources_dict: SourcesDict,
        warning_as_error: bool = ...,
        validator_registry: Optional[ValidatorRegistry] = ...,
    ) -> ValidatedSources: ...


@pytest.fixture()
def run_validation(request: 'FixtureRequest', udf_registry: UDFRegistry) -> RunValidationFunction:
    """Runs a validation, using the validators that the test function has marked.

    It uses the following markers:
        - use_validators([List, Of, Validator, Classes])
        - use_standard_rules_validators()
        - use_udf_registry(UDFRegistry)
        - use_osprey_stdlib()
        - inject_validator_result(validator=ValidatorClass, result=Something)

    Provides a function that takes either a source dictionary, or a single string to use as the sources
    entry-point. Automatically de-dents texts, so you can use triple-quoted strings that are nicely formatted.
    """

    validators: Set[Type[BaseValidator]] = set(
        assert_issubclass(validator, BaseValidator)
        for validator in itertools.chain.from_iterable(
            assert_isinstance(n.args[0], list) for n in request.node.iter_markers('use_validators')
        )
    )
    if request.node.get_closest_marker('use_standard_rules_validators'):
        registry = ValidatorRegistry.instance_with_additional_validators(
            *validators, get_config_registry().get_validator()
        )
    else:
        registry = ValidatorRegistry.from_validator_classes(validators)

    def run_validation(
        sources_dict: SourcesDict,
        warning_as_error: bool = False,
        validator_registry: Optional[ValidatorRegistry] = None,
    ) -> ValidatedSources:
        context = ValidationContext(
            sources=into_sources(sources_dict),
            udf_registry=udf_registry,
            validator_registry=validator_registry or registry,
            warning_as_error=warning_as_error,
        )
        for marker in request.node.iter_markers('inject_validator_result'):
            context._test_only_inject_validator_result(
                assert_issubclass(marker.kwargs['validator'], BaseValidator), marker.kwargs['result']
            )

        return context.run()

    return run_validation


def into_sources(sources_dict: SourcesDict) -> Sources:
    if isinstance(sources_dict, Sources):
        return sources_dict

    if isinstance(sources_dict, str):
        sources_dict = {SOURCE_ENTRY_POINT_PATH: sources_dict}

    for k, v in sources_dict.items():
        sources_dict[k] = dedent(v)

    return Sources.from_dict(sources_dict)


class ExecuteWithResultFunction(Protocol):
    def __call__(
        self,
        sources_dict: SourcesDict,
        data: Optional[Dict[str, object]] = ...,
        secret_data: Optional[Dict[str, str]] = ...,
        action_name: str = ...,
        action_id: int = ...,
        udf_helpers: Optional[UDFHelpers] = None,
        async_pool: Optional[gevent.pool.Pool] = ...,
        action_time: Optional[datetime] = ...,
    ) -> ExecutionResult: ...


class ExecuteFunction(Protocol):
    def __call__(
        self,
        sources_dict: SourcesDict,
        data: Optional[Dict[str, object]] = ...,
        secret_data: Optional[Dict[str, str]] = ...,
        action_name: str = ...,
        action_id: int = ...,
        udf_helpers: Optional[UDFHelpers] = None,
        async_pool: Optional[gevent.pool.Pool] = ...,
        action_time: Optional[datetime] = ...,
        allow_errors: bool = ...,
    ) -> Dict[str, object]: ...


@pytest.fixture()
def execute_with_result(udf_registry: UDFRegistry) -> ExecuteWithResultFunction:
    """Like the execute fixture, but returns the full ExecutionResult."""

    def execute_with_result_(
        sources_dict: SourcesDict,
        data: Optional[Dict[str, object]] = None,
        secret_data: Optional[Dict[str, str]] = None,
        action_name: str = 'test',
        action_id: int = 1,
        udf_helpers: Optional[UDFHelpers] = None,
        async_pool: Optional[gevent.pool.Pool] = None,
        action_time: Optional[datetime] = None,
    ) -> ExecutionResult:
        sources = into_sources(sources_dict)
        config_validator = get_config_registry().get_validator()
        validator_registry = ValidatorRegistry.get_instance().instance_with_additional_validators(config_validator)
        try:
            validated_sources = validate_sources(sources, udf_registry, validator_registry)
        except ValidationFailed as e:
            print(e.rendered())
            raise
        execution_graph = compile_execution_graph(validated_sources)
        action = Action(
            action_id=action_id,
            data=data or {},
            secret_data=secret_data or {},
            action_name=action_name,
            timestamp=action_time or datetime.utcnow(),
        )
        return osprey_execute(execution_graph, udf_helpers or UDFHelpers(), action, async_pool)

    return execute_with_result_


@pytest.fixture()
def execute(execute_with_result: ExecuteWithResultFunction) -> ExecuteFunction:
    """A convenience wrapper that parses, validates and then executes the given sources.

    It uses the following markers:
        - use_udf_registry(UFRegistry)
        - use_osprey_stdlib()
    """

    def execute_(
        sources_dict: SourcesDict,
        data: Optional[Dict[str, object]] = None,
        secret_data: Optional[Dict[str, str]] = None,
        action_name: str = 'test',
        action_id: int = 1,
        udf_helpers: Optional[UDFHelpers] = None,
        async_pool: Optional[gevent.pool.Pool] = None,
        action_time: Optional[datetime] = None,
        allow_errors: bool = False,
    ) -> Dict[str, object]:
        result = execute_with_result(
            sources_dict=sources_dict,
            data=data,
            secret_data=secret_data,
            action_name=action_name,
            action_id=action_id,
            udf_helpers=udf_helpers,
            async_pool=async_pool,
            action_time=action_time,
        )
        if not allow_errors and len(result.error_infos) > 0:
            # Raise the first error, that should help track it down more easily.
            raise result.error_infos[0].error
        extracted_features = result.extracted_features
        # TODO: This needs to be less messy, but for now, we lodge this extra data into the extracted features,
        # as it's needed for druid indexing, but not for tests, so we'll yank these here.
        del extracted_features['__timestamp']
        del extracted_features['__action_id']
        del extracted_features['__error_count']
        # custom features that might not be present
        extracted_features.pop(shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_NAME, None)
        extracted_features.pop('__classifications', None)
        extracted_features.pop('__signals', None)
        extracted_features.pop('__verdicts', None)
        return extracted_features

    return execute_


@pytest.fixture()
def udf_registry(request: 'FixtureRequest') -> UDFRegistry:
    """A fixture that provides the udf registry that a test should use."""
    udf_registry = UDFRegistry()
    if request.node.get_closest_marker('use_osprey_stdlib'):
        from osprey.worker.adaptor.plugin_manager import bootstrap_udfs

        registry, _ = bootstrap_udfs()
        udf_registry.merge(registry)

    for node in request.node.iter_markers('use_udf_registry'):
        for arg in node.args:
            udf_registry.merge(assert_isinstance(arg, UDFRegistry))

    return udf_registry


CheckFailureFunction = Callable[[], ContextManager[None]]


@pytest.fixture()
def check_failure(check_output: CheckOutputFunction) -> CheckFailureFunction:
    """A fixture that provides a context manager that will check that the context it wraps throws a validation failed
    error, and checks the output using the `check_output` fixture."""

    @contextmanager
    def check_failure() -> Iterator[None]:
        with pytest.raises(ValidationFailed) as e:
            yield

        assert check_output(e.value.rendered())

    return check_failure


def pytest_configure(config: 'Config') -> None:
    config.addinivalue_line(
        'markers',
        'use_validators([validator_classes, ...]): used with the `run_validation` fixture, '
        'to specify the validators to execute.',
    )
    config.addinivalue_line(
        'markers',
        'use_standard_rules_validators(): used with the `run_validation` fixture, runs all normal rules validators.',
    )
    config.addinivalue_line(
        'markers',
        'inject_validator_result(validator={...}, result={...}): used with the `run_validation` fixture, '
        'to mock requisite validator results.',
    )
    config.addinivalue_line(
        'markers', 'use_udf_registry(UDFRegistry()): use a given udf registry during validation/execution'
    )
    config.addinivalue_line(
        'markers', 'use_osprey_stdlib(): uses the osprey udf stdlib registry during validation/execution.'
    )
    config.addinivalue_line(
        'markers',
        'vary_output_by_py_version(): instructs the `check_output` feature to record a separate file for '
        'different python major/minor versions',
    )
