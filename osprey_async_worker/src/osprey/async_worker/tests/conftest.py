"""Shared fixtures for async worker tests.

Provides async equivalents of the osprey engine conftest fixtures,
using the async executor instead of the gevent one.
"""

from datetime import datetime
from textwrap import dedent
from typing import Any, Dict, Optional, Sequence, Type, Union

import pytest
from osprey.engine.ast.sources import SOURCE_ENTRY_POINT_PATH, Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validation_context import ValidatedSources, ValidationFailed
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.execution_graph import compile_execution_graph
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.stdlib import get_config_registry
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.singletons import CONFIG

from osprey.async_worker.executor import execute as async_execute

SourcesDict = Union[Sources, str, Dict[str, str]]


def _into_sources(sources_dict: SourcesDict) -> Sources:
    if isinstance(sources_dict, Sources):
        return sources_dict
    if isinstance(sources_dict, str):
        sources_dict = {SOURCE_ENTRY_POINT_PATH: sources_dict}
    for k, v in sources_dict.items():
        sources_dict[k] = dedent(v)
    return Sources.from_dict(sources_dict)


@pytest.fixture(autouse=True)
def config_setup():
    CONFIG.instance().configure_from_env()
    yield
    CONFIG.instance().unconfigure_for_tests()


@pytest.fixture()
def stdlib_udf_registry() -> UDFRegistry:
    """UDF registry with stdlib UDFs only."""
    from osprey.worker._stdlibplugin.udf_register import register_udfs
    from osprey.worker._stdlibplugin.validator_regsiter import register_ast_validators

    # Register standard validators (needed for compile_execution_graph)
    registry = ValidatorRegistry.get_instance()
    for validator in register_ast_validators():
        registry.register_to_instance(validator)

    return UDFRegistry.with_udfs(*register_udfs())


@pytest.fixture()
def async_execute_with_result(stdlib_udf_registry: UDFRegistry):
    """Execute .sml rules using the async executor. Returns full ExecutionResult."""

    async def _execute(
        sources_dict: SourcesDict,
        data: Optional[Dict[str, object]] = None,
        action_name: str = 'test',
        action_id: int = 1,
        udf_helpers: Optional[UDFHelpers] = None,
        udf_registry: Optional[UDFRegistry] = None,
        max_concurrent: int = 12,
        action_time: Optional[datetime] = None,
    ) -> ExecutionResult:
        registry = udf_registry or stdlib_udf_registry
        sources = _into_sources(sources_dict)

        config_validator = get_config_registry().get_validator()
        validator_registry = ValidatorRegistry.get_instance().instance_with_additional_validators(config_validator)

        try:
            validated_sources = validate_sources(sources, registry, validator_registry)
        except ValidationFailed as e:
            print(e.rendered())
            raise

        execution_graph = compile_execution_graph(validated_sources)
        action = Action(
            action_id=action_id,
            data=data or {},
            action_name=action_name,
            timestamp=action_time or datetime.utcnow(),
        )
        return await async_execute(
            execution_graph, udf_helpers or UDFHelpers(), action, max_concurrent=max_concurrent
        )

    return _execute


@pytest.fixture()
def async_execute_fn(async_execute_with_result):
    """Execute .sml rules using the async executor. Returns extracted features dict."""

    async def _execute(
        sources_dict: SourcesDict,
        data: Optional[Dict[str, object]] = None,
        action_name: str = 'test',
        action_id: int = 1,
        udf_helpers: Optional[UDFHelpers] = None,
        udf_registry: Optional[UDFRegistry] = None,
        max_concurrent: int = 12,
        allow_errors: bool = False,
    ) -> Dict[str, object]:
        result = await async_execute_with_result(
            sources_dict=sources_dict,
            data=data,
            action_name=action_name,
            action_id=action_id,
            udf_helpers=udf_helpers,
            udf_registry=udf_registry,
            max_concurrent=max_concurrent,
        )
        if not allow_errors and len(result.error_infos) > 0:
            raise result.error_infos[0].error

        features = result.extracted_features
        # Remove internal features like the gevent conftest does
        for key in ['__timestamp', '__action_id', '__error_count', '__sample_rate',
                     '__entity_label_mutations', '__classifications', '__signals', '__verdicts']:
            features.pop(key, None)
        return features

    return _execute
