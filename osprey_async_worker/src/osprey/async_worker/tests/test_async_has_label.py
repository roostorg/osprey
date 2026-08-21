"""native async `HasLabel` tests"""

import json
from datetime import datetime, timezone
from textwrap import dedent
from types import SimpleNamespace
from typing import Any

import pytest
from osprey.async_worker.adaptor import plugin_manager as pm
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink, AsyncBatchableUDFBase
from osprey.async_worker.engine import AsyncOspreyEngine
from osprey.async_worker.lib.external_service import AsyncExternalService
from osprey.async_worker.sinks.sink.input_stream import AsyncStaticInputStream
from osprey.async_worker.sinks.sink.rules_sink import AsyncRulesSink
from osprey.async_worker.stdlib_udfs import async_mx_lookup
from osprey.async_worker.stdlib_udfs.async_has_label import HasLabel as AsyncHasLabel
from osprey.engine.ast.sources import Sources
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelStatus
from osprey.engine.stdlib.udfs.labels import EmptyEntityError
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.osprey_shared.labels import EntityLabels, LabelReason, LabelReasons, LabelState
from osprey.worker.lib.sources_provider_base import StaticSourcesProvider
from osprey.worker.sinks.utils.acking_contexts_base import NoopAckingContext


def _source_with_labels_config(source: str, labels: set[str]) -> dict[str, str]:
    """add labels config to sml sources"""
    config = json.dumps({'labels': {label: {} for label in labels}})
    return {'main.sml': source, 'config.yaml': config}


def _into_sources(sources_dict: dict[str, str]) -> Sources:
    """dedent sml sources"""
    return Sources.from_dict({path: dedent(contents) for path, contents in sources_dict.items()})


class RecordingSink(AsyncBaseOutputSink):
    """record pushed results"""

    def __init__(self):
        self.results: list[ExecutionResult] = []

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.results.append(result)


class FakeLabelsService(AsyncExternalService[EntityT[Any], EntityLabels]):
    """return deterministic labels and record reads"""

    def __init__(self, error_on_empty: bool = False):
        self.reads: list[EntityT[Any]] = []
        self.error_on_empty = error_on_empty

    async def get_from_service(self, entity: EntityT[Any]) -> EntityLabels:
        self.reads.append(entity)
        if self.error_on_empty:
            return EntityLabels(labels={})
        reason = LabelReason(description='test label', created_at=datetime.now(timezone.utc))
        label_state = LabelState(status=LabelStatus.ADDED, reasons=LabelReasons({'test_reason': reason}))
        return EntityLabels(labels={'trusted': label_state})


class FailingLabelsService(AsyncExternalService[EntityT[Any], EntityLabels]):
    """fail every labels read"""

    async def get_from_service(self, entity: EntityT[Any]) -> EntityLabels:
        raise ValueError(f'labels service failed for {entity.type}/{entity.id}')


class FailOpenLabelsService(FailingLabelsService):
    def handle_read_error(self, entity: EntityT[Any], error: Exception) -> EntityLabels:
        return EntityLabels(labels={})


class FakeResolver:
    async def query_dns(self, domain: str, query_type: str) -> SimpleNamespace:
        if query_type == 'MX':
            data = SimpleNamespace(priority=10, exchange='mail.example.com')
        else:
            data = SimpleNamespace(addr='192.0.2.1')
        return SimpleNamespace(answer=[SimpleNamespace(data=data)])


@pytest.fixture()
def async_udf_registry() -> UDFRegistry:
    """load native async stdlib"""
    from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
    from osprey.worker._stdlibplugin.validator_regsiter import register_ast_validators

    validator_reg = ValidatorRegistry.get_instance()
    for validator in register_ast_validators():
        validator_reg.register_to_instance(validator)

    registry, _helpers = pm.bootstrap_async_udfs(config=None)
    return registry


@pytest.mark.asyncio
async def test_single_label_lookup_uses_async_service_and_preserves_result(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """await one labels read and preserve sync evaluation"""
    service = FakeLabelsService()
    helpers = UDFHelpers()
    helpers.set_udf_helper(AsyncHasLabel, service)

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'TrustedLabel = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert result.error_infos == []
    assert result.extracted_features['TrustedLabel'] is True
    assert len(service.reads) == 1
    assert service.reads[0].type == 'User'
    assert service.reads[0].id == 'u1'


@pytest.mark.asyncio
async def test_same_entity_batch_reads_once_and_preserves_order(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """read once for ordered same-entity results"""
    service = FakeLabelsService()
    helpers = UDFHelpers()
    helpers.set_udf_helper(AsyncHasLabel, service)

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'First = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")\nSecond = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert result.error_infos == []
    assert result.extracted_features['First'] is True
    assert result.extracted_features['Second'] is True
    assert len(service.reads) == 1


@pytest.mark.asyncio
async def test_batch_isolates_empty_label_error_after_successful_read(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """isolate fail-closed errors within one batch read"""
    service = FakeLabelsService(error_on_empty=True)
    helpers = UDFHelpers()
    helpers.set_udf_helper(AsyncHasLabel, service)

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'Normal = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")\nErrorOnEmpty = HasLabel(entity=Entity(type="User", id="u1"), label="trusted", error_on_empty=True)',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert result.extracted_features['Normal'] is False
    assert result.extracted_features['ErrorOnEmpty'] is None
    assert len(result.error_infos) == 1
    error = result.error_infos[0].error
    assert isinstance(error, EmptyEntityError)
    assert len(service.reads) == 1


@pytest.mark.asyncio
async def test_missing_label_helper_reaches_executor_error_handling(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """report a missing helper through executor errors"""
    helpers = UDFHelpers()

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'Result = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert len(result.error_infos) == 1
    error = result.error_infos[0].error
    assert isinstance(error, KeyError)


@pytest.mark.asyncio
async def test_label_service_failure_reaches_executor_error_handling(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """report service failure through executor errors"""
    service = FailingLabelsService()
    helpers = UDFHelpers()
    helpers.set_udf_helper(AsyncHasLabel, service)

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'Result = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert len(result.error_infos) == 1
    error = result.error_infos[0].error
    assert isinstance(error, ValueError)


@pytest.mark.asyncio
async def test_label_service_can_translate_read_failure(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """use the helper fallback after a failed labels read"""
    service = FailOpenLabelsService()
    helpers = UDFHelpers().set_udf_helper(AsyncHasLabel, service)

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'Result = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert result.error_infos == []
    assert result.extracted_features['Result'] is False


@pytest.mark.asyncio
async def test_label_service_can_translate_batched_read_failure(
    async_execute_with_result, async_udf_registry: UDFRegistry
) -> None:
    """use the helper fallback for a same-entity batch"""
    service = FailOpenLabelsService()
    helpers = UDFHelpers().set_udf_helper(AsyncHasLabel, service)

    result = await async_execute_with_result(
        sources_dict=_source_with_labels_config(
            'First = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")\nSecond = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")',
            {'trusted'},
        ),
        data={},
        udf_helpers=helpers,
        udf_registry=async_udf_registry,
    )

    assert result.error_infos == []
    assert result.extracted_features['First'] is False
    assert result.extracted_features['Second'] is False


def test_native_has_label_preserves_sync_metadata_validation_and_routing(
    async_udf_registry: UDFRegistry,
) -> None:
    """preserve sync metadata and native execution guards"""
    async_has_label = async_udf_registry.get('HasLabel')
    assert async_has_label is AsyncHasLabel
    assert async_has_label.__name__ == 'HasLabel'
    assert hasattr(async_has_label, 'is_native_async') and async_has_label.is_native_async
    assert hasattr(async_has_label, 'get_batch_routing_key')
    assert async_has_label.execute is AsyncBatchableUDFBase.execute
    assert async_has_label.execute_batch is AsyncBatchableUDFBase.execute_batch
    assert async_has_label.category.value == 'Engine'


@pytest.mark.asyncio
async def test_async_worker_processes_fake_events_with_native_dns_and_labels(
    async_udf_registry: UDFRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """process native dns and labels through the async worker pipeline"""
    monkeypatch.setattr(async_mx_lookup, '_get_resolver', FakeResolver)
    sources_provider = StaticSourcesProvider(
        sources=_into_sources(
            _source_with_labels_config(
                'MXRecord = MXLookup(domain="example.com")\nTrustedUser = HasLabel(entity=Entity(type="User", id="u1"), label="trusted")\nBothPresent = Rule(when_all=[MXRecord != "", TrustedUser], description="both present")',
                {'trusted'},
            )
        )
    )
    engine = AsyncOspreyEngine(sources_provider=sources_provider, udf_registry=async_udf_registry)
    service = FakeLabelsService()
    helpers = UDFHelpers().set_udf_helper(AsyncHasLabel, service)
    sink = RecordingSink()
    action = Action(action_id=1, data={}, action_name='test', timestamp=datetime.now(timezone.utc))
    input_stream = AsyncStaticInputStream([NoopAckingContext(action)])
    rules_sink = AsyncRulesSink(
        engine=engine,
        input_stream=input_stream,
        output_sink=sink,
        udf_helpers=helpers,
        max_concurrent_udfs=12,
    )

    await rules_sink.run()

    assert len(sink.results) == 1
    result = sink.results[0]
    assert result.error_infos == []
    assert result.extracted_features['MXRecord'] == '192.0.2.1'
    assert result.extracted_features['TrustedUser'] is True
    assert result.extracted_features['BothPresent'] is True
    assert len(service.reads) == 1
