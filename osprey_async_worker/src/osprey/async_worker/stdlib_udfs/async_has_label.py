# pattern: Imperative Shell

"""native async `HasLabel`"""

from typing import Any, Sequence, cast

from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase
from osprey.async_worker.lib.external_service import AsyncExternalService
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.entities import EntityT
from osprey.engine.stdlib.udfs.labels import (
    BatchableHasLabelArguments,
    HasLabelArguments,
)
from osprey.engine.stdlib.udfs.labels import (
    HasLabel as SyncHasLabel,
)
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.labels import EntityLabels
from result import Err, Ok, Result


class HasLabel(AsyncBatchableUDFBase[HasLabelArguments, bool, BatchableHasLabelArguments], SyncHasLabel):
    """await labels while reusing sync validation and evaluation"""

    category = SyncHasLabel.category

    @classmethod
    def _get_udf_base_args(cls) -> tuple[type, ...]:
        """resolve async-first mro type variables"""
        return (HasLabelArguments, bool, BatchableHasLabelArguments)

    async def _read_labels(
        self,
        execution_context: ExecutionContext,
        label_provider: AsyncExternalService[EntityT[Any], EntityLabels],
        entity: EntityT[Any],
    ) -> EntityLabels:
        accessor = execution_context.get_async_external_service_accessor(label_provider)
        try:
            return await accessor.get(entity)
        except Exception as error:
            return label_provider.handle_read_error(entity, error)

    async def async_execute(self, execution_context: ExecutionContext, arguments: HasLabelArguments) -> bool:
        """look up labels for one entity"""
        # async plugins bind this class to an async service
        label_provider = cast(AsyncExternalService[EntityT[Any], EntityLabels], execution_context.get_udf_helper(self))
        entity_labels = await self._read_labels(execution_context, label_provider, arguments.entity)
        return self._execute(execution_context, self.get_batchable_arguments(arguments), entity_labels)

    async def async_execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[BatchableHasLabelArguments],
    ) -> Sequence[Result[bool, Exception]]:
        """look up labels once for a same-entity batch"""
        unique_entities = {arg.entity for arg in arguments}

        if len(unique_entities) != 1:
            raise NotImplementedError(f'batch received {len(unique_entities)} unique entities; expected 1')

        label_provider = cast(AsyncExternalService[EntityT[Any], EntityLabels], execution_context.get_udf_helper(self))
        entity_labels = await self._read_labels(execution_context, label_provider, unique_entities.pop())
        output: list[Result[bool, Exception]] = []
        for args in arguments:
            try:
                output.append(Ok(self._execute(execution_context, args, entity_labels)))
            except Exception as e:
                output.append(Err(e))
        return output
