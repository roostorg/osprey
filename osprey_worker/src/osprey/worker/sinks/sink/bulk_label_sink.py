import time
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Optional, Set

import sentry_sdk
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelStatus
from osprey.worker.lib.bulk_label import TaskStatus
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.pigeon.exceptions import RPCException
from osprey.worker.lib.publisher import BasePublisher
from osprey.worker.lib.singletons import LABELS_PROVIDER
from osprey.worker.lib.storage.bulk_label_task import BASE_DELAY_SECONDS, MAX_ATTEMPTS, BulkLabelTask
from osprey.worker.lib.storage.labels import LabelsProvider
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.sink.output_sink_utils.models import OspreyBulkJobAnalyticsEvent
from osprey.worker.ui_api.osprey.lib.druid import PeriodData, TopNDruidQuery, TopNPoPResponse
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .base_sink import BaseSink

logger = get_logger()

BULK_LABEL_REASON = '_ManuallyBulkLabeled'
# percentage difference allowed between the expected entity count and the actual entities.
# expected entity count is calculated with HyperLogLog, which has a standard error of 0.18% (0.0018)
# https://redis.io/docs/data-types/probabilistic/hyperloglogs/#:~:text=of%20a%20set.-,HyperLogLog%20is%20a%20probabilistic%20data%20structure%20that%20estimates%20the%20cardinality,a%20standard%20error%20of%200.81%25.  # noqa: E501
# 10% expected margin of error (0.1) should be PLENTY of wiggle room & still serve as a safeguard against
# largely innaccurate entity count estimations.
EXPECTED_ENTITY_MARGIN_OF_ERROR = 0.1
MAX_LABEL_SERVICE_RETRIES = 10
# Anything past 10 minutes is truly excessive for Druid querying
DEFAULT_BULK_LABEL_DRUID_QUERY_TIMEOUT = 300 * 1000  # 5min
DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT = 360  # 6min, 1min over Druid timeout
# We will give no limit bulk labels a little bit less time since they are intended to be
# split up into smaller queries.
NO_LIMIT_BULK_LABEL_DRUID_QUERY_TIMEOUT = 180 * 1000  # 3min
NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT = 240  # 4 min, 1min over Druid timeout
BULK_LABEL_DEFAULT_LIMIT = 100_000
# A threshold is required for topN queries, Druid theoretically allows the max value for a 32-bit integer as the
# upper limit, but in practice higher upper thresholds can cause unexpected failures
BULK_LABEL_NO_LIMIT_SIZE = 20_000_000

# TopN no_limit query splitting interval, in seconds
NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX = 60 * 60 * 6  # 6 hour intervals


class UnretryableTaskException(Exception):
    """When this exception is thrown the task will not be retried"""


class InternalDruidException(Exception):
    """
    When this exception is thrown, the Druid query failed due to a
    500 - INTERNAL ERROR status code. The task will be retried.
    """


class BulkLabelSink(BaseSink):
    """A bulk labeling sink that handles one task at a time"""

    def __init__(
        self,
        input_stream: BaseInputStream[BulkLabelTask],
        labels_provider: LabelsProvider,
        engine: OspreyEngine,
        analytics_publisher: BasePublisher,
    ):
        self._input_stream = input_stream
        self._labels_provider = labels_provider
        self._engine = engine
        self._metric_tags = [f'sink:{self.__class__.__name__}']
        self._analytics_publisher = analytics_publisher

    def run(self) -> None:
        for task in self._input_stream:
            assert task.attempts is not None
            assert task.dimension is not None
            tags = self._metric_tags + [
                f'attempt:{task.attempts - 1}',
                f'action:{task.dimension}',
                f'task_id:{task.id}',
            ]
            with metrics.timed('process_single_task', tags):
                if task.attempts >= MAX_ATTEMPTS and task.task_status in TaskStatus.non_final_statuses():
                    # Autofail anything going over the max attempts
                    status = TaskStatus.FAILED
                    task.release(status)
                else:
                    try:
                        self._process_task(task)
                        status = TaskStatus.COMPLETE
                    except Exception as e:
                        logger.exception(
                            f'[task_id:{task.id}] Failed to bulk label entity - attempt {task.attempts}',
                        )
                        sentry_sdk.capture_exception()
                        status = (
                            TaskStatus.FAILED
                            if isinstance(e, UnretryableTaskException) or task.attempts >= MAX_ATTEMPTS
                            else TaskStatus.RETRYING
                        )
                        tags.append(f'exception:{e.__class__.__name__}')
                        task.release(status=status, result=repr(e))

                tags.append(f'status:{status}')
                tags.append(f'prev_status:{task.task_status}')
                metrics.increment('handled_message', tags=tags)

    def _build_top_n_queries(self, task: BulkLabelTask) -> List[TopNDruidQuery]:
        """
        Returns a list of `TopNDruidQuery`s.
        These are separated based on smaller time intervals if the task is set to `no_limit`,
        otherwise it will return a single `TopNDruidQuery` (because our Druid seems to
        handle 100,000-limit topNs at scale but non-limited ones are more intense).
        """
        assert isinstance(task.query, dict)
        query = task.query.copy()
        query['dimension'] = task.dimension
        queries: List[TopNDruidQuery] = []

        query['limit'] = BULK_LABEL_DEFAULT_LIMIT
        claim_until_seconds = DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT
        if task.no_limit:
            query['limit'] = BULK_LABEL_NO_LIMIT_SIZE
            claim_until_seconds = NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT

        # set initial heartbeat in db & for ui
        task.heartbeat(
            status=TaskStatus.COLLECTING,
            new_entity_count=0,
            claim_until_seconds=claim_until_seconds,
        )

        # these are stored as posix timestamp floats
        query_start = float(query['start'])
        query_end = float(query['end'])
        time_delta = query_end - query_start

        def convert_dict_to_top_n_query(query: Any) -> TopNDruidQuery:
            query['start'] = datetime.fromtimestamp(query['start'])
            query['end'] = datetime.fromtimestamp(query['end'])
            return TopNDruidQuery(**query)

        if task.no_limit and time_delta > NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX:
            for next_start in range(int(query_start), int(query_end), NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX):
                next_end = min(next_start + NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX, query_end)
                query['start'], query['end'] = float(next_start), float(next_end)
                queries.append(convert_dict_to_top_n_query(query))
        else:
            queries.append(convert_dict_to_top_n_query(query))

        return queries

    def _collect_entity_ids(self, task: BulkLabelTask) -> Set[str]:
        def _execute_top_n_query_and_get_result(query: TopNDruidQuery) -> Any:
            try:
                query_data: TopNPoPResponse = query.execute(
                    context={'timeout': query_timeout}, calculate_previous_period=False
                )
                if not query_data:
                    return dict()

                if isinstance(query_data, (tuple, list)):
                    query_data = query_data[0]

                if isinstance(query_data, dict) and 'result' in query_data:
                    return query_data['result']

                # Fall back to parsing logic if direct access fails
                current_raw_data: List[PeriodData] = query_data.current_period
                if not current_raw_data:
                    return dict()
                current_period = PeriodData.parse_obj(current_raw_data[0]).dict(exclude={'timestamp'})
                if not isinstance(current_period, dict):
                    return dict()
                return current_period.get('result', [])
            except Exception as e:
                # Unfortunately, PyDruid has some code that makes it difficult to see what error code is thrown...
                # The stringified HTTPError is wrapped in an `OSError` object, so we can only grab the error code
                # via this annoying string.startswith() logic. We only want to retry the queries if Druid is returning
                # us 500s, otherwise we risk improperly handling errors.
                if str(e).startswith('HTTP Error 500'):
                    raise InternalDruidException(f'Druid query ({i} out of {total_num_queries}) failed: {str(e)}')
                else:
                    raise UnretryableTaskException(
                        f'Druid query ({i} out of {total_num_queries}) failed due to an unknown exception: {str(e)}'
                    )

        def _log_before_sleep(retry_state: RetryCallState) -> None:
            retry_in = retry_state.next_action.sleep if retry_state.next_action else 0
            i = retry_state.kwargs.get('i', '?')
            attempt = retry_state.attempt_number

            logger.info(
                f'[task_id:{task.id}][query:{i}/{total_num_queries}] topN query received a 500 code in: '
                f'{time.time() - query_start_time} seconds (attempt {attempt} out of 3).'
            )

            logger.info(f'[task_id:{task.id}][query:{i}/{total_num_queries}] retrying in {retry_in} seconds...')

            task.heartbeat(
                status=TaskStatus.COLLECTING,
                new_entity_count=len(entity_ids),
                claim_until_seconds=int(BASE_DELAY_SECONDS + retry_in),
            )

        @retry(
            wait=wait_exponential(multiplier=60, min=60, max=600),
            stop=stop_after_attempt(MAX_ATTEMPTS),
            retry=retry_if_exception_type(InternalDruidException),
            reraise=True,
            before_sleep=_log_before_sleep,
        )
        def _top_n_query(query: TopNDruidQuery, i: int) -> Any:
            return _execute_top_n_query_and_get_result(query)

        assert task.excluded_entities is not None
        queries = self._build_top_n_queries(task)
        total_num_queries = len(queries)
        entity_ids: Set[str] = set()
        excluded_ids = set(task.excluded_entities)
        query_timeout = (
            DEFAULT_BULK_LABEL_DRUID_QUERY_TIMEOUT if not task.no_limit else NO_LIMIT_BULK_LABEL_DRUID_QUERY_TIMEOUT
        )

        for i, query in enumerate(queries, start=1):
            logger.info(f'[task_id:{task.id}][query:{i}/{total_num_queries}] executing topN query: {query!r}')
            query_start_time = time.time()

            try:
                query_result = _top_n_query(query, i)
            except InternalDruidException:
                raise InternalDruidException(f'Druid query {i}/{total_num_queries} failed due to 3 500s in a row!')

            collected_amount_before = len(entity_ids)
            for entity_data in query_result:
                entity_id = entity_data[task.dimension]
                if entity_id and entity_id not in excluded_ids:
                    entity_ids.add(entity_id)

            collected_amount_after = len(entity_ids)

            logger.info(
                f'[task_id:{task.id}][query:{i}/{total_num_queries}] bulk label druid query completed in: '
                f'{time.time() - query_start_time} seconds. current number of collected entities: '
                f'{collected_amount_after} (added {(collected_amount_after - collected_amount_before)} '
                'new unique entities)'
            )
            if i < total_num_queries:
                task.heartbeat(status=TaskStatus.COLLECTING, new_entity_count=collected_amount_after)
                # nothing specific about 5 seconds; just for breathing room between queries
                time.sleep(5)

        logger.info(
            f'[task_id:{task.id}] All {total_num_queries} topN queries completed. the task will now try to label '
            f'{len(entity_ids)} unique entities.'
        )
        return entity_ids

    def _process_task(self, task: BulkLabelTask) -> None:
        logger.info(f'[task_id:{task.id}] Starting bulk label task')

        entity_ids: Set[str] = self._collect_entity_ids(task)
        # https://docs.sqlalchemy.org/en/14/orm/extensions/mypy.html#introspection-of-columns-based-on-typeengine
        assert task.expected_total_entities_to_label is not None
        assert task.excluded_entities is not None
        assert isinstance(task.dimension, str)
        if not task.total_entities_to_label:
            task.total_entities_to_label = len(entity_ids)

        def _assert_actual_entity_count_is_within_margin_of_error() -> None:
            # This check will allow a bypass of the expected entity margin of error check if the expected
            # entity count is <= 0, which can happen if the bulk job is sent before the approximate entity
            # count calculation is complete.
            assert task.expected_total_entities_to_label is not None
            assert task.total_entities_to_label is not None
            actual_entity_count = task.total_entities_to_label
            expected_entity_count = task.expected_total_entities_to_label
            if expected_entity_count > 0:
                error = abs((expected_entity_count - actual_entity_count) / expected_entity_count)
                if error > EXPECTED_ENTITY_MARGIN_OF_ERROR:
                    raise UnretryableTaskException(
                        f'Expected {expected_entity_count} entities, got {actual_entity_count}'
                        f' (margin of error: {round(error * 100)}%)'
                    )

        try:
            _assert_actual_entity_count_is_within_margin_of_error()
        except UnretryableTaskException as exception:
            excluded_entity_count = len(task.excluded_entities)
            if excluded_entity_count > 0:
                # If we exclude entities but aren't in margin of error for the entity count we have collected,
                # let's assume good intent on the caller and check that the (expected count - excluded count)
                # matches within the margin of error. If such is the case, the expected count will be updated.
                task.expected_total_entities_to_label = task.expected_total_entities_to_label - excluded_entity_count
                _assert_actual_entity_count_is_within_margin_of_error()
            else:
                raise exception

        if task.total_entities_to_label >= BULK_LABEL_NO_LIMIT_SIZE:
            logger.warning(
                f'At upper threshold for bulk jobs. [task_id:{task.id}] bulk label job may not label all entities.'
            )

        entity_type = self._engine.get_feature_name_to_entity_type_mapping()[task.dimension]
        # A sorted list allows us to expect the same indicies in the end, no matter the collection process.
        entity_ids_list = sorted(list(entity_ids))
        # This range resumes work at the place last left off
        for current_entity_index in task.iterate_entity_indices():
            entity_id = entity_ids_list[current_entity_index]
            entity = EntityT(type=entity_type, id=entity_id)
            self._apply_label_mutations(entity, task)

        self._send_bulk_job_analytics(task)

    def _send_bulk_job_analytics(self, task: BulkLabelTask) -> None:
        assert isinstance(task.id, int)
        assert isinstance(task.label_name, str)
        assert isinstance(task.label_reason, str)
        assert isinstance(task.total_entities_to_label, int)
        assert isinstance(task.no_limit, bool)
        analytics_properties = OspreyBulkJobAnalyticsEvent(
            task_id=str(task.id),
            total_entities_to_label=task.total_entities_to_label,
            label_name=task.label_name,
            label_reason=task.label_reason,
            no_limit=task.no_limit,
            expiration_date=task.label_expiry.isoformat() if task.label_expiry else None,
        )
        self._analytics_publisher.publish(analytics_properties)

    def _apply_label_mutations(self, entity: EntityT[Any], task: BulkLabelTask) -> None:
        def _log_before_sleep(retry_state: RetryCallState) -> None:
            assert isinstance(task.entities_labeled, int)

            attempt = retry_state.attempt_number
            sleep_time = retry_state.next_action.sleep if retry_state.next_action else None

            logger.info(
                f'[task_id:{task.id}] Label mutation attempt {attempt} failed, retrying in {sleep_time} seconds.'
            )
            task.heartbeat(status=TaskStatus.LABELLING, new_entity_count=task.entities_labeled)
            return

        @retry(
            wait=wait_exponential(multiplier=1, min=1, max=10),
            stop=stop_after_attempt(MAX_LABEL_SERVICE_RETRIES),
            retry=retry_if_exception_type((RPCException, ServiceUnavailable)),
            reraise=True,
            before_sleep=_log_before_sleep,
        )
        def _do_label_mutation() -> None:
            assert isinstance(task.label_reason, str)
            assert isinstance(task.initiated_by, str)
            assert isinstance(task.label_name, str)
            assert isinstance(task.dimension, str)
            assert isinstance(task.excluded_entities, Iterable)

            _ = self._labels_provider.apply_entity_label_mutations(
                entity=entity,
                mutations=[
                    EntityLabelMutation(
                        label_name=task.label_name,
                        reason_name=BULK_LABEL_REASON,
                        expires_at=task.label_expiry,
                        status=task.label_status,  # type: ignore
                        description='Bulk label (id={BulkLabelTaskId}) by {AdminEmail}: {Reason}',
                        features={
                            'AdminEmail': task.initiated_by,
                            'Reason': task.label_reason,
                            'BulkLabelTaskId': str(task.id),
                        },
                    ),
                ],
            )

        _do_label_mutation()

    @classmethod
    def rollback_task_effects(
        cls,
        engine: OspreyEngine,
        analytics_publisher: BasePublisher,
        webhooks_publisher: BasePublisher,
        task: BulkLabelTask,
        include_ids: Optional[Set[str]] = None,
    ) -> None:
        assert isinstance(task.query, dict)
        assert isinstance(task.label_reason, str)
        assert isinstance(task.initiated_by, str)
        assert isinstance(task.label_name, str)
        assert isinstance(task.dimension, str)
        assert isinstance(task.excluded_entities, Iterable)
        query = task.query
        query['dimension'] = task.dimension
        assert task.label_status == LabelStatus.MANUALLY_ADDED, 'Can only rollback tasks that MANUALLY_ADD for now.'

        print(
            f'[!] Initiating rollback of task {task.id}, total entities to attempt rollback on: '
            f'{task.total_entities_to_label}'
        )

        if include_ids is not None:
            print(f'[!] Will only include {len(include_ids)} entities.')

        print('[~] Re-running druid TopN Query')
        res: TopNPoPResponse = TopNDruidQuery(**task.query).execute(calculate_previous_period=False)
        current_raw_data: List[PeriodData] = res.current_period
        current_period = PeriodData.parse_obj(current_raw_data[0]).dict(exclude={'timestamp'})
        results = current_period.get('result', [])
        print(f'[!] Found {len(results)} entities to attempt rollback on.')

        excluded_ids = set(task.excluded_entities)

        rows_skipped = 0
        rows_excluded = 0
        rows_rolled_back = 0

        feature_name_to_entity_type_mapping = engine.get_feature_name_to_entity_type_mapping()
        labels_provider = LABELS_PROVIDER.instance()
        assert labels_provider is not None, (
            'this code cannot be used because no labels service / provider is supplied for this osprey instance'
        )
        feature_name = task.dimension
        entity_type = feature_name_to_entity_type_mapping[feature_name]

        for rows_processed, row in enumerate(results):
            if rows_processed % 100 == 0:
                rows_pct = (rows_processed / float(len(results))) * 100
                print(
                    f'[~] Working... {rows_processed} / {len(results)} {rows_pct:.2f}% - {rows_excluded} entities '
                    f'excluded {rows_skipped} entities skipped, {rows_rolled_back} entities rolled back.'
                )

            value = row[feature_name]
            if not value:
                rows_skipped += 1
                continue

            entity = EntityT(type=entity_type, id=value)
            if entity.id in excluded_ids:
                rows_excluded += 1
                continue

            if include_ids is not None and str(entity.id) not in include_ids:
                rows_excluded += 1
                continue

            labels = labels_provider.get_from_service(entity)

            # No label anymore, nothing to do.
            label_state = labels.labels.get(task.label_name)
            if not label_state:
                rows_skipped += 1
                continue

            # We need to check that the bulk label is the exclusive reason why this entity may have been
            # phone verified.
            bulk_label_reason = label_state.reasons.get(BULK_LABEL_REASON)
            if not bulk_label_reason or len(label_state.reasons) > 1:
                rows_skipped += 1
                continue

            if bulk_label_reason.features.get('BulkLabelTaskId') != str(task.id):
                rows_skipped += 1
                continue

            # Apply the inverse effect of the rollback.
            _ = labels_provider.apply_entity_label_mutations(
                entity=entity,
                mutations=[
                    EntityLabelMutation(
                        label_name=task.label_name,
                        reason_name='_BulkLabelRollback',
                        status=LabelStatus.MANUALLY_REMOVED,
                        expires_at=datetime.now() + timedelta(hours=2),
                        description=(
                            'Bulk label rollback of (id={BulkLabelTaskId}) '
                            '(initial reason: {Reason}, initially initiated by: {AdminEmail})'
                        ),
                        features={
                            'AdminEmail': task.initiated_by,
                            'Reason': task.label_reason,
                            'BulkLabelTaskId': str(task.id),
                        },
                    ),
                ],
            )
            rows_rolled_back += 1

        print(
            f'[!] DONE! {rows_excluded} entities excluded {rows_skipped} entities skipped, {rows_rolled_back} '
            f'entities rolled back.'
        )

    def stop(self):
        return self._analytics_publisher.stop()
