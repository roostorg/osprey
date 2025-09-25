import csv
import logging
import tempfile
from http.client import NOT_FOUND
from typing import Any, Dict, List, Optional, Set

from flask import Blueprint, Response, abort, jsonify
from osprey.worker.lib.storage.stored_execution_result import (
    bootstrap_execution_result_storage_service,
)
from osprey.worker.ui_api.osprey.lib.abilities import (
    CanBulkLabel,
    CanBulkLabelWithNoLimit,
    CanViewActionData,
    CanViewEventsByAction,
    CanViewEventsByEntity,
    CanViewFeatureData,
    require_ability,
    require_ability_with_request,
)
from pydantic.main import BaseModel

from ..lib.auth import get_current_user
from ..lib.druid import (
    DimensionData,
    GroupByApproximateCountDruidQuery,
    PaginatedScanDruidQuery,
    TimeseriesDruidQuery,
    TopNDruidQuery,
    TopNPoPResponse,
)
from ..lib.marshal import marshal_with
from ..validators.events import BulkLabelTopNRequest

logger = logging.getLogger(__name__)

blueprint = Blueprint('events', __name__)
MAX_CSV_ROWS = 100_000


@blueprint.route('/events/topn', methods=['POST'])
@marshal_with(TopNDruidQuery)
def topn_query(request_model: TopNDruidQuery) -> TopNPoPResponse:
    require_ability_with_request(request_model, CanViewEventsByEntity)
    require_ability_with_request(request_model, CanViewEventsByAction)
    query_filter_ability = get_current_user().get_ability(CanViewEventsByAction)

    if query_filter_ability:
        topn_result = request_model.execute(query_filter_abilities=[query_filter_ability])
    else:
        topn_result = request_model.execute()

    if isinstance(topn_result, ValueError):
        return abort(Response(response=str(topn_result), status=400, mimetype='application/json'))

    return jsonify(topn_result.dict())


@blueprint.route('/events/groupby/approximate-count', methods=['POST'])
@marshal_with(GroupByApproximateCountDruidQuery)
def groupby_count(request_model: GroupByApproximateCountDruidQuery) -> Any:
    require_ability_with_request(request_model, CanViewEventsByEntity)
    require_ability_with_request(request_model, CanViewEventsByAction)
    return jsonify({'count': request_model.execute()})


@blueprint.route('/events/topn/bulk_label', methods=['POST'])
@marshal_with(BulkLabelTopNRequest)
@require_ability(CanBulkLabel)
def topn_bulk_label(bulk_label_request: BulkLabelTopNRequest) -> Any:
    if bulk_label_request.no_limit and not get_current_user().has_ability(CanBulkLabelWithNoLimit):
        return abort(
            401, f"User `{get_current_user().email}` doesn't have ability `{CanBulkLabelWithNoLimit.class_name}`"
        )

    # TODO(ayubun): Support plug-and-play label service
    return abort(501, 'Not Implemented')

    # query = {
    #     'start': datetime.timestamp(bulk_label_request.start),
    #     'end': datetime.timestamp(bulk_label_request.end),
    #     'query_filter': bulk_label_request.query_filter,
    # }

    # if bulk_label_request.entity:
    #     query['entity'] = bulk_label_request.entity.dict()

    # task = BulkLabelTask.enqueue(
    #     initiated_by=get_current_user_email(),
    #     query=query,
    #     dimension=bulk_label_request.dimension,
    #     excluded_entities=bulk_label_request.excluded_entities,
    #     expected_total_entities_to_label=bulk_label_request.expected_entities,
    #     no_limit=bulk_label_request.no_limit,
    #     label_name=bulk_label_request.label_name,
    #     label_status=LabelStatus.Value(bulk_label_request.label_status),
    #     label_reason=bulk_label_request.label_reason,
    #     label_expiry=bulk_label_request.label_expiry,
    # )

    # return jsonify({'task_id': task.id})


@blueprint.route('/events/timeseries', methods=['POST'])
@marshal_with(TimeseriesDruidQuery)
@require_ability(CanViewEventsByEntity)
def timeseries_query(request_model: TimeseriesDruidQuery) -> Any:
    require_ability_with_request(request_model, CanViewEventsByEntity)
    return jsonify(request_model.execute())


class ScanQueryResult(BaseModel):
    events: List[Dict[str, object]]
    next_page: Optional[str]

    class Config:
        arbitrary_types_allowed = True


@blueprint.route('/events/scan', methods=['POST'])
@marshal_with(PaginatedScanDruidQuery)
@require_ability(CanViewEventsByEntity)
@require_ability(CanViewEventsByAction)
def scan_query(request_model: PaginatedScanDruidQuery) -> Any:
    require_ability_with_request(request_model, CanViewEventsByEntity)
    require_ability_with_request(request_model, CanViewEventsByAction)

    query_filter_ability = get_current_user().get_ability(CanViewEventsByAction)
    paginated_scan_results = request_model.execute(query_filter_abilities=[query_filter_ability])

    action_data_censor_ability = get_current_user().get_ability(CanViewActionData)
    feature_data_censor_ability = get_current_user().get_ability(CanViewFeatureData)
    storage_service = bootstrap_execution_result_storage_service()
    events = storage_service.get_many(
        action_ids=paginated_scan_results.action_ids,
        data_censor_abilities=[action_data_censor_ability, feature_data_censor_ability],
    )

    return ScanQueryResult(
        events=[event.dict(include={'id', 'extracted_features', 'timestamp'}) for event in events],
        next_page=paginated_scan_results.next_page,
    )


@blueprint.route('/events/topn/csv', methods=['POST'])
@marshal_with(TopNDruidQuery)
@require_ability(CanViewEventsByEntity)
def topn_query_csv(topn_druid_query: TopNDruidQuery) -> Any:
    topn_druid_query.limit = min(topn_druid_query.limit, MAX_CSV_ROWS)
    require_ability_with_request(topn_druid_query, CanViewEventsByEntity)

    topn_results: TopNPoPResponse = topn_druid_query.execute()

    topn_rows: List[Any] = []
    fieldnames = [
        topn_druid_query.dimension,
        'current_count',
    ]

    # we might have some PoP items that have diffs, and some that do not. we want to make sure we export
    # both without duplicating anything
    included_dimension_keys: Set[str] = set()
    # if this is a PoP comparison query, we will want to do something a little different to include more
    # data in the CSV response~ this data includes previous period counts and the difference calculations
    if topn_results.comparison:
        fieldnames.extend(['previous_count', 'difference', 'percent_diff'])
        for comparison in topn_results.comparison:
            for diff in comparison.differences:
                if diff.dimension_key is None:
                    # how does this even happen lol
                    continue
                if diff.dimension_key not in included_dimension_keys:
                    included_dimension_keys.add(diff.dimension_key)
                    topn_rows.append(
                        {
                            topn_druid_query.dimension: diff.dimension_key,
                            'current_count': diff.current_count,
                            'previous_count': diff.previous_count,
                            'difference': diff.difference,
                            'percent_diff': diff.percentage_change,
                        }
                    )
                else:
                    logger.warning(f'Skipping duplicate dimension key during CSV export: {diff.dimension_key}')

    # now, regardless of whether we have a comparison or not, we want to include any remaining results that
    # have not been included yet. i.e. a comparison query may have some results that do not have diffs.
    for current_period in topn_results.current_period:
        result: List[DimensionData] = current_period.result
        for current_result in result:
            # this is not a beautiful solution, but it should work to snag the dimensions
            # for non-pop based on how we sanitize the results into DimensionData models
            dimension_key: str = str(getattr(current_result, topn_druid_query.dimension))
            if dimension_key not in included_dimension_keys:
                included_dimension_keys.add(dimension_key)
                topn_rows.append(
                    {
                        topn_druid_query.dimension: dimension_key,
                        'current_count': current_result.count,
                    }
                    | (
                        # im not sure if this part is necesarry, but it doesn't hurt to have it:
                        {'previous_count': None, 'difference': None, 'percent_diff': None}
                        if topn_results.comparison
                        else {}
                    )
                )
            else:
                if not topn_results.comparison:
                    logger.warning(f'Skipping duplicate dimension key during CSV export: {dimension_key}')

    csv_file = tempfile.NamedTemporaryFile('w+', delete=False)
    try:
        csvwriter = csv.DictWriter(csv_file, fieldnames=fieldnames)
        csvwriter.writeheader()
        csvwriter.writerows(topn_rows)

        csv_file.flush()
        csv_file.seek(0)
        csv_data = csv_file.read()
    finally:
        csv_file.close()

    return Response(csv_data, mimetype='text/csv', headers={'Content-Disposition': 'attachment'})


@blueprint.route('/events/event/<int:event_id>', methods=['GET'])
@require_ability(CanViewEventsByEntity)
def get_event_data(event_id: int) -> Any:
    action_data_censor_ability = get_current_user().get_ability(CanViewActionData)
    feature_data_censor_ability = get_current_user().get_ability(CanViewFeatureData)
    storage_service = bootstrap_execution_result_storage_service()
    execution_result = storage_service.get_one_with_action_data(
        event_id, [action_data_censor_ability, feature_data_censor_ability]
    )
    if not execution_result:
        return abort(Response(response='Unknown action id', status=NOT_FOUND, mimetype='application/json'))

    return execution_result
