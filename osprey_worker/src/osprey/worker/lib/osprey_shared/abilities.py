from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional, Sequence, Set
from urllib.parse import urlencode

import requests
from osprey.worker.lib.utils.flask_signing import Signer
from osprey.worker.ui_api.osprey.validators.entities import EntityKey
from pydantic.main import BaseModel
from requests import ConnectionError, HTTPError, Timeout
from requests.models import ChunkedEncodingError


class CreateTemporaryAbilityTokenRequest(BaseModel):
    abilities: List[Mapping[str, Any]]


class BulkCreateTemporaryAbilityTokenRequest(BaseModel):
    create_temporary_ability_token_requests: List[CreateTemporaryAbilityTokenRequest]
    creation_origin: str


class BulkCreateTemporaryAbilityTokenResponse(BaseModel):
    ability_tokens: List[str]


@dataclass(frozen=True)
class EntityUrlOptions:
    """Additional options that can be passed to customize the generated links."""

    entity_feature_filters: Set[str] = field(default_factory=set)
    """Applies a feature filter to the entity view, which will filter down events that only match the provided
    entity id for the fields specified. This is applied as a logical OR, if there are more than one values present in
    the set. Meaning, if you specify `UserId` and `TargetUserId`, it will filter down the event stream where the entity
    matches on either UserId or TargetUserId, not both."""


def bulk_create_entity_ability_links(
    rules_api_endpoint: str,
    signer: Signer,
    osprey_ui_endpoint: str,
    osprey_ui_api_endpoint: str,
    creation_origin: str,
    entities: List[EntityKey],
    raise_on_error: bool = False,
    entity_url_options: EntityUrlOptions = EntityUrlOptions(),
) -> Optional[Sequence[str]]:
    abilities_list: List[List[Mapping[str, Any]]] = [
        [
            {'name': 'CAN_VIEW_LABELS_FOR_ENTITY', 'allow_all': True},
            {'name': 'CAN_VIEW_EVENTS_BY_ENTITY', 'allow_specific': [{'type': entity.type, 'id': entity.id}]},
            {'name': 'CAN_MUTATE_ENTITIES', 'allow_specific': [{'type': entity.type, 'id': entity.id}]},
            {'name': 'CAN_MUTATE_LABELS', 'allow_specific': ['bad_bot_quarantine_single']},
            {'name': 'CAN_VIEW_LABELS', 'allow_all': True},
            {'name': 'CAN_VIEW_EVENTS_BY_ACTION', 'allow_all': True},
            {'name': 'CAN_VIEW_ACTION_DATA', 'allow_all': True},
            {'name': 'CAN_VIEW_FEATURE_DATA', 'allow_all': True},
        ]
        for entity in entities
    ]
    try:
        response = bulk_create_temporary_ability_token(
            endpoint=rules_api_endpoint, signer=signer, creation_origin=creation_origin, abilities_list=abilities_list
        )
    except (
        HTTPError,
        ConnectionError,
        Timeout,
        ChunkedEncodingError,
    ) as e:  # If osprey is broken then we don't want to break the rest of the app
        if not raise_on_error:
            return None
        else:
            raise e
    else:
        result = []

        # Build the query args in osprey based on the provided entity url options.
        osprey_query = []
        for entity_feature_filter in entity_url_options.entity_feature_filters:
            osprey_query.append(('entityFeatureFilters', entity_feature_filter))

        if osprey_query:
            osprey_query_str = f'?{urlencode(osprey_query)}'
        else:
            osprey_query_str = ''

        for ability_token, entity in zip(response.ability_tokens, entities):
            query = urlencode({'redirect': f'{osprey_ui_endpoint}entity/{entity.type}/{entity.id}{osprey_query_str}'})
            result.append(f'{osprey_ui_api_endpoint}ability_tokens/{ability_token}?{query}')

        return result


def bulk_create_temporary_ability_token(
    endpoint: str, signer: Signer, creation_origin: str, abilities_list: List[List[Mapping[str, Any]]]
) -> BulkCreateTemporaryAbilityTokenResponse:
    request = BulkCreateTemporaryAbilityTokenRequest(
        creation_origin=creation_origin,
        create_temporary_ability_token_requests=[
            CreateTemporaryAbilityTokenRequest(abilities=abilities) for abilities in abilities_list
        ],
    )
    url = f'{endpoint}ability_tokens/bulk_create'
    payload = request.json().encode()
    headers = signer.sign(payload)
    headers['content-type'] = 'application/json'
    raw_resp = requests.post(url, headers=headers, timeout=2, data=payload)
    raw_resp.raise_for_status()
    return BulkCreateTemporaryAbilityTokenResponse.parse_obj(raw_resp.json())


def create_temporary_ability_token(
    endpoint: str, signer: Signer, creation_origin: str, abilities: List[Mapping[str, Any]]
) -> str:
    request = BulkCreateTemporaryAbilityTokenRequest(
        creation_origin=creation_origin,
        create_temporary_ability_token_requests=[CreateTemporaryAbilityTokenRequest(abilities=abilities)],
    )
    url = f'{endpoint}ability_tokens/bulk_create'
    payload = request.json().encode()
    headers = signer.sign(payload)
    headers['content-type'] = 'application/json'
    raw_resp = requests.post(url, headers=headers, timeout=2, data=payload)
    raw_resp.raise_for_status()
    return BulkCreateTemporaryAbilityTokenResponse.parse_obj(raw_resp.json()).ability_tokens[0]
