from collections.abc import Mapping
from typing import Any

from flask import Blueprint, abort, redirect
from osprey.worker.lib.storage.temporary_ability_token import ConsumptionResult, TemporaryAbilityToken
from osprey.worker.ui_api.osprey.lib.abilities import Ability
from osprey.worker.ui_api.osprey.lib.auth import get_current_user, get_current_user_email
from osprey.worker.ui_api.osprey.lib.marshal import ArgAndViewArgMarshaller, marshal_with
from pydantic import BaseModel

blueprint = Blueprint('abilities', __name__)


class ConsumeTemporaryAbilityTokenRequest(BaseModel, ArgAndViewArgMarshaller):
    redirect: str
    ability_token: str


@blueprint.route('/ability_tokens/<ability_token>', methods=['GET'])
@marshal_with(ConsumeTemporaryAbilityTokenRequest)
def consume_temporary_ability_token(request_data: ConsumeTemporaryAbilityTokenRequest) -> Any:
    result = TemporaryAbilityToken.consume(token=request_data.ability_token, user_email=get_current_user_email())

    #  If the token is already used we still want to redirect for a better user experience.
    #  Otherwise they will use their own token then try to click on the link and won't be redirected like they expect.
    if result == ConsumptionResult.SUCCESS:
        return redirect(request_data.redirect, code=302)
    else:
        return abort(401, result.name)


class GetCurrentUserAbilitiesResponse(BaseModel):
    abilities: Mapping[str, Ability[Any, Any]]


@blueprint.route('/my_abilities', methods=['GET'])
def get_current_user_abilities() -> Any:
    return GetCurrentUserAbilitiesResponse(abilities=get_current_user().get_all_abilities())
