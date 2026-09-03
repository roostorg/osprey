import logging
from typing import Any

from flask import jsonify
from osprey.worker.ui_api.osprey.lib.abilities import CanEditRules, require_ability
from osprey.worker.ui_api.osprey.lib.rules import get_vocabulary

from . import blueprint

logger = logging.getLogger(__name__)


@blueprint.route('/rules/vocabulary', methods=['GET'])
@require_ability(CanEditRules)
def vocabulary() -> Any:
    """Features, UDFs, effects and source files the Rule Builder's dropdowns offer."""
    return jsonify(get_vocabulary().dict())
