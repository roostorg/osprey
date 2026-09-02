import logging
from typing import Any

from flask import jsonify
from osprey.worker.ui_api.osprey.lib.abilities import CanViewRules, require_ability
from osprey.worker.ui_api.osprey.lib.marshal import marshal_with
from osprey.worker.ui_api.osprey.lib.rules import get_rule_source, list_rules
from osprey.worker.ui_api.osprey.validators.rules import RuleSourceQuery

from . import blueprint

logger = logging.getLogger(__name__)


@blueprint.route('/rules', methods=['GET'])
@require_ability(CanViewRules)
def rules_list() -> Any:
    """Return the catalog of Rule(...) definitions across the rules engine."""
    return jsonify(list_rules().dict())


@blueprint.route('/rules/source', methods=['GET'])
@require_ability(CanViewRules)
@marshal_with(RuleSourceQuery)
def get_source(request_model: RuleSourceQuery) -> Any:
    """Return the source of a rule already loaded by the engine (i.e. on disk).

    This is for editing an existing rule; it does not read the rule_drafts table.
    A draft's own SML is loaded from the table via `GET /rules/drafts/<id>`.

    Gated on viewing rather than editing: the catalog above already renders each
    rule's `when_all` conditions and description, so requiring an editing ability to
    read the same logic at full fidelity would draw the boundary in a place the
    catalog has already crossed.
    """
    source = get_rule_source(request_model.path)
    if source is None:
        return jsonify({'error': f'No rule found at {request_model.path!r}.'}), 404
    return jsonify({'path': source.path, 'contents': source.contents})
