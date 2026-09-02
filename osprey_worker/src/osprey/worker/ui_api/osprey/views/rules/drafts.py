import logging
from http import HTTPStatus
from typing import Any

from flask import jsonify
from osprey.engine.ast.grammar import Source
from osprey.worker.lib.storage.rules import Rule, RuleNameTaken
from osprey.worker.ui_api.osprey.lib.abilities import CanDeployRules, CanEditRules, require_ability
from osprey.worker.ui_api.osprey.lib.auth import get_current_user_email
from osprey.worker.ui_api.osprey.lib.marshal import marshal_with
from osprey.worker.ui_api.osprey.lib.rule_builder import parse_into_builder_model
from osprey.worker.ui_api.osprey.lib.rule_deployment import MAIN_SML_PATH, DeployError, deploy_rule
from osprey.worker.ui_api.osprey.lib.rule_validation import validate_draft_source
from osprey.worker.ui_api.osprey.schemas.rule_validation import DraftValidation, ValidationMessage
from osprey.worker.ui_api.osprey.schemas.rules import DraftList, RuleRecord
from osprey.worker.ui_api.osprey.validators.rules import (
    CreateDraftRequest,
    DeployDraftRequest,
    GetDraftRequest,
    ParseIntoBuilderRequest,
    ValidateDraftRequest,
)

from . import blueprint

logger = logging.getLogger(__name__)

# One wording for one rule, in both dispositions: `create_draft` refuses with it (422),
# `validate_draft` warns with it (200). Stated once so they can't drift.
MAIN_SML_NOT_A_DRAFT_TARGET = (
    'main.sml is the engine entry point and cannot be saved as a draft. '
    'Deploy a rule with wire_into_main to add a Require line instead.'
)


def _rule_name_taken_error(request_model: CreateDraftRequest, defined_in: list[str]) -> tuple[Any, int]:
    """409 for a rule name another draft already holds.

    The offending file travels in `defined_in_source_paths` rather than interpolated
    into the message: it is the same field the editor already receives for "this
    identifier is defined over there", so it can link to the draft instead of making
    the user parse a filename out of prose.

    It is a list rather than an optional string on purpose. The lookup runs in a
    separate transaction from the failed write -- Postgres aborts the first one, so it
    has to -- which means there is no snapshot guaranteeing the holder is still there.
    Empty is the honest answer for that case, and needs no special handling.
    """
    return jsonify(
        DraftValidation(
            ok=False,
            errors=[
                ValidationMessage(
                    message=f'The rule name {request_model.rule_name!r} is already used by another draft. '
                    'Rename this rule, or edit that draft instead.',
                    source_path=request_model.path,
                    identifier=request_model.rule_name,
                    defined_in_source_paths=defined_in,
                )
            ],
        ).dict()
    ), HTTPStatus.CONFLICT


@blueprint.route('/rules/drafts', methods=['GET'])
@require_ability(CanEditRules)
def list_drafts() -> Any:
    """The draft rules table: every staged draft, newest-edited first.

    Each row is converted explicitly rather than handing `DraftList` the raw list.
    Pydantic accepts the raw list at runtime -- `RuleRecord` sets `orm_mode`, so
    `BaseModel.validate` falls through to `from_orm` per item -- but the declared
    field type is `list[RuleRecord]`, and pyright rightly rejects a `list[Rule]`.
    mypy's pydantic plugin lets the shortcut through, so it would pass CI and
    squiggle in the editor. Not a trade worth one saved line.
    """
    return jsonify(DraftList(drafts=[RuleRecord.from_orm(rule) for rule in Rule.list_all()]).dict())


@blueprint.route('/rules/drafts', methods=['POST'])
@require_ability(CanEditRules)
@marshal_with(CreateDraftRequest)
def create_draft(request_model: CreateDraftRequest) -> Any:
    # main.sml is the engine entry point; a draft never replaces it wholesale.
    # Reported as a DraftValidation rather than an ad-hoc {'error': ...} so the editor
    # has one failure shape to render for this endpoint.
    if request_model.path == MAIN_SML_PATH:
        return jsonify(
            DraftValidation(
                ok=False,
                errors=[ValidationMessage(message=MAIN_SML_NOT_A_DRAFT_TARGET, source_path=request_model.path)],
            ).dict()
        ), HTTPStatus.UNPROCESSABLE_ENTITY

    # Re-validate server-side: a client that skipped POST /rules/drafts/validate
    # still must not be able to store SML that doesn't compile.
    result = validate_draft_source(request_model.path, request_model.source)
    if result.assemble_error is not None:
        result.errors = [ValidationMessage(message=result.assemble_error, source_path=request_model.path)]

    if not result.ok:
        return jsonify(result.dict()), HTTPStatus.UNPROCESSABLE_ENTITY

    # Rule names are global in SML, and validation above only sees *deployed* rules, so
    # it cannot know another draft already claimed this one. `rule_name` is unique, so
    # let the insert be the check: a pre-flight SELECT would cost a query on every
    # create to answer a question the write answers for free -- and wouldn't be
    # authoritative anyway, since two concurrent creates can both find nothing.
    try:
        draft = Rule.upsert(
            path=request_model.path,
            rule_name=request_model.rule_name,
            sml_source=request_model.source,
            summary=request_model.summary,
            author_email=get_current_user_email(),
        )
    except RuleNameTaken as err:
        logger.info('draft at %r collided on rule_name %r', request_model.path, request_model.rule_name)
        return _rule_name_taken_error(request_model, [str(rule.path) for rule in err.existing])

    return jsonify(RuleRecord.from_orm(draft).dict())


@blueprint.route('/rules/drafts/parse', methods=['POST'])
@require_ability(CanEditRules)
@marshal_with(ParseIntoBuilderRequest)
def parse_into_builder(request_model: ParseIntoBuilderRequest) -> Any:
    """Render existing SML as a Rule Builder model, if it fits the builder's subset.

    Returns 200 either way: `{supported: true, model: {...}}`, or `{supported: false,
    reason: "..."}` when the file uses something the builder can't express. "Can't be
    represented" is an answer, not a failure -- the editor uses it to decide whether to
    offer the Builder toggle at all.

    Serialises snake_case like every other endpoint here. The Builder models used to
    carry camelCase aliases for the UI's benefit; that made them the one exception to
    the API's casing, so conversion now belongs to the client.
    """
    source = Source(path=request_model.path, contents=request_model.source)
    return jsonify(parse_into_builder_model(source).dict())


@blueprint.route('/rules/drafts/validate', methods=['POST'])
@require_ability(CanEditRules)
@marshal_with(ValidateDraftRequest)
def validate_draft(request_model: ValidateDraftRequest) -> Any:
    """Splice the draft into the engine's sources and re-run AST validation.

    A 200 carrying `{ok: false, errors: [...]}` means the SML failed validation --
    the expected case for an editor that validates on every save, and a successful
    API call reporting a failed validation. 4xx is reserved for a malformed
    *request*, which the marshaller rejects before this runs.

    The exception is a draft that couldn't be assembled at all (e.g. main.sml is
    itself broken). That isn't the author's fault and nothing they type will fix
    it, but it still returns 400 with the message synthesised into `errors` so the
    editor shows something rather than an empty list.
    """
    result = validate_draft_source(request_model.path, request_model.source)

    # Validating *against* main.sml is a legitimate question -- "would the rules still
    # compile if main.sml looked like this?" -- so this stays `ok`. But the editor's
    # path field is free text, so someone can type main.sml, write a whole rule, and
    # only discover at save that it can't be stored. Warn at typing time instead.
    if request_model.path == MAIN_SML_PATH:
        result.warnings = [
            *result.warnings,
            ValidationMessage(message=MAIN_SML_NOT_A_DRAFT_TARGET, source_path=request_model.path),
        ]

    if result.assemble_error is not None:
        result.errors = [ValidationMessage(message=result.assemble_error, source_path=request_model.path)]
        return jsonify(result.dict()), HTTPStatus.BAD_REQUEST

    return jsonify(result.dict())


@blueprint.route('/rules/drafts/<int:draft_id>', methods=['GET'])
@require_ability(CanEditRules)
@marshal_with(GetDraftRequest)
def get_draft(request_model: GetDraftRequest) -> Any:
    draft = Rule.get_one_with_id(request_model.draft_id)
    if draft is None:
        return jsonify({'error': f'No draft with id {request_model.draft_id}.'}), HTTPStatus.NOT_FOUND

    return jsonify(RuleRecord.from_orm(draft).dict())


@blueprint.route('/rules/drafts/<int:draft_id>/deploy', methods=['POST'])
@require_ability(CanDeployRules)
@marshal_with(DeployDraftRequest)
def deploy_draft(request_model: DeployDraftRequest) -> Any:
    draft = Rule.get_one_with_id(request_model.draft_id)
    if draft is None:
        return jsonify({'error': f'No draft with id {request_model.draft_id}.'}), HTTPStatus.NOT_FOUND

    result = validate_draft_source(draft.path, draft.sml_source)
    if not result.ok:
        return jsonify(result.dict()), HTTPStatus.UNPROCESSABLE_ENTITY

    # Every refusal deploy_rule can raise carries the status it should map to -- an
    # unconfigured rules directory (503), a main.sml that's missing or doesn't parse
    # (409), a path that escapes the rules directory (400). Without this they'd all
    # surface as an unhandled 500.
    try:
        deployment = deploy_rule(draft, wire_into_main=request_model.wire_into_main)
    except DeployError as exc:
        logger.warning('deploy of draft %s failed: %s', request_model.draft_id, exc)
        return jsonify({'error': str(exc)}), exc.status

    return jsonify(deployment.dict())
