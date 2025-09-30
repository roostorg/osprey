# ruff: noqa: E402
from __future__ import absolute_import

import ddtrace
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.lib.storage.bulk_action_files import get_bulk_action_file_manager
from werkzeug.exceptions import HTTPException

ddtrace.patch_all(gevent=True)

import os
from http import HTTPStatus
from typing import NoReturn, Tuple, Union

import sentry_sdk
from flask import Flask, Response
from osprey.engine.ast_validator.validation_context import ValidationFailed
from osprey.worker.lib import ddtrace_utils
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.flask_utils import OspreyFlask
from sentry_sdk.integrations.flask import FlaskIntegration


def _after_request(response: Response) -> Response:
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', '*')
    response.headers.add('Access-Control-Allow-Methods', '*')
    response.mimetype = 'application/json'

    return response


def _handle_validation_failed(err: ValidationFailed) -> Response:
    return Response(status=HTTPStatus.BAD_REQUEST, response=err.rendered(), mimetype='application/json')


def _handle_exception(e: HTTPException) -> Response:
    return Response(status=e.code, response=e.description, mimetype='application/json')


def _register_with_prefix(app, blueprint):
    app.register_blueprint(blueprint)
    app.register_blueprint(blueprint, url_prefix='/api')


def health() -> Union[str, Tuple[str, int]]:
    # TODO: Real health reporting
    healthy = True
    if not healthy:
        return 'UNHEALTHY', HTTPStatus.SERVICE_UNAVAILABLE
    return 'OK'


def debug_sentry() -> NoReturn:  # type: ignore
    _division_by_zero = 1 / 0  # noqa: F841


def create_app() -> Flask:
    from osprey.worker.lib.singletons import CONFIG
    from osprey.worker.lib.storage import postgres
    from osprey.worker.ui_api.osprey.lib import auth

    from .lib.audit import audit_request
    from .views import (
        abilities,
        bulk_actions,
        bulk_history,
        config,
        docs,
        entities,
        events,
        queries,
        rules_visualizer,
        saved_queries,
    )

    # Avoid double-binding the config in tests where a global autouse fixture already configured it
    config_singleton = CONFIG.instance()
    try:
        config_singleton.configure_from_env()
    except RuntimeError:
        pass
    sentry_dsn = config_singleton.get_str('SENTRY_UI_API_DSN', '')

    if sentry_dsn:
        sentry_sdk.init(dsn=sentry_dsn, integrations=[FlaskIntegration()])

    # Only set up Postgres if configuration provides hosts
    if 'POSTGRES_HOSTS' in os.environ:
        postgres.init_from_config('osprey_db')

    gunicorn_logger = get_logger('gunicorn.error')

    app = OspreyFlask(__name__)

    postgres.init_app(app)
    app.after_request(_after_request)
    app.register_error_handler(ValidationFailed, _handle_validation_failed)
    app.register_error_handler(HTTPException, _handle_exception)

    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.debug = config_singleton.debug
    app.testing = config_singleton.testing

    app.bulk_action_file_manager = get_bulk_action_file_manager()

    auth.init_app(app)
    ENGINE.instance()
    ddtrace_utils.init_app(app, service_name='osprey-ui-api')

    app.add_url_rule('/_health', 'health', health, methods=['GET'])
    app.add_url_rule('/_debug_sentry', 'debug_sentry', debug_sentry, methods=['GET'])

    _register_with_prefix(app, entities.blueprint)
    _register_with_prefix(app, events.blueprint)
    _register_with_prefix(app, queries.blueprint)
    _register_with_prefix(app, config.blueprint)
    _register_with_prefix(app, docs.blueprint)
    _register_with_prefix(app, saved_queries.blueprint)
    _register_with_prefix(app, abilities.blueprint)
    _register_with_prefix(app, bulk_history.blueprint)
    _register_with_prefix(app, rules_visualizer.blueprint)
    _register_with_prefix(app, bulk_actions.blueprint)

    app.after_request(audit_request)

    return app
