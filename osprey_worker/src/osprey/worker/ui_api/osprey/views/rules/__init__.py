"""Views for reading and authoring rules.

One Flask blueprint shared across the modules in this package, so every endpoint
lives under a single `rules.` endpoint namespace (`url_for('rules.get_source')`)
no matter which file defines it. Flask 1.1.4 has no nested blueprints -- those
arrived in Flask 2.0 -- so the `/rules/drafts` prefix is spelled out per route
rather than declared once on a child blueprint.

Status codes are chosen so a client can switch on them to know which body shape it
is holding, without inspecting the payload:

    400  the *request* was malformed -- missing field, path that isn't a .sml. Emitted
         by `marshal_with`, so the body is pydantic's error array, not our shape.
    422  the request was fine but its *content* is not processable -- SML that doesn't
         compile, or main.sml as a draft target. Body is a `DraftValidation`.
    409  conflicts with current state -- the rule name is taken. Body is a
         `DraftValidation`. Deploy also uses it for a main.sml that is missing or
         doesn't parse, with an `{'error': ...}` body.
    404  no such row. Body is `{'error': ...}` -- not a `DraftValidation`, because a
         missing draft has no source path to attach a `ValidationMessage` to, and
         nothing about the request was invalid.
    503  deploy is not configured, or the configured rules directory isn't one. A
         server-side misconfiguration, not anything the caller can fix by retrying
         differently. Body is `{'error': ...}`.
    401  no ability. Emitted by `require_ability` for every endpoint in the app; the
         body is a bare JSON string, and that is deliberately not special-cased here.

Decorator order here is `@require_ability` *above* `@marshal_with`, which is the
opposite of `views/entities.py`. Decorators apply bottom-up, so the one listed
first is the outer wrapper and runs first -- putting authorization ahead of
request parsing. A caller without the ability then gets 401 whatever they sent,
rather than a 400 that depends on their request body and names the fields the
schema wants. Please don't "fix" this back for consistency.
"""

from flask import Blueprint

blueprint = Blueprint('rules', __name__)

# Imported for their side effects: each module decorates `blueprint` above with
# its routes. These MUST come after `blueprint` is assigned -- the sub-modules do
# `from . import blueprint`, which re-enters this partially-executed module, and
# the name has to already exist by then.
from . import catalog, drafts, vocabulary  # noqa: E402,F401  (side-effect imports)
