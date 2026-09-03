import hashlib
from textwrap import dedent

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.storage.rules import Rule

from .conftest import (
    AUTHORING_ABILITIES,
    DEPLOYING_ABILITIES,
    RULE_AUTHORING_SML,
    VALID_DRAFT,
    VIEWING_ABILITIES,
    acl_config,
    rule_authoring_sources,
    set_rules_dir,
)

# Every draft endpoint, as (method, endpoint, view args, JSON body). Endpoint *names*
# rather than URLs: `url_for` needs an application context, which exists inside a test
# but not at import time when parametrize is evaluated.
_DRAFT_ENDPOINTS = [
    pytest.param('post', 'rules.validate_draft', {}, {'path': 'rules/x.sml', 'source': ''}, id='validate'),
    pytest.param('post', 'rules.parse_into_builder', {}, {'path': 'rules/x.sml', 'source': ''}, id='parse'),
    pytest.param('get', 'rules.list_drafts', {}, None, id='list'),
    pytest.param('post', 'rules.create_draft', {}, {}, id='create'),
    pytest.param('get', 'rules.get_draft', {'draft_id': 1}, None, id='get-one'),
    pytest.param('post', 'rules.request_deploy', {'draft_id': 1}, {}, id='request-deploy'),
    pytest.param('post', 'rules.deploy_draft', {'draft_id': 1}, {}, id='deploy'),
]


@pytest.mark.use_rules_sources(rule_authoring_sources(VIEWING_ABILITIES))
@pytest.mark.parametrize(('method', 'endpoint', 'view_args', 'payload'), _DRAFT_ENDPOINTS)
def test_draft_endpoints_require_can_edit_rules(
    client: 'FlaskClient[Response]',
    method: str,
    endpoint: str,
    view_args: dict[str, object],
    payload: object,
) -> None:
    """Every draft endpoint 401s for a user holding only CAN_VIEW_RULES.

    `rules.get_source` and `rules.vocabulary` also require the ability, but their
    equivalents live beside those endpoints in test_rules.py and
    test_rule_vocabulary.py.
    """
    send = getattr(client, method)
    res = send(url_for(endpoint, **view_args), **({} if payload is None else {'json': payload}))
    assert res.status_code == 401


# --- rule draft validation tests -------------------------------------


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_validate_clean_draft_returns_ok(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.validate_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
        },
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['ok'] is True
    assert body['errors'] == []


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_validate_broken_draft_returns_structured_errors(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.validate_draft'),
        json={
            'path': 'rules/broken.sml',
            'source': 'this is not valid SML at all *** !!!',
        },
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['ok'] is False
    assert len(body['errors']) >= 1
    err = body['errors'][0]
    assert set(err.keys()) >= {'message', 'hint', 'source_path', 'line', 'column', 'rendered'}


@pytest.mark.use_rules_sources(
    {
        'config.yaml': acl_config('CAN_VIEW_DOCS', 'CAN_EDIT_RULES'),
        'main.sml': "Import(rules=['models/post.sml'])",
        'models/post.sml': "PostText: str = JsonData(path='$.post_text')",
    }
)
def test_validate_returns_suggested_imports_for_unimported_identifier(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.validate_draft'),
        json={
            'path': 'rules/uses_post_text.sml',
            'source': "MyRule = Rule(when_all=[PostText == 'hi'], description='hi')",
        },
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['ok'] is False
    assert body['suggested_imports'] == ['models/post.sml']
    assert any(e.get('identifier') == 'PostText' for e in body['errors'])


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_validate_clean_draft_has_empty_suggested_imports(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.validate_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
        },
    )
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['suggested_imports'] == []


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_validate_rejects_bad_path(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.validate_draft'),
        json={'path': 'rules/x.txt', 'source': ''},
    )
    assert res.status_code == 400


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_validate_main_sml_warns_but_still_validates(client: 'FlaskClient[Response]') -> None:
    """Validating against main.sml succeeds, but warns it can't be saved as a draft.

    The editor's path field is free text, so a user can type `main.sml`, write a whole
    rule against live feedback, and only learn at Save that it can't be stored. The
    warning surfaces that at typing time, through the channel the editor already
    renders -- without blocking a question that has a real answer ("would the rules
    still compile if main.sml looked like this?").
    """
    res = client.post(
        url_for('rules.validate_draft'),
        # dedent: `use_rules_sources` dedents each source when loading it, so the raw
        # fixture string isn't valid SML on its own.
        json={'path': 'main.sml', 'source': dedent(RULE_AUTHORING_SML['main.sml'])},
    )
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['ok'] is True
    assert res.json['errors'] == []
    assert any('cannot be saved as a draft' in w['message'] for w in res.json['warnings'])


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_validate_other_paths_carry_no_draft_target_warning(client: 'FlaskClient[Response]') -> None:
    """The warning is specific to main.sml, not attached to every validation."""
    res = client.post(
        url_for('rules.validate_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
        },
    )
    assert res.status_code == 200
    assert res.json is not None
    assert not any('cannot be saved as a draft' in w['message'] for w in res.json['warnings'])


# --- Draft table: create / list / get -------------------------------------


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_list_drafts_on_empty_table(client: 'FlaskClient[Response]') -> None:
    """An empty rules table lists nothing rather than erroring.

    Has its own test because `test_create_draft_persists_and_lists` covers listing
    only as a side effect of creating, so it can't run until create_draft exists --
    which would leave list_drafts with no coverage at all in the meantime.
    """
    res = client.get(url_for('rules.list_drafts'))
    assert res.status_code == 200
    assert res.json == {'drafts': []}


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_list_drafts_serialises_a_row(client: 'FlaskClient[Response]') -> None:
    """A stored row comes back as a `DraftSummary`, with `author_email` renamed.

    The empty-table case passes whether or not the row conversion works, so seed the
    table directly rather than going through create_draft. This is the test that pins
    the rename -- `author_email` -> `author` -- and the summary shape.
    """
    source = "SeededRule = Rule(when_all=[PostText == 'x'], description='d')"
    Rule.upsert(
        path='rules/seeded.sml',
        rule_name='SeededRule',
        sml_source=source,
        summary='seeded directly',
        author_email='local-dev@localhost',
    )

    res = client.get(url_for('rules.list_drafts'))
    assert res.status_code == 200
    assert res.json is not None

    drafts = res.json['drafts']
    assert len(drafts) == 1
    row = drafts[0]
    assert row['path'] == 'rules/seeded.sml'
    assert row['rule_name'] == 'SeededRule'
    assert row['author'] == 'local-dev@localhost'
    assert row['status'] == 'draft'
    assert isinstance(row['updated_at'], str)

    # The whole wire shape, so a field added to `DraftSummary` fails here until it is
    # covered rather than quietly going unasserted -- `cid` was added without this test
    # noticing. `source` and `summary` are absent by design: the list is for choosing a
    # draft, and shipping every row's SML would grow with the table rather than with
    # what the page renders.
    assert row.keys() == {
        'id',
        'path',
        'rule_name',
        'cid',
        'author',
        'status',
        'updated_at',
    }
    # Served straight off the row, not recomputed in the view: a second implementation
    # of the hash is one that can disagree with the stored one. This is also what makes
    # the summary useful without `source` -- a client can compare its copy against it.
    assert row['cid'] == hashlib.sha256(source.encode('utf-8')).hexdigest()


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_create_draft_persists_and_lists(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/spam.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': 'catch spam'},
    )
    assert res.status_code == 200
    draft = res.json
    assert draft is not None
    assert draft['path'] == 'rules/spam.sml'
    assert draft['rule_name'] == 'SomeRule'
    assert draft['summary'] == 'catch spam'
    assert draft['status'] == 'draft'
    assert draft['id'] is not None

    res = client.get(url_for('rules.list_drafts'))
    assert res.status_code == 200
    assert res.json is not None
    assert any(d['path'] == 'rules/spam.sml' for d in res.json['drafts'])


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_create_draft_upserts_same_path_in_place(client: 'FlaskClient[Response]') -> None:
    first = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/dupe.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': 'v1'},
    )
    assert first.status_code == 200
    assert first.json is not None
    original_id = first.json['id']

    second = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/dupe.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': 'v2'},
    )
    assert second.status_code == 200
    assert second.json is not None
    assert second.json['id'] == original_id
    assert second.json['summary'] == 'v2'

    res = client.get(url_for('rules.list_drafts'))
    assert res.json is not None
    assert len([d for d in res.json['drafts'] if d['path'] == 'rules/dupe.sml']) == 1


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_create_draft_rejects_duplicate_rule_name_across_drafts(client: 'FlaskClient[Response]') -> None:
    first = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/first.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert first.status_code == 200

    # A different path reusing the same rule name collides: rule names are global in SML,
    # and validation can't see the other draft (it only knows deployed rules).
    second = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/second.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert second.status_code == 409
    assert second.json is not None
    # The 409 body is a DraftValidation like every other failure on this endpoint.
    # The offending file travels structurally, in the same field the editor already
    # receives for "this identifier is defined over there" -- not interpolated into
    # prose, so this asserts the contract rather than the wording.
    err = second.json['errors'][0]
    assert err['identifier'] == 'SomeRule'
    assert err['defined_in_source_paths'] == ['rules/first.sml']
    assert 'SomeRule' in err['message']

    # Re-saving the same path with the same name is an update, not a collision.
    again = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/first.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': 'edit'},
    )
    assert again.status_code == 200


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_create_draft_rejects_invalid_sml(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.create_draft'),
        json={
            'path': 'rules/broken.sml',
            'rule_name': 'Broken',
            'source': "AnotherRule = Rule(when_all=[NonexistentFeature == 'x'], description='x')",
            'summary': '',
        },
    )
    # 422, not 400: the request was well-formed, its SML was not. 400 is reserved for
    # the marshaller rejecting a malformed request, which carries a different body.
    assert res.status_code == 422
    assert res.json is not None
    assert res.json['ok'] is False
    assert len(res.json['errors']) >= 1


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_create_draft_rejects_main_sml(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.create_draft'),
        json={'path': 'main.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert res.status_code == 422


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_create_draft_rejects_bad_rule_name(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/x.sml', 'rule_name': '9 not valid', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert res.status_code == 400


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_get_draft_returns_one_and_404s(client: 'FlaskClient[Response]') -> None:
    created = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/getme.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.get(url_for('rules.get_draft', draft_id=draft_id))
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['path'] == 'rules/getme.sml'
    assert res.json['rule_name'] == 'SomeRule'
    # ids serialise as strings, matching Query.serialize() elsewhere in lib/storage --
    # a 64-bit id would lose precision as a JSON number in JavaScript.
    assert isinstance(res.json['id'], str)

    res = client.get(url_for('rules.get_draft', draft_id=999999))
    assert res.status_code == 404
    # 404 carries {'error': ...}, not a DraftValidation: a missing draft has no source
    # path, and nothing about the request was invalid.
    assert res.json is not None
    assert 'error' in res.json


# --- Draft source is carried, not re-rendered ------------------------------


# Everything a reprint of a parsed AST would destroy: a leading comment, a blank
# line, a trailing comment, and a call spread over several lines. Python's parser
# discards comments outright, and `print_ast` normalises formatting, so a draft
# path that round-tripped through the AST would return none of this intact.
_DRAFT_WITH_COMMENTS = """# Catches obvious spam.
Import(rules=['models/base.sml'])

# Tuned 2026-08 -- do not lower without asking.
SomeRule = Rule(
    when_all=[PostText == 'bye'],   # the actual check
    description='bye',
)
"""


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_draft_source_survives_save_fetch_and_deploy_byte_for_byte(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A draft's SML is stored and served verbatim, never re-rendered from its AST.

    The draft endpoints parse SML to *validate* it and to build the Rule Builder
    model, but the source itself is carried as text: `create_draft` stores
    `request_model.source` unchanged, and deploy writes `rule.sml_source` unchanged.

    This test pins that property rather than the implementation. If someone later
    adds a normalisation step -- reformatting on save, or regenerating source from
    the builder model -- it fails here instead of silently eating every comment an
    author wrote.
    """
    set_rules_dir(monkeypatch, tmp_path)

    created = client.post(
        url_for('rules.create_draft'),
        json={
            'path': 'rules/commented_draft.sml',
            'rule_name': 'SomeRule',
            'source': _DRAFT_WITH_COMMENTS,
            'summary': '',
        },
    )
    assert created.status_code == 200
    assert created.json is not None
    assert created.json['source'] == _DRAFT_WITH_COMMENTS

    fetched = client.get(url_for('rules.get_draft', draft_id=created.json['id']))
    assert fetched.status_code == 200
    assert fetched.json is not None
    assert fetched.json['source'] == _DRAFT_WITH_COMMENTS

    deployed = client.post(url_for('rules.deploy_draft', draft_id=created.json['id']), json={})
    assert deployed.status_code == 200
    assert (tmp_path / 'rules' / 'commented_draft.sml').read_text() == _DRAFT_WITH_COMMENTS


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_parse_into_builder_does_not_alter_the_stored_draft(client: 'FlaskClient[Response]') -> None:
    """Reading a draft into the Rule Builder is a read: it doesn't rewrite the source.

    `parse_into_builder` walks the AST to produce the builder model. It never prints
    the tree back out, so asking for the builder view of a draft can't cost the
    author their comments.
    """
    created = client.post(
        url_for('rules.create_draft'),
        json={
            'path': 'rules/commented_draft.sml',
            'rule_name': 'SomeRule',
            'source': _DRAFT_WITH_COMMENTS,
            'summary': '',
        },
    )
    assert created.json is not None

    parsed = client.post(
        url_for('rules.parse_into_builder'),
        json={'path': 'rules/commented_draft.sml', 'source': _DRAFT_WITH_COMMENTS},
    )
    assert parsed.status_code == 200

    fetched = client.get(url_for('rules.get_draft', draft_id=created.json['id']))
    assert fetched.json is not None
    assert fetched.json['source'] == _DRAFT_WITH_COMMENTS


# --- Requesting a deploy ----------------------------------------------------


@pytest.mark.use_rules_sources(rule_authoring_sources(AUTHORING_ABILITIES))
def test_request_deploy_is_available_to_an_author_who_cannot_deploy(
    client: 'FlaskClient[Response]',
) -> None:
    """The endpoint exists for exactly this user: can edit, cannot deploy.

    Gated on CAN_EDIT_RULES rather than CAN_DEPLOY_RULES on purpose -- gating it on
    deploy would mean only people who can already ship a rule could ask for it to be
    shipped, which is nobody's workflow.
    """
    created = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/handoff.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': 'please ship'},
    )
    assert created.json is not None
    draft_id = created.json['id']
    assert created.json['status'] == 'draft'

    # Same user cannot deploy it...
    deployed = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={})
    assert deployed.status_code == 401

    # ...but can say it is ready.
    res = client.post(url_for('rules.request_deploy', draft_id=draft_id), json={})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['status'] == 'deploy_requested'


@pytest.mark.use_rules_sources(rule_authoring_sources(AUTHORING_ABILITIES))
def test_request_deploy_is_idempotent(client: 'FlaskClient[Response]') -> None:
    created = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/twice.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    first = client.post(url_for('rules.request_deploy', draft_id=draft_id), json={})
    second = client.post(url_for('rules.request_deploy', draft_id=draft_id), json={})
    assert first.status_code == second.status_code == 200
    assert second.json is not None
    assert second.json['status'] == 'deploy_requested'


@pytest.mark.use_rules_sources(rule_authoring_sources(AUTHORING_ABILITIES))
def test_request_deploy_404s_for_an_unknown_draft(client: 'FlaskClient[Response]') -> None:
    res = client.post(url_for('rules.request_deploy', draft_id=999999), json={})
    assert res.status_code == 404


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_a_json_body_cannot_override_the_draft_id_in_the_url(client: 'FlaskClient[Response]') -> None:
    """The path segment selects the row; a body key of the same name must not win.

    `ViewArgAndOptionalJsonBodyMarshaller` merges view args over the body. With the
    order reversed, `POST /rules/drafts/<a>/deploy` carrying `{"draft_id": b}` would act
    on b while every URL, log line and audit record said a.
    """
    first = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/first.sml', 'rule_name': 'FirstRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    second = client.post(
        url_for('rules.create_draft'),
        json={
            'path': 'rules/second.sml',
            'rule_name': 'SecondRule',
            'source': "Import(rules=['models/base.sml'])\nSecondRule = Rule(when_all=[PostText == 'x'], description='x')",
            'summary': '',
        },
    )
    assert first.json is not None and second.json is not None

    res = client.get(url_for('rules.get_draft', draft_id=first.json['id']), json={'draft_id': second.json['id']})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['id'] == first.json['id']
    assert res.json['path'] == 'rules/first.sml'


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_a_non_object_json_body_is_ignored_rather_than_erroring(client: 'FlaskClient[Response]') -> None:
    """`get_json` returns lists and scalars too, and neither can be spread into a dict."""
    created = client.post(
        url_for('rules.create_draft'),
        json={'path': 'rules/scalar.sml', 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None

    res = client.get(url_for('rules.get_draft', draft_id=created.json['id']), json=[1, 2, 3])
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['id'] == created.json['id']
