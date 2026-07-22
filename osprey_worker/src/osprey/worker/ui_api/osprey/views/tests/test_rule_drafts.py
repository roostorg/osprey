import json
from unittest.mock import patch

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.snowflake import Snowflake
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.storage.rule_drafts import RuleDraft


def _set_rules_dir(monkeypatch: pytest.MonkeyPatch, path: object) -> None:
    # Deploy reads OSPREY_RULES_LOCAL_PATH via CONFIG, which is bound once at app
    # setup, so set it on the already-bound config for the deploy handler to see.
    monkeypatch.setenv('OSPREY_RULES_LOCAL_PATH', str(path))
    CONFIG.instance()._config_dict['OSPREY_RULES_LOCAL_PATH'] = str(path)


def _unset_rules_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('OSPREY_RULES_LOCAL_PATH', raising=False)
    CONFIG.instance()._config_dict.pop('OSPREY_RULES_LOCAL_PATH', None)


@pytest.fixture(autouse=True)
def _clear_rule_drafts():
    # The test database is session-scoped, so drafts persist across tests. Start each
    # test with an empty table, or leaked rows trip the rule-name uniqueness check.
    with scoped_session(commit=True) as session:
        session.query(RuleDraft).delete()
    yield


@pytest.fixture(autouse=True)
def _mock_audit_snowflake():
    # The after_request audit hook mints a snowflake id, which normally means an
    # HTTP call to the snowflake-id-worker service. Neutralize it here so tests
    # don't reach for that service (the audit log's persist() is already mocked
    # in the shared conftest).
    with patch('osprey.worker.ui_api.osprey.lib.audit.generate_snowflake', return_value=Snowflake(1)):
        yield


_acl_with_draft_ability = json.dumps(
    {
        'ui_config': {},
        'labels': {},
        'acl': {
            'users': {
                'local-dev@localhost': {
                    'abilities': [
                        {'name': 'CAN_VIEW_DOCS', 'allow_all': True},
                        {'name': 'CAN_EDIT_RULE_DRAFTS', 'allow_all': True},
                    ],
                },
            },
        },
    }
)

_acl_without_draft_ability = json.dumps(
    {
        'ui_config': {},
        'labels': {},
        'acl': {
            'users': {
                'local-dev@localhost': {'abilities': [{'name': 'CAN_VIEW_DOCS', 'allow_all': True}]},
            },
        },
    }
)

_base_sources = {
    'config.yaml': _acl_with_draft_ability,
    'models/base.sml': """
        UserId: str = JsonData(path='$.user_id')
        PostText: str = JsonData(path='$.post_text')
    """,
    'main.sml': """
        Import(rules=['models/base.sml'])

        ContainsHello = Rule(
            when_all=[PostText == 'hello'],
            description='Post contains hello',
        )

        WhenRules(
            rules_any=[ContainsHello],
            then=[DeclareVerdict(verdict=UserId)],
        )
    """,
}


@pytest.mark.use_rules_sources(_base_sources)
def test_get_source_returns_contents(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rule_drafts.get_source'), query_string={'path': 'main.sml'})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['path'] == 'main.sml'
    assert 'ContainsHello' in res.json['contents']


@pytest.mark.use_rules_sources(_base_sources)
def test_get_source_rejects_bad_path(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rule_drafts.get_source'), query_string={'path': '../etc/passwd.sml'})
    assert res.status_code == 400


@pytest.mark.use_rules_sources(_base_sources)
def test_get_source_404_for_unknown_path(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rule_drafts.get_source'), query_string={'path': 'rules/does_not_exist.sml'})
    assert res.status_code == 404


@pytest.mark.use_rules_sources(
    {
        'config.yaml': _acl_without_draft_ability,
        'main.sml': "UserId: str = JsonData(path='$.user_id')",
    }
)
def test_endpoints_require_can_edit_rule_drafts(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rule_drafts.get_source'), query_string={'path': 'main.sml'})
    assert res.status_code == 401
    res = client.post(
        url_for('rule_drafts.validate_draft'),
        json={'path': 'rules/x.sml', 'source': ''},
    )
    assert res.status_code == 401
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/x.sml', 'source': ''},
    )
    assert res.status_code == 401
    res = client.get(url_for('rule_drafts.vocabulary'))
    assert res.status_code == 401
    res = client.get(url_for('rule_drafts.list_drafts'))
    assert res.status_code == 401
    res = client.post(url_for('rule_drafts.create_draft'), json={})
    assert res.status_code == 401
    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=1), json={})
    assert res.status_code == 401


@pytest.mark.use_rules_sources(_base_sources)
def test_validate_clean_draft_returns_ok(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.validate_draft'),
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


@pytest.mark.use_rules_sources(_base_sources)
def test_validate_broken_draft_returns_structured_errors(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.validate_draft'),
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
        'config.yaml': _acl_with_draft_ability,
        'main.sml': "Import(rules=['models/post.sml'])",
        'models/post.sml': "PostText: str = JsonData(path='$.post_text')",
    }
)
def test_validate_returns_suggested_imports_for_unimported_identifier(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.validate_draft'),
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


@pytest.mark.use_rules_sources(_base_sources)
def test_validate_clean_draft_has_empty_suggested_imports(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.validate_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
        },
    )
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['suggested_imports'] == []


@pytest.mark.use_rules_sources(_base_sources)
def test_validate_rejects_bad_path(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.validate_draft'),
        json={'path': 'rules/x.txt', 'source': ''},
    )
    assert res.status_code == 400


@pytest.mark.use_rules_sources(_base_sources)
def test_vocabulary_returns_features_udfs_effects(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rule_drafts.vocabulary'))
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert set(body.keys()) == {'features', 'udfs', 'effects', 'source_files'}

    feature_names = {f['name'] for f in body['features']}
    assert {'UserId', 'PostText'}.issubset(feature_names)
    assert 'ContainsHello' not in feature_names

    udf_names = {u['name'] for u in body['udfs']}
    assert 'JsonData' in udf_names
    assert 'Rule' in udf_names
    assert 'DeclareVerdict' in body['effects']

    assert 'main.sml' in body['source_files']


# --- Draft table: create / list / get -------------------------------------

_VALID_DRAFT = "Import(rules=['models/base.sml'])\nSomeRule = Rule(when_all=[PostText == 'bye'], description='bye')"


@pytest.mark.use_rules_sources(_base_sources)
def test_create_draft_persists_and_lists(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/spam.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': 'catch spam'},
    )
    assert res.status_code == 200
    draft = res.json
    assert draft is not None
    assert draft['path'] == 'rules/spam.sml'
    assert draft['rule_name'] == 'SomeRule'
    assert draft['summary'] == 'catch spam'
    assert draft['status'] == 'draft'
    assert draft['id'] is not None

    res = client.get(url_for('rule_drafts.list_drafts'))
    assert res.status_code == 200
    assert res.json is not None
    assert any(d['path'] == 'rules/spam.sml' for d in res.json['drafts'])


@pytest.mark.use_rules_sources(_base_sources)
def test_create_draft_upserts_same_path_in_place(client: 'FlaskClient[Response]') -> None:
    first = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/dupe.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': 'v1'},
    )
    assert first.status_code == 200
    assert first.json is not None
    original_id = first.json['id']

    second = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/dupe.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': 'v2'},
    )
    assert second.status_code == 200
    assert second.json is not None
    assert second.json['id'] == original_id
    assert second.json['summary'] == 'v2'

    res = client.get(url_for('rule_drafts.list_drafts'))
    assert res.json is not None
    assert len([d for d in res.json['drafts'] if d['path'] == 'rules/dupe.sml']) == 1


@pytest.mark.use_rules_sources(_base_sources)
def test_create_draft_rejects_duplicate_rule_name_across_drafts(client: 'FlaskClient[Response]') -> None:
    first = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/first.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert first.status_code == 200

    # A different path reusing the same rule name collides: rule names are global in SML,
    # and validation can't see the other draft (it only knows deployed rules).
    second = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/second.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert second.status_code == 409
    assert second.json is not None
    assert 'SomeRule' in second.json['error']

    # Re-saving the same path with the same name is an update, not a collision.
    again = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/first.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': 'edit'},
    )
    assert again.status_code == 200


@pytest.mark.use_rules_sources(_base_sources)
def test_create_draft_rejects_invalid_sml(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.create_draft'),
        json={
            'path': 'rules/broken.sml',
            'rule_name': 'Broken',
            'source': "AnotherRule = Rule(when_all=[NonexistentFeature == 'x'], description='x')",
            'summary': '',
        },
    )
    assert res.status_code == 400
    assert res.json is not None
    assert 'error' in res.json


@pytest.mark.use_rules_sources(_base_sources)
def test_create_draft_rejects_main_sml(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'main.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert res.status_code == 400


@pytest.mark.use_rules_sources(_base_sources)
def test_create_draft_rejects_bad_rule_name(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/x.sml', 'rule_name': '9 not valid', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert res.status_code == 400


@pytest.mark.use_rules_sources(_base_sources)
def test_get_draft_returns_one_and_404s(client: 'FlaskClient[Response]') -> None:
    created = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/getme.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.get(url_for('rule_drafts.get_draft', draft_id=draft_id))
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['path'] == 'rules/getme.sml'

    res = client.get(url_for('rule_drafts.get_draft', draft_id=999999))
    assert res.status_code == 404


# --- Draft table: deploy --------------------------------------------------


@pytest.mark.use_rules_sources(_base_sources)
def test_deploy_writes_sml_and_marks_deployed(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    _set_rules_dir(monkeypatch, tmp_path)
    created = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/deploy.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=draft_id), json={})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['status'] == 'deployed'
    assert res.json['deployed_at'] is not None
    assert res.json['main_sml_updated'] is False

    written = tmp_path / 'rules' / 'deploy.sml'
    assert written.exists()
    assert written.read_text() == _VALID_DRAFT


@pytest.mark.use_rules_sources(_base_sources)
def test_deploy_wire_into_main_appends_require(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\n")
    _set_rules_dir(monkeypatch, tmp_path)
    created = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/wired.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['main_sml_updated'] is True
    assert "Require(rule='rules/wired.sml')" in (tmp_path / 'main.sml').read_text()


@pytest.mark.use_rules_sources(_base_sources)
def test_deploy_wire_into_main_is_idempotent(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\nRequire(rule='rules/already.sml')\n")
    _set_rules_dir(monkeypatch, tmp_path)
    created = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/already.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['main_sml_updated'] is False
    assert (tmp_path / 'main.sml').read_text().count("Require(rule='rules/already.sml')") == 1


@pytest.mark.use_rules_sources(_base_sources)
def test_deploy_wire_into_main_409_when_main_missing(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    _set_rules_dir(monkeypatch, tmp_path)
    created = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/nomain.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 409
    # The rule file must not be written when the deploy 409s on a missing main.sml.
    assert not (tmp_path / 'rules' / 'nomain.sml').exists()


@pytest.mark.use_rules_sources(_base_sources)
def test_deploy_503_when_rules_dir_unset(client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch) -> None:
    _unset_rules_dir(monkeypatch)
    created = client.post(
        url_for('rule_drafts.create_draft'),
        json={'path': 'rules/nodir.sml', 'rule_name': 'SomeRule', 'source': _VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None
    draft_id = created.json['id']

    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=draft_id), json={})
    assert res.status_code == 503


@pytest.mark.use_rules_sources(_base_sources)
def test_deploy_404_for_unknown_draft(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    _set_rules_dir(monkeypatch, tmp_path)
    res = client.post(url_for('rule_drafts.deploy_draft', draft_id=999999), json={})
    assert res.status_code == 404
