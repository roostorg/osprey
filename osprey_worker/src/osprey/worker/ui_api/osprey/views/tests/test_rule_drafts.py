import base64
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
import requests_mock as requests_mock_module
from flask import Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.snowflake import Snowflake


@pytest.fixture(autouse=True)
def _mock_audit_snowflake():
    # The after_request audit hook mints a snowflake id, which normally means an
    # HTTP call to the snowflake-id-worker service. Tests that wrap a request in
    # a requests_mock.Mocker would otherwise trip NoMockAddress on that call, so
    # neutralize it here (the audit log's persist() is already mocked in the
    # shared conftest).
    with patch('osprey.worker.ui_api.osprey.lib.audit.generate_snowflake', return_value=Snowflake(1)):
        yield


def _set_github_backend(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    """Configure the github backend with sensible defaults for tests; overrides win."""
    defaults = {
        'OSPREY_RULES_SUBMISSION_BACKEND': 'github',
        'OSPREY_RULES_REPO': 'roostorg/osprey-rules',
        'OSPREY_GITHUB_TOKEN': 'gh_fake_token',
        'OSPREY_RULES_BASE_BRANCH': 'main',
    }
    for k, v in {**defaults, **overrides}.items():
        monkeypatch.setenv(k, v)


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
    res = client.get(url_for('rule_drafts.pending_drafts'))
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


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_returns_503_when_no_backend_configured(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv('OSPREY_RULES_SUBMISSION_BACKEND', raising=False)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
            'rule_name': 'AnotherRule',
            'summary': 'demo',
            'is_new_rule': True,
        },
    )
    assert res.status_code == 503
    body = res.json
    assert body is not None
    assert 'No rule-submission backend is configured' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_returns_503_when_github_missing_required_env(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'github')
    monkeypatch.delenv('OSPREY_RULES_REPO', raising=False)
    monkeypatch.delenv('OSPREY_GITHUB_TOKEN', raising=False)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
            'rule_name': 'AnotherRule',
            'summary': 'demo',
            'is_new_rule': True,
        },
    )
    assert res.status_code == 503
    body = res.json
    assert body is not None
    assert 'OSPREY_RULES_REPO' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_blocks_invalid_sml_before_calling_github(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch)

    with requests_mock_module.Mocker() as m:
        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/bad.sml',
                'source': 'this is not valid SML !!!',
                'rule_name': 'BadRule',
                'summary': 'should not submit',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 400
    assert m.call_count == 0
    body = res.json
    assert body is not None
    assert body['error'].startswith('Validation failed')


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_rejects_bad_rule_name(client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch) -> None:
    _set_github_backend(monkeypatch)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new.sml',
            'source': "X = Rule(when_all=[PostText == 'x'], description='x')",
            'rule_name': '1-not-an-identifier',
            'summary': '',
            'is_new_rule': True,
        },
    )
    assert res.status_code == 400


def test_submission_result_extras_cannot_shadow_canonical_fields() -> None:
    from osprey.worker.ui_api.osprey.views._rule_drafts_backend import SubmissionResult

    result = SubmissionResult(
        title='real title',
        url='https://real.example/pr/1',
        extras={'title': 'spoofed', 'url': 'https://evil.example', 'pr_number': 7},
    )
    out = result.to_json()
    assert out['title'] == 'real title'
    assert out['url'] == 'https://real.example/pr/1'
    assert out['main_sml_updated'] is False
    # Non-colliding extras still pass through for adopters that want them.
    assert out['pr_number'] == 7


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_rejects_main_sml_as_draft_path(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch)
    with requests_mock_module.Mocker() as m:
        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'main.sml',
                'source': 'Import(rules=[])',
                'rule_name': 'Whatever',
                'summary': '',
                'is_new_rule': False,
            },
        )
        # Guard fires before any network call: the entry point is never a draft.
        assert m.call_count == 0
    assert res.status_code == 400
    body = res.json
    assert body is not None
    assert 'main.sml' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_surfaces_github_connection_error_as_502(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch)
    repo_url = 'https://api.github.com/repos/roostorg/osprey-rules'
    with requests_mock_module.Mocker() as m:
        m.get(f'{repo_url}/git/ref/heads/main', exc=requests.exceptions.ConnectTimeout)
        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
            },
        )
    # A forge outage is a structured 502, not an unhandled 500.
    assert res.status_code == 502
    body = res.json
    assert body is not None
    assert 'Could not reach the git host' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_happy_path_creates_branch_commits_and_opens_pr(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch, OSPREY_RULES_PATH_IN_REPO='rules')

    repo_url = 'https://api.github.com/repos/roostorg/osprey-rules'

    with requests_mock_module.Mocker() as m:
        m.get(f'{repo_url}/git/ref/heads/main', json={'object': {'sha': 'BASE_SHA'}})
        m.get(f'{repo_url}/contents/rules/new_rule.sml', status_code=404)
        m.post(f'{repo_url}/git/refs', status_code=201, json={})
        m.put(f'{repo_url}/contents/rules/new_rule.sml', status_code=201, json={})
        m.post(
            f'{repo_url}/pulls',
            status_code=201,
            json={'number': 42, 'html_url': 'https://github.com/roostorg/osprey-rules/pull/42'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                # Bare filename: OSPREY_RULES_PATH_IN_REPO='rules' prepends the
                # subdirectory, so the file lands at rules/new_rule.sml.
                'path': 'new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': 'add bye rule',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['title'] == 'Pull request #42 opened'
    assert body['url'] == 'https://github.com/roostorg/osprey-rules/pull/42'
    assert body['main_sml_updated'] is False
    # GitHub-specific extras are surfaced for adopters that want them.
    assert body['pr_number'] == 42
    assert body['pr_url'] == 'https://github.com/roostorg/osprey-rules/pull/42'
    assert body['path_in_repo'] == 'rules/new_rule.sml'
    assert body['branch'].startswith('rule-draft/local-dev/AnotherRule-')


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_wire_into_main_appends_require_line(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch)

    repo_url = 'https://api.github.com/repos/roostorg/osprey-rules'
    existing_main = "Import(rules=['models/post.sml'])\n\nRequire(rule='rules/post_contains_hello.sml')\n"
    encoded_main = base64.b64encode(existing_main.encode('utf-8')).decode('ascii')

    with requests_mock_module.Mocker() as m:
        m.get(f'{repo_url}/git/ref/heads/main', json={'object': {'sha': 'BASE_SHA'}})
        m.get(f'{repo_url}/contents/rules/new_rule.sml', status_code=404)
        m.post(f'{repo_url}/git/refs', status_code=201, json={})
        m.put(f'{repo_url}/contents/rules/new_rule.sml', status_code=201, json={})
        m.get(f'{repo_url}/contents/main.sml', json={'sha': 'MAIN_SHA', 'content': encoded_main, 'type': 'file'})
        main_put = m.put(f'{repo_url}/contents/main.sml', status_code=200, json={})
        m.post(
            f'{repo_url}/pulls',
            status_code=201,
            json={'number': 99, 'html_url': 'https://github.com/roostorg/osprey-rules/pull/99'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': 'add bye rule and wire it in',
                'is_new_rule': True,
                'wire_into_main': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['main_sml_updated'] is True

    main_put_request = main_put.last_request
    assert main_put_request is not None
    posted = main_put_request.json()
    decoded_new_main = base64.b64decode(posted['content']).decode('utf-8')
    assert "Require(rule='rules/new_rule.sml')" in decoded_new_main
    # The existing Require for post_contains_hello.sml should still be present untouched.
    assert "Require(rule='rules/post_contains_hello.sml')" in decoded_new_main


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_wire_into_main_skips_when_require_already_present(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch)

    repo_url = 'https://api.github.com/repos/roostorg/osprey-rules'
    existing_main = "Require(rule='rules/already_here.sml')\n"
    encoded_main = base64.b64encode(existing_main.encode('utf-8')).decode('ascii')

    with requests_mock_module.Mocker() as m:
        m.get(f'{repo_url}/git/ref/heads/main', json={'object': {'sha': 'BASE_SHA'}})
        m.get(f'{repo_url}/contents/rules/already_here.sml', status_code=404)
        m.post(f'{repo_url}/git/refs', status_code=201, json={})
        m.put(f'{repo_url}/contents/rules/already_here.sml', status_code=201, json={})
        m.get(f'{repo_url}/contents/main.sml', json={'sha': 'MAIN_SHA', 'content': encoded_main, 'type': 'file'})
        main_put = m.put(f'{repo_url}/contents/main.sml', status_code=200, json={})
        m.post(
            f'{repo_url}/pulls',
            status_code=201,
            json={'number': 100, 'html_url': 'https://github.com/roostorg/osprey-rules/pull/100'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/already_here.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
                'wire_into_main': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['main_sml_updated'] is False
    # main.sml fetch happens but the PUT to update it must not.
    assert main_put.call_count == 0


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_409_if_new_rule_file_already_exists(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch)

    repo_url = 'https://api.github.com/repos/roostorg/osprey-rules'

    with requests_mock_module.Mocker() as m:
        m.get(f'{repo_url}/git/ref/heads/main', json={'object': {'sha': 'BASE_SHA'}})
        m.get(
            f'{repo_url}/contents/new_rule.sml',
            json={'sha': 'EXISTING_BLOB_SHA', 'type': 'file'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 409


@pytest.mark.use_rules_sources(_base_sources)
def test_pending_filters_to_rules_path_and_sml(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_github_backend(monkeypatch, OSPREY_RULES_PATH_IN_REPO='rules')

    repo_url = 'https://api.github.com/repos/roostorg/osprey-rules'

    with requests_mock_module.Mocker() as m:
        m.get(
            f'{repo_url}/pulls',
            json=[
                {
                    'number': 1,
                    'title': 'Add rule X',
                    'html_url': 'https://github.com/roostorg/osprey-rules/pull/1',
                    'head': {'ref': 'rule-draft/local-dev/X-1'},
                    'user': {'login': 'someone'},
                    'created_at': '2026-06-30T12:00:00Z',
                },
                {
                    'number': 2,
                    'title': 'Update README',
                    'html_url': 'https://github.com/roostorg/osprey-rules/pull/2',
                    'head': {'ref': 'docs/readme'},
                    'user': {'login': 'someone'},
                    'created_at': '2026-06-30T13:00:00Z',
                },
            ],
        )
        m.get(
            f'{repo_url}/pulls/1/files',
            json=[{'filename': 'rules/x.sml'}],
        )
        m.get(
            f'{repo_url}/pulls/2/files',
            json=[{'filename': 'README.md'}],
        )

        res = client.get(url_for('rule_drafts.pending_drafts'))

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert len(body['pending']) == 1
    entry = body['pending'][0]
    assert entry['title'] == 'Add rule X'
    assert entry['url'] == 'https://github.com/roostorg/osprey-rules/pull/1'
    assert entry['touched_files'] == ['rules/x.sml']
    # GitHub-specific extras carried through for adopters that want them.
    assert entry['pr_number'] == 1


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_round_trips_a_builder_shaped_rule(client: 'FlaskClient[Response]') -> None:
    source = """
Import(rules=['models/post.sml'])

ContainsCat = Rule(
  when_all=[
    TextContains(text=PostText, phrase='cat'),
    EventType == 'create_post',
  ],
  description='looks for cat',
)

WhenRules(
  rules_any=[ContainsCat],
  then=[
    LabelAdd(entity=UserId, label='meow'),
  ],
)
"""
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/contains_cat.sml', 'source': source},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is True
    model = body['model']
    assert model['ruleName'] == 'ContainsCat'
    assert model['description'] == 'looks for cat'
    assert model['conditions'] == [
        {'feature': 'PostText', 'operator': 'includes', 'rhs': 'cat', 'rhsIsFeature': False},
        {'feature': 'EventType', 'operator': '==', 'rhs': 'create_post', 'rhsIsFeature': False},
    ]
    assert model['outcomes'] == [
        {
            'effect': 'LabelAdd',
            'args': [
                {'name': 'entity', 'value': 'UserId', 'isFeature': True},
                {'name': 'label', 'value': 'meow', 'isFeature': False},
            ],
        }
    ]


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_handles_excludes(client: 'FlaskClient[Response]') -> None:
    source = "BlocksCat = Rule(when_all=[not TextContains(text=PostText, phrase='cat')], description='no cat')\n"
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/blocks_cat.sml', 'source': source},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is True
    assert body['model']['conditions'] == [
        {'feature': 'PostText', 'operator': 'excludes', 'rhs': 'cat', 'rhsIsFeature': False},
    ]


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_rejects_multiple_rules(client: 'FlaskClient[Response]') -> None:
    source = (
        "A = Rule(when_all=[PostText == 'a'], description='a')\nB = Rule(when_all=[PostText == 'b'], description='b')\n"
    )
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/multi.sml', 'source': source},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is False
    assert 'multiple Rule definitions' in body['reason']


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_rejects_helper_assigns(client: 'FlaskClient[Response]') -> None:
    source = "Helper = 'cat'\nA = Rule(when_all=[PostText == Helper], description='a')\n"
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/helper.sml', 'source': source},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is False
    assert 'helper assignment' in body['reason']


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_rejects_complex_condition(client: 'FlaskClient[Response]') -> None:
    # A boolean operator inside `when_all` would need Code Editor; the builder is AND-only via row repetition.
    source = "A = Rule(when_all=[PostText == 'a' and EventType == 'create_post'], description='a')\n"
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/complex.sml', 'source': source},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is False
    assert 'Rule Builder cannot represent' in body['reason']


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_rejects_file_with_no_rule(client: 'FlaskClient[Response]') -> None:
    source = "Import(rules=['models/post.sml'])\n"
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/empty.sml', 'source': source},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is False
    assert 'no Rule(...)' in body['reason']


@pytest.mark.use_rules_sources(_base_sources)
def test_parse_into_builder_rejects_syntax_error(client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('rule_drafts.parse_into_builder'),
        json={'path': 'rules/broken.sml', 'source': 'this is not valid SML at all !!!'},
    )
    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['supported'] is False
    assert 'could not parse SML' in body['reason']


@pytest.mark.use_rules_sources(_base_sources)
def test_unknown_backend_value_returns_500(client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'gerrit')
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
            'rule_name': 'AnotherRule',
            'summary': '',
            'is_new_rule': True,
        },
    )
    assert res.status_code == 500
    body = res.json
    assert body is not None
    assert "Unknown OSPREY_RULES_SUBMISSION_BACKEND 'gerrit'" in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_pending_returns_empty_list_for_null_backend(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv('OSPREY_RULES_SUBMISSION_BACKEND', raising=False)
    res = client.get(url_for('rule_drafts.pending_drafts'))
    assert res.status_code == 503
    body = res.json
    assert body is not None
    assert body['pending'] == []


@pytest.mark.use_rules_sources(_base_sources)
def test_submit_github_enterprise_url_is_threaded_into_requests(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    """GitHub Enterprise customers point OSPREY_GITHUB_API_URL at their own host; every API call must use it."""
    _set_github_backend(
        monkeypatch,
        OSPREY_GITHUB_API_URL='https://github.acme.test/api/v3',
        OSPREY_RULES_REPO='acme/rules',
    )

    repo_url = 'https://github.acme.test/api/v3/repos/acme/rules'

    with requests_mock_module.Mocker() as m:
        m.get(f'{repo_url}/git/ref/heads/main', json={'object': {'sha': 'BASE_SHA'}})
        m.get(f'{repo_url}/contents/rules/new_rule.sml', status_code=404)
        m.post(f'{repo_url}/git/refs', status_code=201, json={})
        m.put(f'{repo_url}/contents/rules/new_rule.sml', status_code=201, json={})
        m.post(
            f'{repo_url}/pulls',
            status_code=201,
            json={'number': 5, 'html_url': 'https://github.acme.test/acme/rules/pull/5'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['url'] == 'https://github.acme.test/acme/rules/pull/5'
    # If any call had hit api.github.com instead, the Mocker would have raised NoMockAddress.


@pytest.mark.use_rules_sources(_base_sources)
def test_local_backend_writes_file_and_returns_no_url(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        rules_dir = Path(tmpdir)
        (rules_dir / 'main.sml').write_text("Import(rules=['models/post.sml'])\n", encoding='utf-8')

        monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'local')
        monkeypatch.setenv('OSPREY_RULES_LOCAL_PATH', str(rules_dir))

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': 'add bye rule',
                'is_new_rule': True,
                'wire_into_main': False,
            },
        )

        assert res.status_code == 200
        body = res.json
        assert body is not None
        assert body['url'] is None
        assert 'rules/new_rule.sml' in body['title']
        assert body['main_sml_updated'] is False

        written = (rules_dir / 'rules' / 'new_rule.sml').read_text(encoding='utf-8')
        assert 'AnotherRule' in written


@pytest.mark.use_rules_sources(_base_sources)
def test_local_backend_wires_into_main_when_requested(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        rules_dir = Path(tmpdir)
        (rules_dir / 'main.sml').write_text("Import(rules=['models/post.sml'])\n", encoding='utf-8')

        monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'local')
        monkeypatch.setenv('OSPREY_RULES_LOCAL_PATH', str(rules_dir))

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
                'wire_into_main': True,
            },
        )

        assert res.status_code == 200
        body = res.json
        assert body is not None
        assert body['main_sml_updated'] is True
        updated_main = (rules_dir / 'main.sml').read_text(encoding='utf-8')
        assert "Require(rule='rules/new_rule.sml')" in updated_main


@pytest.mark.use_rules_sources(_base_sources)
def test_local_backend_rejects_path_traversal(client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        rules_dir = Path(tmpdir)
        monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'local')
        monkeypatch.setenv('OSPREY_RULES_LOCAL_PATH', str(rules_dir))

        # The view layer already rejects '..' in the path, so to actually exercise the
        # local backend's own guard we bypass the view-level check by going around the
        # API: instantiate the backend and call submit_draft directly with a traversal path.
        from osprey.worker.ui_api.osprey.views._rule_drafts_backend import RuleDraftBackendError
        from osprey.worker.ui_api.osprey.views._rule_drafts_local import LocalBackend, LocalConfig

        backend = LocalBackend(LocalConfig(rules_dir=rules_dir))
        with pytest.raises(RuleDraftBackendError) as exc_info:
            backend.submit_draft(
                draft_path='../escape.sml',
                sml_source='x = 1',
                rule_name='X',
                summary='',
                author_email='test@local',
                is_new_rule=True,
                wire_into_main=False,
            )
        assert 'escapes the configured rules directory' in exc_info.value.message


def _set_gitlab_backend(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    defaults = {
        'OSPREY_RULES_SUBMISSION_BACKEND': 'gitlab',
        'OSPREY_GITLAB_PROJECT': 'alice/osprey-rules',
        'OSPREY_GITLAB_TOKEN': 'gl_fake_token',
        'OSPREY_RULES_BASE_BRANCH': 'main',
    }
    for k, v in {**defaults, **overrides}.items():
        monkeypatch.setenv(k, v)


@pytest.mark.use_rules_sources(_base_sources)
def test_gitlab_submit_returns_503_when_missing_required_env(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'gitlab')
    monkeypatch.delenv('OSPREY_GITLAB_PROJECT', raising=False)
    monkeypatch.delenv('OSPREY_GITLAB_TOKEN', raising=False)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
            'rule_name': 'AnotherRule',
            'summary': '',
            'is_new_rule': True,
        },
    )
    assert res.status_code == 503
    body = res.json
    assert body is not None
    assert 'OSPREY_GITLAB_PROJECT' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_gitlab_submit_happy_path_opens_merge_request(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_gitlab_backend(monkeypatch, OSPREY_RULES_PATH_IN_REPO='rules')

    project_url = 'https://gitlab.com/api/v4/projects/alice%2Fosprey-rules'
    # OSPREY_RULES_PATH_IN_REPO='rules' prepends the subdir to the bare filename,
    # so the file lands at rules/new_rule.sml -> 'rules%2Fnew_rule.sml' in the URL.
    file_url = f'{project_url}/repository/files/rules%2Fnew_rule.sml'

    with requests_mock_module.Mocker() as m:
        m.get(file_url, status_code=404)
        m.post(f'{project_url}/repository/branches', status_code=201, json={})
        m.post(file_url, status_code=201, json={})
        m.post(
            f'{project_url}/merge_requests',
            status_code=201,
            json={'iid': 7, 'web_url': 'https://gitlab.com/alice/osprey-rules/-/merge_requests/7'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': 'add bye rule',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['title'] == 'Merge request !7 opened'
    assert body['url'] == 'https://gitlab.com/alice/osprey-rules/-/merge_requests/7'
    assert body['main_sml_updated'] is False
    assert body['mr_iid'] == 7
    assert body['path_in_repo'] == 'rules/new_rule.sml'


@pytest.mark.use_rules_sources(_base_sources)
def test_gitlab_self_hosted_url_is_threaded_into_requests(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_gitlab_backend(
        monkeypatch,
        OSPREY_GITLAB_URL='https://gitlab.acme.test',
        OSPREY_GITLAB_PROJECT='acme/rules',
    )

    project_url = 'https://gitlab.acme.test/api/v4/projects/acme%2Frules'
    file_url = f'{project_url}/repository/files/new_rule.sml'

    with requests_mock_module.Mocker() as m:
        m.get(file_url, status_code=404)
        m.post(f'{project_url}/repository/branches', status_code=201, json={})
        m.post(file_url, status_code=201, json={})
        m.post(
            f'{project_url}/merge_requests',
            status_code=201,
            json={'iid': 1, 'web_url': 'https://gitlab.acme.test/acme/rules/-/merge_requests/1'},
        )

        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['url'] == 'https://gitlab.acme.test/acme/rules/-/merge_requests/1'


@pytest.mark.use_rules_sources(_base_sources)
def test_gitlab_pending_filters_to_rules_path_and_sml(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_gitlab_backend(monkeypatch, OSPREY_RULES_PATH_IN_REPO='rules')

    project_url = 'https://gitlab.com/api/v4/projects/alice%2Fosprey-rules'

    with requests_mock_module.Mocker() as m:
        m.get(
            f'{project_url}/merge_requests',
            json=[
                {
                    'iid': 1,
                    'title': 'Add rule X',
                    'web_url': 'https://gitlab.com/alice/osprey-rules/-/merge_requests/1',
                    'source_branch': 'rule-draft/alice/X-1',
                    'author': {'username': 'alice'},
                    'created_at': '2026-07-01T12:00:00Z',
                },
                {
                    'iid': 2,
                    'title': 'Update README',
                    'web_url': 'https://gitlab.com/alice/osprey-rules/-/merge_requests/2',
                    'source_branch': 'docs/readme',
                    'author': {'username': 'alice'},
                    'created_at': '2026-07-01T13:00:00Z',
                },
            ],
        )
        m.get(
            f'{project_url}/merge_requests/1/diffs',
            json=[{'new_path': 'rules/x.sml'}],
        )
        m.get(
            f'{project_url}/merge_requests/2/diffs',
            json=[{'new_path': 'README.md'}],
        )

        res = client.get(url_for('rule_drafts.pending_drafts'))

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert len(body['pending']) == 1
    entry = body['pending'][0]
    assert entry['title'] == 'Add rule X'
    assert entry['url'] == 'https://gitlab.com/alice/osprey-rules/-/merge_requests/1'
    assert entry['touched_files'] == ['rules/x.sml']
    assert entry['mr_iid'] == 1


def _set_tangled_backend(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    defaults = {
        'OSPREY_RULES_SUBMISSION_BACKEND': 'tangled',
        'OSPREY_TANGLED_HANDLE': 'alice.example.test',
        'OSPREY_TANGLED_APP_PASSWORD': 'test-app-pw',
        'OSPREY_TANGLED_REPO': 'alice.example.test/osprey-rules',
        'OSPREY_TANGLED_REPO_DID': 'did:plc:testrepodid',
        'OSPREY_TANGLED_PDS_URL': 'https://pds.example.test',
        'OSPREY_TANGLED_URL': 'https://tangled.example.test',
        'OSPREY_RULES_BASE_BRANCH': 'main',
    }
    for k, v in {**defaults, **overrides}.items():
        monkeypatch.setenv(k, v)


@pytest.mark.use_rules_sources(_base_sources)
def test_tangled_submit_returns_503_when_missing_required_env(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('OSPREY_RULES_SUBMISSION_BACKEND', 'tangled')
    monkeypatch.delenv('OSPREY_TANGLED_HANDLE', raising=False)
    monkeypatch.delenv('OSPREY_TANGLED_APP_PASSWORD', raising=False)
    monkeypatch.delenv('OSPREY_TANGLED_REPO', raising=False)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
            'rule_name': 'AnotherRule',
            'summary': '',
            'is_new_rule': True,
        },
    )
    assert res.status_code == 503
    body = res.json
    assert body is not None
    assert 'OSPREY_TANGLED_HANDLE' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_tangled_submit_happy_path_creates_pull_record(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_tangled_backend(monkeypatch)

    with requests_mock_module.Mocker() as m:
        m.post(
            'https://pds.example.test/xrpc/com.atproto.server.createSession',
            json={
                'did': 'did:plc:alice',
                'accessJwt': 'fake_access_jwt',
                'refreshJwt': 'fake_refresh_jwt',
                'handle': 'alice.example.test',
            },
        )
        upload_blob = m.post(
            'https://pds.example.test/xrpc/com.atproto.repo.uploadBlob',
            json={
                'blob': {
                    '$type': 'blob',
                    'ref': {'$link': 'bafkreitestblobcid'},
                    'mimeType': 'application/gzip',
                    'size': 512,
                }
            },
        )
        create_record = m.post(
            'https://pds.example.test/xrpc/com.atproto.repo.createRecord',
            json={
                'uri': 'at://did:plc:alice/sh.tangled.repo.pull/3lkabc123',
                'cid': 'bafyfakecid',
            },
        )
        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/contains_tangled.sml',
                'source': "Import(rules=['models/base.sml'])\nContainsTangled = Rule(when_all=[PostText == 'tangled'], description='tangled test')",
                'rule_name': 'ContainsTangled',
                'summary': 'test rule from tangled adapter',
                'is_new_rule': True,
            },
        )

    assert res.status_code == 200
    body = res.json
    assert body is not None
    assert body['at_uri'] == 'at://did:plc:alice/sh.tangled.repo.pull/3lkabc123'
    assert body['rkey'] == '3lkabc123'
    # Tangled assigns numeric pull ids downstream during Bobbin indexing, so
    # the adapter always links at the pulls list.
    assert body['url'] == 'https://tangled.example.test/alice.example.test/osprey-rules/pulls'

    assert upload_blob.call_count == 1
    assert upload_blob.last_request.headers['Content-Type'] == 'application/gzip'

    posted = create_record.last_request.json()
    assert posted['collection'] == 'sh.tangled.repo.pull'
    assert posted['repo'] == 'did:plc:alice'
    record = posted['record']
    assert record['$type'] == 'sh.tangled.repo.pull'
    assert record['title'] == 'Add rule ContainsTangled'
    # The record body carries the PR summary, not the rule's SML description.
    assert 'test rule from tangled adapter' in record['body']
    # Nested target: {repo, branch, repoDid}; must not have the old flat fields.
    assert record['target']['repo'] == 'did:plc:testrepodid'
    assert record['target']['repoDid'] == 'did:plc:testrepodid'
    assert record['target']['branch'] == 'main'
    assert 'repo' not in {k for k in record if k != 'target'}
    assert 'targetBranch' not in record
    # source.branch is a cosmetic label; must be present.
    assert record['source']['branch'].startswith('osprey-ui/')
    # rounds[0].patchBlob references the blob we uploaded, not an inline patch.
    assert 'patch' not in record
    assert record['rounds'][0]['patchBlob']['ref']['$link'] == 'bafkreitestblobcid'
    assert record['rounds'][0]['patchBlob']['mimeType'] == 'application/gzip'


@pytest.mark.use_rules_sources(_base_sources)
def test_tangled_backend_rejects_edits_with_501(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_tangled_backend(monkeypatch)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/contains_hello.sml',
            'source': "Import(rules=['models/base.sml'])\nMyRule = Rule(when_all=[PostText == 'hi'], description='hi')",
            'rule_name': 'MyRule',
            'summary': '',
            'is_new_rule': False,
        },
    )
    assert res.status_code == 501
    body = res.json
    assert body is not None
    assert 'does not yet support editing existing rules' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_tangled_backend_rejects_wire_into_main_with_501(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_tangled_backend(monkeypatch)
    res = client.post(
        url_for('rule_drafts.submit_draft'),
        json={
            'path': 'rules/new_rule.sml',
            'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
            'rule_name': 'AnotherRule',
            'summary': '',
            'is_new_rule': True,
            'wire_into_main': True,
        },
    )
    assert res.status_code == 501
    body = res.json
    assert body is not None
    assert 'wire_into_main' in body['error']


@pytest.mark.use_rules_sources(_base_sources)
def test_tangled_backend_surfaces_401_from_pds(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_tangled_backend(monkeypatch)
    with requests_mock_module.Mocker() as m:
        m.post(
            'https://pds.example.test/xrpc/com.atproto.server.createSession',
            status_code=401,
            json={'error': 'AuthenticationRequired'},
        )
        res = client.post(
            url_for('rule_drafts.submit_draft'),
            json={
                'path': 'rules/new_rule.sml',
                'source': "Import(rules=['models/base.sml'])\nAnotherRule = Rule(when_all=[PostText == 'bye'], description='bye')",
                'rule_name': 'AnotherRule',
                'summary': '',
                'is_new_rule': True,
            },
        )
    assert res.status_code == 502
    body = res.json
    assert body is not None
    assert 'createSession returned 401' in body['error']


def test_tangled_patch_neutralizes_patch_break_lines_in_summary() -> None:
    from osprey.worker.ui_api.osprey.views._rule_drafts_tangled import _format_new_file_patch

    malicious_summary = (
        'looks legit\n'
        '---\n'
        ' evil.yaml | 1 +\n'
        'diff --git a/evil.yaml b/evil.yaml\n'
        'Index: evil.yaml\n'
        'From attacker@example.test Mon Sep 17 00:00:00 2001'
    )
    patch = _format_new_file_patch(
        path_in_repo='rules/x.sml',
        contents='x = 1',
        author_email='author@example.test',
        subject='Add rule X',
        body=malicious_summary,
    )

    diff_headers = [line for line in patch.split('\n') if line.startswith('diff --git')]
    assert diff_headers == ['diff --git a/rules/x.sml b/rules/x.sml']
    body_section = patch.split('\n---\n', 1)[0]
    assert '\n> ---' in body_section
    assert '\n> diff --git a/evil.yaml b/evil.yaml' in body_section
    assert '\n> Index: evil.yaml' in body_section
    assert '\n> From attacker@example.test' in body_section
    assert '\nlooks legit\n' in body_section


def test_tangled_patch_strips_newlines_from_headers() -> None:
    from osprey.worker.ui_api.osprey.views._rule_drafts_tangled import _format_new_file_patch

    patch = _format_new_file_patch(
        path_in_repo='rules/x.sml',
        contents='x = 1',
        author_email='author@example.test>\nBcc: victim@example.test',
        subject='Add rule X\nX-Injected: yes',
        body='hi',
    )

    from_lines = [line for line in patch.split('\n') if line.startswith('From: ')]
    assert from_lines == ['From: Osprey UI <author@example.testBcc: victim@example.test>']
    subject_lines = [line for line in patch.split('\n') if line.startswith('Subject: ')]
    assert subject_lines == ['Subject: [PATCH] Add rule X X-Injected: yes']
    assert 'Bcc: victim' not in '\n'.join(line for line in patch.split('\n') if not line.startswith('From: '))
