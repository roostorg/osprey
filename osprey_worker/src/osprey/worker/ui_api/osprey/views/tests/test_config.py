import json

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.singletons import CONFIG

from .conftest import (
    AUTHORING_ABILITIES,
    DEPLOYING_ABILITIES,
    VIEWING_ABILITIES,
    rule_authoring_sources,
    set_rules_dir,
    unset_rules_dir,
)

_rules_source = """
UserId = Entity(type='User', id=1)
GuildId = Entity(type='Guild', id=1)
"""

_ui_config_raw = {
    'external_links': {
        'User': 'https://test.example.com/users/{entity_id}',
        'Guild': 'https://test.example.com/guilds/{entity_id}',
    },
    'default_summary_features': [
        {'actions': ['*'], 'features': ['UserId', 'UserIsBot', 'UserName', 'UserEmail']},
        {'actions': ['user_phone_*'], 'features': ['UserPhoneCarrierName', 'UserPhone']},
    ],
}

_labels_raw = {
    'user_phone_verification_requested': {
        'valid_for': ['User'],
        'connotation': 'negative',
        'description': 'Hello world!',
    }
}

_base_sources_dict = {'config.yaml': json.dumps({'ui_config': _ui_config_raw, 'labels': _labels_raw})}


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            UserId = Entity(type='User', id=1)
            GuildId = Entity(type='Guild', id=1)
            SomeLiteral: str = "hi"
            SomeExtractLiteral: ExtractLiteral[List[int]] = [1, 2, 3]
        """,
        'actions/foo.sml': '',
        'actions/bar.sml': '',
        'user_selfbot.sml': """
        ActionName = GetActionName()

        Action_DmChannelCreated_Selfbot = Rule(
            when_all=[ActionName == 'dm_channel_created'],
            description=f'User joined created a dm channel with a selfbot',
        )""",
    }
)
def test_get_ui_config(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('config.get_config'))
    assert 200 <= res.status_code < 300

    body = res.json

    # Stated as a whole rather than checked off field by field: this is the contract the
    # UI reads, so a field added to `UIConfigMerged` should fail here until it is
    # covered below, and a field removed should fail rather than silently stop being
    # asserted.
    assert body.keys() == {
        'can_deploy_rules',
        'can_edit_rules',
        'current_user',
        'default_summary_features',
        'external_links',
        'feature_name_to_entity_type_mapping',
        'feature_name_to_value_type_mapping',
        'known_action_names',
        'known_feature_locations',
        'label_info_mapping',
        'rule_deployment_enabled',
        'rule_info_mapping',
    }

    assert body['external_links'] == _ui_config_raw['external_links']
    assert body['default_summary_features'] == _ui_config_raw['default_summary_features']
    assert body['feature_name_to_entity_type_mapping'] == {'UserId': 'User', 'GuildId': 'Guild'}
    assert body['feature_name_to_value_type_mapping'] == {
        'ActionName': 'str',
        'Action_DmChannelCreated_Selfbot': 'bool',
        'UserId': 'int',
        'GuildId': 'int',
        'SomeExtractLiteral': 'list<int>',
    }
    assert body['label_info_mapping'] == _labels_raw

    filtered = [
        {'name': item['name'], 'source_path': item['source_path'], 'source_line': item['source_line']}
        for item in body['known_feature_locations']
    ]
    # The source strings defined in this file with the multi-line string
    # literals have an extra newline at the beginning, so source_line seems 1
    # higher than expected
    assert filtered == [
        {'name': 'UserId', 'source_path': 'main.sml', 'source_line': 2},
        {'name': 'GuildId', 'source_path': 'main.sml', 'source_line': 3},
        {'name': 'SomeExtractLiteral', 'source_path': 'main.sml', 'source_line': 5},
        {'name': 'ActionName', 'source_path': 'user_selfbot.sml', 'source_line': 2},
        {'name': 'Action_DmChannelCreated_Selfbot', 'source_path': 'user_selfbot.sml', 'source_line': 4},
    ]
    assert set(body['known_action_names']) == {'foo', 'bar'}
    assert body['current_user'] == {'email': 'local-dev@localhost'}
    assert body['rule_info_mapping'] == {
        'Action_DmChannelCreated_Selfbot': 'User joined created a dm channel with a selfbot'
    }

    # These sources configure no ACL and the autouse `_no_ambient_rules_dir` fixture
    # leaves OSPREY_RULES_PATH unset, so all three rule flags are off. The dedicated
    # tests below turn each on independently.
    assert body['can_edit_rules'] is False
    assert body['can_deploy_rules'] is False
    assert body['rule_deployment_enabled'] is False


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            Str: ExtractLiteral[str] = 'hello'
            Int: ExtractLiteral[int] = 123
            Float: ExtractLiteral[float] = 123.4
            Bool: ExtractLiteral[bool] = True
            OStr: ExtractLiteral[str | None] = 'hello'
            OInt: ExtractLiteral[int | None] = 123
            OFloat: ExtractLiteral[float | None] = 123.4
            OBool: ExtractLiteral[bool | None] = True
            LStr: ExtractLiteral[List[str]] = ['hello']
            LInt: ExtractLiteral[List[int]] = [123]
            LFloat: ExtractLiteral[List[float]] = [123.4]
            LBool: ExtractLiteral[List[bool]] = [True]
            R = Rule(when_all=[True], description='')
        """,
    }
)
def test_config_feature_name_to_value_type_mapping(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('config.get_config'))
    assert res.json['feature_name_to_value_type_mapping'] == {
        'Str': 'str',
        'Int': 'int',
        'Float': 'float',
        'Bool': 'bool',
        'OStr': 'str?',
        'OInt': 'int?',
        'OFloat': 'float?',
        'OBool': 'bool?',
        'LStr': 'list<str>',
        'LInt': 'list<int>',
        'LFloat': 'list<float>',
        'LBool': 'list<bool>',
        'R': 'bool',
    }


# `rule_deployment_enabled` and `can_deploy_rules` are the two independent things that
# both have to hold before `POST /rules/drafts/<id>/deploy` can succeed: the deployment
# has to have a rules directory, and the caller has to hold CAN_DEPLOY_RULES. They are
# reported separately so the UI can distinguish "this deployment doesn't deploy" from
# "you may not" -- which only means anything if they can actually differ, so each of the
# tests below turns on exactly one of them.


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_config_reports_deploy_available_and_permitted(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    set_rules_dir(monkeypatch, tmp_path)

    res = client.get(url_for('config.get_config'))
    assert res.json['rule_deployment_enabled'] is True
    assert res.json['can_deploy_rules'] is True


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_config_reports_deploy_unavailable_when_no_rules_dir_configured(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    """A user who may deploy, on a deployment that cannot.

    This is the case the flag exists for: without it the UI offers the deploy to
    exactly the users authorized to press it, and the API answers 503.
    """
    unset_rules_dir(monkeypatch)

    res = client.get(url_for('config.get_config'))
    assert res.json['rule_deployment_enabled'] is False
    assert res.json['can_deploy_rules'] is True


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_config_reports_deploy_unavailable_when_rules_path_is_not_a_directory(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A set-but-wrong OSPREY_RULES_PATH is as undeployable as an unset one.

    `rules_dir` refuses a path that isn't a directory, so the flag has to refuse it
    too -- checking only that the variable is non-empty would put the button back.
    """
    not_a_directory = tmp_path / 'rules.sml'
    not_a_directory.write_text('')
    set_rules_dir(monkeypatch, not_a_directory)

    res = client.get(url_for('config.get_config'))
    assert res.json['rule_deployment_enabled'] is False


@pytest.mark.use_rules_sources(rule_authoring_sources(AUTHORING_ABILITIES))
def test_config_reports_deploy_not_permitted_without_can_deploy_rules(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A deployment that can deploy, for a user who may not.

    CAN_EDIT_RULES is deliberately not enough -- the same split `test_rule_deployment`
    asserts on the API has to be visible in the config the UI renders from. This is the
    combination the UI has to render as an editor with a disabled deploy control, which
    it can only tell apart by reading `can_edit_rules` and `can_deploy_rules` together.
    """
    set_rules_dir(monkeypatch, tmp_path)

    res = client.get(url_for('config.get_config'))
    assert res.json['rule_deployment_enabled'] is True
    assert res.json['can_edit_rules'] is True
    assert res.json['can_deploy_rules'] is False


@pytest.mark.use_rules_sources(rule_authoring_sources(VIEWING_ABILITIES))
def test_config_reports_no_authoring_for_a_viewer(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A user who may read rules but not author them gets neither flag.

    Without `can_edit_rules` the UI cannot distinguish this from an author who merely
    lacks deploy: both would show `can_deploy_rules: false` on a deployment that can
    deploy, and only one of them should be offered the editor at all.
    """
    set_rules_dir(monkeypatch, tmp_path)

    res = client.get(url_for('config.get_config'))
    assert res.json['rule_deployment_enabled'] is True
    assert res.json['can_edit_rules'] is False
    assert res.json['can_deploy_rules'] is False


# --- Who the request is from ------------------------------------------------
#
# There is no authentication: identity is whatever the request claims. These pin the
# two supported ways to claim it, because the tempting third way -- editing the default
# in `lib/auth.py` -- silently fails every authz test in the suite, which all grant
# their abilities to `local-dev@localhost`.


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_requests_default_to_the_dev_user(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('config.get_config'))
    assert res.json is not None
    assert res.json['current_user'] == {'email': 'local-dev@localhost'}


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_x_test_email_changes_the_caller_for_one_request(client: 'FlaskClient[Response]') -> None:
    """Per-request identity, for driving a second role alongside a browser session."""
    res = client.get(url_for('config.get_config'), headers={'X-Test-Email': 'someone@example.com'})
    assert res.json is not None
    assert res.json['current_user'] == {'email': 'someone@example.com'}
    # An address no ACL mentions gets nothing, which is what makes the header useful
    # for checking what a less-privileged user sees.
    assert res.json['can_edit_rules'] is False


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_dev_user_email_changes_the_default_for_the_process(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    """Process-wide identity, for booting the stack as a particular user.

    This is the supported alternative to editing `set_dummy_claim`, which is what the
    header cannot do: change who the *browser* is, since it sends no header.
    """
    monkeypatch.setitem(CONFIG.instance()._config_dict, 'OSPREY_DEV_USER_EMAIL', 'booted-as@example.com')

    res = client.get(url_for('config.get_config'))
    assert res.json is not None
    assert res.json['current_user'] == {'email': 'booted-as@example.com'}

    # The header still wins over the configured default.
    with_header = client.get(url_for('config.get_config'), headers={'X-Test-Email': 'local-dev@localhost'})
    assert with_header.json is not None
    assert with_header.json['current_user'] == {'email': 'local-dev@localhost'}
