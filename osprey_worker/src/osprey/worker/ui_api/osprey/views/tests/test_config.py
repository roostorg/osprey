import json

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

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

    pop = res.json.pop
    assert pop('external_links') == _ui_config_raw['external_links']
    assert pop('default_summary_features') == _ui_config_raw['default_summary_features']
    assert pop('feature_name_to_entity_type_mapping') == {'UserId': 'User', 'GuildId': 'Guild'}
    assert pop('feature_name_to_value_type_mapping') == {
        'ActionName': 'str',
        'Action_DmChannelCreated_Selfbot': 'bool',
        'UserId': 'int',
        'GuildId': 'int',
        'SomeExtractLiteral': 'list<int>',
    }
    assert pop('label_info_mapping') == _labels_raw

    filtered = [
        {'name': item['name'], 'source_path': item['source_path'], 'source_line': item['source_line']}
        for item in pop('known_feature_locations')
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
    assert set(pop('known_action_names')) == {'foo', 'bar'}
    assert pop('current_user') == {'email': 'local-dev@localhost'}
    assert pop('rule_info_mapping') == {
        'Action_DmChannelCreated_Selfbot': 'User joined created a dm channel with a selfbot'
    }
    assert not res.json


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            Str: ExtractLiteral[str] = 'hello'
            Int: ExtractLiteral[int] = 123
            Float: ExtractLiteral[float] = 123.4
            Bool: ExtractLiteral[bool] = True
            OStr: ExtractLiteral[Optional[str]] = 'hello'
            OInt: ExtractLiteral[Optional[int]] = 123
            OFloat: ExtractLiteral[Optional[float]] = 123.4
            OBool: ExtractLiteral[Optional[bool]] = True
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
