import json
from typing import Any, Type
from unittest.mock import patch

import pytest
from flask import Flask
from osprey.worker.ui_api.osprey.lib.abilities import (
    Ability,
    CanViewActionData,
    CanViewLabels,
    CanViewLabelsForEntity,
    HashableEntityKey,
)
from osprey.worker.ui_api.osprey.lib.users import User

_super_user_test_ability = 'CAN_CREATE_AND_EDIT_SAVED_QUERIES'

config = {
    'main.sml': '',
    'config.yaml': json.dumps(
        {
            'acl': {
                'roles': {
                    'OKTA_USER': [{'name': 'CAN_VIEW_DOCS', 'allow_all': True}],
                    'SUPER_USER': [
                        {
                            # using _super_user_test_ability as a sentinel, it must be a valid ability, but which
                            # doesn't matter as long as it wouldn't otherwise be there
                            'name': _super_user_test_ability,
                            'allow_all': True,
                        }
                    ],
                    'CAN_VIEW_USER_43_AND_44': [
                        {'name': 'CAN_VIEW_LABELS_FOR_ENTITY', 'allow_specific': [{'type': 'User', 'id': '43'}]},
                        {'name': 'CAN_VIEW_LABELS_FOR_ENTITY', 'allow_specific': [{'type': 'User', 'id': '44'}]},
                    ],
                    'ALLOW_ALL_EXCEPT_VIEW_USER_44_AND_45': [
                        {
                            'name': 'CAN_VIEW_LABELS_FOR_ENTITY',
                            'allow_all_except': [{'type': 'User', 'id': '44'}, {'type': 'User', 'id': '45'}],
                        },
                    ],
                    'DATA_CENSOR_MERGE_1': [{'name': 'CAN_VIEW_ACTION_DATA', 'allow_all_except': ['guild_joined:*']}],
                    'DATA_CENSOR_MERGE_2': [{'name': 'CAN_VIEW_ACTION_DATA', 'allow_specific': ['guild_joined:user']}],
                    'DATA_CENSOR_MERGE_3': [{'name': 'CAN_VIEW_ACTION_DATA', 'allow_specific': ['guild_joined:*']}],
                },
                'users': {
                    'test_okta@example.com': {'roles': ['OKTA_USER']},
                    'test_okta_not_super@example.com': {'roles': ['OKTA_USER']},
                    'test_user@example.com': {
                        'abilities': [
                            {'name': 'CAN_VIEW_LABELS_FOR_ENTITY', 'allow_specific': [{'type': 'User', 'id': '42'}]}
                        ]
                    },
                    'test_user_2@example.com': {
                        'roles': ['CAN_VIEW_USER_43_AND_44'],
                        'abilities': [{'name': 'CAN_VIEW_LABELS', 'allow_specific': ['some_label_name']}],
                    },
                    'test_user_3@example.com': {
                        'roles': ['CAN_VIEW_USER_43_AND_44'],
                        'abilities': [
                            {'name': 'CAN_VIEW_LABELS', 'allow_specific': ['some_label_name']},
                            {'name': 'CAN_VIEW_LABELS', 'allow_all': True},
                        ],
                    },
                    'test_user_4@example.com': {
                        'roles': ['ALLOW_ALL_EXCEPT_VIEW_USER_44_AND_45'],
                        'abilities': [{'name': 'CAN_VIEW_LABELS', 'allow_specific': ['some_label_name']}],
                    },
                    'test_user_5@example.com': {
                        'roles': ['ALLOW_ALL_EXCEPT_VIEW_USER_44_AND_45'],
                        'abilities': [
                            {'name': 'CAN_VIEW_LABELS_FOR_ENTITY', 'allow_specific': [{'type': 'User', 'id': '45'}]}
                        ],
                    },
                    'test_user_6@example.com': {'roles': ['DATA_CENSOR_MERGE_1', 'DATA_CENSOR_MERGE_2']},
                    'test_user_7@example.com': {'roles': ['DATA_CENSOR_MERGE_1', 'DATA_CENSOR_MERGE_3']},
                },
            },
        },
    ),
}


@pytest.mark.use_rules_sources(config)
@pytest.mark.parametrize(
    'user_email, ability_class, has_ability',
    [
        ('test_user@example.com', CanViewLabelsForEntity, True),
        ('test_user@example.com', CanViewLabels, False),
        ('test_user_2@example.com', CanViewLabelsForEntity, True),
        ('test_user_2@example.com', CanViewLabels, True),
    ],
)
def test_user_has_ability(
    app: 'Flask', user_email: str, ability_class: Type[Ability[Any, Any]], has_ability: bool
) -> None:
    user = User(email=user_email)
    assert user.is_unregistered_user is False
    assert user.has_ability(ability_class) == has_ability


@pytest.mark.use_rules_sources(config)
@pytest.mark.parametrize(
    'user_email, ability_class, has_ability',
    [
        ('test_user@example.com', CanViewLabelsForEntity, True),
        ('test_user@example.com', CanViewLabels, False),
        ('test_user_2@example.com', CanViewLabelsForEntity, True),
        ('test_user_2@example.com', CanViewLabels, True),
    ],
)
def test_user_get_ability(
    app: 'Flask', user_email: str, ability_class: Type[Ability[Any, Any]], has_ability: bool
) -> None:
    user = User(email=user_email)
    assert user.is_unregistered_user is False
    if has_ability:
        assert user.get_ability(ability_class) is not None
    else:
        assert user.get_ability(ability_class) is None


@pytest.mark.use_rules_sources(config)
@pytest.mark.parametrize(
    'user_email,expected_ability',
    [
        (
            'test_user@example.com',
            CanViewLabelsForEntity(
                name='CAN_VIEW_LABELS_FOR_ENTITY', allow_specific={HashableEntityKey(id='42', type='User')}
            ),
        ),
        (
            'test_user_2@example.com',
            CanViewLabelsForEntity(
                name='CAN_VIEW_LABELS_FOR_ENTITY',
                allow_specific={HashableEntityKey(id='43', type='User'), HashableEntityKey(id='44', type='User')},
            ),
        ),
        (
            'test_user_2@example.com',
            CanViewLabels(name='CAN_VIEW_LABELS', allow_specific={'some_label_name'}),
        ),
        (
            'test_user_3@example.com',
            CanViewLabels(name='CAN_VIEW_LABELS', allow_all=True),
        ),
        (
            'test_user_4@example.com',
            CanViewLabels(name='CAN_VIEW_LABELS', allow_specific={'some_label_name'}),
        ),
        (
            'test_user_4@example.com',
            CanViewLabelsForEntity(
                name='CAN_VIEW_LABELS_FOR_ENTITY',
                allow_all_except={HashableEntityKey(id='44', type='User'), HashableEntityKey(id='45', type='User')},
            ),
        ),
        (
            'test_user_5@example.com',
            CanViewLabelsForEntity(
                name='CAN_VIEW_LABELS_FOR_ENTITY',
                allow_all_except={HashableEntityKey(id='44', type='User')},
            ),
        ),
        (
            'test_user_6@example.com',
            CanViewActionData(
                # For now, the merging of DataCensorAbility has unexpected behaviors; This test will need to be
                # updated once those behaviors change. See: https://app.asana.com/0/1201411113854286/1202522077329614/f
                name='CAN_VIEW_ACTION_DATA',
                allow_all_except={'guild_joined:*'},
            ),
        ),
        (
            'test_user_7@example.com',
            CanViewActionData(
                name='CAN_VIEW_ACTION_DATA',
                allow_all=True,
            ),
        ),
    ],
)
def test_user_ability_list_match(app: 'Flask', user_email: str, expected_ability: Ability[Any, Any]) -> None:
    user = User(email=user_email)
    assert user.get_ability(type(expected_ability)) == expected_ability


@pytest.mark.use_rules_sources(config)
@pytest.mark.parametrize(
    'user_email,is_super',
    [('test_okta@example.com', True), ('test_okta_not_super@example.com', False)],
)
def test_okta_grants(app: Flask, okta_profile_cache: None, user_email: str, is_super: bool) -> None:
    with app.test_request_context('/', headers={'X-Test-Email': user_email}):
        app.preprocess_request()  # type: ignore[no-untyped-call]
        from flask import request

        current_user_abilities = request.current_user.get_all_abilities()  # type: ignore
        has_super_sentinel = 'CAN_CREATE_AND_EDIT_SAVED_QUERIES' in current_user_abilities
        assert is_super == has_super_sentinel


@pytest.fixture
def okta_profile_cache():
    """Mock Okta profile cache to grant super user abilities to test_okta@example.com"""
    from osprey.worker.lib.sources_config.subkeys.acl_config import AclConfig

    original_get_abilities_for_user = AclConfig.get_abilities_for_user

    def mock_get_abilities_for_user(self, user_email: str):
        # Get the original abilities
        abilities = original_get_abilities_for_user(self, user_email)

        # Grant super user abilities to test_okta@example.com
        if user_email == 'test_okta@example.com':
            from osprey.worker.lib.acls.acls import ACL

            super_user_abilities = ACL.get_one('SUPER_USER')
            abilities.extend(super_user_abilities)

        return abilities

    with patch.object(AclConfig, 'get_abilities_for_user', mock_get_abilities_for_user):
        yield
