import copy
from typing import Any, Dict, Optional

from osprey.worker.ui_api.osprey.lib.abilities import (
    CENSOR_TEXT,
    Ability,
    AbilityRegistry,
    CanViewActionData,
    CanViewFeatureData,
    DataCensorAbility,
    QueryFilterAbility,
)
from pydantic import BaseModel


class FakeRequest(BaseModel):
    name: str


_ability_registry = AbilityRegistry()


@_ability_registry.register('TEST_ABILITY')
class TestAbility(Ability[FakeRequest, str]):
    def _request_is_allowed(self, input_model: FakeRequest) -> bool:
        return self.item_is_allowed(input_model.name)


@_ability_registry.register('TEST_QUERY_FILTER_ABILITY')
class TestQueryFilterAbility(QueryFilterAbility[FakeRequest, str]):
    def _get_query_filter(self) -> Optional[Dict[str, Any]]:
        if self.allow_all:
            return None

        elif not self.allow_specific:
            return None

        return {'allow_specific': self.allow_specific}


@_ability_registry.register('TEST_JSON_DATA_CENSOR_ABILITY')
class TestJsonDataCensorAbility(CanViewActionData):
    pass


@_ability_registry.register('TEST_FEATURE_DATA_CENSOR_ABILITY')
class TestFeatureDataCensorAbility(CanViewFeatureData):
    pass


def test_ability_registry() -> None:
    ability_model = _ability_registry.get_ability_model('TEST_ABILITY')
    assert ability_model == TestAbility
    assert ability_model.class_name == 'TEST_ABILITY'


def test_ability() -> None:
    ability = TestAbility(name='TEST_ABILITY', allow_all=True)
    assert ability.request_is_allowed(FakeRequest(name='nothing'))
    assert ability.item_is_allowed('thing')

    ability = TestAbility(name='TEST_ABILITY', allow_all_except={'thing'})
    assert ability.request_is_allowed(FakeRequest(name='different_thing'))
    assert ability.item_is_allowed('different_thing')
    assert not ability.request_is_allowed(FakeRequest(name='thing'))
    assert not ability.item_is_allowed('thing')

    ability = TestAbility(name='TEST_ABILITY', allow_specific={'irrelevant'})
    assert not ability.request_is_allowed(FakeRequest(name='nothing'))
    assert not ability.item_is_allowed('thing')

    ability = TestAbility(name='TEST_ABILITY', allow_specific={'thing'})
    assert not ability.request_is_allowed(FakeRequest(name='nothing'))
    assert ability.request_is_allowed(FakeRequest(name='thing'))
    assert ability.item_is_allowed('thing')


def test_query_filter_ability_registry() -> None:
    ability_model = _ability_registry.get_ability_model('TEST_QUERY_FILTER_ABILITY')
    assert ability_model == TestQueryFilterAbility
    assert ability_model.class_name == 'TEST_QUERY_FILTER_ABILITY'


def test_query_filter_ability() -> None:
    ability = TestQueryFilterAbility(name='TEST_QUERY_FILTER_ABILITY', allow_all=True)
    assert ability._get_query_filter() is None

    ability = TestQueryFilterAbility(name='TEST_QUERY_FILTER_ABILITY', allow_specific={'hello'})
    assert ability._get_query_filter() == {'allow_specific': {'hello'}}


"""
The following tests are a simple test suite to ensure that the DataCensorAbility is functioning appropriately!
"""


def test_json_data_censor_ability_validations() -> None:
    ability = TestJsonDataCensorAbility(
        name='TEST_JSON_DATA_CENSOR_ABILITY', allow_all_except=['a', 'test.path', 'another.test.path']
    )
    assert ability.allow_all_except == {'a', 'test.path', 'another.test.path'}

    ability = TestJsonDataCensorAbility(name='TEST_JSON_DATA_CENSOR_ABILITY', allow_specific=['another.test.path.*'])
    assert ability.allow_specific == {'another.test.path'}

    ability = TestJsonDataCensorAbility(name='TEST_JSON_DATA_CENSOR_ABILITY', allow_all_except=['some.item'])
    assert ability.allow_all_except == {'some.item'}

    ability2 = TestFeatureDataCensorAbility(
        name='TEST_FEATURE_DATA_CENSOR_ABILITY', allow_specific=['somefeaturename', 'anotherfeature']
    )
    assert ability2.allow_specific == {'somefeaturename', 'anotherfeature'}


# This data should test:
# - Accessing single-layered lists (IPs)
# - Accessing single-layered dics (user)
# - Accessing single-layered fields (User-Agent)
test_data_1 = {
    'user': {'name': 'A', 'discriminator': '0001', 'id': 192048286331437056, 'email': 'a@example.com'},
    'premium-type': None,
    'User-Agent': 'OwO-Flare',
    'IPs': ['0.0.0.0', '1.1.1.1', '2.2.2.2'],
    'guild': {'id': 21154681615024128, 'name': 'Example HQ'},
}

# This data should test:
# - Accessing multi-layered dicts/lists (users, users.guilds)
test_data_2 = {
    'users': [
        {
            'name': 'Person A',
            'id': 7,
            'email': 'a@example.com',
            'guilds': [
                {'id': 21154681615024128, 'name': 'Example HQ'},
                {'id': 697474023914733575, 'name': 'Example Support'},
            ],
            'premium-type': 'Premium Classic',
        },
        {
            'name': 'Person B',
            'id': 68,
            'email': 'b@example.com',
            'guilds': [{'id': 697474023914733575, 'name': 'Example Support'}],
            'premium-type': None,
        },
        {
            'name': 'Person C',
            'id': 421,
            'email': 'c@example.com',
            'guilds': [{'id': 21154681615024128, 'name': 'Example HQ'}],
            'premium-type': 'Premium',
        },
    ]
}


def test_json_data_censor_ability_allow_all() -> None:
    ability = TestJsonDataCensorAbility(name='TEST_JSON_DATA_CENSOR_ABILITY', allow_all=True)
    data_1 = copy.deepcopy(test_data_1)
    data_2 = copy.deepcopy(test_data_2)
    assert ability.censor_data(data_1, 'guild_joined') == test_data_1
    assert data_1 == test_data_1
    assert ability.censor_data(data_2, 'guild_joined') == test_data_2
    assert data_2 == test_data_2


def test_json_data_censor_ability_allow_all_except() -> None:
    ability1 = TestJsonDataCensorAbility(name='TEST_JSON_DATA_CENSOR_ABILITY', allow_all_except=['user', 'IPs.*'])
    ability2 = TestJsonDataCensorAbility(name='TEST_JSON_DATA_CENSOR_ABILITY', allow_all_except=['users.guilds.id'])
    censored_data_1 = {
        'user': {'name': CENSOR_TEXT, 'discriminator': CENSOR_TEXT, 'id': CENSOR_TEXT, 'email': CENSOR_TEXT},
        'User-Agent': 'OwO-Flare',
        'premium-type': None,
        'IPs': [CENSOR_TEXT, CENSOR_TEXT, CENSOR_TEXT],
        'guild': {'id': 21154681615024128, 'name': 'Example HQ'},
    }
    censored_data_2 = {
        'users': [
            {
                'name': 'Person A',
                'id': 7,
                'email': 'a@example.com',
                'guilds': [{'id': CENSOR_TEXT, 'name': 'Example HQ'}, {'id': CENSOR_TEXT, 'name': 'Example Support'}],
                'premium-type': 'Premium Classic',
            },
            {
                'name': 'Person B',
                'id': 68,
                'email': 'b@example.com',
                'guilds': [{'id': CENSOR_TEXT, 'name': 'Example Support'}],
                'premium-type': None,
            },
            {
                'name': 'Person C',
                'id': 421,
                'email': 'c@example.com',
                'guilds': [{'id': CENSOR_TEXT, 'name': 'Example HQ'}],
                'premium-type': 'Premium',
            },
        ]
    }
    data_1 = copy.deepcopy(test_data_1)
    data_2 = copy.deepcopy(test_data_2)
    assert ability1.censor_data(data_1, 'guild_joined') == censored_data_1
    assert data_1 == censored_data_1
    assert ability2.censor_data(data_2, 'guild_joined') == censored_data_2
    assert data_2 == censored_data_2


def test_json_data_censor_ability_allow_specific() -> None:
    ability1 = TestJsonDataCensorAbility(
        name='TEST_JSON_DATA_CENSOR_ABILITY', allow_specific=['user.name', 'guild.name']
    )
    ability2 = TestJsonDataCensorAbility(
        name='TEST_JSON_DATA_CENSOR_ABILITY', allow_specific=['users.id', 'users.guilds.id']
    )
    censored_data_1 = {
        'user': {'name': 'A', 'discriminator': CENSOR_TEXT, 'id': CENSOR_TEXT, 'email': CENSOR_TEXT},
        'User-Agent': CENSOR_TEXT,
        'premium-type': CENSOR_TEXT,
        'IPs': [CENSOR_TEXT, CENSOR_TEXT, CENSOR_TEXT],
        'guild': {'id': CENSOR_TEXT, 'name': 'Example HQ'},
    }
    censored_data_2 = {
        'users': [
            {
                'name': CENSOR_TEXT,
                'id': 7,
                'email': CENSOR_TEXT,
                'guilds': [
                    {'id': 21154681615024128, 'name': CENSOR_TEXT},
                    {'id': 697474023914733575, 'name': CENSOR_TEXT},
                ],
                'premium-type': CENSOR_TEXT,
            },
            {
                'name': CENSOR_TEXT,
                'id': 68,
                'email': CENSOR_TEXT,
                'guilds': [{'id': 697474023914733575, 'name': CENSOR_TEXT}],
                'premium-type': CENSOR_TEXT,
            },
            {
                'name': CENSOR_TEXT,
                'id': 421,
                'email': CENSOR_TEXT,
                'guilds': [{'id': 21154681615024128, 'name': CENSOR_TEXT}],
                'premium-type': CENSOR_TEXT,
            },
        ]
    }
    data_1 = copy.deepcopy(test_data_1)
    data_2 = copy.deepcopy(test_data_2)
    assert ability1.censor_data(data_1, 'guild_joined') == censored_data_1
    assert data_1 == censored_data_1
    assert ability2.censor_data(data_2, 'guild_joined') == censored_data_2
    assert data_2 == censored_data_2


def test_json_data_censor_ability_allow_none() -> None:
    """
    "Allow none" functionality is expected to occur internally if a data censor ability is not present
    (which would mean that the user does not have the ability to see the uncensored data at all)
    """
    censored_data = {
        'users': [
            {
                'name': CENSOR_TEXT,
                'id': CENSOR_TEXT,
                'email': CENSOR_TEXT,
                'guilds': [{'id': CENSOR_TEXT, 'name': CENSOR_TEXT}, {'id': CENSOR_TEXT, 'name': CENSOR_TEXT}],
                'premium-type': CENSOR_TEXT,
            },
            {
                'name': CENSOR_TEXT,
                'id': CENSOR_TEXT,
                'email': CENSOR_TEXT,
                'guilds': [{'id': CENSOR_TEXT, 'name': CENSOR_TEXT}],
                'premium-type': CENSOR_TEXT,
            },
            {
                'name': CENSOR_TEXT,
                'id': CENSOR_TEXT,
                'email': CENSOR_TEXT,
                'guilds': [{'id': CENSOR_TEXT, 'name': CENSOR_TEXT}],
                'premium-type': CENSOR_TEXT,
            },
        ]
    }
    data = copy.deepcopy(test_data_2)
    assert DataCensorAbility.censor_all_leafs(data) == censored_data
    assert data == censored_data
