import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

from .conftest import rule_authoring_sources


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_parse_into_builder_returns_a_snake_case_model(client: 'FlaskClient[Response]') -> None:
    """A rule inside the builder's subset comes back as a model, keyed snake_case.

    Every response in this API is snake_case. The Builder models once carried camelCase
    aliases, making them the single exception; this pins the convention so reintroducing
    `alias_generator` fails here rather than quietly changing the wire format.
    """
    res = client.post(
        url_for('rules.parse_into_builder'),
        json={
            'path': 'rules/simple.sml',
            'source': "Simple = Rule(when_all=[PostText == 'hello'], description='says hello')",
        },
    )
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['supported'] is True

    model = res.json['model']
    assert model['rule_name'] == 'Simple'
    assert model['description'] == 'says hello'
    condition = model['conditions'][0]
    assert condition['feature'] == 'PostText'
    assert condition['operator'] == '=='
    assert condition['rhs'] == 'hello'
    assert condition['rhs_is_feature'] is False


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_parse_into_builder_reports_unsupported_shapes(client: 'FlaskClient[Response]') -> None:
    """SML outside the builder's subset is a 200 with a reason, not an error.

    "Can't be represented" is an answer: the editor uses it to decide whether to offer
    the Builder toggle, so refusing the request would tell it nothing useful.
    """
    res = client.post(
        url_for('rules.parse_into_builder'),
        json={
            'path': 'rules/helper.sml',
            'source': (
                "Helper = JsonData(path='$.x')\nWithHelper = Rule(when_all=[Helper == 'hello'], description='d')"
            ),
        },
    )
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['supported'] is False
    assert res.json['reason']


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_parse_into_builder_reports_unparseable_sml(client: 'FlaskClient[Response]') -> None:
    """Source that doesn't parse is also a reason, not a 500."""
    res = client.post(
        url_for('rules.parse_into_builder'),
        json={'path': 'rules/broken.sml', 'source': 'this is not valid SML *** !!!'},
    )
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['supported'] is False
    assert res.json['reason']
