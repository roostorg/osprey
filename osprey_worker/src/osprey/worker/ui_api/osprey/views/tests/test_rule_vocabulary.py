import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

from .conftest import AUTHORING_ABILITIES, VIEWING_ABILITIES, acl_config, rule_authoring_sources


@pytest.mark.use_rules_sources(rule_authoring_sources(VIEWING_ABILITIES))
def test_vocabulary_requires_can_edit_rules(client: 'FlaskClient[Response]') -> None:
    """A user without CAN_EDIT_RULES gets 401, even though the path is valid."""
    res = client.get(url_for('rules.vocabulary'))
    assert res.status_code == 401


@pytest.mark.use_rules_sources(rule_authoring_sources())
def test_vocabulary_returns_features_udfs_effects(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rules.vocabulary'))
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


@pytest.mark.use_rules_sources(
    {
        'config.yaml': acl_config(*AUTHORING_ABILITIES),
        'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')

            ContainsHello = Rule(
                when_all=[PostText == 'hello'],
                description='Post contains hello',
            )

            WhenRules(
                rules_any=[ContainsHello],
                then=[DeclareVerdict(verdict=HashSha256(input=UserId))],
            )
        """,
    }
)
def test_vocabulary_effects_include_calls_nested_in_then(client: 'FlaskClient[Response]') -> None:
    """Effects come from anywhere inside `then=[...]`, not just its top-level entries.

    `then=[DeclareVerdict(verdict=HashSha256(...))]` names two UDFs. A walk that only
    looked at the list's direct items would report `DeclareVerdict` alone, which is
    what `_collect_effects` did before it moved onto the engine's `filter_nodes`.
    """
    res = client.get(url_for('rules.vocabulary'))
    assert res.status_code == 200
    assert res.json is not None

    effects = res.json['effects']
    assert 'DeclareVerdict' in effects
    assert 'HashSha256' in effects


@pytest.mark.use_rules_sources({'config.yaml': acl_config(*AUTHORING_ABILITIES), 'main.sml': ''})
def test_vocabulary_on_empty_engine(client: 'FlaskClient[Response]') -> None:
    """No rules loaded at all: a fresh deployment must not 500.

    Features and effects are derived from what the engine loaded, so both are empty.
    UDFs come from the registry rather than from usage, so they are still populated,
    that asymmetry is the point of the endpoint.
    """
    res = client.get(url_for('rules.vocabulary'))
    assert res.status_code == 200
    assert res.json is not None

    assert res.json['features'] == []
    assert res.json['effects'] == []
    assert res.json['source_files'] == ['main.sml']
    assert len(res.json['udfs']) > 0
