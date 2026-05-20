import json

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

_base_sources_dict = {
    'config.yaml': json.dumps(
        {
            'ui_config': {},
            'labels': {},
            'acl': {
                'users': {
                    'local-dev@localhost': {'abilities': [{'name': 'CAN_VIEW_DOCS', 'allow_all': True}]},
                }
            },
        }
    )
}

_no_ability_sources_dict = {'config.yaml': json.dumps({'ui_config': {}, 'labels': {}})}


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': '',
    }
)
def test_empty_engine_returns_empty_catalog(client: 'FlaskClient[Response]') -> None:
    """Empty engine returns {rules: [], total: 0, when_rules_total: 0, unused_total: 0}."""
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 200
    assert res.json == {'rules': [], 'total': 0, 'when_rules_total': 0, 'unused_total': 0}


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')
            ContainsHello = Rule(
                when_all=[PostText == 'hello'],
                description='Post contains hello',
            )
        """,
    }
)
def test_response_shape_and_basic_rule(client: 'FlaskClient[Response]') -> None:
    """Response shape and the 6 RuleInfo fields against a simple rule."""
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 200

    body = res.json
    assert set(body.keys()) == {'rules', 'total', 'when_rules_total', 'unused_total'}
    assert body['total'] == 1
    assert body['when_rules_total'] == 0
    assert body['unused_total'] == 1
    assert len(body['rules']) == 1

    rule = body['rules'][0]
    expected_fields = {
        'name',
        'source_file',
        'description',
        'when_all',
        'referenced_features',
        'referenced_by_whenrules',
    }
    assert set(rule.keys()) == expected_fields
    assert rule['name'] == 'ContainsHello'
    assert rule['source_file'] == 'main.sml'
    assert rule['description'] == 'Post contains hello'
    assert isinstance(rule['when_all'], list) and len(rule['when_all']) == 1
    assert 'PostText' in rule['when_all'][0]
    assert 'PostText' in rule['referenced_features']
    assert rule['referenced_by_whenrules'] == 0


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')
            FlaggedPhrase: str = JsonData(path='$.phrase')
            RuleWithFmt = Rule(
                when_all=[PostText == FlaggedPhrase],
                description=f'User {UserId} said {FlaggedPhrase}',
            )
        """,
    }
)
def test_referenced_features_from_when_all_and_format_description(client: 'FlaskClient[Response]') -> None:
    """referenced_features unions names from when_all expressions AND a FormatString description."""
    res = client.get(url_for('rules.rules_list'))
    rule = next(r for r in res.json['rules'] if r['name'] == 'RuleWithFmt')

    # PostText and FlaggedPhrase are in when_all; UserId only appears in the description template.
    # All three should appear in referenced_features, sorted.
    assert rule['referenced_features'] == ['FlaggedPhrase', 'PostText', 'UserId']
    # The description ships as the raw template, NOT substituted.
    assert rule['description'] == 'User {UserId} said {FlaggedPhrase}'


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        # WhenRules block textually PRECEDES the Rule it references.
        'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')

            WhenRules(
                rules_any=[ContainsHello],
                then=[DeclareVerdict(verdict=UserId)],
            )

            ContainsHello = Rule(
                when_all=[PostText == 'hello'],
                description='back-referenced',
            )
        """,
    }
)
def test_whenrules_before_rule_back_reference(client: 'FlaskClient[Response]') -> None:
    """A Rule defined textually after a WhenRules referencing it still gets referenced_by_whenrules >= 1."""
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 200

    rule = next(r for r in res.json['rules'] if r['name'] == 'ContainsHello')
    assert rule['referenced_by_whenrules'] >= 1
    assert res.json['when_rules_total'] == 1
    assert res.json['unused_total'] == 0  # The one rule is referenced


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')

            ReferencedRule = Rule(when_all=[PostText == 'a'], description='ref')
            UnusedRule = Rule(when_all=[PostText == 'b'], description='unref')

            WhenRules(
                rules_any=[ReferencedRule],
                then=[DeclareVerdict(verdict=UserId)],
            )
        """,
    }
)
def test_unused_total_excludes_referenced_rules(client: 'FlaskClient[Response]') -> None:
    """unused_total counts only rules with referenced_by_whenrules == 0."""
    res = client.get(url_for('rules.rules_list'))
    body = res.json

    assert body['total'] == 2
    assert body['when_rules_total'] == 1
    assert body['unused_total'] == 1

    by_name = {r['name']: r for r in body['rules']}
    assert by_name['ReferencedRule']['referenced_by_whenrules'] == 1
    assert by_name['UnusedRule']['referenced_by_whenrules'] == 0


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            R = Rule(when_all=[UserId == 'x'], description='r')
        """,
    }
)
def test_dual_route_registration(client: 'FlaskClient[Response]') -> None:
    """GET reachable at both /rules and /api/rules, identical body."""
    res_root = client.get('/rules')
    res_api = client.get('/api/rules')
    assert res_root.status_code == 200
    assert res_api.status_code == 200
    assert res_root.json == res_api.json


@pytest.mark.use_rules_sources(
    {
        **_no_ability_sources_dict,
        'main.sml': "UserId: str = JsonData(path='$.user_id')",
    }
)
def test_endpoint_requires_can_view_docs(client: 'FlaskClient[Response]') -> None:
    """A user without CAN_VIEW_DOCS gets 401."""
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 401
