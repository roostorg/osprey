import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

from .conftest import VIEWING_ABILITIES, rule_authoring_sources, sources_with_acl

# `/rules` and `/rules/source` are gated on CAN_VIEW_RULES, not the CAN_VIEW_DOCS they
# used to share with the docs endpoints. Each test names its ability set explicitly
# rather than sharing a module-level dict, so an authz test reads as what it is.


@pytest.mark.use_rules_sources(
    sources_with_acl(
        {
            'main.sml': '',
        },
        VIEWING_ABILITIES,
    )
)
def test_empty_engine_returns_empty_catalog(client: 'FlaskClient[Response]') -> None:
    """Empty engine returns {rules: [], total: 0, when_rules_total: 0, unused_total: 0}."""
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 200
    assert res.json == {'rules': [], 'total': 0, 'when_rules_total': 0, 'unused_total': 0}


@pytest.mark.use_rules_sources(
    sources_with_acl(
        {
            'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')
            ContainsHello = Rule(
                when_all=[PostText == 'hello'],
                description='Post contains hello',
            )
        """,
        },
        VIEWING_ABILITIES,
    )
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
    sources_with_acl(
        {
            'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            PostText: str = JsonData(path='$.post_text')
            FlaggedPhrase: str = JsonData(path='$.phrase')
            RuleWithFmt = Rule(
                when_all=[PostText == FlaggedPhrase],
                description=f'User {UserId} said {FlaggedPhrase}',
            )
        """,
        },
        VIEWING_ABILITIES,
    )
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
    sources_with_acl(
        {
            # main.sml is iterated FIRST (dict insertion order), and its WhenRules
            # references a Rule defined in an imported source that's iterated
            # SECOND. The two-sub-pass walk must still credit the reference.
            'main.sml': """
            Import(rules=['extra_rules.sml'])

            UserId: str = JsonData(path='$.user_id')

            WhenRules(
                rules_any=[ContainsHello],
                then=[DeclareVerdict(verdict=UserId)],
            )
        """,
            'extra_rules.sml': """
            PostText: str = JsonData(path='$.post_text')

            ContainsHello = Rule(
                when_all=[PostText == 'hello'],
                description='back-referenced',
            )
        """,
        },
        VIEWING_ABILITIES,
    )
)
def test_whenrules_in_main_references_rule_in_imported_source(client: 'FlaskClient[Response]') -> None:
    """A WhenRules in main.sml referencing a Rule in an imported source still credits the reference.

    Sources iterate in dict-insertion order, so main.sml (containing the WhenRules) is walked
    before extra_rules.sml (containing the Rule). A single-pass walk would miss the reference
    because the Rule hasn't been seen yet when the WhenRules is processed. The two-sub-pass
    walk (pass 1 counts refs, pass 2 collects rules and attaches counts) handles this.
    """
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 200

    rule = next(r for r in res.json['rules'] if r['name'] == 'ContainsHello')
    assert rule['referenced_by_whenrules'] == 1
    assert res.json['when_rules_total'] == 1
    assert res.json['unused_total'] == 0  # The one rule is referenced


@pytest.mark.use_rules_sources(
    sources_with_acl(
        {
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
        },
        VIEWING_ABILITIES,
    )
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
    sources_with_acl(
        {
            'main.sml': """
            UserId: str = JsonData(path='$.user_id')
            R = Rule(when_all=[UserId == 'x'], description='r')
        """,
        },
        VIEWING_ABILITIES,
    )
)
def test_dual_route_registration(client: 'FlaskClient[Response]') -> None:
    """GET reachable at both /rules and /api/rules, identical body."""
    res_root = client.get('/rules')
    res_api = client.get('/api/rules')
    assert res_root.status_code == 200
    assert res_api.status_code == 200
    assert res_root.json == res_api.json


@pytest.mark.use_rules_sources(
    sources_with_acl(
        {
            'main.sml': "UserId: str = JsonData(path='$.user_id')",
        },
        [],
    )
)
def test_endpoint_requires_an_ability(client: 'FlaskClient[Response]') -> None:
    """A user with no abilities at all gets 401."""
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 401


@pytest.mark.use_rules_sources(
    sources_with_acl(
        {'main.sml': "UserId: str = JsonData(path='$.user_id')"},
        ['CAN_VIEW_DOCS'],
    )
)
def test_catalog_requires_can_view_rules_not_can_view_docs(client: 'FlaskClient[Response]') -> None:
    """CAN_VIEW_DOCS alone is no longer enough to read the rule catalog.

    `/rules` shipped gated on CAN_VIEW_DOCS, which it shared with the docs and feature
    endpoints. It now has its own CAN_VIEW_RULES, so existing ACLs granting only
    CAN_VIEW_DOCS lose the Rules Registry page until they are updated -- a deliberate
    break, pinned here so it can't happen silently a second time.
    """
    res = client.get(url_for('rules.rules_list'))
    assert res.status_code == 401


# --------------------------------------------------------------------------- #
# GET /rules/source — serves rules the engine has loaded from disk.
#
# Lives here rather than in test_rule_drafts.py because the endpoint serves
# on-disk rules, not drafts: a draft's own SML comes from the rules table via
# `GET /rules/drafts/<id>`. Same CAN_VIEW_RULES as the catalog above: the catalog
# already renders each rule's conditions, so reading the source is not the more
# privileged operation.
#
# These deliberately grant VIEWING_ABILITIES rather than the authoring default. The
# endpoint used to require CAN_EDIT_RULES, and granting a superset would let it drift
# back there without a single test noticing.
# --------------------------------------------------------------------------- #


@pytest.mark.use_rules_sources(rule_authoring_sources(VIEWING_ABILITIES))
def test_get_source_returns_contents(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rules.get_source'), query_string={'path': 'main.sml'})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['path'] == 'main.sml'
    assert 'ContainsHello' in res.json['contents']


@pytest.mark.use_rules_sources(rule_authoring_sources(VIEWING_ABILITIES))
def test_get_source_rejects_bad_path(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rules.get_source'), query_string={'path': '../etc/passwd.sml'})
    assert res.status_code == 400


@pytest.mark.use_rules_sources(rule_authoring_sources(VIEWING_ABILITIES))
def test_get_source_404_for_unknown_path(client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('rules.get_source'), query_string={'path': 'rules/does_not_exist.sml'})
    assert res.status_code == 404


@pytest.mark.use_rules_sources(
    sources_with_acl(
        {
            'main.sml': "UserId: str = JsonData(path='$.user_id')",
        },
        [],
    )
)
def test_get_source_requires_can_view_rules(client: 'FlaskClient[Response]') -> None:
    """A user without CAN_VIEW_RULES gets 401, even though the path is valid."""
    res = client.get(url_for('rules.get_source'), query_string={'path': 'main.sml'})
    assert res.status_code == 401
