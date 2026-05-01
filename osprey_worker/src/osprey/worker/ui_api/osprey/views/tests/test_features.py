import json

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

_base_sources_dict = {'config.yaml': json.dumps({'ui_config': {}, 'labels': {}})}


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': '',
    }
)
def test_empty_engine_returns_empty_catalog(client: 'FlaskClient[Response]') -> None:
    """AC4.11: Empty engine returns {features: [], total: 0, categories: {}, extraction_fns: {}}."""
    res = client.get(url_for('features.features_list'))
    assert res.status_code == 200
    assert res.json == {'features': [], 'total': 0, 'categories': {}, 'extraction_fns': {}}


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            UserId = Entity(type='User', id=1)
        """,
    }
)
def test_response_shape_and_basic_feature(client: 'FlaskClient[Response]') -> None:
    """AC4.1, AC4.2: Response shape and 12-field FeatureInfo."""
    res = client.get(url_for('features.features_list'))
    assert res.status_code == 200

    body = res.json
    assert set(body.keys()) == {'features', 'total', 'categories', 'extraction_fns'}
    assert body['total'] == 1
    assert len(body['features']) == 1

    feat = body['features'][0]
    expected_fields = {
        'name',
        'source_file',
        'source_line',
        'category',
        'type_annotation',
        'extraction_fn',
        'definition',
        'referenced_by_rules',
        'referenced_by_features',
        'referenced_by_whenrules',
        'total_references',
    }
    assert expected_fields.issubset(set(feat.keys()))
    assert feat['name'] == 'UserId'
    assert feat['source_file'] == 'main.sml'
    assert feat['extraction_fn'] == 'Entity'
    assert feat['type_annotation'] is None
    assert feat['referenced_by_rules'] == []
    assert feat['referenced_by_features'] == []
    assert feat['referenced_by_whenrules'] == 0
    assert feat['total_references'] == 0


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            CallFeat = JsonData(path='$.foo')
            FmtFeat = f'hello {CallFeat}'
            ExprFeat = 1 + 2
            CmpFeat = CallFeat == 'x'
            BoolFeat = CallFeat and FmtFeat
            UnaryFeat = not CallFeat
            AttrFeat = CallFeat.id
        """,
    }
)
def test_extraction_fn_inference(client: 'FlaskClient[Response]') -> None:
    """AC4.3: extraction_fn correctly inferred per AST value type."""
    res = client.get(url_for('features.features_list'))
    by_name = {f['name']: f for f in res.json['features']}

    assert by_name['CallFeat']['extraction_fn'] == 'JsonData'
    assert by_name['FmtFeat']['extraction_fn'] == 'FormatString'
    assert by_name['ExprFeat']['extraction_fn'] == 'Expression'
    assert by_name['CmpFeat']['extraction_fn'] == 'Expression'
    assert by_name['BoolFeat']['extraction_fn'] == 'BooleanExpression'
    assert by_name['UnaryFeat']['extraction_fn'] == 'UnaryExpression'
    assert by_name['AttrFeat']['extraction_fn'] == 'Attribute'


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            Feat = JsonData(path='$.foo')
            R = Rule(when_all=[True], description='')
            C = Count(events=[Feat], window='1d', bucket='user_id')
        """,
    }
)
def test_non_feature_calls_excluded(client: 'FlaskClient[Response]') -> None:
    """AC4.4: Rule/Count/etc. excluded from features list."""
    res = client.get(url_for('features.features_list'))
    names = {f['name'] for f in res.json['features']}
    assert 'Feat' in names
    assert 'R' not in names
    assert 'C' not in names


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'models/user.sml': "UserA = JsonData(path='$.a')",
        'models/actions/joined.sml': "UserB = JsonData(path='$.b')",
        'lib/foo.sml': "UserC = JsonData(path='$.c')",
        'counters/active.sml': "UserD = JsonData(path='$.d')",
        'actions/dm.sml': "UserE = JsonData(path='$.e')",
    }
)
def test_category_derivation(client: 'FlaskClient[Response]') -> None:
    """AC4.5: category derived from source path."""
    res = client.get(url_for('features.features_list'))
    by_name = {f['name']: f for f in res.json['features']}

    assert by_name['UserA']['category'] == 'models'
    assert by_name['UserB']['category'] == 'models/actions'
    assert by_name['UserC']['category'] == 'lib'
    assert by_name['UserD']['category'] == 'counters'
    assert by_name['UserE']['category'] == 'actions'


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            FeatA = JsonData(path='$.a')
            FeatB = JsonData(path='$.b')
            FeatC = FeatA
            R1 = Rule(when_all=[FeatA == 'x'], description='r1')
            R2 = Rule(when_all=[FeatB == 'y'], description='r2')
            WhenRules(rules_any=[R1], then=[FeatA])
        """,
    }
)
def test_reference_counting_and_total(client: 'FlaskClient[Response]') -> None:
    """AC4.6, AC4.7, AC4.8, AC4.9: reference counts and total."""
    res = client.get(url_for('features.features_list'))
    by_name = {f['name']: f for f in res.json['features']}

    # FeatA referenced by R1 (rule), FeatC (feature), and the WhenRules then=
    assert by_name['FeatA']['referenced_by_rules'] == ['R1']
    assert by_name['FeatA']['referenced_by_features'] == ['FeatC']
    assert by_name['FeatA']['referenced_by_whenrules'] == 1
    assert by_name['FeatA']['total_references'] == 3

    # FeatB referenced only by R2.
    assert by_name['FeatB']['referenced_by_rules'] == ['R2']
    assert by_name['FeatB']['referenced_by_features'] == []
    assert by_name['FeatB']['referenced_by_whenrules'] == 0
    assert by_name['FeatB']['total_references'] == 1

    # FeatC has no inbound references; itself doesn't count as a self-reference.
    assert by_name['FeatC']['referenced_by_rules'] == []
    assert by_name['FeatC']['referenced_by_features'] == []
    assert by_name['FeatC']['referenced_by_whenrules'] == 0
    assert by_name['FeatC']['total_references'] == 0


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': "Feat = JsonData(path='$.foo')",
    }
)
def test_dual_route_registration(client: 'FlaskClient[Response]') -> None:
    """AC4.10: GET reachable at both /features and /api/features."""
    res_root = client.get('/features')
    res_api = client.get('/api/features')
    assert res_root.status_code == 200
    assert res_api.status_code == 200
    assert res_root.json == res_api.json


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': "Feat = JsonData(path='$.foo')",
    }
)
def test_endpoint_does_not_require_auth(client: 'FlaskClient[Response]') -> None:
    """AC4.13: /features has no @require_ability decorator. Calling it without
    any auth-related headers/cookies returns 200, not 401."""
    res = client.get(url_for('features.features_list'))
    assert res.status_code == 200


@pytest.mark.use_rules_sources(
    {
        **_base_sources_dict,
        'main.sml': """
            Feat = JsonData(path='$.foo')
        """,
    }
)
def test_find_assign_fallback_when_source_line_mismatches(client: 'FlaskClient[Response]') -> None:
    """AC4.12: A feature whose source_line doesn't exactly match the AST Assign
    node's start_line is found via fallback search by target identifier in the
    same source file."""
    from osprey.worker.lib.singletons import ENGINE
    from osprey.worker.ui_api.osprey.views.features import _find_assign_for_feature

    engine = ENGINE.instance()
    sources = engine.execution_graph.validated_sources.sources

    # Real source_line for `Feat` is whatever the engine reports — but the
    # fallback path is what matters here: pass a deliberately-wrong line and
    # confirm the helper still finds the Assign by target identifier.
    bogus_line = 99999
    assign = _find_assign_for_feature(sources, 'Feat', 'main.sml', bogus_line)
    assert assign is not None
    assert assign.target.identifier == 'Feat'

    # Negative control: a feature name that doesn't exist in the source returns None.
    assert _find_assign_for_feature(sources, 'DoesNotExist', 'main.sml', bogus_line) is None
