from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.storage.temporary_ability_token import TemporaryAbilityToken
from osprey.worker.ui_api.osprey.lib.abilities import CanViewLabelsForEntity


def test_create_and_consume_temporary_ability_tokens(app: Flask, client: 'FlaskClient[Response]') -> None:
    ability_token = TemporaryAbilityToken.create(
        abilities=[CanViewLabelsForEntity(allow_all=True, name=CanViewLabelsForEntity.class_name)],
        creation_origin='testing',
    )

    res = client.get(
        url_for('abilities.consume_temporary_ability_token', ability_token=ability_token.token),
        query_string={'redirect': 'http://osprey.com/fake_redirect_location'},
        follow_redirects=False,
    )

    assert res.status_code == 302
    assert res.location == 'http://osprey.com/fake_redirect_location'

    res = client.get(url_for('abilities.get_current_user_abilities'))

    assert res.json == {
        'abilities': {
            'CAN_VIEW_LABELS_FOR_ENTITY': {
                'allow_all': True,
                'allow_specific': None,
                'allow_all_except': None,
                'name': 'CAN_VIEW_LABELS_FOR_ENTITY',
            }
        }
    }
