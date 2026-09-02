"""Shared setup for the view tests.

`use_rules_sources` is a *marker*, so the sources it takes have to be plain
module-level values rather than fixtures — hence the constants and builder here
rather than more fixtures.
"""

import json
from collections.abc import Sequence
from unittest.mock import patch

import pytest
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.snowflake import Snowflake
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.storage.rules import Rule

_TEST_USER_EMAIL = 'local-dev@localhost'

#: A draft that validates against `RULE_AUTHORING_SML`. Shared by the draft tests and
#: the deploy tests, which both need a draft that will actually save.
VALID_DRAFT = "Import(rules=['models/base.sml'])\nSomeRule = Rule(when_all=[PostText == 'bye'], description='bye')"


def acl_config(*abilities: str) -> str:
    """Serialised `config.yaml` granting `abilities` to the test user.

    Called with no abilities it omits the `acl` key entirely, which is how the
    engine expresses "this deployment has no ACL configured" — distinct from a
    user present with an empty ability list.
    """
    config: dict[str, object] = {'ui_config': {}, 'labels': {}}
    if abilities:
        config['acl'] = {
            'users': {
                _TEST_USER_EMAIL: {'abilities': [{'name': name, 'allow_all': True} for name in abilities]},
            }
        }
    return json.dumps(config)


# A small rules tree with two features, one rule, and one effect. Shared by every
# test that needs the engine to have actually loaded something: the rule catalog,
# the vocabulary the builder's dropdowns read, and `GET /rules/source`.
#
# The SML is kept separate from the config so the same tree can be served under
# different ACLs — an authz test wants the identical rules with the ability
# withheld, not a different (smaller) rules tree.
RULE_AUTHORING_SML = {
    'models/base.sml': """
        UserId: str = JsonData(path='$.user_id')
        PostText: str = JsonData(path='$.post_text')
    """,
    'main.sml': """
        Import(rules=['models/base.sml'])

        ContainsHello = Rule(
            when_all=[PostText == 'hello'],
            description='Post contains hello',
        )

        WhenRules(
            rules_any=[ContainsHello],
            then=[DeclareVerdict(verdict=UserId)],
        )
    """,
}

#: Read the rule catalog and rule sources, but author nothing.
VIEWING_ABILITIES = ('CAN_VIEW_RULES',)
#: Read, plus stage drafts in the table. Still cannot deploy.
AUTHORING_ABILITIES = ('CAN_VIEW_RULES', 'CAN_EDIT_RULES')
#: Deploying is a separate ability from editing: writing a draft to the table is
#: reversible and private, publishing it into the engine's rules directory is neither.
#: A user can therefore hold `CAN_EDIT_RULES` and still be refused a deploy.
DEPLOYING_ABILITIES = ('CAN_VIEW_RULES', 'CAN_EDIT_RULES', 'CAN_DEPLOY_RULES')


def sources_with_acl(sml: dict[str, str], abilities: Sequence[str] = AUTHORING_ABILITIES) -> dict[str, str]:
    """An engine sources dict: `sml` plus the `config.yaml` granting `abilities`.

    `use_rules_sources` takes one dict holding both the rules and the config, so every
    test that wants a particular rules tree otherwise has to hand-assemble the config
    entry beside it. Splitting the two arguments lets any tree pair with any ability
    set — which is what an authz test needs, since it wants the *same* rules with a
    grant withheld rather than a different (smaller) tree.
    """
    return {'config.yaml': acl_config(*abilities), **sml}


def rule_authoring_sources(abilities: Sequence[str] = AUTHORING_ABILITIES) -> dict[str, str]:
    """`RULE_AUTHORING_SML` served under an ACL granting `abilities`.

    Pass a narrower sequence to test authorization against the same rules tree —
    `rule_authoring_sources(VIEWING_ABILITIES)` for a user who may read rules but not
    edit them, or `rule_authoring_sources([])` for a deployment with no ACL configured
    at all.
    """
    return sources_with_acl(RULE_AUTHORING_SML, abilities)


def set_rules_dir(monkeypatch: pytest.MonkeyPatch, path: object) -> None:
    """Point deploy at `path`.

    Deploy reads OSPREY_RULES_PATH via CONFIG, which is bound once at app setup, so
    this sets it on the already-bound config for the deploy handler to see.
    `setitem` rather than a plain assignment: CONFIG is a process-wide singleton, so
    an unreverted write leaks into every later test in the session.
    """
    monkeypatch.setenv('OSPREY_RULES_PATH', str(path))
    monkeypatch.setitem(CONFIG.instance()._config_dict, 'OSPREY_RULES_PATH', str(path))


def unset_rules_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave deploy with no rules directory configured."""
    monkeypatch.delenv('OSPREY_RULES_PATH', raising=False)
    monkeypatch.delitem(CONFIG.instance()._config_dict, 'OSPREY_RULES_PATH', raising=False)


@pytest.fixture(autouse=True)
def _no_ambient_rules_dir(monkeypatch: pytest.MonkeyPatch):
    """Stop deploy from writing anywhere outside a test's own `tmp_path`.

    Deploy writes to whatever `OSPREY_RULES_PATH` points at, and a dev environment sets
    it to a real directory inside the repository (`docker-compose.yaml` uses
    `./example_rules`). A deploy test that forgot `set_rules_dir` would therefore commit
    a rule file into the working tree rather than failing -- which has happened.

    Clearing it by default turns that mistake into a 503. Tests that mean to deploy call
    `set_rules_dir(monkeypatch, tmp_path)`, which runs later and wins.
    """
    monkeypatch.delenv('OSPREY_RULES_PATH', raising=False)
    monkeypatch.delitem(CONFIG.instance()._config_dict, 'OSPREY_RULES_PATH', raising=False)


@pytest.fixture(autouse=True)
def _clear_rules():
    """Start every test with an empty rules table.

    The test database is session-scoped, so drafts persist across tests and a leaked
    row trips the rule-name uniqueness constraint in an unrelated test later. Autouse
    across the whole directory rather than per-module: the draft tests and the deploy
    tests both write rows, and a third module doing so would silently reintroduce the
    problem.
    """
    with scoped_session(commit=True) as session:
        session.query(Rule).delete()
    yield


@pytest.fixture(autouse=True)
def _mock_audit_snowflake():
    """Stop the audit hook reaching for the snowflake-id-worker service.

    `audit_request` is an `after_request` hook exempting only `health` and
    `config.get_config`, so *every* request — GETs included — mints a snowflake,
    which is an HTTP call. Patching it here is what lets this directory run
    without the docker stack up. (The audit log's `persist()` is already mocked
    in the shared ui_api conftest.)
    """
    with patch('osprey.worker.ui_api.osprey.lib.audit.generate_snowflake', return_value=Snowflake(1)):
        yield
