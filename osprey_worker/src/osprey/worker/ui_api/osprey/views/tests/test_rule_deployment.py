"""`POST /rules/drafts/<id>/deploy`.

Split out from `test_rule_drafts.py` because deploy is a different concern from
draft CRUD and validation: it touches the filesystem, reads the rules directory out
of config, and edits `main.sml`. The draft tests need none of that.

Deploy writes into the directory the engine loads from (`OSPREY_RULES_PATH`), so
these use `tmp_path` as that directory via `set_rules_dir`.
"""

import pytest
from flask import Response, url_for
from flask.testing import FlaskClient

from .conftest import (
    AUTHORING_ABILITIES,
    DEPLOYING_ABILITIES,
    VALID_DRAFT,
    rule_authoring_sources,
    set_rules_dir,
    unset_rules_dir,
)


def _create_draft(client: 'FlaskClient[Response]', path: str) -> str:
    """Save a valid draft at `path` and return its id."""
    created = client.post(
        url_for('rules.create_draft'),
        json={'path': path, 'rule_name': 'SomeRule', 'source': VALID_DRAFT, 'summary': ''},
    )
    assert created.json is not None, created.data
    return created.json['id']


@pytest.mark.use_rules_sources(rule_authoring_sources(AUTHORING_ABILITIES))
def test_deploy_requires_can_deploy_rules_not_just_can_edit_rules(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """CAN_EDIT_RULES is not enough to deploy; CAN_DEPLOY_RULES is a separate grant.

    This is the whole point of the second ability, so it gets a test that fails if
    deploy is ever moved back onto `CanEditRules`. Saving the draft in the same test
    proves the user really does hold the editing ability -- otherwise a 401 on deploy
    would be indistinguishable from having no access at all.
    """
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/needs_deploy_ability.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={})
    assert res.status_code == 401
    assert not (tmp_path / 'rules' / 'needs_deploy_ability.sml').exists()


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_writes_sml_and_marks_deployed(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/deploy.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={})
    assert res.status_code == 200
    assert res.json is not None
    # The body is a `RuleDeployment`, which wraps the row rather than being it:
    # {rule, main_sml_updated, path_on_disk}.
    assert res.json['rule']['status'] == 'deployed'
    assert res.json['rule']['deployed_at'] is not None
    assert res.json['main_sml_updated'] is False
    # Relative to the rules directory, so the server's directory layout isn't leaked.
    assert res.json['path_on_disk'] == 'rules/deploy.sml'

    written = tmp_path / 'rules' / 'deploy.sml'
    assert written.exists()
    assert written.read_text() == VALID_DRAFT


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_wire_into_main_appends_require(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\n")
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/wired.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['main_sml_updated'] is True
    assert "Require(rule='rules/wired.sml')" in (tmp_path / 'main.sml').read_text()


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_wire_into_main_is_idempotent(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\nRequire(rule='rules/already.sml')\n")
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/already.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['main_sml_updated'] is False
    assert (tmp_path / 'main.sml').read_text().count("Require(rule='rules/already.sml')") == 1


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_wire_into_main_ignores_a_commented_out_require(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A commented-out Require isn't a live one, so the rule still gets wired.

    The regex this replaced read main.sml as text and counted the comment as a live
    require: deploy reported success and wired nothing, leaving the rule inert on disk
    with nothing to indicate it. Parsing sees only what the engine would.
    """
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\n# Require(rule='rules/commented.sml')\n")
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/commented.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 200
    assert res.json is not None
    assert res.json['main_sml_updated'] is True

    main_sml = (tmp_path / 'main.sml').read_text()
    # The author's comment survives verbatim: deploy appends to the file's text rather
    # than reprinting a parsed tree, and a reprint would have dropped every comment in
    # the file (Python's parser discards them, so they aren't there to print).
    assert "# Require(rule='rules/commented.sml')" in main_sml
    assert "\nRequire(rule='rules/commented.sml')\n" in main_sml


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_409_when_main_sml_does_not_parse(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """An unparseable main.sml refuses the deploy rather than appending blind.

    "No Require found" and "couldn't look" are different answers. Appending to a file
    the engine can't compile would break it a second way and bury the original syntax
    error under ours, so the operator fixes main.sml and redeploys.
    """
    (tmp_path / 'main.sml').write_text('Import(rules=[\n')
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/badmain.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 409

    # Neither file is touched. The parse runs before the rule file is written, so a
    # refusal can't leave a deployed-looking .sml behind with main.sml un-wired.
    assert not (tmp_path / 'rules' / 'badmain.sml').exists()
    assert (tmp_path / 'main.sml').read_text() == 'Import(rules=[\n'


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_wire_into_main_409_when_main_missing(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/nomain.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert res.status_code == 409
    # The rule file must not be written when the deploy 409s on a missing main.sml.
    assert not (tmp_path / 'rules' / 'nomain.sml').exists()


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_503_when_rules_dir_unset(client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch) -> None:
    unset_rules_dir(monkeypatch)
    draft_id = _create_draft(client, 'rules/nodir.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={})
    assert res.status_code == 503


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_503_does_not_echo_the_configured_rules_path(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A misconfigured rules directory doesn't put the server's path in the response.

    Deploy errors are returned to the caller verbatim, and this was the only one
    carrying server filesystem state. A client can do nothing with that path -- only
    whoever set OSPREY_RULES_PATH can -- so it stays on the exception for callers that
    want it and out of the body. Flagged by CodeQL as code-scanning/26.
    """
    not_a_directory = tmp_path / 'rules.sml'
    not_a_directory.write_text('')
    set_rules_dir(monkeypatch, not_a_directory)
    draft_id = _create_draft(client, 'rules/misconfigured.sml')

    res = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={})
    assert res.status_code == 503
    assert res.json is not None
    assert str(not_a_directory) not in res.json['error']
    assert str(tmp_path) not in res.json['error']


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_deploy_404_for_unknown_draft(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    set_rules_dir(monkeypatch, tmp_path)
    res = client.post(url_for('rules.deploy_draft', draft_id=999999), json={})
    assert res.status_code == 404


# --- Deploy plan ------------------------------------------------------------
#
# Every state maps onto something `deploy_draft` actually does, so each test below
# pairs a plan with the deploy outcome it predicts. A plan that disagreed with the
# deploy would be worse than no plan: the dialog would enable a button that 409s.


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_for_a_new_rule_with_no_wiring(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/planned.sml')

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.status_code == 200
    assert res.json == {
        'source': {'state': 'valid'},
        'rule_file': {'path': 'rules/planned.sml', 'state': 'new'},
        # Reported even unwired, so the dialog can describe the checkbox it offers.
        'main_sml': {'state': 'missing', 'require_line': None},
        # Two independent facts in one response: the rule file can be written, and the
        # Require cannot be added because there is no main.sml. The checkbox reads the
        # second, so toggling it re-renders instead of re-requesting.
        'deployable': True,
        'wireable_into_main': False,
    }


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_reports_the_require_line_it_would_append(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """`would_append` carries the exact line, so the dialog shows rather than describes.

    This is the difference between "append Require(...) if not already present" and
    "append `Require(rule='rules/wired.sml')`" -- the client cannot know which without
    parsing main.sml itself.
    """
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\n")
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/wired.sml')

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.json is not None
    assert res.json['main_sml'] == {
        'state': 'would_append',
        'require_line': "Require(rule='rules/wired.sml')",
    }
    assert res.json['wireable_into_main'] is True


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_reports_already_required_and_identical_after_a_deploy(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Re-planning after a deploy reports a no-op, which is what makes it a redeploy.

    `identical` is decided by comparing the stored `cid` to a hash of the file on disk,
    so this also pins that deploy writes the source verbatim -- if it rewrote the SML in
    any way, the hashes would diverge and this would read `differs`.
    """
    (tmp_path / 'main.sml').write_text("Import(rules=['models/base.sml'])\n")
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/deployed.sml')
    client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.json is not None
    assert res.json['rule_file']['state'] == 'identical'
    assert res.json['main_sml']['state'] == 'already_required'
    assert res.json['wireable_into_main'] is True


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_reports_differs_when_the_file_was_edited_on_disk(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """An edit made outside the editor shows up as `differs`.

    This is the case `cid` exists for, and the one nothing else detects: the row still
    says `deployed`, the file is still there, and only the hash reveals that what is
    running is no longer what the draft says.
    """
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/tampered.sml')
    client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={})

    (tmp_path / 'rules' / 'tampered.sml').write_text('SomeRule = Rule(when_all=[], description="edited")')

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.json is not None
    assert res.json['rule_file']['state'] == 'differs'
    # Still deployable: overwriting is what deploying is for. The state is a warning,
    # not a blocker.
    assert res.json['deployable'] is True


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_is_not_deployable_when_main_sml_is_unparseable_and_wiring_is_asked_for(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """The state that would otherwise read `would_append` for a deploy that 409s.

    Both halves matter: `unparseable` names the reason, and `deployable: false` saves
    every client from re-deriving that this particular state blocks.
    """
    (tmp_path / 'main.sml').write_text('Import(rules=[\n')
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/blocked.sml')

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.json is not None
    assert res.json['main_sml']['state'] == 'unparseable'
    assert res.json['wireable_into_main'] is False

    # And the deploy it predicted really does refuse.
    deployed = client.post(url_for('rules.deploy_draft', draft_id=draft_id), json={'wire_into_main': True})
    assert deployed.status_code == 409


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_separates_deployability_from_wireability(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A broken main.sml stops the wiring, not the deploy.

    The two are independent facts, which is why they are two fields rather than one
    answer parameterised by a wiring flag: the rule file here is perfectly writable and
    only the `Require` line is impossible. One request settles both, so the dialog
    disables its checkbox and leaves Deploy enabled without asking again.
    """
    (tmp_path / 'main.sml').write_text('Import(rules=[\n')
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/either_way.sml')

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.json is not None
    assert res.json['deployable'] is True
    assert res.json['wireable_into_main'] is False


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_503s_when_deploy_is_not_configured(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plan that succeeded where the deploy 503s is the mismatch this prevents."""
    draft_id = _create_draft(client, 'rules/nodir_plan.sml')
    unset_rules_dir(monkeypatch)

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.status_code == 503


@pytest.mark.use_rules_sources(rule_authoring_sources(DEPLOYING_ABILITIES))
def test_plan_404s_for_an_unknown_draft(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    set_rules_dir(monkeypatch, tmp_path)
    res = client.get(url_for('rules.deploy_plan', draft_id=999999))
    assert res.status_code == 404


@pytest.mark.use_rules_sources(rule_authoring_sources(AUTHORING_ABILITIES))
def test_plan_requires_can_deploy_rules(
    client: 'FlaskClient[Response]', monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """The plan is gated with deploy, not editing: it is only useful to a deployer."""
    set_rules_dir(monkeypatch, tmp_path)
    draft_id = _create_draft(client, 'rules/plan_authz.sml')

    res = client.get(url_for('rules.deploy_plan', draft_id=draft_id))
    assert res.status_code == 401
