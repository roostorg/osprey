import subprocess
import sys
import uuid

import sqlalchemy
from osprey.worker.lib.osprey_shared.labels import EntityLabels
from osprey.worker.lib.storage import postgres

# Mirrors the `osprey-cli` console script. It has to run in a fresh interpreter: the test runner
# has already monkey-patched gevent, which hides import-order problems in the CLI module.
_CONSOLE_SCRIPT = 'import sys; from osprey.worker.lib.cli import cli; sys.exit(cli())'


def test_osprey_cli_help_runs_in_fresh_interpreter() -> None:
    result = subprocess.run(
        [sys.executable, '-c', _CONSOLE_SCRIPT, '--help'],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    assert 'push-rules' in result.stdout


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-c', _CONSOLE_SCRIPT, *args],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _stored_labels(entity_key: str) -> EntityLabels | None:
    # Read the row the shipped Postgres labels service (example_plugins) writes.
    with postgres.scoped_session() as session:
        stored = session.execute(
            sqlalchemy.text('SELECT labels FROM entity_labels WHERE entity_key = :key'), {'key': entity_key}
        ).scalar_one_or_none()
    return None if stored is None else EntityLabels.deserialize(stored)


def test_apply_label_initializes_the_labels_database() -> None:
    # Regression: the CLI never bound the config, so the Postgres labels service's connection was never
    # set up and every call failed with "Database osprey_db has not yet been initialized!". The CLI runs
    # in a fresh interpreter, where nothing else configures it, as it does for a real user.
    entity_id = f'cli-apply-label-{uuid.uuid4()}'

    result = _run_cli('apply-label', 'User', entity_id, 'cli_test_label', 'manually-added')

    assert result.returncode == 0, result.stderr
    labels = _stored_labels(f'User/{entity_id}')
    assert labels is not None, 'apply-label exited 0 but stored no labels'
    reason = labels.labels['cli_test_label'].reasons['CliLabelMutationWithoutEffects']
    assert reason.expires_at is None


def test_apply_label_expire_instantly_is_a_flag() -> None:
    entity_id = f'cli-apply-label-{uuid.uuid4()}'

    result = _run_cli('apply-label', 'User', entity_id, 'cli_test_label', 'manually-added', '--expire-instantly')

    assert result.returncode == 0, result.stderr
    labels = _stored_labels(f'User/{entity_id}')
    assert labels is not None, 'apply-label exited 0 but stored no labels'
    assert labels.labels['cli_test_label'].reasons['CliLabelMutationWithoutEffects'].expires_at is not None


def test_apply_label_expire_instantly_takes_no_value() -> None:
    # It used to take a string, and any value, even "False", made the label expire.
    result = _run_cli(
        'apply-label', 'User', 'unused', 'cli_test_label', 'manually-added', '--expire-instantly', 'False'
    )

    assert result.returncode != 0
    assert 'unexpected extra argument (False)' in result.stderr
