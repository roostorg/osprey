import subprocess
import sys

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
