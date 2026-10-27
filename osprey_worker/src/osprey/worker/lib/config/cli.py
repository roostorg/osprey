"""`osprey-config`: inspect Osprey's configuration.

Deliberately light: it doesn't monkey-patch, start tracing or import the engine, so it runs in any Osprey container
(or against another process's environment via `--env-file`) without side effects.
"""

import os
from typing import TextIO

import click
from osprey.worker.lib.config.catalog import ALL_COMPONENTS, PYTHON_COMPONENTS
from osprey.worker.lib.config.dump import build_dump, git_commit, parse_env_file, render


@click.group()
def cli() -> None:
    """Inspect Osprey's configuration."""


@cli.command()
@click.option(
    '--component',
    'components',
    multiple=True,
    type=click.Choice(ALL_COMPONENTS),
    help=f'Component to include (repeatable). Default: {", ".join(PYTHON_COMPONENTS)}.',
)
@click.option(
    '--env-file',
    type=click.File('r'),
    help='Read the environment from this file ("-" for stdin) instead of this process: KEY=VALUE lines or '
    'NUL-separated /proc/<pid>/environ. Use it to dump another process, such as the coordinator.',
)
@click.option('--format', 'output_format', type=click.Choice(['json', 'yaml']), default='json', show_default=True)
def dump(components: tuple[str, ...], env_file: TextIO | None, output_format: str) -> None:
    """Print the effective configuration with secrets redacted, for pasting into bug reports."""
    env = parse_env_file(env_file.read()) if env_file is not None else dict(os.environ)
    click.echo(render(build_dump(env, components or PYTHON_COMPONENTS, commit=git_commit()), output_format), nl=False)


if __name__ == '__main__':
    cli()
