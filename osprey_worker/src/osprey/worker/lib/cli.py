# mypy: ignore-errors
import sys
from pathlib import Path  # noqa: E402

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation, LabelStatus
from osprey.worker.lib.patcher import patch_all
from osprey.worker.lib.singletons import LABELS_PROVIDER  # noqa: E402

patch_all()  # please ensure this occurs before *any* other imports !


import datetime  # noqa: E402
import os  # noqa: E402
from typing import Any, Optional, Set  # noqa: E402

import click  # noqa: E402
from osprey.worker.lib.osprey_logging import configure_logging  # noqa: E402

configure_logging()

# Import safety record and common protos
from osprey.engine.ast.sources import Sources  # noqa: E402
from osprey.worker.lib.sources_publisher import (  # noqa: E402
    upload_dependencies_mapping,
    validate_and_push,
)
from osprey.worker.lib.storage import (  # noqa: E402
    access_audit_log,  # noqa: E402
    labels,
    stored_execution_result,
)
from osprey.worker.lib.utils.click_utils import EnumChoicePb2  # noqa: E402


@click.group()
def cli() -> None:
    pass


class CliCommandProgressTracker:
    """
    A simple class to interface with to provide progress print statements for the CLI.
    """

    def __init__(self, total_actions: int) -> None:
        self.total_actions = total_actions
        self.current_action_count = 0
        if total_actions <= 0:
            raise Exception('There must be at least 1 action for progress tracking')
        self._actions_per_print = int(total_actions / min(100, max(total_actions / 10, 1)))

    def increment(self, amount: int = 1) -> None:
        self.current_action_count += amount
        if self.current_action_count % self._actions_per_print == 0:
            print(
                f'Progress: {int(self.current_action_count / self.total_actions * 100)}% '
                f'({self.current_action_count} / {self.total_actions})'
            )


@cli.command()
@click.argument('rules_path', type=click.Path(dir_okay=True, file_okay=False, exists=True))
@click.option('--dry-run/--no-dry-run', is_flag=True, help='Validate rules without pushing.')
@click.option('--suppress-warnings', is_flag=True, help='Skip printing any warnings from validation.')
def push_rules(rules_path: str, dry_run: bool, suppress_warnings: bool) -> None:
    sources_path = Path(rules_path)
    if not validate_and_push(Sources.from_path(sources_path), dry_run, suppress_warnings):
        sys.exit(1)


@cli.command()
@click.argument('rules_path', type=click.Path(dir_okay=True, file_okay=False, exists=True))
@click.option('--suppress-warnings', is_flag=True, help='Skip printing any warnings from validation.')
def compute_and_upload_dependencies_mapping(rules_path: str, suppress_warnings: bool) -> None:
    sources_path = Path(rules_path)
    if not upload_dependencies_mapping(Sources.from_path(sources_path), suppress_warnings):
        sys.exit(1)


@cli.command()
@click.option('--auto-import/--no-auto-import', '-i', default=True)
def shell(auto_import: str) -> None:
    import sys

    osprey_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append(osprey_path)

    namespace = {}
    if auto_import:
        import importlib

        from werkzeug.utils import find_modules

        for module_name in find_modules('osprey_lib', include_packages=False, recursive=True):
            # Don't accidentally import model tests.
            if 'test' in module_name:
                continue

            try:
                module = importlib.import_module(module_name)
                models = {
                    k: v
                    for k, v in module.__dict__.items()
                    if (
                        not (k.startswith('__') and k.endswith('__'))
                        and hasattr(v, '__module__')
                        and v.__module__ == module
                    )
                }
                if models:
                    namespace.update(models)
                    print('>>> from {} import {}'.format(module, ', '.join(k for k in sorted(models))))
            except ImportError:
                pass

    namespace_overrides = {
        'labels': labels,
        'access_audit_log': access_audit_log,
        'stored_execution_result': stored_execution_result,
        'EntityT': EntityT,
        # 'Entity': Entity,
        'EntityLabelMutation': EntityLabelMutation,
        'LabelStatus': LabelStatus,
    }

    namespace.update(namespace_overrides)

    try:
        from IPython import start_ipython
        from traitlets.config import Config

        c = Config()
        c.InteractiveShellApp.exec_lines = ['print("IPython shell initialized with custom namespace")']

        print('\u001b[32m\u001b[1m>>> Starting ipython shell!\u001b[0m')

        # Start IPython with our namespace
        start_ipython(argv=[], config=c, user_ns=namespace)

    except ImportError:
        import code
        import rlcompleter

        c = rlcompleter.Completer(namespace)

        # Setup readline for autocomplete.
        try:
            # noinspection PyUnresolvedReferences
            import readline

            readline.set_completer(c.complete)
            readline.parse_and_bind('tab: complete')
            readline.parse_and_bind('set show-all-if-ambiguous on')
            readline.parse_and_bind(r'"\C-r": reverse-search-history')
            readline.parse_and_bind(r'"\C-s": forward-search-history')

        except ImportError:
            pass

        def is_probably_local_dev() -> Any:
            import socket

            hostname = socket.gethostname()
            return not any(x in hostname for x in ['-prd', '-stg'])

        def install_ipython() -> None:
            import os.path
            import subprocess
            import sys

            print('\u001b[31m\u001b[1m>>> Installing ipython\u001b[0m')
            pip_path = os.path.join(os.path.dirname(sys.executable), 'pip')
            subprocess.check_output([pip_path, 'install', 'ipython==7.20.0'])
            print('\u001b[32m\u001b[1m>>> Install complete, restart your shell!\u001b[0m')

        if is_probably_local_dev():
            namespace['install_ipython'] = install_ipython
            print(
                '\u001b[31m\u001b[1m>>> Starting built-in shell (type \u001b[4minstall_ipython()'
                '\u001b[0m\u001b[31m\u001b[1m to use the \u001b[7mdeluxe\u2122\u001b[0m\u001b[31m'
                '\u001b[1m shell experience.)\u001b[0m'
            )

        code.InteractiveConsole(namespace).interact()


def get_lines_from_file_as_set(file_path: str) -> Set[str]:
    """
    Collects all lines from a file in an unordered set.
    This collection does not include empty lines.
    """
    lines = []
    with open(file_path) as f:
        lines = f.readlines()
        for i in range(len(lines) - 1, -1, -1):
            lines[i] = lines[i].replace('\n', '')
            if not lines[i] or lines[i] == '':
                del lines[i]
    return set(lines)


@cli.command()
@click.argument('entity_type')
@click.argument('entity_id')
@click.argument('label_name')
@click.argument('label_status', type=EnumChoicePb2(LabelStatus))
@click.option(
    '--reason',
    help=(
        'If specified, the reason the label is being applied.'
        ' Should be camel case, without spaces. Defaults to "CliLabelMutation".'
    ),
)
@click.option(
    '--description',
    help=(
        'If specified, the description for why the label is being applied.'
        ' Should be an English sentence. Defaults to "Manually changed from the command line for debugging."'
    ),
)
@click.option(
    '--expire-instantly',
    default=False,
    help=('Boolean option to make the label expire instantly. Supplying False means the label does not expire.'),
)
def apply_label(
    entity_type: str,
    entity_id: str,
    label_name: str,
    label_status: LabelStatus,
    reason: Optional[str],
    description: Optional[str],
    expire_instantly: bool,
) -> None:
    """Manually apply a label to an entity.

    Mainly intended to be used for debugging purposes or importing lists of labels from external sources.
    """
    if expire_instantly:
        mutation = EntityLabelMutation(
            label_name=label_name,
            reason_name=reason or 'CliLabelMutationWithoutEffects',
            status=label_status,
            description=description or 'Manually changed from the command line for debugging.',
            expires_at=(datetime.datetime.now() + datetime.timedelta(seconds=5)),
        )
    else:
        mutation = EntityLabelMutation(
            label_name=label_name,
            reason_name=reason or 'CliLabelMutationWithoutEffects',
            status=label_status,
            description=description or 'Manually changed from the command line for debugging.',
        )

    provider = LABELS_PROVIDER.instance()
    assert provider is not None, (
        'this CLI cannot be used because no labels service / provider is supplied for this osprey instance'
    )

    result = provider.apply_entity_label_mutations(entity=EntityT(type=entity_type, id=entity_id), mutations=[mutation])

    print(result)


@cli.command()
@click.argument('entity_type')
@click.argument('entity_ids_file_path')
@click.argument('label_name')
@click.argument('label_status', type=EnumChoicePb2(LabelStatus))
@click.option(
    '--reason',
    help=(
        'If specified, the reason the label is being applied.'
        ' Should be camel case, without spaces. Defaults to "CliLabelMutationWithoutEffects".'
    ),
)
@click.option(
    '--description',
    help=(
        'If specified, the description for why the label is being applied.'
        ' Should be an English sentence. Defaults to "Manually changed from the command line for debugging."'
    ),
)
@click.option(
    '--expire-instantly',
    default=False,
    help=('Boolean option to make the label expire instantly. Supplying False means the label does not expire.'),
)
def bulk_apply_label(
    entity_type: str,
    entity_ids_file_path: str,
    label_name: str,
    label_status: LabelStatus,
    reason: Optional[str],
    description: Optional[str],
    expire_instantly: bool,
) -> None:
    """Manually apply a label to all entity IDs in the provided file at the file path.

    Mainly intended to be used for debugging purposes or importing lists of labels from external sources.
    """
    entity_ids = get_lines_from_file_as_set(file_path=entity_ids_file_path)
    # I found that it *generally* took ~10ms per request; Multiply by 10.05 for 5% latency headroom
    expire_timestamp = datetime.datetime.now() + datetime.timedelta(milliseconds=int(len(entity_ids) * 10.05))
    print(f'Found {len(entity_ids)} entity IDs to label.\nETA: {int(len(entity_ids) * 10.05 / 100)} second(s)')
    if expire_instantly:
        mutation = EntityLabelMutation(
            label_name=label_name,
            reason_name=reason or 'CliLabelMutationWithoutEffects',
            status=label_status,
            description=description or 'Manually changed from the command line for debugging.',
            expires_at=expire_timestamp,
        )
    else:
        mutation = EntityLabelMutation(
            label_name=label_name,
            reason_name=reason or 'CliLabelMutationWithoutEffects',
            status=label_status,
            description=description or 'Manually changed from the command line for debugging.',
        )

    progress_tracker: CliCommandProgressTracker = CliCommandProgressTracker(total_actions=len(entity_ids))
    provider = LABELS_PROVIDER.instance()
    assert provider is not None, (
        'this code cannot be used because no labels service / provider is supplied for this osprey instance'
    )
    for entity_id in entity_ids:
        _ = provider.apply_entity_label_mutations(
            entity=EntityT(type=entity_type, id=entity_id),
            mutations=[mutation],
        )
        progress_tracker.increment()

    print(f'Bulk labelling complete! Total labels applied: {progress_tracker.total_actions}')
