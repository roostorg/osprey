"""Writing a rule's SML into the directory the engine loads from.

Deploying is a filesystem hand-off: the rule is written into the directory the
engine reads, and the engine picks it up on its next load.

**Deploy does not currently work with the etcd sources provider.** The engine
chooses its provider in `bootstrap_engine_with_helpers`: with `OSPREY_RULES_PATH`
set it reads that directory (`StaticSourcesProvider`), and with it unset it reads
etcd (`EtcdSourcesProvider`). Only the first is supported here. Under etcd a
written file is inert -- nothing publishes it until `osprey push-rules <dir>` is
run against the directory -- so deploy would report success for a rule that never
goes live, and `_main_requires` would be checking a directory's `main.sml` rather
than the one actually running.

Supporting etcd is not a matter of calling `sources_publisher.validate_and_push`:
that is a CLI entry point (it reports by `print`, returns a bare bool, and
re-bootstraps the UDF and validator registries per call), and more importantly
`publish_sources` *replaces* the whole etcd key, so publishing a directory would
push every other file in it too -- reverting unrelated rules to whatever that
checkout happens to hold. A correct etcd deploy would overlay the single changed
rule onto the engine's current sources and publish that, with no directory
involved. A DB-backed SourcesProvider reading deployed rules straight from the
table would remove the whole question; see the PR notes.

Failures raise `DeployError` subclasses rather than returning Flask responses, so
this module stays free of HTTP concerns. Each carries the `status` a view should
map it to, so the choice of status lives with the failure that motivates it rather
than being re-decided per call site. Turning one into a response is the view's job
-- see `views/rules/drafts.deploy_draft`.
"""

from __future__ import annotations

from pathlib import Path

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.errors import OspreySyntaxError
from osprey.engine.ast.grammar import Call, Source, String
from osprey.engine.ast.py_ast import transform
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage.rules import Rule
from osprey.worker.ui_api.osprey.lib.ast_utils import get_func_identifier
from osprey.worker.ui_api.osprey.schemas.rules import RuleDeployment, RuleRecord

# The engine's entry point: deploy appends `Require` lines to it, and
# `views/rules/drafts` rejects it as a draft target. Named once so those two
# can't drift apart.
MAIN_SML_PATH = 'main.sml'


class DeployError(Exception):
    """Base for deploy failures. `status` is the HTTP status the view should return."""

    status = 500


class DeployNotConfigured(DeployError):
    status = 503

    def __init__(self) -> None:
        super().__init__(
            'Deploy is not configured. Set OSPREY_RULES_PATH to the directory the '
            'engine loads rules from, so deployed rules are written where they will '
            'be read. Deploy does not support the etcd sources provider.'
        )


class RulesDirNotADirectory(DeployError):
    """OSPREY_RULES_PATH is set but doesn't point at a directory.

    The configured path is kept on `path` rather than interpolated into the message,
    because these messages are returned to the caller: a client can do nothing with a
    server-side path, and echoing filesystem state back is the one thing the deploy
    errors would otherwise leak. The operator who set the variable can read it, and
    naming it in `DeployNotConfigured` is enough to point them at it.
    """

    status = 503

    def __init__(self, path: str) -> None:
        super().__init__('The configured rules directory is not a directory.')
        self.path = path


class RulePathEscapesRulesDir(DeployError):
    status = 400

    def __init__(self, path: str) -> None:
        super().__init__(f'Rule path {path!r} escapes the rules directory.')


class MainSmlMissing(DeployError):
    status = 409

    def __init__(self) -> None:
        super().__init__(f'wire_into_main requested but {MAIN_SML_PATH} does not exist in the rules directory.')


class MainSmlUnparseable(DeployError):
    """main.sml doesn't compile, so we can't tell whether it already requires the rule.

    Refusing rather than appending blind: a `Require` line added to a file the engine
    can't parse makes a broken file broken in a second way, and buries the original
    syntax error under ours. The operator fixes main.sml, then redeploys.

    The parse error is kept on `reason` rather than interpolated into the message, for
    the same reason as `RulesDirNotADirectory`: these messages go to the caller, and
    `str(OspreySyntaxError)` is an internal repr of a `Span` object rather than anything
    a client can act on. Naming the file is the actionable part -- whoever fixes
    main.sml reads the error from the engine directly. The exception is also chained
    (`raise ... from`), so the cause is available to anything logging with `exc_info`.
    """

    status = 409

    def __init__(self, reason: str) -> None:
        super().__init__(f'{MAIN_SML_PATH} could not be parsed, so the rule was not deployed.')
        self.reason = reason


def rules_dir() -> Path:
    """The directory deploy writes into.

    This is `OSPREY_RULES_PATH`, the same key the engine loads from, and deliberately
    not a separate one: writing anywhere else deploys nothing, because the file would
    sit in a directory the engine never reads while the API reported success.

    An earlier `OSPREY_RULES_LOCAL_PATH` named the write target separately. That made
    sense when this module had pluggable submission backends and `local` was one
    option alongside opening a GitHub PR, but the backends are gone and the only
    topology that could still want the two to differ is etcd, which deploy does not
    support (see the module docstring).
    """
    raw = CONFIG.instance().get_str('OSPREY_RULES_PATH', '').strip()
    if not raw:
        raise DeployNotConfigured()
    directory = Path(raw)
    if not directory.is_dir():
        raise RulesDirNotADirectory(raw)
    return directory


def is_deploy_available() -> bool:
    """Whether this deployment can deploy rules at all.

    Answered by running the same `rules_dir()` the deploy path runs rather than
    re-reading `OSPREY_RULES_PATH`, because a second copy of "is it configured?" is
    free to disagree with the first -- and the disagreement surfaces as a UI that
    offers a deploy the API then refuses with a 503, which is the thing this exists
    to prevent.

    This is a property of the deployment, not of the user: it says nothing about
    whether the caller may deploy. `CanDeployRules` answers that, and the two are
    reported separately by `/config` so the UI can tell "this deployment doesn't
    deploy" from "you may not".
    """
    try:
        rules_dir()
    except DeployError:
        return False
    return True


def _resolve_within(directory: Path, rule_path: str) -> Path | None:
    """Resolve rule_path inside directory, or None if it would escape via `..`/symlink."""
    candidate = (directory / rule_path).resolve()
    try:
        candidate.relative_to(directory.resolve())
    except ValueError:
        return None
    return candidate


def _main_requires(main_sml: str, rule_path: str) -> bool:
    """Whether main.sml already has a `Require(rule=<rule_path>)`.

    Parsed rather than matched with a regex. The regex this replaces read the file as
    text, so a commented-out `# Require(rule='rules/spam.sml')` counted as a live
    require: deploy reported success and wired nothing. It also only recognised the
    single-line spelling by accident, and real rule files use the multi-line one.

    Raises `MainSmlUnparseable` if main.sml doesn't compile -- "no Require found" and
    "couldn't look" are different answers, and only one of them means it's safe to
    append.

    This is a read: the tree is walked and dropped, never printed. That's why the
    parser discarding comments costs nothing here, while `_append_require` below has
    to stay textual.
    """
    try:
        root = transform(Source(path=MAIN_SML_PATH, contents=main_sml))
    except OspreySyntaxError as exc:
        raise MainSmlUnparseable(str(exc)) from exc

    for call in filter_nodes(root, Call):
        if get_func_identifier(call) != 'Require':
            continue
        argument = call.find_argument('rule')
        if argument is not None and isinstance(argument.value, String) and argument.value.value == rule_path:
            return True
    return False


def _append_require(main_sml: str, rule_path: str) -> str:
    """Append a `Require` line to main.sml's text.

    Deliberately a string concatenation rather than an AST edit. Printing a parsed
    tree back out would drop every comment and blank line in the file -- Python's
    parser discards comments entirely, so they aren't in the tree to print. Appending
    to the raw text leaves everything above the new line byte-for-byte untouched.
    """
    suffix = f"\nRequire(rule='{rule_path}')\n"
    if main_sml and not main_sml.endswith('\n'):
        suffix = '\n' + suffix
    return main_sml + suffix


def deploy_rule(rule: Rule, *, wire_into_main: bool = False) -> RuleDeployment:
    """Write `rule`'s SML into the rules directory and mark the row deployed.

    With `wire_into_main`, also append a `Require(rule=...)` line to main.sml so
    the rule takes effect — the file on its own is inert until something
    requires it.
    """
    directory = rules_dir()

    target = _resolve_within(directory, rule.path)
    if target is None:
        raise RulePathEscapesRulesDir(rule.path)

    # Everything that can refuse the deploy happens before anything is written: a
    # missing or unparseable main.sml would otherwise leave the rule file on disk
    # while the deploy reports failure. The new main.sml contents are computed here
    # too -- `_append_require` is pure -- so the write below is the only side effect.
    main_path = directory / MAIN_SML_PATH
    pending_main_sml: str | None = None
    if wire_into_main:
        if not main_path.exists():
            raise MainSmlMissing()
        main_contents = main_path.read_text(encoding='utf-8')
        if not _main_requires(main_contents, rule.path):
            pending_main_sml = _append_require(main_contents, rule.path)

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(rule.sml_source, encoding='utf-8')

    main_sml_updated = pending_main_sml is not None
    if pending_main_sml is not None:
        main_path.write_text(pending_main_sml, encoding='utf-8')

    deployed = Rule.mark_deployed(rule.id) or rule

    return RuleDeployment(
        rule=RuleRecord.from_orm(deployed),
        main_sml_updated=main_sml_updated,
        # Report the path relative to the rules directory, not the absolute server
        # path (which would leak the deployment's directory layout to the client).
        path_on_disk=str(target.relative_to(directory.resolve())),
    )
