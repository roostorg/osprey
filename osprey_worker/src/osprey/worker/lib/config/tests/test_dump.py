import json
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from osprey.worker.lib.config import dump as dump_module
from osprey.worker.lib.config.catalog import CATALOG, PYTHON_COMPONENTS, ConfigKey
from osprey.worker.lib.config.cli import cli
from osprey.worker.lib.config.dump import (
    app_commit,
    app_version,
    build_config,
    build_dump,
    config_entry,
    parse_env_file,
    render,
)
from osprey.worker.lib.config.redaction import REDACTED

# Mirrors the osprey-ui-api / osprey-worker environment in docker-compose.yaml, plus secrets of every shape.
SECRETS = [
    'FoolishPassword',
    'minioadmin123',
    'sentrykey123',
    'hunter2',
    'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U',
]
ENV = {
    'POSTGRES_HOSTS': '{"osprey_db":"postgresql://osprey:FoolishPassword@postgres:5432/osprey"}',
    'OSPREY_MINIO_ACCESS_KEY': 'minioadmin',
    'OSPREY_MINIO_SECRET_KEY': 'minioadmin123',
    'OSPREY_KAFKA_BOOTSTRAP_SERVERS': '["osprey-kafka:29092"]',
    'OSPREY_RULES_SINK_NUM_WORKERS': '1',
    'SENTRY_UI_API_DSN': 'https://sentrykey123@o1.ingest.sentry.io/5',
    'ETCD_PEERS': 'http://root:hunter2@etcd:2379',
    'DD_SERVICE': SECRETS[4],
    'DRUID_URL': 'http://druid-broker:8082',
    'GOOGLE_APPLICATION_CREDENTIALS': '/secrets/sa.json',
    'UNRELATED_SECRET_NOT_IN_CATALOG': 'should-never-appear',
    'SENTRY_RULES_SINK_DSN': '',
}


def _dump(env: dict[str, str] = ENV) -> dict[str, object]:
    return build_dump(
        env,
        PYTHON_COMPONENTS,
        now=datetime(2026, 10, 27, tzinfo=UTC),
        version='1.2.1',
        commit='3cac93be2002b45c77a2d47038aa74648e2d9b8f',
        plugins=['stdlib'],
    )


def test_entry_sources() -> None:
    key = ConfigKey('DRUID_DATASOURCE', ('ui_api',), default='osprey.execution_results')
    assert config_entry(key, {}) == {'value': 'osprey.execution_results', 'set': False, 'source': 'default'}
    assert config_entry(key, {'DRUID_DATASOURCE': 'x'}) == {'value': 'x', 'set': True, 'source': 'env'}

    required = ConfigKey('DRUID_URL', ('ui_api',))
    assert config_entry(required, {}) == {'value': None, 'set': False, 'source': 'default'}

    derived = ConfigKey('URL', ('common',), derived_default=lambda env: f'http://{env.get("HOST", "localhost")}')
    assert config_entry(derived, {'HOST': 'h'}) == {'value': 'http://h', 'set': False, 'source': 'derived'}


def test_entry_redaction_and_set_unset() -> None:
    key = ConfigKey('OSPREY_MINIO_SECRET_KEY', ('common',), default='minioadmin123')
    # Defaults of sensitive keys are redacted too: dev defaults are still passwords.
    assert config_entry(key, {}) == {'value': REDACTED, 'set': False, 'source': 'default', 'redacted': True}
    assert config_entry(key, {'OSPREY_MINIO_SECRET_KEY': 's'}) == {
        'value': REDACTED,
        'set': True,
        'source': 'env',
        'redacted': True,
    }
    assert config_entry(key, {'OSPREY_MINIO_SECRET_KEY': ''}) == {'value': '', 'set': True, 'source': 'env'}
    unset = ConfigKey('API_TOKEN', ('common',))
    assert config_entry(unset, {}) == {'value': None, 'set': False, 'source': 'default'}


def test_env_values_parsed_like_config_from_env() -> None:
    config = build_config(ENV, ['common', 'worker'])
    assert config['worker']['OSPREY_KAFKA_BOOTSTRAP_SERVERS']['value'] == ['osprey-kafka:29092']
    assert config['common']['POSTGRES_HOSTS'] == {
        'value': {'osprey_db': f'postgresql://osprey:{REDACTED}@postgres:5432/osprey'},
        'set': True,
        'source': 'env',
        'redacted': True,
    }
    assert config['common']['ETCD_PEERS']['value'] == f'http://root:{REDACTED}@etcd:2379'
    # Invalid JSON is shown (redacted) rather than crashing the dump.
    bad = build_config({'POSTGRES_HOSTS': '{oops'}, ['common'])
    assert bad['common']['POSTGRES_HOSTS']['value'] == '{oops'


def test_dump_shape() -> None:
    dump = _dump()
    assert list(dump) == ['schema_version', 'generated_at', 'app', 'runtime', 'plugins', 'config']
    assert dump['schema_version'] == 1
    assert dump['generated_at'] == '2026-10-27T00:00:00+00:00'
    assert dump['app'] == {
        'name': 'osprey',
        'version': '1.2.1',
        'commit': '3cac93be2002b45c77a2d47038aa74648e2d9b8f',
    }
    assert set(dump['runtime']) == {'python', 'platform'}  # type: ignore[arg-type]
    assert dump['plugins'] == ['stdlib']
    assert list(dump['config']) == sorted(PYTHON_COMPONENTS)  # type: ignore[arg-type]
    for keys in dump['config'].values():  # type: ignore[attr-defined]
        assert list(keys) == sorted(keys)


def test_only_catalogued_keys_are_dumped() -> None:
    rendered = render(_dump(), 'json')
    assert 'UNRELATED_SECRET_NOT_IN_CATALOG' not in rendered
    assert 'should-never-appear' not in rendered
    catalogued = {k.name for k in CATALOG}
    for keys in json.loads(rendered)['config'].values():
        assert set(keys) <= catalogued


def test_no_secret_leaks_in_any_format() -> None:
    for output_format in ('json', 'yaml'):
        rendered = render(_dump(), output_format)
        for secret in SECRETS:
            assert secret not in rendered, (output_format, secret)
        assert 'http://druid-broker:8082' in rendered
        assert '/secrets/sa.json' not in rendered


def test_render_is_stable_and_parseable() -> None:
    assert render(_dump(), 'json') == render(_dump(), 'json')
    assert json.loads(render(_dump(), 'json')) == yaml.safe_load(render(_dump(), 'yaml'))


def test_parse_env_file() -> None:
    assert parse_env_file('A=1\n# comment\n\nexport B=x=y\nC=\nnot-an-assignment\n') == {
        'A': '1',
        'B': 'x=y',
        'C': '',
    }
    # /proc/<pid>/environ: NUL-separated, values may contain newlines.
    assert parse_env_file('A=1\0PEM=line1\nline2\0') == {'A': '1', 'PEM': 'line1\nline2'}


def test_cli_dump_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / 'coordinator.env'
    env_file.write_text('ETCD_PASSWORD=hunter2\nOSPREY_COORDINATOR_BIDI_STREAM_PORT=19950\n')
    result = CliRunner().invoke(cli, ['dump', '--component', 'coordinator', '--env-file', str(env_file)])
    assert result.exit_code == 0, result.output
    dump = json.loads(result.output)
    assert list(dump['config']) == ['coordinator']
    coordinator = dump['config']['coordinator']
    assert coordinator['ETCD_PASSWORD'] == {'value': REDACTED, 'set': True, 'source': 'env', 'redacted': True}
    assert coordinator['OSPREY_COORDINATOR_BIDI_STREAM_PORT'] == {'value': '19950', 'set': True, 'source': 'env'}
    assert coordinator['OSPREY_KAFKA_BOOTSTRAP_SERVERS']['value'] == 'localhost:9092'
    assert 'hunter2' not in result.output


def test_cli_dump_yaml_from_stdin() -> None:
    result = CliRunner().invoke(cli, ['dump', '--format', 'yaml', '--env-file', '-'], input='DRUID_URL=http://d:1\n')
    assert result.exit_code == 0, result.output
    assert yaml.safe_load(result.output)['config']['ui_api']['DRUID_URL']['value'] == 'http://d:1'


def test_app_version_and_commit_from_build_args(monkeypatch: pytest.MonkeyPatch) -> None:
    assert app_version({'OSPREY_VERSION': '1.2.1'}) == '1.2.1'
    assert app_commit({'OSPREY_COMMIT': 'abc123'}) == 'abc123'
    # An image built without the args has them set to ''; fall back to the package and checkout.
    monkeypatch.setattr(dump_module, 'git_commit', lambda: None)
    assert app_commit({'OSPREY_COMMIT': ''}) is None
    assert app_version({'OSPREY_VERSION': ''}) == metadata.version('osprey-worker')


def test_cli_reports_build_args(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('OSPREY_VERSION', '1.2.1')
    monkeypatch.setenv('OSPREY_COMMIT', 'abc123')
    result = CliRunner().invoke(cli, ['dump', '--env-file', '-'], input='')
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)['app'] == {'name': 'osprey', 'version': '1.2.1', 'commit': 'abc123'}
