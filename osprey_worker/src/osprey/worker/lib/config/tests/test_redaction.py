import json

import pytest
from osprey.worker.lib.config.redaction import (
    REDACTED,
    is_sensitive_key,
    key_tokens,
    looks_high_entropy,
    redact_entry,
    redact_string,
    redact_structure,
)

# Fixed fake secrets, so failures are reproducible. None of these are real credentials.
HEX_40 = 'c0757e99b9bd9451635094fbf1771c182256edc4'
URLSAFE_43 = 'eyF0178vIQ9pSpLtzDuvOldxZRSM_ESfDv5WN3tjzWs'
LOWER_ALNUM_32 = 'n0k172ntf0z2s0lhnvdqtu5c09pzgnw5'
JWT = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U'
PEM = '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASC\n-----END PRIVATE KEY-----\n'


def test_key_tokens() -> None:
    assert key_tokens('fooBar_BAZ-qux.KEY') == ['foo', 'bar', 'baz', 'qux', 'key']
    assert key_tokens('HTTPServerURL') == ['http', 'server', 'url']


@pytest.mark.parametrize(
    'key',
    [
        'OSPREY_MINIO_SECRET_KEY',
        'OSPREY_MINIO_ACCESS_KEY',
        'API_KEY',
        'APIKEY',
        'apiKey',
        'GROQ_SECRET_KEY',
        'SLACK_APP_BEARER_TOKEN',
        'SESSION_SECRET',
        'ETCD_PASSWORD',
        'DB_PASSWD',
        'MYSQL_PWD',
        'DBPASSWORD',  # compound inside a single token
        'SENTRY_UI_API_DSN',
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GCP_SA_JSON',
        'OSPREY_COORDINATOR_SERVICE_ACCOUNT',
        'serviceAccount',
        'TLS_CERT',
        'PRIVATE_KEY_PATH',
        'WEBHOOK_SIGNING_SECRET',
        'PASSWORD_SALT',
        'SESSION_COOKIE_NAME',
        'SLACK_WEBHOOK_URL',
        'OTEL_EXPORTER_OTLP_HEADERS',
        'BASIC_AUTH',
        'AUTH_ENABLED',  # fail-closed: a bool under an auth key is still redacted unless the catalog allows it
        'PUBSUB_ENCRYPTION_KEY_URI',  # matched by name; the catalog marks this one safe
        'jwt',
    ],
)
def test_sensitive_keys(key: str) -> None:
    assert is_sensitive_key(key)


@pytest.mark.parametrize(
    'key',
    [
        'SCYLLA_KEYSPACE',
        'KAFKA_KEEPALIVE_MS',
        'AUTHOR',
        'MONKEY_PATCH',
        'TOKENIZER_MODEL',
        'PASSTHROUGH',
        'SECRETARY_EMAIL',
        'DRUID_URL',
        'OSPREY_KAFKA_BOOTSTRAP_SERVERS',
        'SNOWFLAKE_API_ENDPOINT',
        'POSTGRES_HOSTS',
        'GOOGLE_CLOUD_PROJECT',
        'CERTAINTY_THRESHOLD',
        'PRIVATEER',
    ],
)
def test_non_sensitive_keys(key: str) -> None:
    assert not is_sensitive_key(key)


@pytest.mark.parametrize('token', [HEX_40, URLSAFE_43, LOWER_ALNUM_32, 'a' * 31 + '1' + HEX_40])
def test_high_entropy_tokens(token: str) -> None:
    assert looks_high_entropy(token)


@pytest.mark.parametrize(
    'token',
    [
        'my-service-name-for-the-k8s-cluster-2024',  # lower-case slug, entropy 4.1
        'coop-server-production-us-east-1-blue',
        '/config/osprey/rules-sink-sources',
        'osprey-bulk-action-file-upload-stg',  # no digit
        'deadbeefdeadbeefdeadbeefdeadbeef1',  # hex, but repetitive
        'a' * 40 + '1',
        HEX_40[:31],  # too short
        'osprey.execution_results.with.dots.2024',  # dots aren't in any charset
    ],
)
def test_low_entropy_tokens(token: str) -> None:
    assert not looks_high_entropy(token)


@pytest.mark.parametrize(
    'value,expected',
    [
        (
            'postgresql://osprey:FoolishPassword@postgres:5432/osprey',
            f'postgresql://osprey:{REDACTED}@postgres:5432/osprey',
        ),
        # A password containing `@` must not leak its tail.
        ('postgresql://osprey:p@ss@w0rd@db:5432/x', f'postgresql://osprey:{REDACTED}@db:5432/x'),
        # Userinfo without a password is usually a token.
        ('https://ghp_abc123@github.com/org/repo.git', f'https://{REDACTED}@github.com/org/repo.git'),
        ('https://abc123@o1.ingest.sentry.io/5', f'https://{REDACTED}@o1.ingest.sentry.io/5'),
        # Several URLs in one value.
        (
            'http://u:p1@etcd-a:2379,http://u:p2@etcd-b:2379',
            f'http://u:{REDACTED}@etcd-a:2379,http://u:{REDACTED}@etcd-b:2379',
        ),
        (
            'https://api.example.com/v1?token=abc&region=us',
            f'https://api.example.com/v1?token={REDACTED}&region=us',
        ),
        (
            'postgresql://db/osprey?sslmode=require&sslpassword=hunter2',
            f'postgresql://db/osprey?sslmode=require&sslpassword={REDACTED}',
        ),
        ('Server=db;Password=hunter2;Database=x', f'Server=db;Password={REDACTED};Database=x'),
        ('service.name=osprey,api_key=abc,env=prod', f'service.name=osprey,api_key={REDACTED},env=prod'),
        ('--user=u --password=pw', f'--user=u --password={REDACTED}'),
    ],
)
def test_url_masking_keeps_host_and_path(value: str, expected: str) -> None:
    assert redact_string(value) == (expected, True)


@pytest.mark.parametrize(
    'value',
    [
        PEM,
        f'prefix {PEM}',
        JWT,
        f'Bearer {JWT}',
        f'https://hooks.example.com/services/{URLSAFE_43}',
        f'https://api.example.com/{HEX_40}/status',
        URLSAFE_43,
        f'a,{LOWER_ALNUM_32}',
        f'SG.abcdEFGH1234.{URLSAFE_43}',  # dotted key format (SendGrid-style)
        'postgres://u:pa/ss@h:5432/db',  # unparseable userinfo: fail closed
    ],
)
def test_values_redacted_entirely(value: str) -> None:
    assert redact_string(value) == (REDACTED, True)


@pytest.mark.parametrize(
    'value',
    [
        'http://snowflake-id-worker:8088',
        'http://druid-broker:8082',
        'osprey-kafka:29092',
        'minio:9000',
        '/osprey/example_rules',
        'local-dev@localhost',
        'osprey.actions_input',
        'gcp-kms://projects/p/locations/global/keyRings/r/cryptoKeys/k',
        'https://api.example.com/v1?region=us&page=2',
        'my-service-name-for-the-k8s-cluster-2024',
        'true',
        '',
    ],
)
def test_safe_values_untouched(value: str) -> None:
    assert redact_string(value) == (value, False)


def test_json_string_is_walked() -> None:
    value = json.dumps({'osprey_db': 'postgresql://osprey:pw@postgres:5432/osprey', 'other': 'x'})
    assert redact_string(value) == (
        {'osprey_db': f'postgresql://osprey:{REDACTED}@postgres:5432/osprey', 'other': 'x'},
        True,
    )
    assert redact_string('{"a": 1}') == ('{"a": 1}', False)
    assert redact_string('{not json') == ('{not json', False)


@pytest.mark.parametrize(
    'blob',
    [
        {'type': 'service_account', 'project_id': 'p', 'client_email': 'sa@p.iam.gserviceaccount.com'},
        {'private_key': 'whatever', 'other': 1},
    ],
)
def test_service_account_json_redacted_entirely(blob: dict[str, object]) -> None:
    assert redact_structure(blob) == (REDACTED, True)
    assert redact_string(json.dumps(blob)) == (REDACTED, True)


def test_nested_keys_use_key_rules() -> None:
    value = {'hosts': ['a:1', 'b:2'], 'auth': {'password': 'pw', 'user': 'u'}, 'timeout': 5, 'api_key': ''}
    assert redact_structure(value) == (
        {'hosts': ['a:1', 'b:2'], 'auth': REDACTED, 'timeout': 5, 'api_key': ''},
        True,
    )
    assert redact_structure(['a', 1, None, True]) == (['a', 1, None, True], False)


def test_redact_entry() -> None:
    # By key, whatever the value's type.
    assert redact_entry('OSPREY_MINIO_SECRET_KEY', 'minioadmin123') == (REDACTED, True)
    assert redact_entry('AUTH_ENABLED', True) == (REDACTED, True)
    assert redact_entry('API_KEY', ['a', 'b']) == (REDACTED, True)
    # Unset and empty hide nothing, so "is it configured?" stays answerable.
    assert redact_entry('API_KEY', None) == (None, False)
    assert redact_entry('API_KEY', '') == ('', False)
    # Catalog overrides.
    assert redact_entry('PUBSUB_ENCRYPTION_KEY_URI', 'gcp-kms://projects/p/cryptoKeys/k', sensitive=False) == (
        'gcp-kms://projects/p/cryptoKeys/k',
        False,
    )
    assert redact_entry('DATABASE', 'anything', sensitive=True) == (REDACTED, True)
    # Value shape still applies when the key is marked safe.
    assert redact_entry('SAFE', 'x://u:pw@h', sensitive=False) == (f'x://u:{REDACTED}@h', True)
    assert redact_entry('DRUID_URL', 'http://druid-broker:8082') == ('http://druid-broker:8082', False)
    assert redact_entry('PORT', 5004) == (5004, False)
