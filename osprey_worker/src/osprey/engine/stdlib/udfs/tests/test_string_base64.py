import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.string_base64 import Base64Decode, Base64Encode
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(Base64Encode, Base64Decode)),
]


def test_string_base64Encode(execute: ExecuteFunction) -> None:
    data = execute(
        """
        TextToEncode = Base64Encode(input="Sergals are just cheese-shaped dragons.")
        Base64ToDecode = Base64Decode(input="U2VyZ2FscyBhcmUganVzdCBjaGVlc2Utc2hhcGVkIGRyYWdvbnMu")
        """
    )
    assert data == {
        'TextToEncode': 'U2VyZ2FscyBhcmUganVzdCBjaGVlc2Utc2hhcGVkIGRyYWdvbnMu',
        'Base64ToDecode': 'Sergals are just cheese-shaped dragons.',
    }
