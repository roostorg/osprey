import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.string_hashes import HashMd5, HashSha1, HashSha256, HashSha512
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(HashMd5, HashSha1, HashSha256, HashSha512)),
]

# Split out MD5 test since it requires usedforsecurity=False in 3.9 and will fail in the future.


def test_hash_md5(execute: ExecuteFunction) -> None:
    data = execute(
        """
        MD5 = HashMd5(input="Once upon a midnight dreary,")
        """
    )
    assert data == {
        'MD5': '51d897cc6d5969ebad4694af5af363cb',
    }


def test_hash_sha(execute: ExecuteFunction) -> None:
    data = execute(
        """
        SHA_1 = HashSha1(input="while I pondered, weak and weary,")
        SHA_256 = HashSha256(input="Over many a quaint and curious volume of forgotten lore")
        SHA_512 = HashSha512(input="While I nodded, nearly napping,")
        """
    )
    assert data == {
        'SHA_1': '356781aac570928f03451ec6472dd972274d57d0',
        'SHA_256': 'a7e2dd005e17caaf034e26f4d2a0c64b847dcca9e629e5265452182227ef7906',
        'SHA_512': '4b9a1b6059ab90d401bf7f1e60bd6179d81469c393863c1d9ee8d6541ba3982c778666237b2ad02dddcfd8917b80fce5d222d60cabcc81612faf60ab48bcf1be',  # noqa: E501
    }
