"""Shared, cached detection of whether GCP credentials are resolvable.

Used by the publisher factory to decide whether to build a real Pub/Sub
publisher or hand back a noop one (e.g. local dev or adopter environments
without GCP). The result is cached at module level so google.auth.default() is
probed at most once per process, and guarded by a lock so concurrent callers
probe only once.
"""

import threading

import google.auth
from google.auth.exceptions import DefaultCredentialsError

_gcp_credentials_available: bool | None = None
_gcp_credentials_lock = threading.Lock()


def gcp_credentials_available() -> bool:
    global _gcp_credentials_available
    if _gcp_credentials_available is None:
        with _gcp_credentials_lock:
            # Re-check under the lock so concurrent callers probe google.auth.default() only once.
            if _gcp_credentials_available is None:
                try:
                    google.auth.default()
                    _gcp_credentials_available = True
                except DefaultCredentialsError:
                    _gcp_credentials_available = False
    return _gcp_credentials_available
