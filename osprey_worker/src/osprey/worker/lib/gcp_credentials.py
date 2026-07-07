"""Shared detection of whether GCP Pub/Sub publishing should be active.

Used by the Pub/Sub publishers to decide whether to run for real or degrade to
a noop (e.g. local dev or adopter environments without GCP). Publishing is off
when DISABLE_GCP_PUBSUB is set, or when GCP credentials cannot be resolved. The
credential result is cached at module level so google.auth.default() is probed
at most once per process, and guarded by a lock so concurrent constructors
probe only once.
"""

import os
import threading

import google.auth
from google.auth.exceptions import DefaultCredentialsError

_gcp_credentials_available: bool | None = None
_gcp_credentials_lock = threading.Lock()


def gcp_pubsub_disabled() -> bool:
    return os.environ.get('DISABLE_GCP_PUBSUB', '').lower() == 'true'


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
