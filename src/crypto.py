"""Authenticated payload encryption helpers for the application protocol."""

import base64
import hashlib
import os


def get_fernet(secret=None):
    """Return a Fernet cipher derived from the shared APP_SECRET."""
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError(
            "Install dependencies with: py -m pip install -r requirements.txt"
        ) from exc

    secret = secret or os.environ.get("APP_SECRET")
    if not secret or len(secret) < 16:
        raise RuntimeError("Set APP_SECRET to the same secret (at least 16 characters) on all peers.")
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_bytes(data, cipher=None):
    return (cipher or get_fernet()).encrypt(data)


def decrypt_bytes(token, cipher=None):
    return (cipher or get_fernet()).decrypt(token)
