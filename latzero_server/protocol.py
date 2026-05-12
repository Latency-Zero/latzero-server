"""
Protocol helpers for newline-delimited JSON messages.
"""

import json
from typing import Any, Dict


def encode_message(message: Dict[str, Any]) -> bytes:
    """Encode a message as newline-delimited JSON."""
    return (json.dumps(message, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")


def decode_message(raw: bytes) -> Dict[str, Any]:
    """Decode one JSON line into a message dictionary."""
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Protocol message must decode to an object")
    return data
