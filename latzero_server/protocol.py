"""
Protocol helpers for newline-delimited JSON messages.

Uses orjson when available (3-5x faster than stdlib json) with automatic
fallback to the standard library.  Both encode and decode are intentionally
kept as thin wrappers so the rest of the codebase never imports json directly.
"""

from typing import Any, Dict

try:
    import orjson as _json_lib  # type: ignore[import]

    def encode_message(message: Dict[str, Any]) -> bytes:
        """Encode a message as newline-delimited JSON (orjson fast path)."""
        return _json_lib.dumps(message) + b"\n"

    def decode_message(raw: bytes) -> Dict[str, Any]:
        """Decode one JSON line into a message dictionary (orjson fast path)."""
        data = _json_lib.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Protocol message must decode to an object")
        return data

except ImportError:
    import json as _stdlib_json

    def encode_message(message: Dict[str, Any]) -> bytes:  # type: ignore[misc]
        """Encode a message as newline-delimited JSON (stdlib fallback)."""
        return (_stdlib_json.dumps(message, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")

    def decode_message(raw: bytes) -> Dict[str, Any]:  # type: ignore[misc]
        """Decode one JSON line into a message dictionary (stdlib fallback)."""
        data = _stdlib_json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Protocol message must decode to an object")
        return data
