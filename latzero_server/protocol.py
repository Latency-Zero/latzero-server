"""Shared TCP/WS JSON validation with stable stdlib numeric semantics."""

import json
import math
from typing import Any, Dict, Union


def validate_message(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("Protocol message must decode to an object")
    if not isinstance(data.get("type"), str) or not data["type"]:
        raise ValueError("Message type must be a nonempty string")
    if data.get("payload") is not None and not isinstance(data["payload"], dict):
        raise ValueError("Message payload must be an object or null")
    for field in ("request_id", "client_id", "pool"):
        if data.get(field) is not None and not isinstance(data[field], str):
            raise ValueError(f"{field} must be a string or null")
    stack = [data]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
    return data


def encode_message(message: Dict[str, Any]) -> bytes:
    return (json.dumps(message, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


def decode_message(raw: Union[bytes, str]) -> Dict[str, Any]:
    return validate_message(json.loads(raw))
