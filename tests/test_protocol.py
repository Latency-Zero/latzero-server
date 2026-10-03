import math

import pytest

from latzero_server.config import ServerConfig
from latzero_server.protocol import decode_message, encode_message
from latzero_server.server import LatZeroServer


@pytest.mark.parametrize("raw", [
    b"[]", b"null", b'"hello"', b'{"type":[]}', b'{"type":{}}',
    b'{"type":"hello","payload":[]}', b'{"type":"hello","request_id":1}',
    b'{"type":"hello","pool":{}}', b'{"type":"hello","payload":{"v":NaN}}',
    b'{"type":"hello","payload":{"v":1e999}}', b'\xff',
])
def test_shared_decoder_rejects_invalid_frames(raw):
    with pytest.raises((ValueError, UnicodeError)):
        decode_message(raw)


def test_codec_roundtrip_preserves_large_integer_unicode_and_fractional_seconds():
    message = {"type": "set_buffer", "pool": None, "payload": {
        "value": [2 ** 100, "\u03bb\U0001f680"], "ttl": 0.125,
    }}
    assert decode_message(encode_message(message)) == message
    assert decode_message(encode_message(message).decode()) == message
    with pytest.raises(ValueError):
        encode_message({"type": "hello", "payload": {"value": math.nan}})


@pytest.mark.parametrize("options", [
    {"min_workers": 5, "max_workers": 4}, {"max_connections": 0},
    {"controller_interval": 0}, {"rpc_timeout": math.inf}, {"write_timeout": -1},
    {"port": 65536}, {"port": 65535}, {"max_frame_bytes": False},
    {"connection_hwm_bytes": 1025, "connection_critical_bytes": 1024},
    {"websocket_origins": "*"}, {"websocket_compression": "anything"},
    {"scale_down_threshold": 0}, {"websocket_enabled": "false"},
])
def test_invalid_runtime_limits_fail_before_start(options):
    with pytest.raises(ValueError):
        ServerConfig(**options)


def test_constructor_defers_loop_bound_primitives_for_python_38(tmp_path):
    daemon = LatZeroServer(ServerConfig(data_dir=tmp_path))
    assert daemon._lifecycle_lock is None
    assert daemon._worker_pool._work_available is None
    assert daemon._worker_pool._drained is None
