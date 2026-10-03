"""Raw-file fidelity, filename isolation, and verified legacy recovery."""

from copy import deepcopy
import hashlib
import json

import pytest

from latzero_server.models import BufferEntry, PoolState
from latzero_server.persistence import SnapshotStore


def make_pool(pool_id="demo", value=None):
    pool = PoolState(pool_id=pool_id, auth_required=True, auth_token_hash="hash123")
    pool.buffers["persisted"] = BufferEntry(
        value={"nested": ["value", {"number": 1.25, "null": None}]} if value is None else value,
        updated_at=1700000000.5, updated_by="client-a", persistent=True,
        ttl=0.125, version=2,
    )
    return pool


def raw_snapshot(store, pool_id):
    return json.loads(store._path_for_pool(pool_id).read_text(encoding="utf-8"))


def write_legacy(store, pool):
    path = store._legacy_path_for_pool(pool.pool_id)
    path.write_text(json.dumps(pool.snapshot()), encoding="utf-8")
    return path


def test_snapshot_store_roundtrip(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = make_pool(value={"hello": "world", "unicode": "\u00e9\u96ea", "items": [True, 2, None]})
    pool.buffers["forever"] = BufferEntry(
        value=[1, {"a": 2}], updated_at=1.0, updated_by="client-b",
        persistent=True, ttl=None, version=7,
    )
    pool.buffers["ephemeral"] = BufferEntry(
        value={"secret-temp-value": True}, updated_at=2.0,
        updated_by="client-b", persistent=False, ttl=None, version=1,
    )
    pool.clients["private-client"] = object()
    pool.subscriptions["ephemeral"] = {"private-client"}
    pool.processes["private-process"] = object()
    expected = deepcopy(pool.snapshot())

    store.save_pool(pool)
    raw = store._path_for_pool("demo").read_text(encoding="utf-8")
    assert json.loads(raw) == expected
    assert "secret-temp-value" not in raw
    assert "ephemeral" not in raw
    assert "private-client" not in raw
    assert "private-process" not in raw
    assert "expires_at" not in raw
    assert "size_bytes" not in raw
    assert SnapshotStore(tmp_path).load_pools() == {"demo": expected}
    assert store.health["writes"] == 1
    assert store.health["healthy"]


@pytest.mark.parametrize("ttl", [None, 0, 0.0, 0.0001, 600])
def test_snapshot_preserves_valid_ttl_metadata(tmp_path, ttl):
    store = SnapshotStore(tmp_path)
    pool = make_pool()
    pool.buffers["persisted"].ttl = ttl
    store.save_pool(pool)
    payload = raw_snapshot(store, "demo")["buffers"]["persisted"]
    assert payload == pool.buffers["persisted"].to_dict()
    assert store.load_pools()["demo"]["buffers"]["persisted"] == payload


def test_synchronous_save_captures_deep_owned_snapshot(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path)
    pool = make_pool()
    expected = deepcopy(pool.snapshot())
    write = store._write_file

    def mutate_live_pool(snapshot):
        assert isinstance(snapshot, dict)
        assert snapshot["buffers"]["persisted"]["value"] is not pool.buffers["persisted"].value
        pool.buffers["persisted"].value["nested"].append("not captured")
        pool.buffers.clear()
        return write(snapshot)

    monkeypatch.setattr(store, "_write_file", mutate_live_pool)
    store.save_pool(pool)
    assert raw_snapshot(store, "demo") == expected


def test_ephemeral_only_pool_creates_no_file(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = PoolState(pool_id="ephemeral-only")
    pool.buffers["temp"] = BufferEntry(
        value={"temp": True}, updated_at=1.0, updated_by="client", persistent=False,
    )
    store.save_pool(pool)
    assert list(tmp_path.iterdir()) == []
    assert store.load_pools() == {}
    assert store.health["skipped_writes"] == 1


def test_unchanged_persistent_snapshot_does_not_rewrite_file(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path)
    pool = make_pool()
    store.save_pool(pool)
    path = store._path_for_pool("demo")
    raw = path.read_bytes()
    inode_and_time = (path.stat().st_ino, path.stat().st_mtime_ns)

    def unexpected_tempfile(*args, **kwargs):
        raise AssertionError("Unchanged snapshot should not create a temp file")

    monkeypatch.setattr("latzero_server.persistence.tempfile.NamedTemporaryFile", unexpected_tempfile)
    pool.buffers["ephemeral"] = BufferEntry(value="temporary", updated_at=2.0, updated_by="client")
    store.save_pool(pool)
    assert path.read_bytes() == raw
    assert (path.stat().st_ino, path.stat().st_mtime_ns) == inode_and_time
    assert store.health["writes"] == 1
    assert store.health["skipped_writes"] == 1


def test_pool_filenames_hash_exact_utf8_identity(tmp_path):
    store = SnapshotStore(tmp_path)
    pool_ids = ["A", "a", "a/b", "a_b", "\u00e9", "e\u0301", "../", "CON"]
    expected = {}
    for pool_id in pool_ids:
        pool = make_pool(pool_id, value=pool_id)
        store.save_pool(pool)
        expected[pool_id] = pool.snapshot()
        digest = hashlib.sha256(pool_id.encode("utf-8")).hexdigest()
        assert store._path_for_pool(pool_id).name == "pool-{}.json".format(digest)
    paths = list(tmp_path.glob("*.json"))
    assert len(paths) == len(pool_ids)
    assert len({path.name.lower() for path in paths}) == len(pool_ids)
    assert SnapshotStore(tmp_path).load_pools() == expected
    store.delete_pool("a/b")
    expected.pop("a/b")
    assert SnapshotStore(tmp_path).load_pools() == expected


@pytest.mark.parametrize("pool_id", ["legacy/demo", "z-pool"])
def test_actual_legacy_fallback_and_canonical_precedence(tmp_path, pool_id):
    store = SnapshotStore(tmp_path)
    pool = make_pool(pool_id, value="old")
    legacy = write_legacy(store, pool)
    backup = legacy.read_bytes()
    assert SnapshotStore(tmp_path).load_pools() == {pool.pool_id: pool.snapshot()}
    pool.buffers["persisted"].value = "new"
    pool.buffers["persisted"].version += 1
    store.save_pool(pool)
    assert legacy.read_bytes() == backup
    assert SnapshotStore(tmp_path).load_pools() == {pool.pool_id: pool.snapshot()}
    legacy.write_bytes(backup)
    assert SnapshotStore(tmp_path).load_pools()[pool.pool_id]["buffers"]["persisted"]["value"] == "new"


def test_legacy_auth_only_pool_remains_loadable(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = PoolState(pool_id="auth", auth_required=True, auth_token_hash="hash")
    legacy = write_legacy(store, pool)
    raw = legacy.read_bytes()
    assert store.load_pools() == {"auth": pool.snapshot()}
    store.save_pool(pool)
    assert legacy.read_bytes() == raw
    assert SnapshotStore(tmp_path).load_pools() == {"auth": pool.snapshot()}


@pytest.mark.parametrize("delete", [False, True])
def test_empty_canonical_tombstone_prevents_legacy_resurrection(tmp_path, delete):
    store = SnapshotStore(tmp_path)
    pool = make_pool("legacy")
    legacy = write_legacy(store, pool)
    backup = legacy.read_bytes()
    if delete:
        store.delete_pool(pool.pool_id)
    else:
        pool.auth_required = False
        pool.auth_token_hash = None
        pool.buffers.clear()
        store.save_pool(pool)
    assert legacy.read_bytes() == backup
    assert raw_snapshot(store, pool.pool_id) == PoolState(pool_id=pool.pool_id).snapshot()
    assert SnapshotStore(tmp_path).load_pools() == {}


@pytest.mark.parametrize("pool_id,other_id", [("a_b", "a/b"), ("a", "A")])
def test_colliding_legacy_is_preserved_not_deleted_or_claimed(tmp_path, pool_id, other_id):
    store = SnapshotStore(tmp_path)
    other = make_pool(other_id, value="must survive")
    legacy = write_legacy(store, other)
    backup = legacy.read_bytes()
    store.save_pool(PoolState(pool_id=pool_id))
    store.delete_pool(pool_id)
    assert not store._path_for_pool(pool_id).exists()
    assert legacy.read_bytes() == backup
    assert SnapshotStore(tmp_path).load_pools() == {other_id: other.snapshot()}
    pool = make_pool(pool_id, value="independent")
    store.save_pool(pool)
    assert legacy.read_bytes() == backup
    assert SnapshotStore(tmp_path).load_pools() == {
        other_id: other.snapshot(), pool_id: pool.snapshot(),
    }


def test_invalid_legacy_file_is_preserved(tmp_path):
    store = SnapshotStore(tmp_path)
    legacy = store._legacy_path_for_pool("demo")
    legacy.write_bytes(b'{"pool_id":"demo","buffers":"corrupt"}')
    backup = legacy.read_bytes()
    store.save_pool(PoolState(pool_id="demo"))
    assert legacy.read_bytes() == backup
    assert not store._path_for_pool("demo").exists()
    assert store.load_pools() == {}
    assert store.health["load_errors"] == 1
    assert not store.health["healthy"]
    assert store.health["error"]


def test_persistent_replaced_by_ephemeral_does_not_restore_either_value(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = PoolState(pool_id="demo")
    pool.buffers["value"] = BufferEntry(
        value="persisted", updated_at=1.0, updated_by="client", persistent=True,
    )
    store.save_pool(pool)
    pool.buffers["value"] = BufferEntry(
        value="ephemeral replacement", updated_at=2.0, updated_by="client",
        persistent=False, version=2,
    )
    store.save_pool(pool)
    assert raw_snapshot(store, pool.pool_id) == PoolState(pool_id=pool.pool_id).snapshot()
    assert SnapshotStore(tmp_path).load_pools() == {}


def test_empty_snapshot_does_not_hide_legacy_read_failure(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path)
    legacy = write_legacy(store, make_pool())
    raw = legacy.read_bytes()
    read_bytes = type(legacy).read_bytes

    def denied_read(path):
        if path == legacy:
            raise PermissionError("injected legacy read failure")
        return read_bytes(path)

    monkeypatch.setattr(type(legacy), "read_bytes", denied_read)
    with pytest.raises(PermissionError, match="legacy read failure"):
        store.save_pool(PoolState(pool_id="demo"))
    assert not store.health["healthy"]
    assert not store._path_for_pool("demo").exists()
    monkeypatch.setattr(type(legacy), "read_bytes", read_bytes)
    assert legacy.read_bytes() == raw


def test_invalid_canonical_falls_back_without_overwriting_unverified_data(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = make_pool()
    legacy = write_legacy(store, pool)
    canonical = store._path_for_pool(pool.pool_id)
    canonical.write_bytes(b'{"broken":')
    assert store.load_pools() == {pool.pool_id: pool.snapshot()}
    with pytest.raises(ValueError):
        store.save_pool(pool)
    assert canonical.read_bytes() == b'{"broken":'
    assert json.loads(legacy.read_text(encoding="utf-8")) == pool.snapshot()
    assert not list(tmp_path.glob("*.tmp"))


def test_mismatched_canonical_identity_cannot_destroy_another_pool(tmp_path):
    store = SnapshotStore(tmp_path)
    other = make_pool("other")
    canonical = store._path_for_pool("demo")
    canonical.write_text(json.dumps(other.snapshot()), encoding="utf-8")
    raw = canonical.read_bytes()
    assert store.load_pools() == {}
    with pytest.raises(ValueError, match="different pool"):
        store.delete_pool("demo")
    assert canonical.read_bytes() == raw


def test_canonical_filename_namespace_collision_preserves_real_legacy_pool(tmp_path):
    store = SnapshotStore(tmp_path)
    collision_id = store._path_for_pool("demo").stem
    other = make_pool(collision_id, value="legacy owner")
    legacy = write_legacy(store, other)
    assert legacy == store._path_for_pool("demo")
    raw = legacy.read_bytes()
    assert store.load_pools() == {collision_id: other.snapshot()}
    with pytest.raises(ValueError, match="different pool"):
        store.save_pool(make_pool("demo"))
    with pytest.raises(ValueError, match="different pool"):
        store.delete_pool("demo")
    assert legacy.read_bytes() == raw
    store.save_pool(other)
    assert legacy.read_bytes() == raw
    assert store.load_pools() == {collision_id: other.snapshot()}


def test_synchronous_delete_clears_pending_pool_reference(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = make_pool()
    store.save_pool(pool)
    pool.buffers["persisted"].value = "pending newer data"
    store.enqueue(pool)
    store.delete_pool(pool.pool_id)
    assert store.health["dirty_pools"] == 0
    assert SnapshotStore(tmp_path).load_pools() == {}


@pytest.mark.parametrize("invalid", [None, [], "not a snapshot", 2, True])
def test_loader_rejects_nonobject_snapshots(tmp_path, invalid):
    path = tmp_path / "demo.json"
    path.write_text(json.dumps(invalid), encoding="utf-8")
    store = SnapshotStore(tmp_path)
    assert store.load_pools() == {}
    assert store.health["load_errors"] == 1
    assert path.exists()


@pytest.mark.parametrize("field,invalid", [
    ("pool_id", []), ("pool_id", ""), ("auth_required", 1),
    ("auth_required", "false"), ("auth_token_hash", []),
    ("auth_token_hash", None), ("auth_token_hash", ""), ("buffers", []),
])
def test_loader_rejects_invalid_snapshot_metadata(tmp_path, field, invalid):
    data = make_pool().snapshot()
    data[field] = invalid
    path = tmp_path / "demo.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert SnapshotStore(tmp_path).load_pools() == {}
    assert path.exists()


@pytest.mark.parametrize("field,invalid", [
    ("persistent", False), ("persistent", 1), ("persistent", "true"),
    ("version", 0), ("version", -1), ("version", 1.25), ("version", True),
    ("updated_by", []), ("updated_by", None),
    ("updated_at", None), ("updated_at", -1), ("updated_at", True),
    ("updated_at", float("nan")), ("updated_at", float("inf")),
    ("ttl", -1), ("ttl", True), ("ttl", "1"),
    ("ttl", float("nan")), ("ttl", float("inf")), ("ttl", float("-inf")),
    ("value", {"nonfinite": float("nan")}),
])
def test_loader_rejects_invalid_buffer_metadata_and_content(tmp_path, field, invalid):
    data = make_pool().snapshot()
    data["buffers"]["persisted"][field] = invalid
    path = tmp_path / "demo.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert SnapshotStore(tmp_path).load_pools() == {}
    assert path.exists()


def test_loader_rejects_nonfinite_expiry_sum(tmp_path):
    pool = make_pool()
    pool.buffers["persisted"].updated_at = 1e308
    pool.buffers["persisted"].ttl = 1e308
    store = SnapshotStore(tmp_path)
    write_legacy(store, pool)
    assert store.load_pools() == {}
    with pytest.raises(ValueError, match="expiry"):
        store.save_pool(pool)


@pytest.mark.parametrize("buffers", [{"": {}}, {"invalid": None}, {"invalid": []}])
def test_loader_rejects_invalid_buffer_mapping(tmp_path, buffers):
    data = make_pool().snapshot()
    data["buffers"] = buffers
    (tmp_path / "demo.json").write_text(json.dumps(data), encoding="utf-8")
    assert SnapshotStore(tmp_path).load_pools() == {}


def test_loader_rejects_missing_value_and_duplicate_json_keys(tmp_path):
    data = make_pool().snapshot()
    del data["buffers"]["persisted"]["value"]
    path = tmp_path / "demo.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert SnapshotStore(tmp_path).load_pools() == {}
    path.write_bytes(b'{"pool_id":"demo","pool_id":"other","buffers":{}}')
    assert SnapshotStore(tmp_path).load_pools() == {}


def test_loader_does_not_claim_unrelated_json_files(tmp_path):
    data = make_pool().snapshot()
    path = tmp_path / "unrelated.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert SnapshotStore(tmp_path).load_pools() == {}
    assert path.exists()


@pytest.mark.parametrize("option,value", [
    ("batch_window", -1), ("batch_window", True), ("batch_window", float("nan")),
    ("max_dirty_pools", 0), ("max_dirty_pools", True), ("max_dirty_pools", 1.5),
    ("retry_initial_delay", 0), ("retry_initial_delay", float("inf")),
    ("retry_max_delay", 0), ("shutdown_timeout", 0),
    ("shutdown_timeout", float("inf")), ("shutdown_max_retries", 0),
])
def test_constructor_rejects_invalid_bounds(tmp_path, option, value):
    with pytest.raises(ValueError):
        SnapshotStore(tmp_path, **{option: value})


def test_constructor_rejects_inverted_retry_bounds(tmp_path):
    with pytest.raises(ValueError):
        SnapshotStore(tmp_path, retry_initial_delay=2, retry_max_delay=1)
