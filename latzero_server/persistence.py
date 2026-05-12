"""
Snapshot persistence for latzero-server pools.
"""

import json
from pathlib import Path
from typing import Dict

from .models import PoolState


class SnapshotStore:
    """Persists pool metadata and persistent buffers to JSON files."""

    def __init__(self, data_dir: Path):
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)

    def _path_for_pool(self, pool_id: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in pool_id)
        return self._data_dir / f"{safe}.json"

    def load_pools(self) -> Dict[str, dict]:
        """Load all persisted pool snapshots."""
        loaded: Dict[str, dict] = {}
        for path in self._data_dir.glob("*.json"):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
                pool_id = data.get("pool_id")
                if pool_id:
                    loaded[pool_id] = data
            except Exception:
                continue
        return loaded

    def save_pool(self, pool: PoolState) -> None:
        """Persist one pool snapshot."""
        path = self._path_for_pool(pool.pool_id)
        snapshot = pool.snapshot()
        if not snapshot["buffers"] and not snapshot["auth_required"]:
            if path.exists():
                path.unlink()
            return
        with path.open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2, sort_keys=True)

    def delete_pool(self, pool_id: str) -> None:
        """Remove a persisted pool snapshot."""
        path = self._path_for_pool(pool_id)
        if path.exists():
            path.unlink()
