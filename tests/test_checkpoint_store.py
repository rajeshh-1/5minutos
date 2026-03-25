from __future__ import annotations

from pathlib import Path

import pytest

from src.runtime.checkpoint_store import CheckpointStore


def test_checkpoint_save_and_load(tmp_path: Path) -> None:
    path = tmp_path / "reports" / "live" / "checkpoints.json"
    store = CheckpointStore(path)
    payload = {"a.jsonl": 10, "b.jsonl": 42}
    store.save(payload)
    loaded = store.load()
    assert loaded == payload


def test_checkpoint_atomic_write_replaces_file(tmp_path: Path) -> None:
    path = tmp_path / "checkpoints.json"
    store = CheckpointStore(path)
    store.save({"x": 1})
    store.save({"x": 2, "y": 3})
    loaded = store.load()
    assert loaded == {"x": 2, "y": 3}
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_checkpoint_lock_timeout(tmp_path: Path) -> None:
    path = tmp_path / "checkpoints.json"
    store = CheckpointStore(path, lock_timeout_sec=0.05)
    lock_file = path.with_suffix(path.suffix + ".lock")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_file.write_text("busy", encoding="utf-8")
    with pytest.raises(TimeoutError):
        store.save({"x": 1})

