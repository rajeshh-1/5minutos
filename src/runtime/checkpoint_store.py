from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


CHECKPOINT_VERSION = 1


def _normalize_offsets(raw: object) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        try:
            out[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return out


def _calc_checksum(*, version: int, offsets: dict[str, int]) -> str:
    canonical = {
        "version": int(version),
        "offsets": {k: int(offsets[k]) for k in sorted(offsets)},
    }
    encoded = json.dumps(canonical, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CheckpointStore:
    def __init__(self, path: str | Path, lock_timeout_sec: float = 2.0) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.lock_timeout_sec = float(lock_timeout_sec)

    @contextmanager
    def _lock(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + max(0.1, float(self.lock_timeout_sec))
        while True:
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.close(fd)
                break
            except FileExistsError:
                if time.time() > deadline:
                    raise TimeoutError(f"checkpoint lock timeout: {self.lock_path}")
                time.sleep(0.02)
        try:
            yield
        finally:
            try:
                self.lock_path.unlink(missing_ok=True)
            except Exception:
                pass

    def load(self) -> dict[str, int]:
        with self._lock():
            if not self.path.exists():
                return {}
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"[checkpoint] warning: failed to parse {self.path}: {exc}")
                return {}
            if not isinstance(payload, dict):
                print(f"[checkpoint] warning: invalid checkpoint format in {self.path}")
                return {}
            # Backward compatibility with legacy offset-only format.
            if "offsets" not in payload:
                return _normalize_offsets(payload)

            version = payload.get("version")
            offsets = _normalize_offsets(payload.get("offsets", {}))
            checksum = str(payload.get("checksum", ""))
            if version != CHECKPOINT_VERSION:
                print(
                    f"[checkpoint] warning: unsupported version={version} in {self.path}, expected={CHECKPOINT_VERSION}"
                )
                return {}
            expected_checksum = _calc_checksum(version=int(version), offsets=offsets)
            if checksum != expected_checksum:
                print(f"[checkpoint] warning: checksum mismatch in {self.path}, checkpoint ignored")
                return {}
            return offsets

    def save(self, offsets: dict[str, int]) -> None:
        with self._lock():
            normalized = _normalize_offsets(offsets)
            payload = {
                "version": CHECKPOINT_VERSION,
                "offsets": normalized,
                "checksum": _calc_checksum(version=CHECKPOINT_VERSION, offsets=normalized),
            }
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
            os.replace(str(tmp_path), str(self.path))
