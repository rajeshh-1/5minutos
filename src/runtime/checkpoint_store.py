from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


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
            out: dict[str, int] = {}
            for key, value in payload.items():
                try:
                    out[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue
            return out

    def save(self, offsets: dict[str, int]) -> None:
        with self._lock():
            payload = {str(k): int(v) for k, v in offsets.items()}
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
            os.replace(str(tmp_path), str(self.path))

