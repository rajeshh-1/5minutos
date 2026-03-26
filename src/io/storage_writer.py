from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def iso_utc(dt: datetime | None = None) -> str:
    now = dt or datetime.now(timezone.utc)
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_day(dt: datetime | None = None) -> str:
    now = dt or datetime.now(timezone.utc)
    return now.astimezone(timezone.utc).strftime("%Y-%m-%d")


@dataclass
class MetadataState:
    rows_prices: int = 0
    rows_orderbook: int = 0
    rows_trades: int = 0
    discarded_rows: int = 0
    errors_count: int = 0
    warnings: list[str] = field(default_factory=list)
    discard_reasons: dict[str, int] = field(default_factory=dict)


class JsonlStorageWriter:
    def __init__(
        self,
        *,
        raw_dir: str | Path,
        market_folder: str,
        source: str,
        started_at_utc: str,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.market_folder = str(market_folder).strip().lower()
        self.source = str(source).strip()
        self.started_at_utc = str(started_at_utc)
        self.state = MetadataState()

    def _folder(self) -> Path:
        folder = self.raw_dir / self.market_folder
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def data_paths(self, *, now_utc: datetime) -> dict[str, Path]:
        day = utc_day(now_utc)
        folder = self._folder()
        return {
            "prices": folder / f"prices_{day}.jsonl",
            "orderbook": folder / f"orderbook_{day}.jsonl",
            "trades": folder / f"trades_{day}.jsonl",
            "metadata": folder / f"metadata_{day}.json",
        }

    def ensure_daily_files(self, *, now_utc: datetime) -> dict[str, Path]:
        paths = self.data_paths(now_utc=now_utc)
        for key in ("prices", "orderbook", "trades"):
            path = paths[key]
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
        self.write_metadata(now_utc=now_utc)
        return paths

    def append_row(self, *, kind: str, row: dict[str, Any], now_utc: datetime) -> None:
        if kind not in {"prices", "orderbook", "trades"}:
            raise ValueError(f"unknown kind: {kind}")
        paths = self.data_paths(now_utc=now_utc)
        payload = dict(row)
        payload.setdefault("source", self.source)
        with paths[kind].open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
        if kind == "prices":
            self.state.rows_prices += 1
        elif kind == "orderbook":
            self.state.rows_orderbook += 1
        else:
            self.state.rows_trades += 1

    def add_warning(self, warning: str) -> None:
        msg = str(warning).strip()
        if not msg:
            return
        if msg not in self.state.warnings:
            self.state.warnings.append(msg)

    def add_error(self) -> None:
        self.state.errors_count += 1

    def register_discard(self, reason: str) -> None:
        self.state.discarded_rows += 1
        key = str(reason or "unknown_discard").strip() or "unknown_discard"
        self.state.discard_reasons[key] = int(self.state.discard_reasons.get(key, 0)) + 1

    def write_metadata(self, *, now_utc: datetime) -> Path:
        paths = self.data_paths(now_utc=now_utc)
        uptime = max(
            0,
            int(
                (
                    now_utc.astimezone(timezone.utc)
                    - datetime.fromisoformat(self.started_at_utc.replace("Z", "+00:00"))
                ).total_seconds()
            ),
        )
        payload = {
            "market_folder": self.market_folder,
            "source": self.source,
            "rows_prices": int(self.state.rows_prices),
            "rows_orderbook": int(self.state.rows_orderbook),
            "rows_trades": int(self.state.rows_trades),
            "discarded_rows": int(self.state.discarded_rows),
            "discard_reasons": dict(sorted(self.state.discard_reasons.items())),
            "errors_count": int(self.state.errors_count),
            "warnings": list(self.state.warnings),
            "started_at_utc": self.started_at_utc,
            "last_update_utc": iso_utc(now_utc),
            "collector_uptime_sec": int(uptime),
        }
        with paths["metadata"].open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=True, indent=2)
        return paths["metadata"]
