from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.io.market_data_collector import (
    CollectorConfig,
    EndpointConfig,
    MarketConfig,
    MarketDataCollector,
    RequestConfig,
)


def _base_config() -> CollectorConfig:
    return CollectorConfig(
        source="test-source",
        market_folder="sol5m",
        market=MarketConfig(
            market_key_prefix="SOL5M",
            coin="sol",
            bucket_min=5,
            slug_static="sol-updown-5m-1700000000",
            slug_prefix="sol-updown-5m-",
            labels_up=("up", "yes"),
            labels_down=("down", "no"),
        ),
        endpoints=EndpointConfig(),
        request=RequestConfig(timeout_sec=1.0, retries=0, backoff_sec=0.0),
    )


class FakeCollector(MarketDataCollector):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._fake_trades: list[dict] = []

    def _resolve_market(self, *, slug: str) -> None:
        self.market.slug = slug
        self.market.condition_id = "cond_1"
        self.market.token_up = "token_up"
        self.market.token_down = "token_down"
        self.market.label_up = "Up"
        self.market.label_down = "Down"
        self.market.market_close_utc = "2026-03-25T10:05:00Z"
        self.market.market_key = "SOL5M_2026-03-25T10:05:00Z"

    def _fetch_book(self, token_id: str) -> dict:
        if token_id == "token_up":
            return {
                "bids": [{"price": 0.48, "size": 100}],
                "asks": [{"price": 0.52, "size": 120}],
            }
        return {
            "bids": [{"price": 0.47, "size": 90}],
            "asks": [{"price": 0.53, "size": 110}],
        }

    def _fetch_midpoint(self, token_id: str) -> float | None:
        return 0.50

    def _fetch_trades(self, condition_id: str) -> list[dict]:
        return list(self._fake_trades)


def test_collector_collect_once_writes_prices_orderbook_trades_and_metadata(tmp_path: Path) -> None:
    collector = FakeCollector(
        config=_base_config(),
        raw_dir=tmp_path / "data" / "raw",
        interval_sec=0.1,
        runtime_sec=0,
        book_depth=15,
        trade_limit=150,
        collect_orderbook=True,
        collect_trades=True,
    )
    collector._fake_trades = [
        {
            "timestamp": "2026-03-25T10:00:01Z",
            "id": "t1",
            "asset": "token_up",
            "side": "buy",
            "price": 0.51,
            "size": 12.0,
        }
    ]
    now = datetime(2026, 3, 25, 10, 0, 2, tzinfo=timezone.utc)
    counts = collector.collect_once(now_utc=now)
    assert counts == {"prices": 1, "orderbook": 1, "trades": 1}

    folder = tmp_path / "data" / "raw" / "sol5m"
    assert (folder / "prices_2026-03-25.jsonl").exists()
    assert (folder / "orderbook_2026-03-25.jsonl").exists()
    assert (folder / "trades_2026-03-25.jsonl").exists()
    metadata_path = folder / "metadata_2026-03-25.json"
    assert metadata_path.exists()
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["rows_prices"] == 1
    assert payload["rows_orderbook"] == 1
    assert payload["rows_trades"] == 1
    assert payload["errors_count"] == 0


def test_collector_registers_discarded_rows_reason(tmp_path: Path) -> None:
    collector = FakeCollector(
        config=_base_config(),
        raw_dir=tmp_path / "data" / "raw",
        interval_sec=0.1,
        runtime_sec=0,
        book_depth=15,
        trade_limit=150,
        collect_orderbook=True,
        collect_trades=True,
    )
    collector._fake_trades = [
        {
            "timestamp": "2026-03-25T10:00:01Z",
            "id": "bad",
            "asset": "token_up",
            "side": "buy",
            "price": 1.5,
            "size": 12.0,
        },
        {
            "timestamp": "2026-03-25T10:00:02Z",
            "id": "ok",
            "asset": "token_down",
            "side": "sell",
            "price": 0.49,
            "size": 2.0,
        },
    ]
    now = datetime(2026, 3, 25, 10, 0, 5, tzinfo=timezone.utc)
    counts = collector.collect_once(now_utc=now)
    assert counts["trades"] == 1

    metadata_path = tmp_path / "data" / "raw" / "sol5m" / "metadata_2026-03-25.json"
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["discarded_rows"] >= 1
    assert payload["discard_reasons"].get("trade_price_out_of_range", 0) >= 1


def test_collector_rotates_files_by_utc_date(tmp_path: Path) -> None:
    collector = FakeCollector(
        config=_base_config(),
        raw_dir=tmp_path / "data" / "raw",
        interval_sec=0.1,
        runtime_sec=0,
        book_depth=15,
        trade_limit=150,
        collect_orderbook=False,
        collect_trades=False,
    )
    collector.collect_once(now_utc=datetime(2026, 3, 25, 23, 59, 58, tzinfo=timezone.utc))
    collector.collect_once(now_utc=datetime(2026, 3, 26, 0, 0, 2, tzinfo=timezone.utc))

    folder = tmp_path / "data" / "raw" / "sol5m"
    assert (folder / "prices_2026-03-25.jsonl").exists()
    assert (folder / "prices_2026-03-26.jsonl").exists()
    assert (folder / "metadata_2026-03-25.json").exists()
    assert (folder / "metadata_2026-03-26.json").exists()
