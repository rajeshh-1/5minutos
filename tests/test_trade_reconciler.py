from __future__ import annotations

from src.io.trade_reconciler import dedupe_trade_rows, reconcile_trade_sets


def test_dedupe_trade_rows_by_trade_id() -> None:
    rows = [
        {"trade_id": "t1", "market_key": "SOL5M_X"},
        {"trade_id": "t1", "market_key": "SOL5M_X"},
        {"id": "t2", "market_key": "SOL5M_X"},
        {"transactionHash": "0xaaa", "asset": "assetA", "timestamp": 1, "market_key": "SOL5M_X"},
        {"transactionHash": "0xaaa", "asset": "assetA", "timestamp": 1, "market_key": "SOL5M_X"},
    ]
    deduped = dedupe_trade_rows(rows)
    ids = sorted(str(row["trade_id"]) for row in deduped)
    assert ids == ["0xaaa:assetA:1", "t1", "t2"]


def test_reconcile_trade_sets_coverage_and_missing() -> None:
    realtime = [
        {"trade_id": "a", "market_key": "SOL5M_A"},
        {"trade_id": "b", "market_key": "SOL5M_A"},
        {"trade_id": "b", "market_key": "SOL5M_A"},
    ]
    backfill = [
        {"trade_id": "a", "market_key": "SOL5M_A"},
        {"trade_id": "c", "market_key": "SOL5M_A"},
        {"trade_id": "c", "market_key": "SOL5M_A"},
    ]
    report = reconcile_trade_sets(realtime_rows=realtime, backfill_rows=backfill, market_key="SOL5M_A")
    assert report["realtime_count"] == 2
    assert report["backfill_count"] == 2
    assert report["overlap_count"] == 1
    assert report["missing_in_realtime"] == ["c"]
    assert report["coverage_pct"] == 50.0

