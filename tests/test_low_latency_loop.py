from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from scripts.run_live_mvp import LiveMVPConfig, LiveMVPRunner


def _build_runner(tmp_path: Path) -> LiveMVPRunner:
    cfg = LiveMVPConfig.from_dict(
        {
            "market_scope": "SOL5M",
            "target_profit_pct": 1.0,
            "target_profit_max_pct": 2.0,
            "poll_interval_sec": 0.01,
            "api_top_interval_ms": 100,
            "api_book_interval_ms": 250,
            "api_trades_interval_ms": 400,
            "api_request_timeout_ms": 350,
            "api_request_retries": 0,
            "api_request_backoff_ms": 0,
            "api_parallel_workers": 2,
            "api_max_markets_per_cycle": 2,
            "api_max_data_freshness_ms": 1500,
            "api_skip_unchanged_book": True,
            "api_backoff_on_error": True,
            "api_max_rps_guard": 25.0,
            "raw_dir": str(tmp_path / "raw"),
            "report_file": str(tmp_path / "report.csv"),
        }
    )
    return LiveMVPRunner(
        cfg=cfg,
        runtime_sec=0,
        raw_dir_override=tmp_path / "raw",
        report_file_override=tmp_path / "report.csv",
        market_source="file",
        dry_run=True,
        enable_live=False,
        heartbeat_sec=1.0,
    )


def test_endpoint_backoff_grows_and_recovers(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    runner._mark_endpoint_outcome(endpoint="book", ok=False, error_message="status_code=429")
    first = int(runner._api_endpoint_backoff_ms["book"])
    assert first > 0

    runner._mark_endpoint_outcome(endpoint="book", ok=False, error_message="timeout")
    second = int(runner._api_endpoint_backoff_ms["book"])
    assert second >= first

    runner._mark_endpoint_outcome(endpoint="book", ok=True)
    recovered = int(runner._api_endpoint_backoff_ms["book"])
    assert recovered <= second


def test_market_scheduler_respects_endpoint_intervals(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    market_key = "BTC5M_2026-03-30T12:10:00Z"
    now_ms = 1_000

    schedule = runner._market_schedule(market_key=market_key, now_ms=now_ms)
    due_at = int(schedule["next_top_ms"])
    assert not runner._is_endpoint_due(market_key=market_key, endpoint="top", now_ms=due_at - 1)
    assert runner._is_endpoint_due(market_key=market_key, endpoint="top", now_ms=due_at)
    runner._schedule_market_endpoint(market_key=market_key, endpoint="top", now_ms=now_ms)
    assert not runner._is_endpoint_due(market_key=market_key, endpoint="top", now_ms=now_ms + 1)

    next_due = int(runner._api_market_schedule_ms[market_key]["next_top_ms"])
    assert runner._is_endpoint_due(market_key=market_key, endpoint="top", now_ms=next_due)


def test_snapshot_unchanged_skips_decision_work(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    snapshot = {
        "market_key": "ETH5M_2026-03-30T12:10:00Z",
        "ts_utc": "2026-03-30T12:07:00Z",
        "feed_source": "rest",
        "yes_ask": 0.48,
        "no_ask": 0.53,
        "sum_ask": 1.01,
        "yes_bid": 0.47,
        "no_bid": 0.52,
        "yes_ask_size": 100.0,
        "no_ask_size": 100.0,
        "yes_spread": 0.01,
        "no_spread": 0.01,
        "seconds_to_close": 240,
    }

    runner._process_snapshot(snapshot)
    logged_after_first = int(runner.actions_logged)
    runner._process_snapshot(snapshot)

    assert int(runner.actions_logged) == logged_after_first
    assert len(runner._api_skip_unchanged_events) >= 1


def test_trade_dedupe_uses_trade_id_and_timestamp(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    rows = [
        {"id": "t1", "timestamp": "1"},
        {"id": "t1", "timestamp": "1"},
        {"id": "t2", "timestamp": "2"},
    ]
    inserted_first = runner._dedupe_trades_rows(market_key="SOL5M_2026-03-30T12:10:00Z", rows=rows)
    inserted_second = runner._dedupe_trades_rows(market_key="SOL5M_2026-03-30T12:10:00Z", rows=rows)

    assert inserted_first == 2
    assert inserted_second == 0


def test_select_markets_prioritizes_pending_and_respects_cap(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    now_utc = datetime(2026, 3, 30, 12, 7, 0, tzinfo=timezone.utc)
    now_ms = int(now_utc.timestamp() * 1000)
    resolved = [
        ("BTC5M_2026-03-30T12:10:00Z", "up1", "down1", "c1"),
        ("ETH5M_2026-03-30T12:10:00Z", "up2", "down2", "c2"),
        ("SOL5M_2026-03-30T12:10:00Z", "up3", "down3", "c3"),
    ]
    for market_key, *_ in resolved:
        runner._api_market_schedule_ms[market_key] = {
            "next_top_ms": now_ms,
            "next_book_ms": now_ms,
            "next_trades_ms": now_ms,
        }
    runner.pending_by_market["SOL5M_2026-03-30T12:10:00Z"] = None  # type: ignore[assignment]

    selected = runner._select_markets_for_cycle(
        resolved_markets=resolved,
        now_utc=now_utc,
        now_ms=now_ms,
    )

    assert len(selected) == 2
    assert any(row[0] == "SOL5M_2026-03-30T12:10:00Z" for row in selected)


def test_stale_snapshot_blocks_new_entry(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    snap = {
        "market_key": "BTC5M_2026-03-30T12:10:00Z",
        "ts_utc": "2026-03-30T12:07:00Z",
        "feed_source": "ws_stale",
        "yes_ask": 0.48,
        "no_ask": 0.49,
        "sum_ask": 0.97,
        "yes_bid": 0.47,
        "no_bid": 0.48,
        "yes_ask_size": 500.0,
        "no_ask_size": 500.0,
        "yes_spread": 0.01,
        "no_spread": 0.01,
        "seconds_to_close": 120,
        "data_age_ms": 5000.0,
    }
    runner._process_snapshot(snap)
    runner._flush_report_buffer_if_needed(force=True)

    report = Path(runner.report_file)
    text = report.read_text(encoding="utf-8")
    assert "blocked_stale_snapshot" in text
