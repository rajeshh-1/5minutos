from __future__ import annotations

import json
from pathlib import Path

from src.core.config import StrategyConfig
from src.runtime.checkpoint_store import CheckpointStore
from src.runtime.paper_live_engine import PaperLiveEngine, SESSION_AGE_TIMEOUT


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")


def _cfg() -> StrategyConfig:
    return StrategyConfig(
        execution_mode="paper",
        market_scope="SOL5M",
        stake_usd_per_leg=1.0,
        target_profit_pct=3.0,
        target_mode="net",
        estimated_costs=0.01,
        max_trades_per_market=1,
        max_open_positions=1,
        entry_cutoff_sec=20,
        leg2_timeout_ms=120000,
        max_unwind_loss_bps=5000.0,
    )


def test_input_path_dedup_prefers_sol5m_dir(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    prices_root = raw_dir / "prices_2026-03-25.jsonl"
    prices_sol = raw_dir / "sol5m" / "prices_2026-03-25.jsonl"
    _append_jsonl(
        prices_root,
        [{"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_DUP", "yes_ask": 0.40, "no_ask": 0.60}],
    )
    _append_jsonl(
        prices_sol,
        [{"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_DUP", "yes_ask": 0.41, "no_ask": 0.59}],
    )
    out_dir = tmp_path / "reports" / "live"

    engine = PaperLiveEngine(config=_cfg(), raw_dir=raw_dir, out_dir=out_dir, interval_sec=0.1, seed=42)
    snap = engine.run_one_cycle()

    assert snap["rows_read_cycle"]["prices"] == 1
    assert "SOL5M_DUP" in engine.market_sessions


def test_trade_log_buffer_flushes_to_disk(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    prices = raw_dir / "sol5m" / "prices_2026-03-25.jsonl"
    _append_jsonl(
        prices,
        [
            {"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_BUF", "yes_ask": 0.40, "no_ask": 0.60},
            {"timestamp_utc": "2026-03-25T00:00:02Z", "market_key": "SOL5M_BUF", "yes_ask": 0.40, "no_ask": 0.60},
            {"timestamp_utc": "2026-03-25T00:00:03Z", "market_key": "SOL5M_BUF", "yes_ask": 0.39, "no_ask": 0.61},
        ],
    )

    out_dir = tmp_path / "reports" / "live"
    engine = PaperLiveEngine(
        config=_cfg(),
        raw_dir=raw_dir,
        out_dir=out_dir,
        interval_sec=0.1,
        seed=42,
        max_log_buffer=1,
    )
    engine.run_one_cycle()
    engine.shutdown()

    trade_log_path = out_dir / "trade_log_live.csv"
    assert trade_log_path.exists()
    assert trade_log_path.read_text(encoding="utf-8").count("\n") >= 2
    assert len(engine.trade_log_rows) == 0


def test_session_age_timeout_generates_explicit_reason_code(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    prices = raw_dir / "sol5m" / "prices_2026-03-25.jsonl"
    _append_jsonl(
        prices,
        [
            {"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_AGE", "yes_ask": 0.40, "no_ask": 0.70},
            {"timestamp_utc": "2026-03-25T00:00:04Z", "market_key": "SOL5M_AGE", "yes_ask": 0.45, "no_ask": 0.70},
        ],
    )
    out_dir = tmp_path / "reports" / "live"
    checkpoint_file = out_dir / "checkpoints.json"

    engine = PaperLiveEngine(
        config=_cfg(),
        raw_dir=raw_dir,
        out_dir=out_dir,
        interval_sec=0.1,
        seed=42,
        checkpoint_store=CheckpointStore(checkpoint_file),
        use_checkpoint=True,
        max_session_age_sec=1,
    )
    snap = engine.run_one_cycle()
    engine.shutdown()

    assert snap["metrics"]["unwind_count"] >= 1
    assert engine.reason_counts[SESSION_AGE_TIMEOUT] >= 1
    assert snap["metrics"]["timeout_rate"] >= 0.0


def test_checkpoint_corruption_fallback_returns_empty_offsets(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "reports" / "live" / "checkpoints.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text('{"version":1,"offsets":{"a.jsonl":10},"checksum":"broken"}', encoding="utf-8")
    store = CheckpointStore(checkpoint_path)
    assert store.load() == {}
