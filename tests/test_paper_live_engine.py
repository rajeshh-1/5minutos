from __future__ import annotations

import json
from pathlib import Path

from src.core.config import StrategyConfig
from src.runtime.checkpoint_store import CheckpointStore
from src.runtime.paper_live_engine import PaperLiveEngine


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
        entry_cutoff_sec=60,
        leg2_timeout_ms=1200,
        max_unwind_loss_bps=50.0,
    )


def test_paper_live_engine_incremental_cycles_and_outputs(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    prices_file = raw_dir / "sol5m" / "prices_2026-03-25.jsonl"
    _append_jsonl(
        prices_file,
        [
            {"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_A", "yes_ask": 0.40, "no_ask": 0.60},
            {"timestamp_utc": "2026-03-25T00:00:02Z", "market_key": "SOL5M_A", "yes_ask": 0.40, "no_ask": 0.62},
        ],
    )

    out_dir = tmp_path / "reports" / "live"
    checkpoint_path = out_dir / "checkpoints.json"
    engine = PaperLiveEngine(
        config=_cfg(),
        raw_dir=raw_dir,
        out_dir=out_dir,
        interval_sec=0.1,
        seed=42,
        checkpoint_store=CheckpointStore(checkpoint_path),
        use_checkpoint=True,
    )

    snap1 = engine.run_one_cycle()
    assert snap1["rows_read_cycle"]["prices"] == 2
    assert (out_dir / "metrics_live.json").exists()
    assert (out_dir / "events.log").exists()
    assert (out_dir / "top_policies_live.csv").exists()
    assert checkpoint_path.exists()

    _append_jsonl(
        prices_file,
        [{"timestamp_utc": "2026-03-25T00:00:03Z", "market_key": "SOL5M_A", "yes_ask": 0.39, "no_ask": 0.50}],
    )
    snap2 = engine.run_one_cycle()
    assert snap2["rows_read_cycle"]["prices"] == 1
    assert "hedge_success_rate" in snap2["metrics"]
    assert "p99_loss" in snap2["metrics"]


def test_paper_live_engine_resumes_from_checkpoint(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    prices_file = raw_dir / "sol5m" / "prices_2026-03-25.jsonl"
    _append_jsonl(
        prices_file,
        [
            {"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_A", "yes_ask": 0.40, "no_ask": 0.60},
            {"timestamp_utc": "2026-03-25T00:00:02Z", "market_key": "SOL5M_A", "yes_ask": 0.40, "no_ask": 0.62},
        ],
    )
    out_dir = tmp_path / "reports" / "live"
    checkpoint_path = out_dir / "checkpoints.json"

    engine_a = PaperLiveEngine(
        config=_cfg(),
        raw_dir=raw_dir,
        out_dir=out_dir,
        interval_sec=0.1,
        seed=42,
        checkpoint_store=CheckpointStore(checkpoint_path),
        use_checkpoint=True,
    )
    snap_a = engine_a.run_one_cycle()
    assert snap_a["rows_read_cycle"]["prices"] == 2

    _append_jsonl(
        prices_file,
        [{"timestamp_utc": "2026-03-25T00:00:03Z", "market_key": "SOL5M_A", "yes_ask": 0.39, "no_ask": 0.50}],
    )

    engine_b = PaperLiveEngine(
        config=_cfg(),
        raw_dir=raw_dir,
        out_dir=out_dir,
        interval_sec=0.1,
        seed=42,
        checkpoint_store=CheckpointStore(checkpoint_path),
        use_checkpoint=True,
    )
    snap_b = engine_b.run_one_cycle()
    assert snap_b["rows_read_cycle"]["prices"] == 1

