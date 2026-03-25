from __future__ import annotations

import csv
import json
from pathlib import Path

from src.core.config import load_strategy_config
from src.runtime.replay_engine import load_replay_events, run_replay


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["timestamp_utc", "market_key", "up_price", "down_price", "seconds_to_close"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_config(path: Path) -> None:
    payload = {
        "execution_mode": "paper",
        "market_scope": "SOL5M",
        "stake_usd_per_leg": 1.0,
        "target_profit_pct": 3.0,
        "target_mode": "net",
        "estimated_costs": 0.01,
        "max_trades_per_market": 1,
        "max_open_positions": 1,
        "entry_cutoff_sec": 60,
        "leg2_timeout_ms": 1200,
        "max_unwind_loss_bps": 50.0,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _sample_rows() -> list[dict[str, object]]:
    return [
        {"timestamp_utc": "2026-03-25T00:00:01Z", "market_key": "SOL5M_A", "up_price": 0.40, "down_price": 0.60, "seconds_to_close": 180},
        {"timestamp_utc": "2026-03-25T00:00:02Z", "market_key": "SOL5M_A", "up_price": 0.41, "down_price": 0.62, "seconds_to_close": 170},
        {"timestamp_utc": "2026-03-25T00:00:03Z", "market_key": "SOL5M_A", "up_price": 0.39, "down_price": 0.50, "seconds_to_close": 160},
        {"timestamp_utc": "2026-03-25T00:00:04Z", "market_key": "SOL5M_B", "up_price": 0.55, "down_price": 0.45, "seconds_to_close": 180},
        {"timestamp_utc": "2026-03-25T00:00:05Z", "market_key": "SOL5M_B", "up_price": 0.58, "down_price": 0.44, "seconds_to_close": 170},
        {"timestamp_utc": "2026-03-25T00:00:06Z", "market_key": "SOL5M_C", "up_price": 0.40, "down_price": 0.60, "seconds_to_close": 180},
        {"timestamp_utc": "2026-03-25T00:00:07Z", "market_key": "SOL5M_C", "up_price": 0.41, "down_price": 0.70, "seconds_to_close": 170},
        {"timestamp_utc": "2026-03-25T00:00:08Z", "market_key": "SOL5M_C", "up_price": 0.42, "down_price": 0.72, "seconds_to_close": 160},
    ]


def test_replay_engine_processes_small_csv_and_generates_outputs(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    cfg_path = tmp_path / "paper_sol5m_usd1.json"
    out_dir = tmp_path / "reports"
    _write_csv(csv_path, _sample_rows())
    _write_config(cfg_path)

    cfg = load_strategy_config(cfg_path)
    events = load_replay_events(csv_path)
    result = run_replay(events=events, config=cfg, out_dir=out_dir, seed=42, leg1_side="auto")

    assert result.summary_path.exists()
    assert result.trade_log_path.exists()
    assert result.reason_codes_path.exists()

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    for key in (
        "events_processed",
        "trades_attempted",
        "leg1_opened",
        "leg2_hedged",
        "hedge_success_rate",
        "unwind_count",
        "pnl_total",
        "pnl_per_trade",
        "final_state_counts",
        "top_reason_codes",
    ):
        assert key in summary


def test_reason_codes_output_contains_expected_events(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    cfg_path = tmp_path / "paper_sol5m_usd1.json"
    out_dir = tmp_path / "reports"
    _write_csv(csv_path, _sample_rows())
    _write_config(cfg_path)

    cfg = load_strategy_config(cfg_path)
    events = load_replay_events(csv_path)
    run_replay(events=events, config=cfg, out_dir=out_dir, seed=42, leg1_side="auto")

    with (out_dir / "reason_codes.csv").open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    reasons = {row["reason_code"] for row in rows}
    assert "leg1_opened" in reasons
    assert "below_target_profit" in reasons
    assert ("leg2_not_found_timeout" in reasons) or ("unwind_on_timeout" in reasons)


def test_replay_engine_is_deterministic_for_same_seed(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    cfg_path = tmp_path / "paper_sol5m_usd1.json"
    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"
    _write_csv(csv_path, _sample_rows())
    _write_config(cfg_path)

    cfg = load_strategy_config(cfg_path)
    events = load_replay_events(csv_path)
    result_a = run_replay(events=events, config=cfg, out_dir=out_a, seed=99, leg1_side="auto")
    result_b = run_replay(events=events, config=cfg, out_dir=out_b, seed=99, leg1_side="auto")

    assert result_a.summary == result_b.summary
    assert result_a.trade_log_path.read_text(encoding="utf-8") == result_b.trade_log_path.read_text(encoding="utf-8")
    assert result_a.reason_codes_path.read_text(encoding="utf-8") == result_b.reason_codes_path.read_text(encoding="utf-8")

