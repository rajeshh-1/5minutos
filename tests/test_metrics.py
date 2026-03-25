from __future__ import annotations

import csv
import json
from pathlib import Path

from src.runtime.metrics import ReadinessThresholds, evaluate_go_no_go, evaluate_readiness_from_files, max_drawdown_pct, quantile


def test_quantile_p95_and_p99() -> None:
    values = [0.0, 0.1, 0.2, 0.3, 0.8]
    assert quantile(values, 0.95) > 0.3
    assert quantile(values, 0.99) > quantile(values, 0.95)


def test_max_drawdown_pct() -> None:
    pnl_series = [0.2, -0.1, -0.2, 0.1]
    dd = max_drawdown_pct(pnl_series, initial_equity=1.0)
    assert dd > 0.0
    assert dd < 100.0


def test_go_no_go_positive_case() -> None:
    metrics = {
        "hedge_failed_rate": 0.01,
        "pnl_per_trade": 0.05,
        "max_drawdown_pct": 5.0,
        "p99_loss": 0.20,
    }
    decision = evaluate_go_no_go(metrics, ReadinessThresholds())
    assert decision.status == "GO"
    assert decision.blockers == []


def test_go_no_go_negative_case_with_blockers() -> None:
    metrics = {
        "hedge_failed_rate": 0.50,
        "pnl_per_trade": -0.01,
        "max_drawdown_pct": 25.0,
        "p99_loss": 1.20,
    }
    decision = evaluate_go_no_go(metrics, ReadinessThresholds())
    assert decision.status == "NO_GO"
    assert len(decision.blockers) == 4


def test_readiness_from_files_emits_expected_blockers(tmp_path: Path) -> None:
    summary_file = tmp_path / "replay_summary.json"
    trade_log_file = tmp_path / "trade_log.csv"
    reason_codes_file = tmp_path / "reason_codes.csv"

    summary_file.write_text(
        json.dumps(
            {
                "events_processed": 4,
                "trades_attempted": 2,
                "leg1_opened": 2,
                "leg2_hedged": 0,
                "unwind_count": 2,
                "pnl_total": -0.1,
                "final_state_counts": {"CLOSED": 2},
                "top_reason_codes": [["below_target_profit", 2]],
            }
        ),
        encoding="utf-8",
    )
    with trade_log_file.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "event_ts",
                "market_key",
                "action",
                "state_from",
                "state_to",
                "p1_price",
                "p2_price",
                "pnl_estimate",
                "reason_code",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "event_ts": "2026-03-25T00:00:01Z",
                "market_key": "SOL5M_A",
                "action": "open_leg1",
                "state_from": "IDLE",
                "state_to": "LEG1_OPEN",
                "p1_price": "0.40",
                "p2_price": "",
                "pnl_estimate": "0.0",
                "reason_code": "leg1_opened",
            }
        )
        writer.writerow(
            {
                "event_ts": "2026-03-25T00:00:02Z",
                "market_key": "SOL5M_A",
                "action": "close_position",
                "state_from": "UNWIND",
                "state_to": "CLOSED",
                "p1_price": "0.40",
                "p2_price": "",
                "pnl_estimate": "-0.05",
                "reason_code": "unwind_on_timeout",
            }
        )
    with reason_codes_file.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["reason_code", "count", "pct"])
        writer.writeheader()
        writer.writerow({"reason_code": "leg1_opened", "count": 2, "pct": 50})
        writer.writerow({"reason_code": "below_target_profit", "count": 2, "pct": 50})

    result = evaluate_readiness_from_files(
        summary_file=summary_file,
        trade_log_file=trade_log_file,
        reason_codes_file=reason_codes_file,
        thresholds=ReadinessThresholds(),
    )
    assert result["status"] == "NO_GO"
    assert any("hedge_failed_rate" in b for b in result["blockers"])
    assert any("pnl_per_trade" in b for b in result["blockers"])

