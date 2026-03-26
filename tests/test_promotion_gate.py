from __future__ import annotations

import json
from pathlib import Path

from src.runtime.promotion_gate import HOLD, PROMOTE, PromotionThresholds, evaluate_promotion_gate


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def test_promotion_gate_promote_happy_path(tmp_path: Path) -> None:
    stability = tmp_path / "stability_metrics.json"
    top_configs = tmp_path / "top_configs.json"
    paper = tmp_path / "paper_readiness.json"

    _write_json(stability, {"rows_total": 10, "rows_go": 4, "rows_no_go": 6})
    _write_json(
        top_configs,
        [
            {
                "config_id": "cfg_a",
                "pnl_per_trade_mean": 0.02,
                "hedge_failed_rate_mean": 0.01,
                "max_drawdown_pct_mean": 5.0,
                "p99_loss_mean": 0.2,
                "pnl_per_trade_std": 0.005,
                "hedge_success_rate_std": 0.03,
            }
        ],
    )
    _write_json(paper, {"status": "GO", "metrics": {"pnl_per_trade": 0.02}, "critical_errors": []})

    result = evaluate_promotion_gate(
        stability_metrics_file=stability,
        top_configs_file=top_configs,
        paper_readiness_file=paper,
        thresholds=PromotionThresholds(),
    )
    assert result["status"] == PROMOTE
    assert result["blockers"] == []


def test_promotion_gate_hold_with_blockers(tmp_path: Path) -> None:
    stability = tmp_path / "stability_metrics.json"
    top_configs = tmp_path / "top_configs.json"
    paper = tmp_path / "paper_readiness.json"

    _write_json(stability, {"rows_total": 8, "rows_go": 0, "rows_no_go": 8})
    _write_json(
        top_configs,
        [
            {
                "config_id": "cfg_bad",
                "pnl_per_trade_mean": -0.01,
                "hedge_failed_rate_mean": 0.20,
                "max_drawdown_pct_mean": 20.0,
                "p99_loss_mean": 0.7,
                "pnl_per_trade_std": 0.03,
                "hedge_success_rate_std": 0.2,
            }
        ],
    )
    _write_json(paper, {"status": "NO_GO", "metrics": {"pnl_per_trade": -0.1}, "critical_errors": ["runtime_parse_error"]})

    result = evaluate_promotion_gate(
        stability_metrics_file=stability,
        top_configs_file=top_configs,
        paper_readiness_file=paper,
        thresholds=PromotionThresholds(),
    )
    assert result["status"] == HOLD
    assert any("stability_rows_go_is_zero" in blocker for blocker in result["blockers"])
    assert any("paper_readiness_not_go" in blocker for blocker in result["blockers"])
    assert any("hedge_failed_rate" in blocker for blocker in result["blockers"])
