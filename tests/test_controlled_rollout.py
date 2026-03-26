from __future__ import annotations

import json
from pathlib import Path

from scripts.run_controlled_rollout import LIVE_CONFIRMATION_TOKEN, run_controlled_rollout


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _rollout_config() -> dict:
    return {
        "kill_switch_enabled": True,
        "circuit_breaker_enabled": True,
        "thresholds": {
            "hedge_failed_rate_max": 0.02,
            "max_drawdown_pct_max": 12.0,
            "p99_loss_max": 0.40,
        },
    }


def test_live_rollout_blocked_without_confirmation(tmp_path: Path) -> None:
    cfg = tmp_path / "cfg.json"
    gate = tmp_path / "gate.json"
    _write_json(cfg, _rollout_config())
    _write_json(gate, {"status": "PROMOTE", "selected_metrics": {"hedge_failed_rate": 0.01}})

    result = run_controlled_rollout(
        config_file=cfg,
        promotion_gate_file=gate,
        mode="live",
        enable_live=True,
        confirm_live="WRONG_TOKEN",
        runtime_sec=60,
        out_dir=tmp_path / "out",
    )
    assert result["final_status"] == "aborted"
    assert result["live_confirmed"] is False
    assert result["promotion_gate_status"] == "PROMOTE"


def test_live_rollout_blocked_when_gate_is_hold(tmp_path: Path) -> None:
    cfg = tmp_path / "cfg.json"
    gate = tmp_path / "gate.json"
    _write_json(cfg, _rollout_config())
    _write_json(gate, {"status": "HOLD", "selected_metrics": {"hedge_failed_rate": 0.01}})

    result = run_controlled_rollout(
        config_file=cfg,
        promotion_gate_file=gate,
        mode="live",
        enable_live=True,
        confirm_live=LIVE_CONFIRMATION_TOKEN,
        runtime_sec=60,
        out_dir=tmp_path / "out",
    )
    assert result["final_status"] == "aborted"
    assert result["promotion_gate_status"] == "HOLD"


def test_rollout_triggers_rollback_on_threshold_breach(tmp_path: Path) -> None:
    cfg = tmp_path / "cfg.json"
    gate = tmp_path / "gate.json"
    session_metrics = tmp_path / "session_metrics.json"
    _write_json(cfg, _rollout_config())
    _write_json(gate, {"status": "PROMOTE", "selected_metrics": {"hedge_failed_rate": 0.01}})
    _write_json(
        session_metrics,
        {
            "hedge_failed_rate": 0.40,
            "max_drawdown_pct": 5.0,
            "p99_loss": 0.2,
            "pnl_per_trade": 0.02,
        },
    )

    result = run_controlled_rollout(
        config_file=cfg,
        promotion_gate_file=gate,
        mode="paper",
        enable_live=False,
        confirm_live="",
        runtime_sec=60,
        out_dir=tmp_path / "out",
        session_metrics_file=session_metrics,
    )
    assert result["final_status"] == "completed"
    assert result["kill_switch_status"] == "triggered"
    assert result["rollback_triggered"] is True
    assert "rollback_on_hedge_failed_rate_breach" in result["rollback_reason_codes"]
