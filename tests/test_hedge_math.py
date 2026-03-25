from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.core.config import TARGET_MODE_GROSS, TARGET_MODE_NET, StrategyConfig, load_strategy_config
from src.strategy.hedge_math import calculate_leg2_max_price, is_hedge_entry_valid


def test_leg2_max_price_net_mode_uses_costs() -> None:
    target = calculate_leg2_max_price(
        p1_price=0.40,
        target_profit_pct=3.0,
        estimated_costs=0.02,
        target_mode=TARGET_MODE_NET,
    )
    assert target.p2_max == pytest.approx(0.55, rel=1e-9)


def test_leg2_max_price_gross_mode_ignores_costs() -> None:
    target = calculate_leg2_max_price(
        p1_price=0.40,
        target_profit_pct=3.0,
        estimated_costs=0.02,
        target_mode=TARGET_MODE_GROSS,
    )
    assert target.p2_max == pytest.approx(0.57, rel=1e-9)


def test_hedge_entry_validates_against_threshold() -> None:
    assert (
        is_hedge_entry_valid(
            p1_price=0.45,
            p2_price=0.50,
            target_profit_pct=3.0,
            estimated_costs=0.01,
            target_mode=TARGET_MODE_NET,
        )
        is True
    )
    assert (
        is_hedge_entry_valid(
            p1_price=0.45,
            p2_price=0.53,
            target_profit_pct=3.0,
            estimated_costs=0.01,
            target_mode=TARGET_MODE_NET,
        )
        is False
    )


def test_negative_inputs_or_invalid_mode_raise() -> None:
    with pytest.raises(ValueError):
        calculate_leg2_max_price(p1_price=-0.01)
    with pytest.raises(ValueError):
        calculate_leg2_max_price(p1_price=0.40, target_profit_pct=-1.0)
    with pytest.raises(ValueError):
        calculate_leg2_max_price(p1_price=0.40, estimated_costs=-0.01)
    with pytest.raises(ValueError):
        calculate_leg2_max_price(p1_price=0.40, target_mode="invalid-mode")


def test_load_strategy_config_from_json(tmp_path: Path) -> None:
    cfg_file = tmp_path / "paper_sol5m_usd1.json"
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
    cfg_file.write_text(json.dumps(payload), encoding="utf-8")
    cfg = load_strategy_config(cfg_file)
    assert isinstance(cfg, StrategyConfig)
    assert cfg.execution_mode == "paper"
    assert cfg.market_scope == "SOL5M"
    assert cfg.target_profit_pct == pytest.approx(3.0, rel=1e-9)

