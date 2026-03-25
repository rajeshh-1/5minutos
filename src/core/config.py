from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TARGET_MODE_GROSS = "gross"
TARGET_MODE_NET = "net"
VALID_TARGET_MODES = {TARGET_MODE_GROSS, TARGET_MODE_NET}


@dataclass(frozen=True)
class StrategyConfig:
    execution_mode: str = "paper"
    market_scope: str = "SOL5M"
    stake_usd_per_leg: float = 1.0
    target_profit_pct: float = 3.0
    target_mode: str = TARGET_MODE_NET
    estimated_costs: float = 0.0
    max_trades_per_market: int = 1
    max_open_positions: int = 1
    entry_cutoff_sec: int = 60
    leg2_timeout_ms: int = 1200
    max_unwind_loss_bps: float = 50.0

    def __post_init__(self) -> None:
        if self.execution_mode not in {"paper", "live"}:
            raise ValueError("execution_mode must be 'paper' or 'live'")
        if self.market_scope != "SOL5M":
            raise ValueError("market_scope must be 'SOL5M' in this phase")
        if float(self.stake_usd_per_leg) <= 0:
            raise ValueError("stake_usd_per_leg must be > 0")
        if float(self.target_profit_pct) <= 0:
            raise ValueError("target_profit_pct must be > 0")
        if self.target_mode not in VALID_TARGET_MODES:
            raise ValueError(f"target_mode must be one of {sorted(VALID_TARGET_MODES)}")
        if float(self.estimated_costs) < 0:
            raise ValueError("estimated_costs must be >= 0")
        if int(self.max_trades_per_market) < 1:
            raise ValueError("max_trades_per_market must be >= 1")
        if int(self.max_open_positions) < 1:
            raise ValueError("max_open_positions must be >= 1")
        if int(self.entry_cutoff_sec) < 0:
            raise ValueError("entry_cutoff_sec must be >= 0")
        if int(self.leg2_timeout_ms) <= 0:
            raise ValueError("leg2_timeout_ms must be > 0")
        if float(self.max_unwind_loss_bps) < 0:
            raise ValueError("max_unwind_loss_bps must be >= 0")

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "StrategyConfig":
        return cls(
            execution_mode=str(raw.get("execution_mode", "paper")).strip().lower(),
            market_scope=str(raw.get("market_scope", "SOL5M")).strip().upper(),
            stake_usd_per_leg=float(raw.get("stake_usd_per_leg", 1.0)),
            target_profit_pct=float(raw.get("target_profit_pct", 3.0)),
            target_mode=str(raw.get("target_mode", TARGET_MODE_NET)).strip().lower(),
            estimated_costs=float(raw.get("estimated_costs", 0.0)),
            max_trades_per_market=int(raw.get("max_trades_per_market", 1)),
            max_open_positions=int(raw.get("max_open_positions", 1)),
            entry_cutoff_sec=int(raw.get("entry_cutoff_sec", 60)),
            leg2_timeout_ms=int(raw.get("leg2_timeout_ms", 1200)),
            max_unwind_loss_bps=float(raw.get("max_unwind_loss_bps", 50.0)),
        )


def load_strategy_config(path: str | Path) -> StrategyConfig:
    cfg_path = Path(path)
    payload = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config JSON must be an object")
    return StrategyConfig.from_dict(payload)

