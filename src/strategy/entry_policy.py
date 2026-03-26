from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

EPS = 1e-9


class EntryRegime(str, Enum):
    HIGH_EDGE = "HIGH_EDGE"
    MID_EDGE = "MID_EDGE"
    LOW_EDGE = "LOW_EDGE"
    NO_TRADE = "NO_TRADE"


NO_TRADE_SUM_ASK = "no_trade_sum_ask"
BLOCKED_BY_SPREAD = "blocked_by_spread"
BLOCKED_BY_DEPTH = "blocked_by_depth"
BLOCKED_BY_CUTOFF = "blocked_by_cutoff"
BLOCKED_BY_TRADE_LIMIT = "blocked_by_trade_limit"
ENTERED_HIGH_EDGE = "entered_high_edge"
ENTERED_MID_EDGE = "entered_mid_edge"
ENTERED_LOW_EDGE = "entered_low_edge"


@dataclass(frozen=True)
class EntryPolicyConfig:
    max_sum_ask_global: float = 0.99
    high_edge_threshold: float = 0.95
    mid_edge_threshold: float = 0.98
    low_edge_threshold: float = 0.99
    min_seconds_to_close: int = 75
    max_spread: float = 0.02
    min_depth_buffer_mult: float = 1.2
    max_trades_per_market: int = 1
    max_unwind_loss_bps: float = 120.0
    regime_timeouts_ms_high_edge: int = 1200
    regime_timeouts_ms_mid_edge: int = 2500
    regime_timeouts_ms_low_edge: int = 3000

    def __post_init__(self) -> None:
        if not (0.0 < float(self.high_edge_threshold) <= float(self.mid_edge_threshold) <= float(self.low_edge_threshold)):
            raise ValueError("entry thresholds must satisfy high <= mid <= low")
        if float(self.max_sum_ask_global) < float(self.low_edge_threshold):
            raise ValueError("max_sum_ask_global must be >= low_edge_threshold")
        if int(self.min_seconds_to_close) < 0:
            raise ValueError("min_seconds_to_close must be >= 0")
        if float(self.max_spread) < 0.0:
            raise ValueError("max_spread must be >= 0")
        if float(self.min_depth_buffer_mult) <= 0.0:
            raise ValueError("min_depth_buffer_mult must be > 0")
        if int(self.max_trades_per_market) < 1:
            raise ValueError("max_trades_per_market must be >= 1")
        if float(self.max_unwind_loss_bps) < 0.0:
            raise ValueError("max_unwind_loss_bps must be >= 0")
        if int(self.regime_timeouts_ms_high_edge) <= 0:
            raise ValueError("regime_timeouts_ms_high_edge must be > 0")
        if int(self.regime_timeouts_ms_mid_edge) <= 0:
            raise ValueError("regime_timeouts_ms_mid_edge must be > 0")
        if int(self.regime_timeouts_ms_low_edge) <= 0:
            raise ValueError("regime_timeouts_ms_low_edge must be > 0")

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "EntryPolicyConfig":
        timeouts_raw = raw.get("regime_timeouts_ms", {})
        if not isinstance(timeouts_raw, dict):
            timeouts_raw = {}
        return cls(
            max_sum_ask_global=float(raw.get("max_sum_ask_global", 0.99)),
            high_edge_threshold=float(raw.get("high_edge_threshold", 0.95)),
            mid_edge_threshold=float(raw.get("mid_edge_threshold", 0.98)),
            low_edge_threshold=float(raw.get("low_edge_threshold", 0.99)),
            min_seconds_to_close=int(raw.get("min_seconds_to_close", 75)),
            max_spread=float(raw.get("max_spread", 0.02)),
            min_depth_buffer_mult=float(raw.get("min_depth_buffer_mult", 1.2)),
            max_trades_per_market=int(raw.get("max_trades_per_market", 1)),
            max_unwind_loss_bps=float(raw.get("max_unwind_loss_bps", 120.0)),
            regime_timeouts_ms_high_edge=int(timeouts_raw.get("high_edge", 1200)),
            regime_timeouts_ms_mid_edge=int(timeouts_raw.get("mid_edge", 2500)),
            regime_timeouts_ms_low_edge=int(timeouts_raw.get("low_edge", 3000)),
        )


@dataclass(frozen=True)
class EntryDecision:
    allow_entry: bool
    regime: EntryRegime
    reason_code: str
    entry_mode: str
    leg2_timeout_ms: int
    sum_ask: float
    detail: str


def load_entry_policy_config(path: str | Path | None = None) -> EntryPolicyConfig:
    if path is None:
        return EntryPolicyConfig()
    policy_path = Path(path)
    if not policy_path.exists():
        return EntryPolicyConfig()
    payload = json.loads(policy_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("entry policy JSON must be an object")
    return EntryPolicyConfig.from_dict(payload)


def classify_regime(sum_ask: float, cfg: EntryPolicyConfig) -> EntryRegime:
    val = float(sum_ask)
    if val <= float(cfg.high_edge_threshold) + EPS:
        return EntryRegime.HIGH_EDGE
    if val <= float(cfg.mid_edge_threshold) + EPS:
        return EntryRegime.MID_EDGE
    if val <= float(cfg.low_edge_threshold) + EPS:
        return EntryRegime.LOW_EDGE
    return EntryRegime.NO_TRADE


def _entry_payload(
    *,
    allow_entry: bool,
    regime: EntryRegime,
    reason_code: str,
    entry_mode: str,
    leg2_timeout_ms: int,
    sum_ask: float,
    detail: str,
) -> EntryDecision:
    return EntryDecision(
        allow_entry=bool(allow_entry),
        regime=regime,
        reason_code=str(reason_code),
        entry_mode=str(entry_mode),
        leg2_timeout_ms=int(leg2_timeout_ms),
        sum_ask=float(sum_ask),
        detail=str(detail),
    )


def evaluate_entry(
    *,
    cfg: EntryPolicyConfig,
    sum_ask: float,
    spread_yes: float,
    spread_no: float,
    depth_yes: float,
    depth_no: float,
    stake_usd_per_leg: float,
    seconds_to_close: int,
    trades_opened_in_market: int,
) -> EntryDecision:
    regime = classify_regime(float(sum_ask), cfg)
    if regime == EntryRegime.HIGH_EDGE:
        entry_mode = "aggressive"
        timeout_ms = int(cfg.regime_timeouts_ms_high_edge)
        success_code = ENTERED_HIGH_EDGE
    elif regime == EntryRegime.MID_EDGE:
        entry_mode = "semi_aggressive"
        timeout_ms = int(cfg.regime_timeouts_ms_mid_edge)
        success_code = ENTERED_MID_EDGE
    elif regime == EntryRegime.LOW_EDGE:
        entry_mode = "passive_limit"
        timeout_ms = int(cfg.regime_timeouts_ms_low_edge)
        success_code = ENTERED_LOW_EDGE
    else:
        entry_mode = "no_trade"
        timeout_ms = int(cfg.regime_timeouts_ms_low_edge)
        success_code = NO_TRADE_SUM_ASK

    if int(trades_opened_in_market) >= int(cfg.max_trades_per_market):
        return _entry_payload(
            allow_entry=False,
            regime=regime,
            reason_code=BLOCKED_BY_TRADE_LIMIT,
            entry_mode=entry_mode,
            leg2_timeout_ms=timeout_ms,
            sum_ask=sum_ask,
            detail=(
                f"trades_opened_in_market={int(trades_opened_in_market)} "
                f">= max_trades_per_market={int(cfg.max_trades_per_market)}"
            ),
        )

    if int(seconds_to_close) <= int(cfg.min_seconds_to_close):
        return _entry_payload(
            allow_entry=False,
            regime=regime,
            reason_code=BLOCKED_BY_CUTOFF,
            entry_mode=entry_mode,
            leg2_timeout_ms=timeout_ms,
            sum_ask=sum_ask,
            detail=(
                f"seconds_to_close={int(seconds_to_close)} "
                f"<= min_seconds_to_close={int(cfg.min_seconds_to_close)}"
            ),
        )

    if float(sum_ask) > float(cfg.max_sum_ask_global) + EPS or regime == EntryRegime.NO_TRADE:
        return _entry_payload(
            allow_entry=False,
            regime=regime,
            reason_code=NO_TRADE_SUM_ASK,
            entry_mode=entry_mode,
            leg2_timeout_ms=timeout_ms,
            sum_ask=sum_ask,
            detail=f"sum_ask={float(sum_ask):.6f} > max_sum_ask_global={float(cfg.max_sum_ask_global):.6f}",
        )

    if float(spread_yes) > float(cfg.max_spread) + EPS or float(spread_no) > float(cfg.max_spread) + EPS:
        return _entry_payload(
            allow_entry=False,
            regime=regime,
            reason_code=BLOCKED_BY_SPREAD,
            entry_mode=entry_mode,
            leg2_timeout_ms=timeout_ms,
            sum_ask=sum_ask,
            detail=(
                f"spread_yes={float(spread_yes):.6f}, spread_no={float(spread_no):.6f}, "
                f"max_spread={float(cfg.max_spread):.6f}"
            ),
        )

    required_depth = float(stake_usd_per_leg) * float(cfg.min_depth_buffer_mult)
    if float(depth_yes) < required_depth or float(depth_no) < required_depth:
        return _entry_payload(
            allow_entry=False,
            regime=regime,
            reason_code=BLOCKED_BY_DEPTH,
            entry_mode=entry_mode,
            leg2_timeout_ms=timeout_ms,
            sum_ask=sum_ask,
            detail=(
                f"depth_yes={float(depth_yes):.6f}, depth_no={float(depth_no):.6f}, "
                f"required_depth={required_depth:.6f}"
            ),
        )

    return _entry_payload(
        allow_entry=True,
        regime=regime,
        reason_code=success_code,
        entry_mode=entry_mode,
        leg2_timeout_ms=timeout_ms,
        sum_ask=sum_ask,
        detail="entry_allowed",
    )
