from __future__ import annotations

from src.core.config import StrategyConfig
from src.core.reason_codes import (
    BELOW_TARGET_PROFIT,
    ENTRY_CUTOFF_BLOCKED,
    LEG1_OPENED,
    LEG2_HEDGE_FILLED,
    LEG2_NOT_FOUND_TIMEOUT,
    UNWIND_ON_LOSS_LIMIT,
    UNWIND_ON_TIMEOUT,
)
from src.strategy.state_machine import LeggingState, LeggingStateMachine


def _cfg(**overrides: object) -> StrategyConfig:
    base = dict(
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
    base.update(overrides)
    return StrategyConfig(**base)


def test_open_leg1_transitions_idle_to_leg1_open_with_reason() -> None:
    sm = LeggingStateMachine(config=_cfg())
    ok = sm.open_leg1(p1_price=0.45, seconds_to_close=120, logical_ts=1)
    assert ok is True
    assert sm.state == LeggingState.LEG1_OPEN
    assert sm.audit_log[-1].reason_code == LEG1_OPENED
    assert sm.audit_log[-1].logical_ts == 1


def test_open_leg1_blocked_by_entry_cutoff() -> None:
    sm = LeggingStateMachine(config=_cfg(entry_cutoff_sec=60))
    ok = sm.open_leg1(p1_price=0.45, seconds_to_close=60, logical_ts=1)
    assert ok is False
    assert sm.state == LeggingState.IDLE
    assert sm.audit_log[-1].reason_code == ENTRY_CUTOFF_BLOCKED
    assert sm.audit_log[-1].from_state == LeggingState.IDLE
    assert sm.audit_log[-1].to_state == LeggingState.IDLE


def test_valid_hedge_transitions_leg1_open_to_hedged() -> None:
    sm = LeggingStateMachine(config=_cfg())
    assert sm.open_leg1(p1_price=0.45, seconds_to_close=200, logical_ts=1)
    ok = sm.try_open_leg2(p2_price=0.50, logical_ts=2)
    assert ok is True
    assert sm.state == LeggingState.HEDGED
    assert sm.audit_log[-1].reason_code == LEG2_HEDGE_FILLED


def test_invalid_hedge_keeps_leg1_open_and_emits_below_target_profit() -> None:
    sm = LeggingStateMachine(config=_cfg())
    assert sm.open_leg1(p1_price=0.45, seconds_to_close=200, logical_ts=1)
    ok = sm.try_open_leg2(p2_price=0.53, logical_ts=2)
    assert ok is False
    assert sm.state == LeggingState.LEG1_OPEN
    assert sm.audit_log[-1].reason_code == BELOW_TARGET_PROFIT
    assert sm.audit_log[-1].from_state == LeggingState.LEG1_OPEN
    assert sm.audit_log[-1].to_state == LeggingState.LEG1_OPEN


def test_timeout_path_transitions_to_unwind_then_closed() -> None:
    sm = LeggingStateMachine(config=_cfg(leg2_timeout_ms=1200))
    assert sm.open_leg1(p1_price=0.45, seconds_to_close=200, logical_ts=1)
    # 2 seconds later => timeout (2000ms > 1200ms)
    ok = sm.try_open_leg2(p2_price=0.50, logical_ts=3)
    assert ok is True
    assert sm.state == LeggingState.UNWIND
    reasons = [r.reason_code for r in sm.audit_log]
    assert LEG2_NOT_FOUND_TIMEOUT in reasons
    assert UNWIND_ON_TIMEOUT in reasons
    assert sm.close_position(logical_ts=4) is True
    assert sm.state == LeggingState.CLOSED


def test_loss_limit_path_transitions_to_unwind_then_closed() -> None:
    sm = LeggingStateMachine(config=_cfg(max_unwind_loss_bps=50.0))
    assert sm.open_leg1(p1_price=0.45, seconds_to_close=200, logical_ts=1)
    assert sm.unwind_on_loss(observed_loss_bps=55.0, logical_ts=2) is True
    assert sm.state == LeggingState.UNWIND
    assert sm.audit_log[-1].reason_code == UNWIND_ON_LOSS_LIMIT
    assert sm.close_position(logical_ts=3) is True
    assert sm.state == LeggingState.CLOSED


def test_reason_codes_per_path_are_consistent() -> None:
    sm_profit = LeggingStateMachine(config=_cfg())
    assert sm_profit.open_leg1(p1_price=0.45, seconds_to_close=200, logical_ts=1)
    assert sm_profit.try_open_leg2(p2_price=0.53, logical_ts=2) is False
    assert sm_profit.audit_log[-1].reason_code == BELOW_TARGET_PROFIT

    sm_timeout = LeggingStateMachine(config=_cfg(leg2_timeout_ms=1000))
    assert sm_timeout.open_leg1(p1_price=0.45, seconds_to_close=200, logical_ts=1)
    assert sm_timeout.try_open_leg2(p2_price=0.50, logical_ts=3) is True
    timeout_reasons = [r.reason_code for r in sm_timeout.audit_log]
    assert LEG2_NOT_FOUND_TIMEOUT in timeout_reasons
    assert UNWIND_ON_TIMEOUT in timeout_reasons

