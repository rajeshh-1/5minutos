from __future__ import annotations

from src.strategy.entry_policy import (
    BLOCKED_BY_CUTOFF,
    BLOCKED_BY_DEPTH,
    BLOCKED_BY_SPREAD,
    BLOCKED_BY_TRADE_LIMIT,
    ENTERED_HIGH_EDGE,
    ENTERED_LOW_EDGE,
    ENTERED_MID_EDGE,
    EntryPolicyConfig,
    EntryRegime,
    classify_regime,
    evaluate_entry,
)


def _cfg() -> EntryPolicyConfig:
    return EntryPolicyConfig(
        max_sum_ask_global=0.99,
        high_edge_threshold=0.95,
        mid_edge_threshold=0.98,
        low_edge_threshold=0.99,
        min_seconds_to_close=75,
        max_spread=0.02,
        min_depth_buffer_mult=1.2,
        max_trades_per_market=1,
        max_unwind_loss_bps=120.0,
        regime_timeouts_ms_high_edge=1200,
        regime_timeouts_ms_mid_edge=2500,
        regime_timeouts_ms_low_edge=3000,
    )


def test_classify_regime_boundaries() -> None:
    cfg = _cfg()
    assert classify_regime(0.95, cfg) == EntryRegime.HIGH_EDGE
    assert classify_regime(0.9501, cfg) == EntryRegime.MID_EDGE
    assert classify_regime(0.98, cfg) == EntryRegime.MID_EDGE
    assert classify_regime(0.985, cfg) == EntryRegime.LOW_EDGE
    assert classify_regime(0.99, cfg) == EntryRegime.LOW_EDGE
    assert classify_regime(0.9901, cfg) == EntryRegime.NO_TRADE


def test_blocked_by_spread() -> None:
    decision = evaluate_entry(
        cfg=_cfg(),
        sum_ask=0.95,
        spread_yes=0.03,
        spread_no=0.01,
        depth_yes=2.0,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=200,
        trades_opened_in_market=0,
    )
    assert not decision.allow_entry
    assert decision.reason_code == BLOCKED_BY_SPREAD


def test_blocked_by_depth() -> None:
    decision = evaluate_entry(
        cfg=_cfg(),
        sum_ask=0.96,
        spread_yes=0.01,
        spread_no=0.01,
        depth_yes=0.9,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=200,
        trades_opened_in_market=0,
    )
    assert not decision.allow_entry
    assert decision.reason_code == BLOCKED_BY_DEPTH


def test_blocked_by_cutoff() -> None:
    decision = evaluate_entry(
        cfg=_cfg(),
        sum_ask=0.96,
        spread_yes=0.01,
        spread_no=0.01,
        depth_yes=2.0,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=75,
        trades_opened_in_market=0,
    )
    assert not decision.allow_entry
    assert decision.reason_code == BLOCKED_BY_CUTOFF


def test_regime_timeouts_and_entry_modes() -> None:
    cfg = _cfg()
    high = evaluate_entry(
        cfg=cfg,
        sum_ask=0.94,
        spread_yes=0.01,
        spread_no=0.01,
        depth_yes=2.0,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=200,
        trades_opened_in_market=0,
    )
    assert high.allow_entry
    assert high.reason_code == ENTERED_HIGH_EDGE
    assert high.leg2_timeout_ms == 1200

    mid = evaluate_entry(
        cfg=cfg,
        sum_ask=0.97,
        spread_yes=0.01,
        spread_no=0.01,
        depth_yes=2.0,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=200,
        trades_opened_in_market=0,
    )
    assert mid.allow_entry
    assert mid.reason_code == ENTERED_MID_EDGE
    assert mid.leg2_timeout_ms == 2500

    low = evaluate_entry(
        cfg=cfg,
        sum_ask=0.985,
        spread_yes=0.01,
        spread_no=0.01,
        depth_yes=2.0,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=200,
        trades_opened_in_market=0,
    )
    assert low.allow_entry
    assert low.reason_code == ENTERED_LOW_EDGE
    assert low.leg2_timeout_ms == 3000


def test_max_trades_per_market_respected() -> None:
    decision = evaluate_entry(
        cfg=_cfg(),
        sum_ask=0.94,
        spread_yes=0.01,
        spread_no=0.01,
        depth_yes=2.0,
        depth_no=2.0,
        stake_usd_per_leg=1.0,
        seconds_to_close=200,
        trades_opened_in_market=1,
    )
    assert not decision.allow_entry
    assert decision.reason_code == BLOCKED_BY_TRADE_LIMIT

