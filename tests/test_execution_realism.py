from __future__ import annotations

from src.runtime.execution_realism import ExecutionRealismConfig, ExecutionRealismLayer


def _cfg() -> ExecutionRealismConfig:
    return ExecutionRealismConfig(
        exec_model="pessimistic",
        latency_mean_ms=200,
        latency_p95_ms=900,
        cancel_prob_base=0.08,
        partial_fill_prob_base=0.25,
        hedge_fail_prob_base=0.07,
        slippage_base_bps=10.0,
        slippage_depth_alpha=40.0,
        close_window_sec=45,
        close_window_penalty_mult=2.0,
        min_fill_ratio=0.30,
    )


def test_slippage_increases_with_order_size_over_depth() -> None:
    layer = ExecutionRealismLayer(_cfg(), seed=42)
    s_small = layer.estimate_slippage_bps(order_size=1.0, depth=200.0, volatility=0.01, seconds_to_close=120)
    s_large = layer.estimate_slippage_bps(order_size=8.0, depth=200.0, volatility=0.01, seconds_to_close=120)
    assert s_large > s_small


def test_close_window_penalty_worsens_fill_cancel_and_hedge_fail() -> None:
    cfg = _cfg()
    far_layer = ExecutionRealismLayer(cfg, seed=99)
    near_layer = ExecutionRealismLayer(cfg, seed=99)

    n = 300
    far_fill = 0.0
    near_fill = 0.0
    far_cancel = 0
    near_cancel = 0
    far_fail = 0
    near_fail = 0
    for _ in range(n):
        ff = far_layer.estimate_fill_ratio(order_size=1.0, depth=1.0, volatility=0.02, seconds_to_close=120)
        nf = near_layer.estimate_fill_ratio(order_size=1.0, depth=1.0, volatility=0.02, seconds_to_close=10)
        far_fill += ff
        near_fill += nf
        if far_layer.should_cancel(volatility=0.02, seconds_to_close=120, fill_ratio=ff):
            far_cancel += 1
        if near_layer.should_cancel(volatility=0.02, seconds_to_close=10, fill_ratio=nf):
            near_cancel += 1
        if far_layer.should_hedge_fail(volatility=0.02, seconds_to_close=120, fill_ratio=ff):
            far_fail += 1
        if near_layer.should_hedge_fail(volatility=0.02, seconds_to_close=10, fill_ratio=nf):
            near_fail += 1

    assert (near_fill / n) < (far_fill / n)
    assert (near_cancel / n) > (far_cancel / n)
    assert (near_fail / n) > (far_fail / n)


def test_execution_realism_is_deterministic_with_same_seed() -> None:
    cfg = _cfg()
    a = ExecutionRealismLayer(cfg, seed=777)
    b = ExecutionRealismLayer(cfg, seed=777)

    seq_a: list[tuple[int, float, bool, bool]] = []
    seq_b: list[tuple[int, float, bool, bool]] = []
    for _ in range(25):
        latency_a = a.sample_latency_ms(volatility=0.01, seconds_to_close=30)
        fill_a = a.estimate_fill_ratio(order_size=1.0, depth=1.2, volatility=0.01, seconds_to_close=30)
        cancel_a = a.should_cancel(volatility=0.01, seconds_to_close=30, fill_ratio=fill_a)
        fail_a = a.should_hedge_fail(volatility=0.01, seconds_to_close=30, fill_ratio=fill_a)
        seq_a.append((latency_a, round(fill_a, 8), cancel_a, fail_a))

        latency_b = b.sample_latency_ms(volatility=0.01, seconds_to_close=30)
        fill_b = b.estimate_fill_ratio(order_size=1.0, depth=1.2, volatility=0.01, seconds_to_close=30)
        cancel_b = b.should_cancel(volatility=0.01, seconds_to_close=30, fill_ratio=fill_b)
        fail_b = b.should_hedge_fail(volatility=0.01, seconds_to_close=30, fill_ratio=fill_b)
        seq_b.append((latency_b, round(fill_b, 8), cancel_b, fail_b))

    assert seq_a == seq_b
