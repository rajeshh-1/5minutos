from __future__ import annotations

import csv
import json
import random
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TARGET_MODE_NET, StrategyConfig
from src.core.reason_codes import LEG1_OPENED, LEG2_HEDGE_FILLED
from src.runtime.execution_realism import (
    ExecutionRealismConfig,
    ExecutionRealismLayer,
    load_execution_realism_config_if_exists,
)
from src.strategy.state_machine import LeggingState, LeggingStateMachine


REQUIRED_COLUMNS = {
    "timestamp_utc",
    "market_key",
    "up_price",
    "down_price",
    "seconds_to_close",
}
DEFAULT_EXECUTION_REALISM_CONFIG_PATH = Path("configs/execution_realism_pessimistic.json")


@dataclass(frozen=True)
class ReplayEvent:
    logical_ts: int
    timestamp_utc: str
    market_key: str
    up_price: float
    down_price: float
    seconds_to_close: int
    synthetic_depth: float = 0.0
    synthetic_volatility: float = 0.0
    has_synthetic_fields: bool = False


@dataclass(frozen=True)
class ReplayRunResult:
    summary_path: Path
    trade_log_path: Path
    reason_codes_path: Path
    summary: dict[str, Any]


def _safe_float(value: Any, *, name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid float for {name}: {value!r}") from exc
    return out


def _safe_int(value: Any, *, name: str) -> int:
    try:
        out = int(float(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid int for {name}: {value!r}") from exc
    return out


def _pctl(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(float(v) for v in values)
    qn = max(0.0, min(1.0, float(q)))
    if len(vals) == 1:
        return vals[0]
    pos = qn * (len(vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return (vals[lo] * (1.0 - frac)) + (vals[hi] * frac)


def load_replay_events(csv_path: str | Path, event_cap: int | None = None) -> list[ReplayEvent]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("input CSV is missing header")
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            raise ValueError(f"input CSV missing columns: {sorted(missing)}")
        has_synthetic_fields = ("synthetic_depth" in reader.fieldnames) or ("synthetic_volatility" in reader.fieldnames)
        rows: list[dict[str, str]] = list(reader)

    rows.sort(key=lambda row: str(row.get("timestamp_utc", "")))
    if event_cap is not None and int(event_cap) > 0:
        rows = rows[: int(event_cap)]

    events: list[ReplayEvent] = []
    for idx, row in enumerate(rows, start=1):
        events.append(
            ReplayEvent(
                logical_ts=idx,
                timestamp_utc=str(row["timestamp_utc"]).strip(),
                market_key=str(row["market_key"]).strip(),
                up_price=_safe_float(row["up_price"], name="up_price"),
                down_price=_safe_float(row["down_price"], name="down_price"),
                seconds_to_close=_safe_int(row["seconds_to_close"], name="seconds_to_close"),
                synthetic_depth=_safe_float(row.get("synthetic_depth", 0.0), name="synthetic_depth"),
                synthetic_volatility=_safe_float(row.get("synthetic_volatility", 0.0), name="synthetic_volatility"),
                has_synthetic_fields=bool(has_synthetic_fields),
            )
        )
    return events


def _select_leg_prices(event: ReplayEvent, *, leg1_side: str, rng: random.Random) -> tuple[str, float, float]:
    side = str(leg1_side).strip().lower()
    if side not in {"up", "down", "auto"}:
        raise ValueError("leg1_side must be one of: up, down, auto")

    if side == "auto":
        if event.up_price < event.down_price:
            side = "up"
        elif event.down_price < event.up_price:
            side = "down"
        else:
            side = "up" if rng.random() < 0.5 else "down"

    if side == "up":
        return side, float(event.up_price), float(event.down_price)
    return side, float(event.down_price), float(event.up_price)


def _calc_loss_bps(open_price: float, current_price: float) -> float:
    base = max(1e-12, float(open_price))
    return max(0.0, ((float(open_price) - float(current_price)) / base) * 10000.0)


def _calc_hedged_pnl(cfg: StrategyConfig, p1_price: float, p2_price: float) -> float:
    costs = float(cfg.estimated_costs) if cfg.target_mode == TARGET_MODE_NET else 0.0
    edge = 1.0 - float(p1_price) - float(p2_price) - costs
    return edge * float(cfg.stake_usd_per_leg)


def _calc_unwind_pnl(cfg: StrategyConfig, loss_bps: float) -> float:
    return -(float(loss_bps) / 10000.0) * float(cfg.stake_usd_per_leg)


def run_replay(
    *,
    events: list[ReplayEvent],
    config: StrategyConfig,
    out_dir: str | Path = "reports",
    seed: int = 42,
    leg1_side: str = "auto",
) -> ReplayRunResult:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary_path = out / "replay_summary.json"
    trade_log_path = out / "trade_log.csv"
    reason_codes_path = out / "reason_codes.csv"

    rng = random.Random(int(seed))
    use_realism = any(bool(event.has_synthetic_fields) for event in events)
    realism_layer: ExecutionRealismLayer | None = None
    realism_cfg = ExecutionRealismConfig()
    if use_realism:
        realism_cfg = load_execution_realism_config_if_exists(DEFAULT_EXECUTION_REALISM_CONFIG_PATH)
        realism_layer = ExecutionRealismLayer(realism_cfg, seed=int(seed) + 101)

    reason_counts: Counter[str] = Counter()
    final_state_counts: Counter[str] = Counter()
    trade_log_rows: list[dict[str, Any]] = []

    trades_attempted = 0
    leg1_opened = 0
    leg2_hedged = 0
    unwind_count = 0
    pnl_total = 0.0
    closed_trades = 0

    sim_attempted = 0
    sim_partial_count = 0
    sim_cancel_count = 0
    sim_hedge_fail_count = 0
    sim_latency_ms: list[float] = []
    sim_slippage_bps: list[float] = []

    active_sm: LeggingStateMachine | None = None
    active_market_key: str | None = None
    active_leg1_side: str | None = None
    active_leg1_open_price: float | None = None
    active_fill_ratio = 1.0

    def append_new_records(
        *,
        sm: LeggingStateMachine,
        old_len: int,
        event: ReplayEvent,
        p1_price: float | None,
        p2_price: float | None,
        close_pnl_estimate: float = 0.0,
        simulated_latency_ms: int | None = None,
        simulated_fill_ratio: float | None = None,
        simulated_slippage_bps: float | None = None,
        simulated_cancelled: bool = False,
        simulated_hedge_fail: bool = False,
    ) -> None:
        nonlocal leg1_opened, leg2_hedged, unwind_count
        for rec in sm.audit_log[old_len:]:
            pnl_estimate = close_pnl_estimate if rec.action == "close_position" else 0.0
            trade_log_rows.append(
                {
                    "event_ts": event.timestamp_utc,
                    "market_key": event.market_key,
                    "action": rec.action,
                    "state_from": rec.from_state.value,
                    "state_to": rec.to_state.value,
                    "p1_price": "" if p1_price is None else round(float(p1_price), 8),
                    "p2_price": "" if p2_price is None else round(float(p2_price), 8),
                    "pnl_estimate": round(float(pnl_estimate), 10),
                    "reason_code": rec.reason_code,
                    "simulated_latency_ms": "" if simulated_latency_ms is None else int(simulated_latency_ms),
                    "simulated_fill_ratio": (
                        "" if simulated_fill_ratio is None else round(float(simulated_fill_ratio), 8)
                    ),
                    "simulated_slippage_bps": (
                        "" if simulated_slippage_bps is None else round(float(simulated_slippage_bps), 6)
                    ),
                    "simulated_cancelled": bool(simulated_cancelled),
                    "simulated_hedge_fail": bool(simulated_hedge_fail),
                }
            )
            reason_counts[rec.reason_code] += 1
            if rec.reason_code == LEG1_OPENED and rec.action == "open_leg1":
                leg1_opened += 1
            if rec.reason_code == LEG2_HEDGE_FILLED and rec.action == "try_open_leg2":
                leg2_hedged += 1
            if rec.to_state == LeggingState.UNWIND:
                unwind_count += 1

    for event in events:
        if active_sm is None:
            side, p1, _p2 = _select_leg_prices(event, leg1_side=leg1_side, rng=rng)
            active_sm = LeggingStateMachine(config=config)
            active_market_key = event.market_key
            active_leg1_side = side
            active_leg1_open_price = p1
            trades_attempted += 1
            old_len = len(active_sm.audit_log)
            opened = active_sm.open_leg1(
                p1_price=p1,
                seconds_to_close=event.seconds_to_close,
                logical_ts=event.logical_ts,
            )
            append_new_records(sm=active_sm, old_len=old_len, event=event, p1_price=p1, p2_price=None)
            if not opened:
                active_sm = None
                active_market_key = None
                active_leg1_side = None
                active_leg1_open_price = None
                active_fill_ratio = 1.0
            continue

        if active_market_key is None or active_leg1_side is None or active_leg1_open_price is None:
            active_sm = None
            continue

        if active_leg1_side == "up":
            p1_cur = float(event.up_price)
            p2_cur = float(event.down_price)
        else:
            p1_cur = float(event.down_price)
            p2_cur = float(event.up_price)

        simulated_latency = None
        simulated_fill_ratio = None
        simulated_slippage = None
        simulated_cancelled = False
        simulated_hedge_fail = False
        p2_exec = p2_cur

        if realism_layer is not None:
            safe_depth = float(event.synthetic_depth) if float(event.synthetic_depth) > 0 else max(
                1.0, float(config.stake_usd_per_leg) * 5.0
            )
            safe_volatility = max(0.0, float(event.synthetic_volatility))
            simulated_latency = realism_layer.sample_latency_ms(
                volatility=safe_volatility,
                seconds_to_close=event.seconds_to_close,
            )
            simulated_fill_ratio = realism_layer.estimate_fill_ratio(
                order_size=float(config.stake_usd_per_leg),
                depth=safe_depth,
                volatility=safe_volatility,
                seconds_to_close=event.seconds_to_close,
            )
            simulated_slippage = realism_layer.estimate_slippage_bps(
                order_size=float(config.stake_usd_per_leg),
                depth=safe_depth,
                volatility=safe_volatility,
                seconds_to_close=event.seconds_to_close,
            )
            simulated_cancelled = realism_layer.should_cancel(
                volatility=safe_volatility,
                seconds_to_close=event.seconds_to_close,
                fill_ratio=float(simulated_fill_ratio),
            )
            simulated_hedge_fail = realism_layer.should_hedge_fail(
                volatility=safe_volatility,
                seconds_to_close=event.seconds_to_close,
                fill_ratio=float(simulated_fill_ratio),
            )
            sim_attempted += 1
            sim_latency_ms.append(float(simulated_latency))
            sim_slippage_bps.append(float(simulated_slippage))
            if float(simulated_fill_ratio) < 0.999999:
                sim_partial_count += 1
            if simulated_cancelled:
                sim_cancel_count += 1
            if simulated_hedge_fail:
                sim_hedge_fail_count += 1

        old_len = len(active_sm.audit_log)
        if simulated_cancelled or simulated_hedge_fail:
            active_sm.timeout_leg2(logical_ts=event.logical_ts)
            append_new_records(
                sm=active_sm,
                old_len=old_len,
                event=event,
                p1_price=active_leg1_open_price,
                p2_price=p2_cur,
                simulated_latency_ms=simulated_latency,
                simulated_fill_ratio=simulated_fill_ratio,
                simulated_slippage_bps=simulated_slippage,
                simulated_cancelled=simulated_cancelled,
                simulated_hedge_fail=simulated_hedge_fail,
            )
        else:
            if simulated_slippage is not None:
                p2_exec = float(p2_cur) * (1.0 + (float(simulated_slippage) / 10000.0))
            hedged = active_sm.try_open_leg2(p2_price=p2_exec, logical_ts=event.logical_ts)
            append_new_records(
                sm=active_sm,
                old_len=old_len,
                event=event,
                p1_price=active_leg1_open_price,
                p2_price=p2_exec,
                simulated_latency_ms=simulated_latency,
                simulated_fill_ratio=simulated_fill_ratio,
                simulated_slippage_bps=simulated_slippage,
                simulated_cancelled=simulated_cancelled,
                simulated_hedge_fail=simulated_hedge_fail,
            )
            if hedged and simulated_fill_ratio is not None:
                active_fill_ratio = max(0.0, min(1.0, float(simulated_fill_ratio)))

        loss_bps = _calc_loss_bps(active_leg1_open_price, p1_cur)
        if active_sm.state == LeggingState.LEG1_OPEN:
            old_len = len(active_sm.audit_log)
            active_sm.unwind_on_loss(observed_loss_bps=loss_bps, logical_ts=event.logical_ts)
            append_new_records(
                sm=active_sm,
                old_len=old_len,
                event=event,
                p1_price=active_leg1_open_price,
                p2_price=p2_exec,
                simulated_latency_ms=simulated_latency,
                simulated_fill_ratio=simulated_fill_ratio,
                simulated_slippage_bps=simulated_slippage,
                simulated_cancelled=simulated_cancelled,
                simulated_hedge_fail=simulated_hedge_fail,
            )

        if active_sm.state in {LeggingState.HEDGED, LeggingState.UNWIND}:
            state_before_close = active_sm.state
            if state_before_close == LeggingState.HEDGED:
                close_pnl = _calc_hedged_pnl(config, active_leg1_open_price, p2_exec) * max(0.0, min(1.0, active_fill_ratio))
                if active_fill_ratio < 0.999999:
                    residual = 1.0 - max(0.0, min(1.0, active_fill_ratio))
                    close_pnl -= residual * 0.01 * float(config.stake_usd_per_leg)
            else:
                close_pnl = _calc_unwind_pnl(config, loss_bps)

            old_len = len(active_sm.audit_log)
            active_sm.close_position(logical_ts=event.logical_ts)
            append_new_records(
                sm=active_sm,
                old_len=old_len,
                event=event,
                p1_price=active_leg1_open_price,
                p2_price=p2_exec,
                close_pnl_estimate=close_pnl,
                simulated_latency_ms=simulated_latency,
                simulated_fill_ratio=simulated_fill_ratio,
                simulated_slippage_bps=simulated_slippage,
                simulated_cancelled=simulated_cancelled,
                simulated_hedge_fail=simulated_hedge_fail,
            )

            if active_sm.state == LeggingState.CLOSED:
                final_state_counts[active_sm.state.value] += 1
                closed_trades += 1
                pnl_total += close_pnl
            active_sm = None
            active_market_key = None
            active_leg1_side = None
            active_leg1_open_price = None
            active_fill_ratio = 1.0

    if active_sm is not None and active_sm.state == LeggingState.LEG1_OPEN and events:
        last_event = events[-1]
        old_len = len(active_sm.audit_log)
        active_sm.timeout_leg2(logical_ts=last_event.logical_ts + 1)
        append_new_records(
            sm=active_sm,
            old_len=old_len,
            event=last_event,
            p1_price=active_leg1_open_price,
            p2_price=None,
        )
        old_len = len(active_sm.audit_log)
        close_pnl = _calc_unwind_pnl(config, 0.0)
        active_sm.close_position(logical_ts=last_event.logical_ts + 1)
        append_new_records(
            sm=active_sm,
            old_len=old_len,
            event=last_event,
            p1_price=active_leg1_open_price,
            p2_price=None,
            close_pnl_estimate=close_pnl,
        )
        if active_sm.state == LeggingState.CLOSED:
            final_state_counts[active_sm.state.value] += 1
            closed_trades += 1
            pnl_total += close_pnl

    hedge_success_rate = (float(leg2_hedged) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    pnl_per_trade = (pnl_total / float(closed_trades)) if closed_trades > 0 else 0.0
    top_reason_codes = reason_counts.most_common(10)
    partial_fill_rate = (float(sim_partial_count) / float(sim_attempted)) if sim_attempted > 0 else 0.0
    cancel_rate = (float(sim_cancel_count) / float(sim_attempted)) if sim_attempted > 0 else 0.0
    hedge_fail_rate_simulated = (float(sim_hedge_fail_count) / float(sim_attempted)) if sim_attempted > 0 else 0.0
    avg_slippage_bps = (sum(sim_slippage_bps) / float(len(sim_slippage_bps))) if sim_slippage_bps else 0.0
    p95_latency_ms = _pctl(sim_latency_ms, 0.95) if sim_latency_ms else 0.0

    summary: dict[str, Any] = {
        "events_processed": len(events),
        "trades_attempted": trades_attempted,
        "leg1_opened": leg1_opened,
        "leg2_hedged": leg2_hedged,
        "hedge_success_rate": round(hedge_success_rate, 10),
        "unwind_count": unwind_count,
        "pnl_total": round(pnl_total, 10),
        "pnl_per_trade": round(pnl_per_trade, 10),
        "final_state_counts": dict(final_state_counts),
        "top_reason_codes": top_reason_codes,
        "execution_realism_enabled": bool(use_realism),
        "execution_realism_model": realism_cfg.exec_model if use_realism else "disabled",
        "partial_fill_rate": round(partial_fill_rate, 10),
        "cancel_rate": round(cancel_rate, 10),
        "hedge_fail_rate_simulated": round(hedge_fail_rate_simulated, 10),
        "avg_slippage_bps": round(avg_slippage_bps, 10),
        "p95_latency_ms": round(float(p95_latency_ms), 10),
    }

    with trade_log_path.open("w", encoding="utf-8", newline="") as fh:
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
                "simulated_latency_ms",
                "simulated_fill_ratio",
                "simulated_slippage_bps",
                "simulated_cancelled",
                "simulated_hedge_fail",
            ],
        )
        writer.writeheader()
        writer.writerows(trade_log_rows)

    total_reasons = sum(reason_counts.values())
    with reason_codes_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["reason_code", "count", "pct"])
        writer.writeheader()
        for reason_code, count in reason_counts.most_common():
            pct = (float(count) / float(total_reasons) * 100.0) if total_reasons > 0 else 0.0
            writer.writerow({"reason_code": reason_code, "count": int(count), "pct": round(pct, 6)})

    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    return ReplayRunResult(
        summary_path=summary_path,
        trade_log_path=trade_log_path,
        reason_codes_path=reason_codes_path,
        summary=summary,
    )
