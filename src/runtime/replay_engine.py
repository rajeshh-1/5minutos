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
from src.strategy.state_machine import LeggingState, LeggingStateMachine


REQUIRED_COLUMNS = {
    "timestamp_utc",
    "market_key",
    "up_price",
    "down_price",
    "seconds_to_close",
}


@dataclass(frozen=True)
class ReplayEvent:
    logical_ts: int
    timestamp_utc: str
    market_key: str
    up_price: float
    down_price: float
    seconds_to_close: int


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


def load_replay_events(csv_path: str | Path, event_cap: int | None = None) -> list[ReplayEvent]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("input CSV is missing header")
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            raise ValueError(f"input CSV missing columns: {sorted(missing)}")
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
    reason_counts: Counter[str] = Counter()
    final_state_counts: Counter[str] = Counter()
    trade_log_rows: list[dict[str, Any]] = []

    trades_attempted = 0
    leg1_opened = 0
    leg2_hedged = 0
    unwind_count = 0
    pnl_total = 0.0
    closed_trades = 0

    active_sm: LeggingStateMachine | None = None
    active_market_key: str | None = None
    active_leg1_side: str | None = None
    active_leg1_open_price: float | None = None

    def append_new_records(
        *,
        sm: LeggingStateMachine,
        old_len: int,
        event: ReplayEvent,
        p1_price: float | None,
        p2_price: float | None,
        close_pnl_estimate: float = 0.0,
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

        old_len = len(active_sm.audit_log)
        active_sm.try_open_leg2(p2_price=p2_cur, logical_ts=event.logical_ts)
        append_new_records(
            sm=active_sm,
            old_len=old_len,
            event=event,
            p1_price=active_leg1_open_price,
            p2_price=p2_cur,
        )

        loss_bps = _calc_loss_bps(active_leg1_open_price, p1_cur)
        if active_sm.state == LeggingState.LEG1_OPEN:
            old_len = len(active_sm.audit_log)
            active_sm.unwind_on_loss(observed_loss_bps=loss_bps, logical_ts=event.logical_ts)
            append_new_records(
                sm=active_sm,
                old_len=old_len,
                event=event,
                p1_price=active_leg1_open_price,
                p2_price=p2_cur,
            )

        if active_sm.state in {LeggingState.HEDGED, LeggingState.UNWIND}:
            state_before_close = active_sm.state
            if state_before_close == LeggingState.HEDGED:
                close_pnl = _calc_hedged_pnl(config, active_leg1_open_price, p2_cur)
            else:
                close_pnl = _calc_unwind_pnl(config, loss_bps)

            old_len = len(active_sm.audit_log)
            active_sm.close_position(logical_ts=event.logical_ts)
            append_new_records(
                sm=active_sm,
                old_len=old_len,
                event=event,
                p1_price=active_leg1_open_price,
                p2_price=p2_cur,
                close_pnl_estimate=close_pnl,
            )

            if active_sm.state == LeggingState.CLOSED:
                final_state_counts[active_sm.state.value] += 1
                closed_trades += 1
                pnl_total += close_pnl
            active_sm = None
            active_market_key = None
            active_leg1_side = None
            active_leg1_open_price = None

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

