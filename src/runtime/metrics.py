from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.reason_codes import BELOW_TARGET_PROFIT, LEG2_NOT_FOUND_TIMEOUT, UNWIND_ON_TIMEOUT


@dataclass(frozen=True)
class ReadinessThresholds:
    hedge_failed_rate_max: float = 0.02
    max_drawdown_pct_max: float = 12.0
    p99_loss_max: float = 0.40


@dataclass(frozen=True)
class GoNoGoDecision:
    status: str
    blockers: list[str]


@dataclass(frozen=True)
class StabilityThresholds:
    pnl_per_trade_std_max: float = 0.02
    hedge_success_rate_std_max: float = 0.10


@dataclass(frozen=True)
class RobustnessWeights:
    pnl_per_trade: float = 0.35
    hedge_success_rate: float = 0.25
    hedge_failed_rate: float = 0.15
    max_drawdown_pct: float = 0.10
    p99_loss: float = 0.10
    variance_penalty: float = 0.05


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    q_clamped = max(0.0, min(1.0, float(q)))
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q_clamped
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    w_hi = pos - lo
    w_lo = 1.0 - w_hi
    return (ordered[lo] * w_lo) + (ordered[hi] * w_hi)


def mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(float(v) for v in values) / float(len(values)))


def stddev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    avg = mean(values)
    var = sum((float(v) - avg) ** 2 for v in values) / float(len(values))
    return float(math.sqrt(max(0.0, var)))


def max_drawdown_pct(pnl_series: list[float], initial_equity: float = 1.0) -> float:
    equity = float(initial_equity)
    peak = float(initial_equity)
    max_dd = 0.0
    for pnl in pnl_series:
        equity += float(pnl)
        peak = max(peak, equity)
        dd = ((peak - equity) / max(1e-12, peak)) * 100.0
        max_dd = max(max_dd, dd)
    return max_dd


def _load_summary(summary_file: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(summary_file).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("summary file must contain a JSON object")
    return payload


def _load_trade_log(trade_log_file: str | Path) -> list[dict[str, str]]:
    with Path(trade_log_file).open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    return rows


def _load_reason_codes(reason_codes_file: str | Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    with Path(reason_codes_file).open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            code = str(row.get("reason_code", "")).strip()
            if not code:
                continue
            counts[code] = int(float(row.get("count", 0) or 0))
    return counts


def _extract_trade_level_features(trade_log_rows: list[dict[str, str]]) -> tuple[list[float], int, int]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in trade_log_rows:
        key = str(row.get("market_key", "")).strip()
        grouped[key].append(row)

    close_pnls: list[float] = []
    timeout_trades = 0
    below_target_trades = 0
    for rows in grouped.values():
        reason_codes = {str(r.get("reason_code", "")).strip() for r in rows}
        if LEG2_NOT_FOUND_TIMEOUT in reason_codes or UNWIND_ON_TIMEOUT in reason_codes:
            timeout_trades += 1
        if BELOW_TARGET_PROFIT in reason_codes:
            below_target_trades += 1
        for row in rows:
            if str(row.get("action", "")).strip() == "close_position":
                close_pnls.append(_safe_float(row.get("pnl_estimate", 0.0), default=0.0))
    return close_pnls, timeout_trades, below_target_trades


def compute_metrics(
    *,
    summary: dict[str, Any],
    trade_log_rows: list[dict[str, str]],
    reason_code_counts: dict[str, int],
) -> dict[str, Any]:
    trades_attempted = int(summary.get("trades_attempted", 0))
    leg1_opened = int(summary.get("leg1_opened", 0))
    leg2_hedged = int(summary.get("leg2_hedged", 0))
    unwind_count = int(summary.get("unwind_count", 0))
    pnl_total = float(summary.get("pnl_total", 0.0))

    close_pnls, timeout_trades, below_target_trades = _extract_trade_level_features(trade_log_rows)
    closed_trades = len(close_pnls)
    pnl_per_trade = (pnl_total / float(closed_trades)) if closed_trades > 0 else 0.0

    losses = [max(0.0, -float(p)) for p in close_pnls]
    p95_loss = quantile(losses, 0.95)
    p99_loss = quantile(losses, 0.99)
    drawdown_pct = max_drawdown_pct(close_pnls, initial_equity=1.0)

    hedge_success_rate = (float(leg2_hedged) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    hedge_failed = max(0, leg1_opened - leg2_hedged)
    hedge_failed_rate = (float(hedge_failed) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    timeout_rate = (float(timeout_trades) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    below_target_profit_rate = (float(below_target_trades) / float(leg1_opened)) if leg1_opened > 0 else 0.0

    top_reason_codes = sorted(reason_code_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        "trades_attempted": trades_attempted,
        "leg1_opened": leg1_opened,
        "leg2_hedged": leg2_hedged,
        "hedge_success_rate": round(hedge_success_rate, 10),
        "unwind_count": unwind_count,
        "pnl_total": round(pnl_total, 10),
        "pnl_per_trade": round(pnl_per_trade, 10),
        "max_drawdown_pct": round(drawdown_pct, 10),
        "p95_loss": round(p95_loss, 10),
        "p99_loss": round(p99_loss, 10),
        "hedge_failed_rate": round(hedge_failed_rate, 10),
        "timeout_rate": round(timeout_rate, 10),
        "below_target_profit_rate": round(below_target_profit_rate, 10),
        "top_reason_codes": top_reason_codes,
    }


def compute_live_metrics_from_aggregates(
    *,
    trades_attempted: int,
    leg1_opened: int,
    leg2_hedged: int,
    unwind_count: int,
    pnl_total: float,
    close_pnls: list[float],
    timeout_trades: int,
    below_target_trades: int,
    reason_code_counts: dict[str, int],
) -> dict[str, Any]:
    closed_trades = len(close_pnls)
    pnl_per_trade = (float(pnl_total) / float(closed_trades)) if closed_trades > 0 else 0.0
    losses = [max(0.0, -float(p)) for p in close_pnls]
    p95_loss = quantile(losses, 0.95)
    p99_loss = quantile(losses, 0.99)
    drawdown_pct = max_drawdown_pct(close_pnls, initial_equity=1.0)

    hedge_success_rate = (float(leg2_hedged) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    hedge_failed = max(0, int(leg1_opened) - int(leg2_hedged))
    hedge_failed_rate = (float(hedge_failed) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    timeout_rate = (float(timeout_trades) / float(leg1_opened)) if leg1_opened > 0 else 0.0
    below_target_profit_rate = (float(below_target_trades) / float(leg1_opened)) if leg1_opened > 0 else 0.0

    top_reason_codes = sorted(reason_code_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        "trades_attempted": int(trades_attempted),
        "leg1_opened": int(leg1_opened),
        "leg2_hedged": int(leg2_hedged),
        "hedge_success_rate": round(hedge_success_rate, 10),
        "unwind_count": int(unwind_count),
        "pnl_total": round(float(pnl_total), 10),
        "pnl_per_trade": round(pnl_per_trade, 10),
        "max_drawdown_pct": round(drawdown_pct, 10),
        "p95_loss": round(p95_loss, 10),
        "p99_loss": round(p99_loss, 10),
        "hedge_failed_rate": round(hedge_failed_rate, 10),
        "timeout_rate": round(timeout_rate, 10),
        "below_target_profit_rate": round(below_target_profit_rate, 10),
        "top_reason_codes": top_reason_codes,
    }


def evaluate_go_no_go(metrics: dict[str, Any], thresholds: ReadinessThresholds, critical_errors: list[str] | None = None) -> GoNoGoDecision:
    blockers: list[str] = []
    critical = list(critical_errors or [])
    for err in critical:
        blockers.append(f"critical_error: {err}")

    if float(metrics.get("hedge_failed_rate", 1.0)) > float(thresholds.hedge_failed_rate_max):
        blockers.append(
            f"hedge_failed_rate {float(metrics['hedge_failed_rate']):.6f} > {float(thresholds.hedge_failed_rate_max):.6f}"
        )
    if float(metrics.get("pnl_per_trade", 0.0)) <= 0.0:
        blockers.append(f"pnl_per_trade {float(metrics['pnl_per_trade']):.6f} <= 0")
    if float(metrics.get("max_drawdown_pct", 0.0)) > float(thresholds.max_drawdown_pct_max):
        blockers.append(
            f"max_drawdown_pct {float(metrics['max_drawdown_pct']):.6f} > {float(thresholds.max_drawdown_pct_max):.6f}"
        )
    if float(metrics.get("p99_loss", 0.0)) > float(thresholds.p99_loss_max):
        blockers.append(f"p99_loss {float(metrics['p99_loss']):.6f} > {float(thresholds.p99_loss_max):.6f}")

    status = "GO" if not blockers else "NO_GO"
    return GoNoGoDecision(status=status, blockers=blockers)


def evaluate_hardened_go_no_go(
    *,
    metrics: dict[str, Any],
    readiness_thresholds: ReadinessThresholds,
    stability_thresholds: StabilityThresholds,
    pnl_per_trade_std: float,
    hedge_success_rate_std: float,
    critical_errors: list[str] | None = None,
) -> GoNoGoDecision:
    base = evaluate_go_no_go(metrics, readiness_thresholds, critical_errors=critical_errors)
    blockers = list(base.blockers)
    if float(pnl_per_trade_std) > float(stability_thresholds.pnl_per_trade_std_max):
        blockers.append(
            f"pnl_per_trade_std {float(pnl_per_trade_std):.6f} > {float(stability_thresholds.pnl_per_trade_std_max):.6f}"
        )
    if float(hedge_success_rate_std) > float(stability_thresholds.hedge_success_rate_std_max):
        blockers.append(
            "hedge_success_rate_std "
            f"{float(hedge_success_rate_std):.6f} > {float(stability_thresholds.hedge_success_rate_std_max):.6f}"
        )
    status = "GO" if not blockers else "NO_GO"
    return GoNoGoDecision(status=status, blockers=blockers)


def compute_robustness_score(
    *,
    pnl_per_trade: float,
    hedge_success_rate: float,
    hedge_failed_rate: float,
    max_drawdown_pct: float,
    p99_loss: float,
    pnl_per_trade_std: float,
    hedge_success_rate_std: float,
    weights: RobustnessWeights | None = None,
) -> float:
    w = weights or RobustnessWeights()
    variance_penalty = max(0.0, float(pnl_per_trade_std)) + max(0.0, float(hedge_success_rate_std))
    score = (
        (float(w.pnl_per_trade) * float(pnl_per_trade))
        + (float(w.hedge_success_rate) * float(hedge_success_rate))
        - (float(w.hedge_failed_rate) * float(hedge_failed_rate))
        - (float(w.max_drawdown_pct) * (max(0.0, float(max_drawdown_pct)) / 100.0))
        - (float(w.p99_loss) * max(0.0, float(p99_loss)))
        - (float(w.variance_penalty) * variance_penalty)
    )
    return round(float(score), 10)


def evaluate_readiness_from_files(
    *,
    summary_file: str | Path,
    trade_log_file: str | Path,
    reason_codes_file: str | Path,
    thresholds: ReadinessThresholds,
) -> dict[str, Any]:
    critical_errors: list[str] = []
    summary: dict[str, Any] = {}
    trade_log_rows: list[dict[str, str]] = []
    reason_counts: dict[str, int] = {}

    try:
        summary = _load_summary(summary_file)
    except Exception as exc:
        critical_errors.append(f"summary_parse_error: {exc}")
    try:
        trade_log_rows = _load_trade_log(trade_log_file)
    except Exception as exc:
        critical_errors.append(f"trade_log_parse_error: {exc}")
    try:
        reason_counts = _load_reason_codes(reason_codes_file)
    except Exception as exc:
        critical_errors.append(f"reason_codes_parse_error: {exc}")

    if not summary:
        summary = {}
    metrics = compute_metrics(summary=summary, trade_log_rows=trade_log_rows, reason_code_counts=reason_counts)
    decision = evaluate_go_no_go(metrics, thresholds, critical_errors=critical_errors)
    return {
        "status": decision.status,
        "blockers": decision.blockers,
        "thresholds": {
            "hedge_failed_rate_max": thresholds.hedge_failed_rate_max,
            "max_drawdown_pct_max": thresholds.max_drawdown_pct_max,
            "p99_loss_max": thresholds.p99_loss_max,
        },
        "metrics": metrics,
        "critical_errors": critical_errors,
    }
