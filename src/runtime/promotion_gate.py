from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROMOTE = "PROMOTE"
HOLD = "HOLD"


@dataclass(frozen=True)
class PromotionThresholds:
    pnl_per_trade_min: float = 0.0
    hedge_failed_rate_max: float = 0.02
    max_drawdown_pct_max: float = 12.0
    p99_loss_max: float = 0.40
    pnl_per_trade_std_max: float = 0.02
    hedge_success_rate_std_max: float = 0.10


@dataclass(frozen=True)
class PromotionDecision:
    status: str
    blockers: list[str]
    selected_metrics: dict[str, float]
    inputs: dict[str, str]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _load_optional_json(path: str | Path | None) -> tuple[dict[str, Any], str]:
    if path is None or not str(path).strip():
        return {}, ""
    p = Path(path)
    if not p.exists():
        return {}, str(p)
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{p} must contain a JSON object")
    return payload, str(p)


def _load_required_json(path: str | Path) -> tuple[dict[str, Any], str]:
    p = Path(path)
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{p} must contain a JSON object")
    return payload, str(p)


def _extract_best_config(top_configs_payload: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any]:
    if isinstance(top_configs_payload, list):
        rows = [row for row in top_configs_payload if isinstance(row, dict)]
    elif isinstance(top_configs_payload, dict):
        maybe_rows = top_configs_payload.get("top_configs")
        rows = [row for row in (maybe_rows or []) if isinstance(row, dict)]
    else:
        rows = []
    if not rows:
        return {}
    return rows[0]


def evaluate_promotion_gate(
    *,
    stability_metrics_file: str | Path,
    top_configs_file: str | Path,
    paper_readiness_file: str | Path | None = None,
    thresholds: PromotionThresholds | None = None,
) -> dict[str, Any]:
    th = thresholds or PromotionThresholds()
    blockers: list[str] = []

    stability_payload, stability_path = _load_required_json(stability_metrics_file)
    top_payload_raw = json.loads(Path(top_configs_file).read_text(encoding="utf-8"))
    if not isinstance(top_payload_raw, (dict, list)):
        raise ValueError("top configs file must contain JSON array or object")
    top_path = str(Path(top_configs_file))
    paper_payload, paper_path = _load_optional_json(paper_readiness_file)

    rows_go = int(stability_payload.get("rows_go", 0))
    if rows_go <= 0:
        blockers.append("stability_rows_go_is_zero")

    best_config = _extract_best_config(top_payload_raw)
    if not best_config:
        blockers.append("top_configs_empty")

    paper_status = str(paper_payload.get("status", "")).upper() if paper_payload else ""
    if paper_payload and paper_status != "GO":
        blockers.append("paper_readiness_not_go")

    critical_errors = list(paper_payload.get("critical_errors", [])) if paper_payload else []
    if critical_errors:
        blockers.append("paper_readiness_has_critical_errors")

    # Stability metrics (preferred from top config), with fallback to paper_readiness metrics.
    paper_metrics = paper_payload.get("metrics", {}) if paper_payload else {}
    selected_metrics = {
        "pnl_per_trade": _safe_float(best_config.get("pnl_per_trade_mean"), _safe_float(paper_metrics.get("pnl_per_trade"))),
        "hedge_failed_rate": _safe_float(
            best_config.get("hedge_failed_rate_mean"),
            _safe_float(paper_metrics.get("hedge_failed_rate")),
        ),
        "max_drawdown_pct": _safe_float(
            best_config.get("max_drawdown_pct_mean"),
            _safe_float(paper_metrics.get("max_drawdown_pct")),
        ),
        "p99_loss": _safe_float(best_config.get("p99_loss_mean"), _safe_float(paper_metrics.get("p99_loss"))),
        "pnl_per_trade_std": _safe_float(best_config.get("pnl_per_trade_std"), 0.0),
        "hedge_success_rate_std": _safe_float(best_config.get("hedge_success_rate_std"), 0.0),
    }

    if selected_metrics["pnl_per_trade"] <= float(th.pnl_per_trade_min):
        blockers.append(
            f"pnl_per_trade {selected_metrics['pnl_per_trade']:.6f} <= {float(th.pnl_per_trade_min):.6f}"
        )
    if selected_metrics["hedge_failed_rate"] > float(th.hedge_failed_rate_max):
        blockers.append(
            "hedge_failed_rate "
            f"{selected_metrics['hedge_failed_rate']:.6f} > {float(th.hedge_failed_rate_max):.6f}"
        )
    if selected_metrics["max_drawdown_pct"] > float(th.max_drawdown_pct_max):
        blockers.append(
            "max_drawdown_pct "
            f"{selected_metrics['max_drawdown_pct']:.6f} > {float(th.max_drawdown_pct_max):.6f}"
        )
    if selected_metrics["p99_loss"] > float(th.p99_loss_max):
        blockers.append(f"p99_loss {selected_metrics['p99_loss']:.6f} > {float(th.p99_loss_max):.6f}")
    if selected_metrics["pnl_per_trade_std"] > float(th.pnl_per_trade_std_max):
        blockers.append(
            "pnl_per_trade_std "
            f"{selected_metrics['pnl_per_trade_std']:.6f} > {float(th.pnl_per_trade_std_max):.6f}"
        )
    if selected_metrics["hedge_success_rate_std"] > float(th.hedge_success_rate_std_max):
        blockers.append(
            "hedge_success_rate_std "
            f"{selected_metrics['hedge_success_rate_std']:.6f} > {float(th.hedge_success_rate_std_max):.6f}"
        )

    status = PROMOTE if not blockers else HOLD
    decision = PromotionDecision(
        status=status,
        blockers=blockers,
        selected_metrics=selected_metrics,
        inputs={
            "stability_metrics_file": stability_path,
            "top_configs_file": top_path,
            "paper_readiness_file": paper_path,
        },
    )
    return {
        "status": decision.status,
        "blockers": decision.blockers,
        "selected_metrics": decision.selected_metrics,
        "thresholds": {
            "pnl_per_trade_min": th.pnl_per_trade_min,
            "hedge_failed_rate_max": th.hedge_failed_rate_max,
            "max_drawdown_pct_max": th.max_drawdown_pct_max,
            "p99_loss_max": th.p99_loss_max,
            "pnl_per_trade_std_max": th.pnl_per_trade_std_max,
            "hedge_success_rate_std_max": th.hedge_success_rate_std_max,
        },
        "inputs": decision.inputs,
        "paper_status": paper_status,
        "critical_errors": critical_errors,
    }
