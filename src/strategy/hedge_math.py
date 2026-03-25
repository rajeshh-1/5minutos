from __future__ import annotations

from dataclasses import dataclass

from src.core.config import TARGET_MODE_GROSS, TARGET_MODE_NET, VALID_TARGET_MODES


def _to_decimal_pct(value_pct: float) -> float:
    out = float(value_pct) / 100.0
    if out < 0:
        raise ValueError("percentage must be non-negative")
    return out


def _safe_non_negative(name: str, value: float) -> float:
    out = float(value)
    if out < 0:
        raise ValueError(f"{name} must be >= 0")
    return out


@dataclass(frozen=True)
class HedgeTarget:
    p2_max: float
    target_profit_pct: float
    estimated_costs: float
    target_mode: str


def calculate_leg2_max_price(
    *,
    p1_price: float,
    target_profit_pct: float = 3.0,
    estimated_costs: float = 0.0,
    target_mode: str = TARGET_MODE_NET,
) -> HedgeTarget:
    mode = str(target_mode).strip().lower()
    if mode not in VALID_TARGET_MODES:
        raise ValueError(f"target_mode must be one of {sorted(VALID_TARGET_MODES)}")
    p1 = _safe_non_negative("p1_price", p1_price)
    costs = _safe_non_negative("estimated_costs", estimated_costs)
    target_profit = _to_decimal_pct(float(target_profit_pct))

    # Required formula:
    # p2_max = 1 - target_profit - p1 - estimated_costs
    # Gross mode ignores estimated_costs in the entry threshold.
    costs_component = costs if mode == TARGET_MODE_NET else 0.0
    p2_max = 1.0 - target_profit - p1 - costs_component
    return HedgeTarget(
        p2_max=float(p2_max),
        target_profit_pct=float(target_profit_pct),
        estimated_costs=float(costs),
        target_mode=mode,
    )


def is_hedge_entry_valid(
    *,
    p1_price: float,
    p2_price: float,
    target_profit_pct: float = 3.0,
    estimated_costs: float = 0.0,
    target_mode: str = TARGET_MODE_NET,
) -> bool:
    p2 = _safe_non_negative("p2_price", p2_price)
    target = calculate_leg2_max_price(
        p1_price=p1_price,
        target_profit_pct=target_profit_pct,
        estimated_costs=estimated_costs,
        target_mode=target_mode,
    )
    return p2 <= target.p2_max

