from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExecutionRealismConfig:
    exec_model: str = "pessimistic"
    latency_mean_ms: int = 250
    latency_p95_ms: int = 1200
    cancel_prob_base: float = 0.08
    partial_fill_prob_base: float = 0.25
    hedge_fail_prob_base: float = 0.07
    slippage_base_bps: float = 12.0
    slippage_depth_alpha: float = 35.0
    close_window_sec: int = 45
    close_window_penalty_mult: float = 1.8
    min_fill_ratio: float = 0.35

    def __post_init__(self) -> None:
        if str(self.exec_model).strip().lower() != "pessimistic":
            raise ValueError("exec_model must be 'pessimistic'")
        if int(self.latency_mean_ms) <= 0:
            raise ValueError("latency_mean_ms must be > 0")
        if int(self.latency_p95_ms) < int(self.latency_mean_ms):
            raise ValueError("latency_p95_ms must be >= latency_mean_ms")
        for field_name in ("cancel_prob_base", "partial_fill_prob_base", "hedge_fail_prob_base"):
            value = float(getattr(self, field_name))
            if value < 0.0 or value > 1.0:
                raise ValueError(f"{field_name} must be in [0, 1]")
        if float(self.slippage_base_bps) < 0.0:
            raise ValueError("slippage_base_bps must be >= 0")
        if float(self.slippage_depth_alpha) < 0.0:
            raise ValueError("slippage_depth_alpha must be >= 0")
        if int(self.close_window_sec) < 0:
            raise ValueError("close_window_sec must be >= 0")
        if float(self.close_window_penalty_mult) < 1.0:
            raise ValueError("close_window_penalty_mult must be >= 1")
        if float(self.min_fill_ratio) <= 0.0 or float(self.min_fill_ratio) > 1.0:
            raise ValueError("min_fill_ratio must be in (0, 1]")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExecutionRealismConfig":
        return cls(
            exec_model=str(payload.get("exec_model", "pessimistic")).strip().lower(),
            latency_mean_ms=int(payload.get("latency_mean_ms", 250)),
            latency_p95_ms=int(payload.get("latency_p95_ms", 1200)),
            cancel_prob_base=float(payload.get("cancel_prob_base", 0.08)),
            partial_fill_prob_base=float(payload.get("partial_fill_prob_base", 0.25)),
            hedge_fail_prob_base=float(payload.get("hedge_fail_prob_base", 0.07)),
            slippage_base_bps=float(payload.get("slippage_base_bps", 12.0)),
            slippage_depth_alpha=float(payload.get("slippage_depth_alpha", 35.0)),
            close_window_sec=int(payload.get("close_window_sec", 45)),
            close_window_penalty_mult=float(payload.get("close_window_penalty_mult", 1.8)),
            min_fill_ratio=float(payload.get("min_fill_ratio", 0.35)),
        )


def load_execution_realism_config(path: str | Path) -> ExecutionRealismConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("execution realism config must be a JSON object")
    return ExecutionRealismConfig.from_dict(payload)


def load_execution_realism_config_if_exists(path: str | Path) -> ExecutionRealismConfig:
    cfg_path = Path(path)
    if not cfg_path.exists():
        return ExecutionRealismConfig()
    return load_execution_realism_config(cfg_path)


def _clip(value: float, *, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


class ExecutionRealismLayer:
    def __init__(self, config: ExecutionRealismConfig, *, seed: int = 42) -> None:
        self.config = config
        self.rng = random.Random(int(seed))

        ratio = max(1.0001, float(config.latency_p95_ms) / max(1.0, float(config.latency_mean_ms)))
        self._lat_sigma = max(0.01, math.log(ratio) / 1.645)
        self._lat_mu = math.log(max(1.0, float(config.latency_mean_ms))) - (0.5 * self._lat_sigma * self._lat_sigma)

    def _close_penalty(self, *, seconds_to_close: int) -> float:
        if int(seconds_to_close) <= int(self.config.close_window_sec):
            return float(self.config.close_window_penalty_mult)
        return 1.0

    def apply_close_window_penalty(self, value: float, *, seconds_to_close: int) -> float:
        return float(value) * self._close_penalty(seconds_to_close=seconds_to_close)

    def sample_latency_ms(self, *, volatility: float = 0.0, seconds_to_close: int = 9999) -> int:
        base = self.rng.lognormvariate(self._lat_mu, self._lat_sigma)
        vol_mult = 1.0 + max(0.0, float(volatility)) * 0.5
        lat = base * vol_mult
        lat = self.apply_close_window_penalty(lat, seconds_to_close=seconds_to_close)
        return int(max(1, round(lat)))

    def estimate_fill_ratio(
        self,
        *,
        order_size: float,
        depth: float,
        volatility: float = 0.0,
        seconds_to_close: int = 9999,
    ) -> float:
        safe_size = max(1e-9, float(order_size))
        safe_depth = max(1e-9, float(depth))
        depth_ratio = safe_depth / safe_size
        base_fill = _clip(depth_ratio / (1.0 + max(0.0, float(volatility))), lo=0.0, hi=1.0)

        partial_prob = float(self.config.partial_fill_prob_base) * self._close_penalty(seconds_to_close=seconds_to_close)
        partial_prob = _clip(partial_prob * (1.0 + max(0.0, float(volatility))), lo=0.0, hi=0.98)
        if self.rng.random() < partial_prob:
            haircut = 0.35 + (0.5 * self.rng.random())
            fill = base_fill * haircut
        else:
            fill = base_fill

        if 0.0 < fill < float(self.config.min_fill_ratio):
            fill = float(self.config.min_fill_ratio)
        if int(seconds_to_close) <= int(self.config.close_window_sec):
            fill = fill / max(1.0, float(self.config.close_window_penalty_mult))
        return _clip(fill, lo=0.0, hi=1.0)

    def estimate_slippage_bps(
        self,
        *,
        order_size: float,
        depth: float,
        volatility: float = 0.0,
        seconds_to_close: int = 9999,
    ) -> float:
        safe_depth = max(1e-9, float(depth))
        safe_size = max(1e-9, float(order_size))
        depth_component = float(self.config.slippage_depth_alpha) * (safe_size / safe_depth)
        vol_component = max(0.0, float(volatility)) * 100.0
        bps = float(self.config.slippage_base_bps) + depth_component + vol_component
        bps = self.apply_close_window_penalty(bps, seconds_to_close=seconds_to_close)
        return max(0.0, float(bps))

    def should_cancel(
        self,
        *,
        volatility: float = 0.0,
        seconds_to_close: int = 9999,
        fill_ratio: float = 1.0,
    ) -> bool:
        prob = float(self.config.cancel_prob_base)
        prob = prob * (1.0 + max(0.0, float(volatility)))
        prob = prob * self._close_penalty(seconds_to_close=seconds_to_close)
        prob = prob + max(0.0, 1.0 - float(fill_ratio)) * 0.2
        return self.rng.random() < _clip(prob, lo=0.0, hi=0.95)

    def should_hedge_fail(
        self,
        *,
        volatility: float = 0.0,
        seconds_to_close: int = 9999,
        fill_ratio: float = 1.0,
    ) -> bool:
        prob = float(self.config.hedge_fail_prob_base)
        prob = prob * (1.0 + max(0.0, float(volatility)))
        prob = prob * self._close_penalty(seconds_to_close=seconds_to_close)
        prob = prob + max(0.0, 1.0 - float(fill_ratio)) * 0.25
        return self.rng.random() < _clip(prob, lo=0.0, hi=0.95)
