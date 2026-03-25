from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

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
from src.strategy.hedge_math import is_hedge_entry_valid


class LeggingState(str, Enum):
    IDLE = "IDLE"
    LEG1_OPEN = "LEG1_OPEN"
    HEDGED = "HEDGED"
    UNWIND = "UNWIND"
    CLOSED = "CLOSED"


@dataclass(frozen=True)
class AuditRecord:
    logical_ts: int
    from_state: LeggingState
    to_state: LeggingState
    action: str
    reason_code: str
    detail: str


@dataclass
class LeggingStateMachine:
    config: StrategyConfig
    state: LeggingState = LeggingState.IDLE
    audit_log: list[AuditRecord] = field(default_factory=list)
    leg1_price: Optional[float] = None
    leg1_open_ts: Optional[int] = None
    _logical_clock: int = 0
    _last_unwind_reason: str = UNWIND_ON_TIMEOUT

    def _next_ts(self, logical_ts: int | None = None) -> int:
        if logical_ts is not None:
            if logical_ts > self._logical_clock:
                self._logical_clock = int(logical_ts)
            return int(logical_ts)
        self._logical_clock += 1
        return self._logical_clock

    def _append_record(
        self,
        *,
        action: str,
        reason_code: str,
        detail: str,
        from_state: LeggingState,
        to_state: LeggingState,
        logical_ts: int | None,
    ) -> None:
        self.audit_log.append(
            AuditRecord(
                logical_ts=self._next_ts(logical_ts),
                from_state=from_state,
                to_state=to_state,
                action=action,
                reason_code=reason_code,
                detail=detail,
            )
        )

    def open_leg1(self, *, p1_price: float, seconds_to_close: int, logical_ts: int | None = None) -> bool:
        if self.state != LeggingState.IDLE:
            return False
        if int(seconds_to_close) <= int(self.config.entry_cutoff_sec):
            self._append_record(
                action="open_leg1",
                reason_code=ENTRY_CUTOFF_BLOCKED,
                detail=f"seconds_to_close={seconds_to_close} <= entry_cutoff_sec={self.config.entry_cutoff_sec}",
                from_state=self.state,
                to_state=self.state,
                logical_ts=logical_ts,
            )
            return False
        from_state = self.state
        self.state = LeggingState.LEG1_OPEN
        self.leg1_price = float(p1_price)
        self.leg1_open_ts = self._next_ts(logical_ts)
        self.audit_log.append(
            AuditRecord(
                logical_ts=self.leg1_open_ts,
                from_state=from_state,
                to_state=self.state,
                action="open_leg1",
                reason_code=LEG1_OPENED,
                detail=f"leg1 opened at price={float(p1_price):.8f}",
            )
        )
        return True

    def try_open_leg2(self, *, p2_price: float, logical_ts: int | None = None) -> bool:
        if self.state != LeggingState.LEG1_OPEN or self.leg1_price is None:
            return False
        now_ts = self._next_ts(logical_ts)
        if self.leg1_open_ts is not None:
            elapsed_ms = max(0, (int(now_ts) - int(self.leg1_open_ts)) * 1000)
            if elapsed_ms > int(self.config.leg2_timeout_ms):
                return self.timeout_leg2(logical_ts=now_ts)
        is_valid = is_hedge_entry_valid(
            p1_price=float(self.leg1_price),
            p2_price=float(p2_price),
            target_profit_pct=float(self.config.target_profit_pct),
            estimated_costs=float(self.config.estimated_costs),
            target_mode=self.config.target_mode,
        )
        if is_valid:
            from_state = self.state
            self.state = LeggingState.HEDGED
            self._append_record(
                action="try_open_leg2",
                reason_code=LEG2_HEDGE_FILLED,
                detail=f"leg2 accepted at price={float(p2_price):.8f}",
                from_state=from_state,
                to_state=self.state,
                logical_ts=now_ts,
            )
            return True
        self._append_record(
            action="try_open_leg2",
            reason_code=BELOW_TARGET_PROFIT,
            detail=f"leg2 rejected at price={float(p2_price):.8f}",
            from_state=self.state,
            to_state=self.state,
            logical_ts=now_ts,
        )
        return False

    def timeout_leg2(self, *, logical_ts: int | None = None) -> bool:
        if self.state != LeggingState.LEG1_OPEN:
            return False
        now_ts = self._next_ts(logical_ts)
        self._append_record(
            action="timeout_leg2",
            reason_code=LEG2_NOT_FOUND_TIMEOUT,
            detail="leg2 not found within timeout window",
            from_state=self.state,
            to_state=self.state,
            logical_ts=now_ts,
        )
        from_state = self.state
        self.state = LeggingState.UNWIND
        self._last_unwind_reason = UNWIND_ON_TIMEOUT
        self._append_record(
            action="timeout_leg2",
            reason_code=UNWIND_ON_TIMEOUT,
            detail=f"unwind due to leg2 timeout > {self.config.leg2_timeout_ms}ms",
            from_state=from_state,
            to_state=self.state,
            logical_ts=now_ts,
        )
        return True

    def unwind_on_loss(self, *, observed_loss_bps: float, logical_ts: int | None = None) -> bool:
        if self.state != LeggingState.LEG1_OPEN:
            return False
        if float(observed_loss_bps) < float(self.config.max_unwind_loss_bps):
            return False
        from_state = self.state
        self.state = LeggingState.UNWIND
        self._last_unwind_reason = UNWIND_ON_LOSS_LIMIT
        self._append_record(
            action="unwind_on_loss",
            reason_code=UNWIND_ON_LOSS_LIMIT,
            detail=(
                f"observed_loss_bps={float(observed_loss_bps):.4f} >= "
                f"max_unwind_loss_bps={float(self.config.max_unwind_loss_bps):.4f}"
            ),
            from_state=from_state,
            to_state=self.state,
            logical_ts=logical_ts,
        )
        return True

    def close_position(self, *, logical_ts: int | None = None) -> bool:
        if self.state not in {LeggingState.HEDGED, LeggingState.UNWIND}:
            return False
        from_state = self.state
        self.state = LeggingState.CLOSED
        reason = LEG2_HEDGE_FILLED if from_state == LeggingState.HEDGED else self._last_unwind_reason
        self._append_record(
            action="close_position",
            reason_code=reason,
            detail=f"position closed from state={from_state.value}",
            from_state=from_state,
            to_state=self.state,
            logical_ts=logical_ts,
        )
        return True

