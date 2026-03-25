from __future__ import annotations

LEG1_OPENED = "leg1_opened"
LEG2_HEDGE_FILLED = "leg2_hedge_filled"
LEG2_NOT_FOUND_TIMEOUT = "leg2_not_found_timeout"
UNWIND_ON_LOSS_LIMIT = "unwind_on_loss_limit"
UNWIND_ON_TIMEOUT = "unwind_on_timeout"
BELOW_TARGET_PROFIT = "below_target_profit"
ENTRY_CUTOFF_BLOCKED = "entry_cutoff_blocked"

VALID_REASON_CODES = {
    LEG1_OPENED,
    LEG2_HEDGE_FILLED,
    LEG2_NOT_FOUND_TIMEOUT,
    UNWIND_ON_LOSS_LIMIT,
    UNWIND_ON_TIMEOUT,
    BELOW_TARGET_PROFIT,
    ENTRY_CUTOFF_BLOCKED,
}

