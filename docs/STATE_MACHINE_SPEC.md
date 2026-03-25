# State Machine Spec

## Estados
- `IDLE`
- `LEG1_OPEN`
- `HEDGED`
- `UNWIND`
- `CLOSED`

## Fluxos validos
- `IDLE -> LEG1_OPEN -> HEDGED -> CLOSED`
- `IDLE -> LEG1_OPEN -> UNWIND -> CLOSED`

## Reason codes minimos
- `leg1_opened`
- `leg2_hedge_filled`
- `leg2_not_found_timeout`
- `unwind_on_loss_limit`
- `unwind_on_timeout`
- `below_target_profit`
- `entry_cutoff_blocked`
