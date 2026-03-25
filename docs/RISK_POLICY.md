# Risk Policy

## Guardrails paper-first
- `stake_usd_per_leg = 1.0`
- `max_trades_per_market = 1`
- `max_open_positions = 1`
- `target_profit_pct >= 3.0`
- `require_two_leg_feasibility = true`

## Bloqueios
- Entrada negada se lucro alvo nao fecha.
- Entrada negada proximo do cutoff.
- Unwind obrigatorio em timeout/perda alem do limite.
