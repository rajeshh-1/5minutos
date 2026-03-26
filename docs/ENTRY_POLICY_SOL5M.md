# ENTRY POLICY SOL5M

## Objetivo
Aplicar filtro de entrada por regime para reduzir entradas ruins no paper runtime.

## Regras de entrada
- `sum_ask = yes_ask + no_ask`
- Regimes:
  - `HIGH_EDGE`: `sum_ask <= 0.95`
  - `MID_EDGE`: `0.95 < sum_ask <= 0.98`
  - `LOW_EDGE`: `0.98 < sum_ask <= 0.99`
  - `NO_TRADE`: `sum_ask > 0.99`
- Filtros obrigatorios antes da entrada:
  - `seconds_to_close > 75`
  - `spread_yes <= 0.02`
  - `spread_no <= 0.02`
  - `depth_yes >= stake * 1.2`
  - `depth_no >= stake * 1.2`

## Timeouts por regime
- `HIGH_EDGE`: `1200ms`
- `MID_EDGE`: `2500ms`
- `LOW_EDGE`: `3000ms`

## Protecoes
- `max_trades_per_market = 1`
- `max_unwind_loss_bps = 120` (default da politica)

## Config
Arquivo: `configs/entry_policy_sol5m.json`

## Logs
O `trade_log_live.csv` passa a incluir:
- `regime`
- `sum_ask`
- `entry_mode`
- `entry_policy_detail`

Reason codes de decisao:
- `no_trade_sum_ask`
- `blocked_by_spread`
- `blocked_by_depth`
- `blocked_by_cutoff`
- `entered_high_edge`
- `entered_mid_edge`
- `entered_low_edge`
