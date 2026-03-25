# Replay Spec

## Input CSV minimo
- `timestamp_utc`
- `market_key`
- `up_price`
- `down_price`
- `seconds_to_close`

## Outputs obrigatorios
- `reports/replay_summary.json`
- `reports/trade_log.csv`
- `reports/reason_codes.csv`

## Metricas minimas
- `trades_attempted`
- `trades_opened`
- `hedged_count`
- `unwind_count`
- `pnl_total`
- distribuicao de reason codes
