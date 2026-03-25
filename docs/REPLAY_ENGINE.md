# Replay Engine

## Objetivo
Executar estrategia de legging de forma offline em CSV, sem ordens reais, e gerar artefatos auditaveis.

## Input CSV minimo
- `timestamp_utc`
- `market_key`
- `up_price`
- `down_price`
- `seconds_to_close`

## CLI
```bash
python scripts/run_replay.py \
  --input <path_csv> \
  --config configs/paper_sol5m_usd1.json \
  --out-dir reports \
  --seed 42 \
  --event-cap 0 \
  --leg1-side auto
```

## Outputs
- `replay_summary.json`
- `trade_log.csv`
- `reason_codes.csv`

## Regras principais
- reutiliza `state_machine` do STEP 3
- hedge somente quando `p2 <= p2_max` (via `hedge_math`)
- timeout/perda podem levar para `UNWIND`
- toda transicao vira linha auditavel com `reason_code`

