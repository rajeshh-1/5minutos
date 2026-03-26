# Execution Realism (Pessimistic)

## Objetivo
Adicionar friccao de execucao no dry run para aproximar cenarios de PRD sem enviar ordens reais.

## Componentes
- `src/runtime/execution_realism.py`
  - modelo `pessimistic` com:
    - latencia amostrada
    - fill ratio parcial
    - slippage por profundidade/volatilidade
    - cancelamento simulado
    - falha de hedge simulada
    - penalidade de janela de fechamento
- `configs/execution_realism_pessimistic.json`
  - parametros default conservadores

## Integracao
- `src/runtime/replay_engine.py`
  - ativa realism automaticamente quando CSV possui colunas sintéticas:
    - `synthetic_depth`
    - `synthetic_volatility`
  - novos campos no `trade_log.csv`:
    - `simulated_latency_ms`
    - `simulated_fill_ratio`
    - `simulated_slippage_bps`
    - `simulated_cancelled`
    - `simulated_hedge_fail`
  - novas metricas no `replay_summary.json`:
    - `partial_fill_rate`
    - `cancel_rate`
    - `hedge_fail_rate_simulated`
    - `avg_slippage_bps`
    - `p95_latency_ms`

- `src/runtime/paper_live_engine.py`
  - integracao minima da camada pessimista para latencia/fill/slippage/cancel/falha.

## Geracao de dados sintéticos
Script:
- `scripts/generate_synthetic_sol5m_data.py`

Saida:
- `data/synthetic/sol5m_synth_replay.csv`

Colunas:
- `timestamp_utc`
- `market_key`
- `up_price`
- `down_price`
- `seconds_to_close`
- `synthetic_depth`
- `synthetic_volatility`

CLI:
```bash
python scripts/generate_synthetic_sol5m_data.py --minutes 120 --seed 42 --out-file data/synthetic/sol5m_synth_replay.csv --regime mixed
```

## Fluxo recomendado sem dados reais
```bash
python scripts/generate_synthetic_sol5m_data.py --minutes 120 --seed 42 --out-file data/synthetic/sol5m_synth_replay.csv
python scripts/run_replay.py --input data/synthetic/sol5m_synth_replay.csv --config configs/paper_sol5m_usd1.json --out-dir reports/synth_replay_seed42 --seed 42
```
