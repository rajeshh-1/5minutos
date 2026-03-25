# Paper Live Monitoring

## Objetivo
Executar a estrategia em modo paper quase tempo real, lendo dados locais incrementalmente e mantendo status operacional visivel.

## Componentes
- `src/runtime/paper_live_engine.py`
  - loop por ciclos
  - leitura incremental de arquivos locais
  - aplicacao de state machine por mercado
  - snapshots em `reports/live/metrics_live.json`
  - eventos em `reports/live/events.log`
  - CSV simplificado em `reports/live/top_policies_live.csv`
- `src/runtime/checkpoint_store.py`
  - offsets por arquivo em JSON
  - lock por arquivo
  - escrita atomica com temp + replace
- `scripts/run_paper_live.py`
  - runner principal do engine
- `scripts/live_status.py`
  - painel de status em terminal

## Comandos
```bash
python scripts/run_paper_live.py \
  --config configs/paper_sol5m_usd1.json \
  --raw-dir <raw_dir> \
  --interval-sec 2 \
  --runtime-sec 120 \
  --checkpoint-file reports/live/checkpoints.json \
  --out-dir reports/live \
  --seed 42
```

```bash
python scripts/live_status.py \
  --metrics-file reports/live/metrics_live.json \
  --checkpoint-file reports/live/checkpoints.json \
  --refresh-sec 2 \
  --no-clear
```

## Shutdown limpo
- `Ctrl+C` interrompe loop.
- engine grava snapshot final (`engine_status=stopped`).
- checkpoint e offsets sao persistidos no encerramento.

## Indicadores no painel
- status da engine (`running/stopped`)
- throughput por ciclo (prices/trades/orderbook/total)
- hedge_success_rate
- pnl_total
- max_drawdown_pct
- p99_loss
- top reason codes

