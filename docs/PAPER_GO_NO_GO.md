# Paper GO/NO_GO

## Metricas
- `trades_attempted`: tentativas de abertura de leg1.
- `leg1_opened`: leg1 efetivamente aberta.
- `leg2_hedged`: leg2 preenchida sob criterio de hedge.
- `hedge_success_rate`: `leg2_hedged / leg1_opened`.
- `unwind_count`: quantidade de transicoes para `UNWIND`.
- `pnl_total`: soma de `pnl_estimate` em fechamentos.
- `pnl_per_trade`: `pnl_total / closed_trades`.
- `max_drawdown_pct`: drawdown maximo na curva de PnL acumulada.
- `p95_loss`, `p99_loss`: quantis das perdas por trade (unidade de stake).
- `hedge_failed_rate`: `(leg1_opened - leg2_hedged) / leg1_opened`.
- `timeout_rate`: trades com timeout (`leg2_not_found_timeout` ou `unwind_on_timeout`) / `leg1_opened`.
- `below_target_profit_rate`: trades com `below_target_profit` / `leg1_opened`.

## Regra GO/NO_GO (padrao)
GO apenas se TODOS:
- `hedge_failed_rate <= 0.02`
- `pnl_per_trade > 0`
- `max_drawdown_pct <= 12.0`
- `p99_loss <= 0.40`
- sem erro critico de parsing/execucao

Caso contrario: `NO_GO` com `blockers`.

## Script
```bash
python scripts/evaluate_paper_readiness.py \
  --summary-file reports/step4_example/replay_summary.json \
  --trade-log-file reports/step4_example/trade_log.csv \
  --reason-codes-file reports/step4_example/reason_codes.csv
```

Saida:
- stdout: status final e blockers
- `reports/paper_readiness.json`

## Checklist pre-live supervisionado
1. Replay consistente em mais de uma janela de dados.
2. GO repetido em seeds/cortes diferentes.
3. Blockers zerados.
4. Revisao manual de reason codes dominantes.

