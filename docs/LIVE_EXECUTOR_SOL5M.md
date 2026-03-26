# LIVE Executor SOL5M

## Objetivo
Adicionar execucao real de ordens Polymarket ao runner MVP com travas explicitas.

## Arquivos
- `scripts/run_live_mvp.py`
- `src/execution/polymarket_auth.py`
- `src/execution/polymarket_live_executor.py`
- `src/core/env_loader.py`
- `.env.example`

## Modos
- `--dry-run true` (default): nao envia ordens reais.
- `--enable-live true --dry-run false`: envia ordens reais (somente com confirmacao valida).

## Confirmacao obrigatoria para live
Use exatamente:
`--confirm-live I_UNDERSTAND_THE_RISK`

Sem esse token, o runner aborta.

## Variaveis obrigatorias (live real)
- `POLY_API_KEY`
- `POLY_PASSPHRASE`
- `POLY_SECRET` (ou `POLY_API_SECRET`)
- `POLY_ADDRESS` (aceita fallback `POLY_FUNDER`)
- `POLY_PRIVATE_KEY`

## Fluxo de execucao
1. valida regra de entrada
2. envia perna 1 (BUY)
3. tenta hedge perna 2 (BUY) por `leg2_timeout_ms`
4. se hedge falhar, executa unwind da perna 1 (SELL)
5. loga tudo em CSV

## Regras de risco mantidas
- `max_trades_per_market=1`
- `max_daily_loss_usd`
- kill-switch em erro critico de execucao

## Comandos
Dry-run:
```bash
python scripts/run_live_mvp.py --config configs/live_mvp_sol5m.json --runtime-sec 120 --dry-run true
```

Live real (apenas quando aprovado):
```bash
python scripts/run_live_mvp.py --config configs/live_mvp_sol5m.json --runtime-sec 120 --enable-live true --dry-run false --confirm-live I_UNDERSTAND_THE_RISK
```

## Auditoria no CSV
O report inclui:
- `order_id_leg1`, `order_id_leg2`
- `fill_status_leg1`, `fill_status_leg2`
- `hedge_latency_ms`
- `unwind_triggered`
- `reason_code`
- `pnl_delta`, `pnl_total`
