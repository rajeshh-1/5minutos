# LIVE MVP (SOL5M)

## Objetivo
Runner minimo para operacao controlada:
1. valida regra
2. entra na perna 1
3. tenta hedge na perna 2
4. se falhar no timeout, faz unwind imediato da perna 1
5. loga tudo em CSV

## Arquivos
- Config: `configs/live_mvp_sol5m.json`
- Runner: `scripts/run_live_mvp.py`
- Saida: `reports/live_mvp_trades.csv`

## Regras MVP
- mercado: `SOL5M`
- faixa de preco: `0.05 <= ask <= 0.95`
- entrada: `yes_ask + no_ask <= 0.99`
- stake por perna: `1.0`
- maximo por mercado: `1`
- timeout da perna 2: `2000ms`
- se perna 2 falhar: unwind imediato da perna 1
- kill switch diario: `max_daily_loss_usd = 3`

## Como rodar
```bash
python scripts/run_live_mvp.py --config configs/live_mvp_sol5m.json --runtime-sec 0
```

`runtime-sec=0` roda ate Ctrl+C.

## Campos de log (CSV)
- `action`: `enter_leg1`, `hedge_leg2`, `unwind_leg1`, `skip_entry`
- `status`: `open`, `closed`, `blocked`
- `reason_code`: motivo de entrada, bloqueio ou saida
- `pnl_delta`: PnL da acao
- `pnl_total`: PnL acumulado do dia

## Observacao
Este MVP usa os arquivos de orderbook em `data/raw/...` como feed de execucao.
