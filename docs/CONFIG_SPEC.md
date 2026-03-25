# Config Spec

## Arquivo base
`configs/paper_sol5m_usd1.json`

## Campos minimos esperados
- `execution_mode`: `paper`
- `market_scope`: `SOL5M`
- `stake_usd_per_leg`: `1.0`
- `target_profit_pct`: `>= 3.0`
- `max_trades_per_market`: `1`
- `max_open_positions`: `1`
- `entry_cutoff_sec`
- `leg2_timeout_ms`
- `max_unwind_loss_bps`

## Regras
- Tipos estritos (numero/bool/string/lista).
- Valores fora de range devem falhar no startup.
- Nenhuma credencial no arquivo de config.
