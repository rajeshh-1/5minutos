# Hedge Math Spec

## Parametros
- `target_profit_pct` (default 3.0)
- `p1_price`
- `estimated_costs`
- `target_mode` (`gross` ou `net`)

## Formula principal
`p2_max = 1 - target_profit - p1 - estimated_costs`

Onde:
- `target_profit = target_profit_pct / 100`
- `estimated_costs` agrega fee + slippage + risco de leg

## Regras
- Se `p2_max <= 0`, entrada invalida.
- Em modo `net`, custos entram explicitamente no calculo.
- Em modo `gross`, custos podem ser tratados separadamente no scoring.
