# LIVE Rollout Playbook (Controlled)

## Objetivo
Executar promocao paper -> live com gate formal, sem habilitar execucao real automaticamente.

## Regras de seguranca
- `live` fica bloqueado por padrao.
- Para `live`, os dois requisitos sao obrigatorios:
  - `--enable-live true`
  - `--confirm-live I_UNDERSTAND_THE_RISK`
- Se gate estiver `HOLD`, `live` e bloqueado automaticamente.

## Checklist pre-live
- Gate atualizado e valido:
  - `reports/stability/stability_metrics.json`
  - `reports/tuning/top_configs.json`
  - `reports/paper_readiness.json` (quando existir)
- Config de rollout revisada (`configs/live_rollout_template.json`)
- Kill switch e circuit breaker habilitados
- Runtime inicial curto definido (ex: 300s)
- Plano de rollback preparado

## 1) Avaliar promotion gate
```bash
python scripts/evaluate_promotion_gate.py \
  --stability-metrics reports/stability/stability_metrics.json \
  --top-configs reports/tuning/top_configs.json \
  --paper-readiness reports/paper_readiness.json \
  --out-file reports/live/promotion_gate.json
```

Saida:
- `PROMOTE` ou `HOLD`
- lista de blockers
- `reports/live/promotion_gate.json`

## 2) Rollout paper (seguro)
```bash
python scripts/run_controlled_rollout.py \
  --config configs/live_rollout_template.json \
  --promotion-gate-file reports/live/promotion_gate.json \
  --mode paper \
  --enable-live false \
  --runtime-sec 300 \
  --out-dir reports/live
```

## 3) Rollout shadow
```bash
python scripts/run_controlled_rollout.py \
  --config configs/live_rollout_template.json \
  --promotion-gate-file reports/live/promotion_gate.json \
  --mode shadow \
  --enable-live false \
  --runtime-sec 300 \
  --out-dir reports/live
```

## 4) Micro-live candidate (somente com gate PROMOTE + confirmacao dupla)
```bash
python scripts/run_controlled_rollout.py \
  --config configs/live_rollout_template.json \
  --promotion-gate-file reports/live/promotion_gate.json \
  --mode live \
  --enable-live true \
  --confirm-live I_UNDERSTAND_THE_RISK \
  --runtime-sec 180 \
  --out-dir reports/live
```

Observacao:
- Nesta fase o runner mantem envio real desabilitado por padrao de seguranca do projeto.
- Objetivo do modo `live` aqui e validar gate + controles operacionais.

## Critérios de abort imediato
- `hedge_failed_rate` acima do limite operacional
- `max_drawdown_pct` acima do limite operacional
- `p99_loss` acima do limite operacional
- qualquer erro operacional critico no preflight

## Rollback em 1 comando
```bash
python scripts/run_controlled_rollout.py \
  --config configs/live_rollout_template.json \
  --promotion-gate-file reports/live/promotion_gate.json \
  --mode paper \
  --enable-live false \
  --runtime-sec 60 \
  --out-dir reports/live
```

## Artefato principal de sessao
- `reports/live/rollout_session.json`
  - `kill_switch_status`
  - `rollback_triggered`
  - `rollback_reason_codes`
  - `final_status`
