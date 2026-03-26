# STEP 7 - Hardening + Auto-Tuning + Stability Report

## Objetivo
Fortalecer o runtime paper-live, evitar regressao operacional em sessoes longas e adicionar selecao automatica de parametros com criterio de estabilidade entre seeds.

## O que foi adicionado

### 1) Hardening do runtime paper-live
- Deduplicacao de inputs:
  - `raw/sol5m` tem prioridade.
  - arquivos com mesmo nome em `raw/` sao ignorados para evitar leitura dupla.
- Controle de memoria:
  - buffer em memoria de logs de trade com limite configuravel (`--max-log-buffer`).
  - flush automatico para `reports/live/trade_log_live.csv`.
- Sessao envelhecida:
  - timeout por idade de sessao (`--max-session-age-sec`).
  - reason code explicito: `session_age_timeout`.
- Checkpoint resiliente:
  - lock de arquivo.
  - escrita atomica (`tmp + replace`).
  - `version` + `checksum` no payload.
  - fallback seguro para checkpoint legado/corrompido.

### 2) Auto-tuning paper (coarse -> refine)
- Script: `scripts/tune_params.py`
- Parametros tunados:
  - `target_profit_pct`
  - `leg2_timeout_ms`
  - `max_unwind_loss_bps`
  - `entry_cutoff_sec`
- Fluxo:
  - fase coarse para varrer faixa ao redor do config base.
  - fase refine nas melhores configuracoes coarse.
- Suporte multi-seed:
  - `--seed-list 42,99,123`
- Artefatos:
  - `reports/tuning/tuning_results.csv`
  - `reports/tuning/tuning_results.json`
  - `reports/tuning/top_configs.json`

### 3) Score robusto e GO/NO-GO hardened
Score (com pesos explicitos):
- `+ 0.35 * pnl_per_trade`
- `+ 0.25 * hedge_success_rate`
- `- 0.15 * hedge_failed_rate`
- `- 0.10 * (max_drawdown_pct / 100)`
- `- 0.10 * p99_loss`
- `- 0.05 * (pnl_per_trade_std + hedge_success_rate_std)`

GO hardened exige:
- `hedge_failed_rate <= 2%`
- `pnl_per_trade > 0`
- `max_drawdown_pct <= threshold`
- `p99_loss <= threshold`
- estabilidade entre seeds:
  - `pnl_per_trade_std <= threshold`
  - `hedge_success_rate_std <= threshold`

### 4) Stability report
- Script: `scripts/generate_stability_report.py`
- Consolida os resultados do tuning e gera:
  - `reports/stability/stability_metrics.json`
  - `reports/stability/stability_summary.md`
- Seleciona 3 perfis recomendados:
  - conservador
  - moderado
  - agressivo

## Comandos principais
```bash
python scripts/run_paper_live.py --help
python scripts/tune_params.py --help
python scripts/generate_stability_report.py --help
```

### Smoke tuning
```bash
python scripts/tune_params.py \
  --input logs/sample_replay_step4.csv \
  --config configs/paper_sol5m_usd1.json \
  --out-dir reports/tuning \
  --seed-list 42,99 \
  --coarse-limit 4 \
  --top-k 2 \
  --refine-per-top 2 \
  --event-cap 50
```

### Gerar stability report
```bash
python scripts/generate_stability_report.py \
  --tuning-results-json reports/tuning/tuning_results.json \
  --out-dir reports/stability \
  --min-runtime-min 60
```
