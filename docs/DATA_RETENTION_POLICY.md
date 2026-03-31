# Data Retention Policy

Objetivo: manter o repositório enxuto e evitar commit de artefatos pesados de runtime.

## O que fica versionado

- `docs/**`
- `configs/**`
- `scripts/**`
- `tests/**`
- amostras pequenas (ex.: `samples/`, quando existir)

## O que nao fica versionado

- `data/raw/**`
- `reports/live/**`
- `reports/tuning/_runs/**`
- `logs/*.log`
- `archives/runtime/**`

## Politica padrao

Arquivo: `configs/artifact_retention.json`

- `keep_days_raw`: 3
- `keep_days_reports`: 7
- `keep_days_logs`: 7
- `max_keep_files_per_group`: 50
- `dry_run_default`: true
- `archive_dir`: `archives/runtime`

## Como rodar cleanup

Planejamento sem apagar nada:

```bash
python scripts/cleanup_artifacts.py --dry-run true
```

Apagar arquivos antigos:

```bash
python scripts/cleanup_artifacts.py --dry-run false
```

Arquivar (mover para `archives/runtime/cleanup/...`) em vez de apagar:

```bash
python scripts/cleanup_artifacts.py --dry-run false --archive true
```

## Como arquivar para auditoria (zip + checksum)

Somente simular:

```bash
python scripts/archive_runtime_data.py --dry-run true
```

Criar zip e index:

```bash
python scripts/archive_runtime_data.py --dry-run false
```

Criar zip e remover originais apos arquivar:

```bash
python scripts/archive_runtime_data.py --dry-run false --delete-after-archive true
```

Saidas:

- zips em `archives/runtime/YYYY-MM-DD/*.zip`
- checksums SHA256 no `archives/runtime/index.json`

## Rotina recomendada

Diario (cron / agendador):

1. `cleanup_artifacts.py --dry-run false`
2. `archive_runtime_data.py --dry-run false --delete-after-archive true` (opcional, quando precisar auditoria compactada)

Semanal:

1. Revisar `archives/runtime/index.json`
2. Conferir espaco em disco
3. Ajustar `keep_days_*` se necessario
