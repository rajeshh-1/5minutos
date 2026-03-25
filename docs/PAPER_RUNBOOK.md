# Paper Runbook

## Setup
1. Preparar config paper.
2. Validar ambiente com quality gate.
3. Rodar replay em dataset curto.

## Fluxo diario
1. Atualizar dados de replay.
2. Rodar simulacao.
3. Avaliar summary + reason codes.
4. Ajustar parametros e repetir.

## Comandos base
```bash
python -m compileall -q src scripts tests
pytest -q
# (futuro) python scripts/run_replay.py --input <csv>
```
