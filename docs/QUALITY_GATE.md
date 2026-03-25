# Quality Gate

## Comandos obrigatorios
```bash
python -m compileall -q src scripts tests
pytest -q
```

## Criterio de aprovacao
- Compile sem erro.
- Suite de testes verde.
- Sem regressao de contrato de arquivos/config.
