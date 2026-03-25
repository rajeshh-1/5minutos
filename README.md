# SOL5M Legging (Paper-First)

Projeto base para estrategia de arbitragem/legging em SOL5M com foco em:
- simulacao e replay antes de qualquer live
- controle de risco e rastreabilidade
- evolucao incremental por STEPs

## Estrutura inicial
- `src/core`: modelos e utilitarios centrais
- `src/strategy`: logica de estrategia e state machine
- `src/runtime`: motores de execucao/replay
- `src/io`: entrada/saida de dados
- `scripts`: CLIs e automacoes
- `tests`: testes automatizados
- `configs`: configuracoes versionadas
- `logs`, `reports`: artefatos operacionais

## Validacao (STEP 1)
```bash
python -m compileall -q src scripts tests
pytest -q
```
