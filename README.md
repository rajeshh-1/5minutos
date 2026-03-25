# SOL5M Legging (Paper-First)

Projeto novo focado em estrategia de arbitragem/legging para mercado SOL5M, com desenvolvimento em etapas, validacao por testes e zero execucao real nesta fase.

## Objetivo
- Construir stack paper-first para avaliar viabilidade de legging em SOL5M.
- Controlar risco com guardrails explicitos.
- Evoluir por STEPs com entregas verificaveis.

## Estrutura
- `docs/`: especificacoes e guias operacionais
- `src/core`: config, reason codes e utilitarios centrais
- `src/strategy`: hedge math e state machine
- `src/runtime`: replay/simulacao
- `src/io`: formatos e persistencia
- `scripts/`: entrypoints de validacao e replay
- `tests/`: suite de testes
- `configs/`: configuracoes paper

## Documentacao
Ver `docs/INDEX.md` para lista completa de documentos do projeto.
