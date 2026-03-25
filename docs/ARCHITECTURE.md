# Architecture

## Objetivo
Arquitetura simples, testavel e rastreavel para estrategia SOL5M sem envio real de ordens.

## Modulos
- `src/core`
  - config e validacoes
  - reason codes padronizados
- `src/strategy`
  - hedge math
  - state machine de legging
- `src/runtime`
  - motor de replay e simulacao
- `src/io`
  - leitura CSV e escrita de relatorios

## Principios
- Funcoes pequenas e coesas.
- Sem segredos hardcoded.
- Sem side effects ocultos.
- Logs objetivos por decisao.
