# Architecture (STEP 1)

## Objetivo
Base limpa para estrategia SOL5M em modo paper-first, sem envio real de ordens.

## Camadas
- `core`: contratos de dados, config, reason codes
- `strategy`: decisao de entrada/hedge/unwind
- `runtime`: orquestracao de replay/simulacao
- `io`: leitura/escrita de fontes e relatorios

## Principios
- baixo acoplamento
- funcoes pequenas e testaveis
- logs explicitos
- workflow orientado a testes
