# Strategy Spec (SOL5M)

## Escopo
Estrategia de legging para mercado SOL5M em modo paper-first.

## Objetivos tecnicos
- Entrar apenas quando alvo de lucro atender threshold configurado.
- Revalidar condicoes antes da segunda perna.
- Encerrar por hedge completo ou unwind controlado.

## Fora de escopo nesta fase
- Execucao real de ordens.
- Integracao com credenciais de corretora.
- Automacao live sem gates de seguranca.
