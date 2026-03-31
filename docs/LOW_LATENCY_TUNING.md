# Low Latency Tuning (Live MVP)

Objetivo: reduzir latencia do loop de market data + decisao sem quebrar seguranca operacional.

## Config Principal

No `configs/live_mvp_sol5m.json`:

- `api_top_interval_ms`: cadence de top/midpoint (default `100`)
- `api_book_interval_ms`: cadence de book completo (default `250`)
- `api_trades_interval_ms`: cadence de trades (default `400`)
- `api_request_timeout_ms`: timeout por request (default `350`)
- `api_request_retries`: retries por request no coletor (default `0` em low-latency)
- `api_request_backoff_ms`: backoff por retry no coletor (default `0` em low-latency)
- `api_parallel_workers`: workers paralelos (default `4`)
- `api_max_markets_per_cycle`: limita fanout por ciclo (default `4`)
- `api_max_data_freshness_ms`: bloqueia entrada com snapshot velho (default `1500`)
- `api_skip_unchanged_book`: ignora snapshot igual (default `true`)
- `api_backoff_on_error`: backoff adaptativo por endpoint (default `true`)
- `api_max_rps_guard`: guard rail de RPS total (default `25.0`)

## Loop e Cadencia

- Decisao e disparada por evento: snapshot novo (hash/sinal alterado).
- `WS` e usado como caminho rapido.
- `REST` entra como fallback e para preaquecer cache WS.
- Startup faz warmup da API (resolve mercados e semeia parte dos books no cache WS) para reduzir pico de latencia no primeiro ciclo.
- Midpoint/top e book rodam em cadencias separadas.
- Selecionador de ciclo prioriza mercado pendente e mercado "quente" (mais perto do fechamento).
- Quando WS estiver stale, bot evita bloquear em REST para mercado sem posicao pendente.
- Trades sao consultados em background (nao bloqueiam o loop de decisao) com dedupe por `trade_id + timestamp`.

## Controle de Erro e Rate Limit

- Erros retryable (`timeout`, `429`, `5xx`) aumentam intervalo efetivo temporariamente.
- Sucessos reduzem o backoff gradualmente.
- Guard rail de RPS evita burst acima do teto configurado.

## Metricas de Performance

No heartbeat e no CSV:

- `loop_cycle_ms`
- `data_freshness_ms`
- `decision_latency_ms`
- `p95_api_latency_ms`
- `errors_last_1m`
- `requests_last_1m`
- `skip_unchanged_rate`

## Validacao

```bash
python -m compileall -q src scripts tests
pytest -q tests/test_low_latency_loop.py
python scripts/run_live_mvp.py --help
python scripts/run_live_mvp.py --config configs/live_mvp_sol5m.json --runtime-sec 180 --dry-run true
```

## Meta de Smoke

- `median loop_cycle_ms <= 120ms`
- `p95_api_latency_ms <= 350ms`
- `errors_last_1m < 2%`
- sem crash
