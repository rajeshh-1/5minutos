# Real Data Collection (SOL5M)

## Objetivo
Coletar dados reais de mercado (somente leitura) para calibracao de execucao:
- prices
- orderbook
- trades

Sem execucao de ordens.

## Arquivos
- `scripts/collect_sol5m_real.py`
- `src/io/market_data_collector.py`
- `src/io/storage_writer.py`
- `configs/data_collection_sol5m.json`

## Saida (append-only)
Pasta base: `data/raw/sol5m/`
- `prices_YYYY-MM-DD.jsonl`
- `orderbook_YYYY-MM-DD.jsonl`
- `trades_YYYY-MM-DD.jsonl`
- `metadata_YYYY-MM-DD.json`

## Campos minimos
### prices
- `timestamp_utc`
- `market_key`
- `best_bid`
- `best_ask`
- `midpoint`
- `spread`
- `source`

### orderbook
- `timestamp_utc`
- `market_key`
- `bids`
- `asks`
- `depth_used`
- `source`

### trades
- `timestamp_utc`
- `market_key`
- `trade_id`
- `side`
- `price`
- `size`
- `source`

## Metadata continuo
Atualiza em cada ciclo:
- `rows_prices`
- `rows_orderbook`
- `rows_trades`
- `errors_count`
- `warnings`
- `discarded_rows`
- `discard_reasons`
- `started_at_utc`
- `last_update_utc`
- `collector_uptime_sec`

## Execucao
```bash
python scripts/collect_sol5m_real.py \
  --config configs/data_collection_sol5m.json \
  --raw-dir data/raw \
  --interval-sec 1.0 \
  --runtime-sec 120 \
  --book-depth 15 \
  --trade-limit 150
```

Opcoes uteis:
- `--no-orderbook`
- `--no-trades`
- `--checkpoint-file reports/live/collector_checkpoint.json`

## Robustez implementada
- retry/backoff em HTTP GET
- Ctrl+C com shutdown limpo
- flush por ciclo (append + close)
- rollover UTC por nome de arquivo diario
- descarte de linhas invalidas com motivo em metadata
