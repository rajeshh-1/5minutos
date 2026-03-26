# BACKFILL TERMINAL B (SOL5M CLOSED MARKET)

## Objective
Terminal B runs continuously and backfills trades for each newly closed SOL5M market, then reconciles backfill vs realtime capture.

## Inputs
- `reports/live/last_closed_market.json` (written by Terminal A)
- `data/raw/sol5m/trades_YYYY-MM-DD.jsonl` (realtime trades stream)
- `configs/data_collection_sol5m.json` (API hosts + retry config)

Expected `last_closed_market.json` payload:

```json
{
  "market_key": "SOL5M_2026-03-26T03:20:00Z",
  "condition_id": "0xabc123...",
  "close_time_utc": "2026-03-26T03:20:00Z"
}
```

Terminal A can publish this file with:

```bash
python scripts/collect_sol5m_real.py \
  --config configs/data_collection_sol5m.json \
  --raw-dir data/raw \
  --last-closed-file reports/live/last_closed_market.json
```

## Outputs
- `data/raw/sol5m/backfill/trades_<condition_id>.jsonl`
- `data/raw/sol5m/backfill/meta_<condition_id>.json`
- `reports/live/reconciliation_<condition_id>.json`
- `reports/live/backfill_state.json`

## Run Loop (Terminal B)
```bash
python scripts/run_backfill_loop.py \
  --poll-sec 20 \
  --backfill-interval-sec 300 \
  --page-size 500 \
  --max-pages 0 \
  --raw-dir data/raw \
  --state-file reports/live/backfill_state.json
```

Notes:
- `--max-pages 0` means fetch until pagination end.
- `--backfill-interval-sec 300` waits 5 minutes after market close before backfill.
- Loop runs until `Ctrl+C`.

## Run Single Backfill
```bash
python scripts/backfill_market_trades.py \
  --condition-id 0xabc123... \
  --market-key SOL5M_2026-03-26T03:20:00Z \
  --close-time-utc 2026-03-26T03:20:00Z \
  --raw-dir data/raw \
  --page-size 500 \
  --max-pages 0
```

## Reconciliation Fields
Generated file `reports/live/reconciliation_<condition_id>.json` includes:
- `realtime_count`
- `backfill_count`
- `overlap_count`
- `missing_in_realtime`
- `coverage_pct`

`coverage_pct` formula:
- `overlap_count / backfill_count * 100`

## Failure Handling
- API failures use retry/backoff from `configs/data_collection_sol5m.json`.
- Errors do not crash the loop; the cycle logs warning and continues.
- Processed markets are tracked in `backfill_state.json` to avoid reprocessing.
