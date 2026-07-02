# Crypto Market Data Puller

Small Rust CLI for pulling public Bitfinex BTC trade history to daily Parquet files and recording realtime websocket market data to compressed raw files.

The default symbol is `tBTCUSD`. Files are written under a normalized symbol directory, so `tBTCUSD` writes to `./tbtcusd/`.

Without `--start`, the tool fetches one recent page. With `--start`, it pages forward in ascending timestamp order until it reaches `--end`, receives a short page, or hits `--max-trades`.

## Usage

Fetch a small recent sample:

```sh
cargo run -- --limit 3 --max-trades 3
```

Pull a bounded historical window:

```sh
cargo run -- \
  --start 2026-06-14T00:00:00Z \
  --end 2026-06-16T00:00:00Z
```

Keep the request rate conservative:

```sh
cargo run -- --start 2026-06-14T00:00:00Z --rpm 10
```

Resume a long pull from the last completed daily file:

```sh
cargo run --release -- \
  --symbol tBTCUSD \
  --start 2021-01-01T00:00:00Z \
  --rpm 10 \
  --output-dir /mnt/burner-archive/bitfinex
```

The same command can be rerun after an interruption. Use `--ignore-checkpoint` to force the command to start from `--start`.

Write the partitioned output tree under another directory:

```sh
cargo run -- --start 2026-06-14T00:00:00Z --output-dir data
```

## Realtime Websocket Capture

The `stream` subcommand records raw websocket messages first. This keeps the source feed intact while we are still deciding which normalized features to build.

Record the stable BTC websocket venues to daily compressed JSONL under `/mnt/burner-archive/ws_raw`. This shortcut records Bitfinex, Hibachi, Deribit, and Hyperliquid; Extended is excluded because its public orderbook websocket currently drops with server ping timeouts.

```sh
cargo run --release btc
```

When passing extra recorder flags through Cargo, use `--` before the binary arguments:

```sh
cargo run --release -- btc \
  --output-dir /mnt/burner-archive/ws_raw \
  --max-messages 2
```

Record Bitfinex depth-25 books and trades:

```sh
cargo run --release -- stream \
  --venue bitfinex \
  --bitfinex-symbol tBTCUSD \
  --output-dir /mnt/burner-archive/ws_raw
```

Record Extended perp orderbook, trades, funding, mark price, and index price streams, plus spot BTC orderbook and trades:

```sh
cargo run --release -- stream \
  --venue extended \
  --extended-market BTC-USD \
  --extended-spot-market BTCSPOT-USD \
  --output-dir /mnt/burner-archive/ws_raw
```

Extended's public orderbook stream is captured raw. Build top-25 depth features from the full orderbook stream during normalization.

Record Bitfinex and Extended at the same time:

```sh
cargo run --release -- stream \
  --venue bitfinex,extended \
  --bitfinex-symbol tBTCUSD \
  --extended-market BTC-USD \
  --extended-spot-market BTCSPOT-USD \
  --output-dir /mnt/burner-archive/ws_raw
```

Record Hibachi with the multi-topic subscription:

```sh
cargo run --release -- stream \
  --venue hibachi \
  --hibachi-symbol BTC/USDT-P \
  --output-dir /mnt/burner-archive/ws_raw
```

Override the Hibachi websocket endpoint with `--hibachi-url` or `HIBACHI_WS_URL` if their environment changes.

Record Deribit BTC futures and options. On startup the recorder fetches current BTC futures/options with `public/get_instruments`, subscribes to each instrument's incremental ticker and trade streams, and keeps the lifecycle subscriptions open so newly-created instruments are added automatically:

```sh
cargo run --release -- stream \
  --venue deribit \
  --deribit-kind future,option \
  --output-dir /mnt/burner-archive/ws_raw
```

Record all supported websocket venues at the same time:

```sh
cargo run --release -- stream \
  --venue bitfinex,extended,hibachi,deribit,hyperliquid \
  --bitfinex-symbol tBTCUSD \
  --extended-market BTC-USD \
  --extended-spot-market BTCSPOT-USD \
  --hibachi-symbol BTC/USDT-P \
  --deribit-kind future,option \
  --output-dir /mnt/burner-archive/ws_raw
```

Override the Deribit websocket endpoint with `--deribit-url` or `DERIBIT_WS_URL`. Deribit trade follow-up subscriptions use `--deribit-trades-interval 100ms` by default; allowed values are `raw`, `100ms`, and `agg2`. The Deribit recorder enables Deribit's JSON-RPC heartbeat on connect and answers `test_request` messages with `public/test`; websocket protocol pings are best-effort on this feed so delayed pong control frames do not force reconnects while the high-channel market-data stream is still active.

Record Hyperliquid BTC spot trades and order book updates through `hypersdk`. By default the recorder resolves the current BTC spot market from Hyperliquid spot metadata, currently `UBTC/USDC`:

```sh
cargo run --release -- stream \
  --venue hyperliquid \
  --output-dir /mnt/burner-archive/ws_raw
```

Use `--hyperliquid-spot-coin` to override the SDK subscription coin if needed.

The recorder sends websocket protocol pings every 20 seconds by default. Bitfinex and Hibachi reconnect if a heartbeat pong is not received before the next tick. Extended and Deribit keep websocket protocol pings best-effort: Extended sends its own server pings, and Deribit uses its JSON-RPC heartbeat/test flow. Incoming server ping frames are flushed as pongs immediately. Tune this with `--heartbeat-secs`, or set it to `0` to disable client pings and Deribit's JSON-RPC heartbeat setup.

Use `--max-messages` for a quick smoke test, and `Ctrl+C` to stop a long capture cleanly:

```sh
cargo run --release -- stream \
  --venue bitfinex \
  --bitfinex-symbol tBTCUSD \
  --output-dir /tmp/modl_ws_smoke \
  --max-messages 2
```

Realtime files are append-only JSONL compressed with Zstd:

```text
ws_raw/
  bitfinex/tbtcusd/book_l25/tbtcusd_book_l25_26-06-14.jsonl.zst
  bitfinex/tbtcusd/trades/tbtcusd_trades_26-06-14.jsonl.zst
  extended/btc-usd/orderbook/btc-usd_orderbook_26-06-14.jsonl.zst
  extended/btcspot-usd/orderbook/btcspot-usd_orderbook_26-06-14.jsonl.zst
  extended/btcspot-usd/trades/btcspot-usd_trades_26-06-14.jsonl.zst
  hibachi/btc_usdt-p/market_data/btc_usdt-p_market_data_26-06-14.jsonl.zst
  deribit/btc/instrument_creation/btc_instrument_creation_26-06-14.jsonl.zst
  deribit/btc/instrument_state/btc_instrument_state_26-06-14.jsonl.zst
  deribit/btc/incremental_ticker/btc_incremental_ticker_26-06-14.jsonl.zst
  deribit/btc/trades/btc_trades_26-06-14.jsonl.zst
  hyperliquid/ubtc_usdc/book/ubtc_usdc_book_26-06-14.jsonl.zst
  hyperliquid/ubtc_usdc/trades/ubtc_usdc_trades_26-06-14.jsonl.zst
```

Each line is a capture envelope with `received_at`, `received_mts`, `exchange`, `symbol`, `channel`, `connection_id`, and either the raw text frame as `payload_text` or the raw binary frame as `payload_base64`.

## Normalizing Raw Websocket Files

Use the separate `modl-normalize` binary after a UTC day file has closed. The normalizer reads supported non-Extended raw websocket files in bounded batches and writes daily Parquet datasets. Extended remains raw-only for now because its full orderbook stream still needs dedicated depth-state handling.

```sh
cargo run --release --bin modl-normalize -- \
  --date 2026-06-29 \
  --input-dir /mnt/burner-archive/ws_raw \
  --output-dir /mnt/burner-archive/ws_normalized
```

The default input and output directories are `/mnt/burner-archive/ws_raw` and `/mnt/burner-archive/ws_normalized`, so this is enough for the normal archive:

```sh
cargo run --release --bin modl-normalize -- --date 2026-06-29
```

Build both executables when you want to run capture and normalization side by side:

```sh
cargo build --release --bins
target/release/modl btc
target/release/modl-normalize --date 2026-06-29
```

Normalized files are daily Parquet datasets:

```text
ws_normalized/
  bitfinex/tbtcusd/book_l25/tbtcusd_book_l25_26-06-29.parquet
  bitfinex/tbtcusd/trades/tbtcusd_trades_26-06-29.parquet
  deribit/btc/instruments/btc_instruments_26-06-29.parquet
  deribit/btc/instrument_state/btc_instrument_state_26-06-29.parquet
  deribit/btc/incremental_ticker/btc_incremental_ticker_26-06-29.parquet
  deribit/btc/trades/btc_trades_26-06-29.parquet
  hibachi/btc_usdt-p/funding/btc_usdt-p_funding_26-06-29.parquet
  hibachi/btc_usdt-p/orderbook/btc_usdt-p_orderbook_26-06-29.parquet
  hibachi/btc_usdt-p/prices/btc_usdt-p_prices_26-06-29.parquet
  hibachi/btc_usdt-p/quotes/btc_usdt-p_quotes_26-06-29.parquet
  hibachi/btc_usdt-p/trades/btc_usdt-p_trades_26-06-29.parquet
  hyperliquid/ubtc_usdc/book/ubtc_usdc_book_26-06-29.parquet
  hyperliquid/ubtc_usdc/control/ubtc_usdc_control_26-06-29.parquet
  hyperliquid/ubtc_usdc/trades/ubtc_usdc_trades_26-06-29.parquet
```

Deribit `instruments` is keyed by `instrument_name`. Deribit `incremental_ticker` and `trades` include the raw event values plus joined `kind`, `expiration_timestamp`, `strike`, `option_type`, and `settlement_period`. Bitfinex trades preserve snapshots, `te`, and `tu` rows; use `is_final = true` to select canonical snapshot/`tu` trades for features. Trade sides are canonicalized to `buy`/`sell` where the venue sends side tokens, with provider tokens retained as `raw_taker_side` for Hibachi and `raw_side` for Hyperliquid. Bitfinex and Hyperliquid book datasets are normalized as one row per book level in each snapshot/update message. Hibachi's multi-topic stream is split into `trades`, `orderbook`, `quotes`, `prices`, and `funding`. Financial numeric values are stored as UTF-8 decimal strings in this layer to avoid precision loss; downstream feature jobs can derive float columns from these exact strings when needed for model training.

## Paper Trading Recorder

The paper trading recorder notebook builds an append-only JSONL event log for the current quote-policy candidate. It records decision, replay projection, market snapshot, submit, ack, cancel, cancel ack, and fill events with a hash chain so paper/live behavior can be compared against the historical replay model. The paper trading runtime notebook consumes public trades through a paper order manager, applies queue-ahead, and compares independent replay-style orders with realistic quote replacement.

Run it with the burner notebook environment:

```sh
MPLBACKEND=Agg PYTHONDONTWRITEBYTECODE=1 \
  /home/skier/Documents/burner/btc-vol-strategy/.venv/bin/python
```

Open `notebooks/paper_trading_recorder.ipynb` or `notebooks/paper_trading_runtime.ipynb` in Jupyter for the interactive versions. By default dry-run JSONL is written under `/tmp/modl_paper_trading_recorder` and runtime JSONL under `/tmp/modl_paper_trading_runtime`. For persistent paper logs, set:

```sh
export MODL_PAPER_OUTPUT_ROOT=/mnt/burner-archive/paper_trading
export MODL_PAPER_RUNTIME_OUTPUT_ROOT=/mnt/burner-archive/paper_trading_runtime
export MODL_PAPER_RUN_ID=paper-$(date -u +%Y%m%dT%H%M%SZ)
export MODL_PAPER_RUNTIME_RUN_ID=runtime-$(date -u +%Y%m%dT%H%M%SZ)
```

## Rate Limit Behavior

Bitfinex public REST limits are IP-based and vary by endpoint. The CLI defaults to `--rpm 10`, spacing requests through one shared async rate gate. Clone the same `BitfinexClient` if you add more workers later; do not create one client per worker unless you also share a limiter above them.

If Bitfinex returns `ERR_RATE_LIMIT` or HTTP 429, the puller sleeps for 65 seconds before retrying.

## Checkpointing

Each symbol directory contains a checkpoint file:

```text
tbtcusd/.checkpoint.json
```

The checkpoint advances only after a UTC day file is closed successfully and the next UTC day begins. If the process stops mid-day, rerunning the command resumes after the last completed day and rewrites the interrupted day.

## Output

For `tBTCUSD`, daily files are named like this:

```text
tbtcusd/tbtcusd_26-06-14.parquet
tbtcusd/tbtcusd_26-06-15.parquet
```

The Parquet schema is:

```text
exchange    utf8
symbol      utf8
id          int64
mts         int64
timestamp   utf8
side        utf8
amount      utf8
amount_abs  utf8
price       utf8
```

`amount`, `amount_abs`, and `price` are stored as decimal strings to preserve precision and avoid float conversion.
