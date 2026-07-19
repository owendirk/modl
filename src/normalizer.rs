use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::NaiveDate;
use clap::{Args, Parser};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    DEFAULT_WS_OUTPUT_DIR, daily_stream_file_path, deribit_message_channel, deribit_message_data,
    parse_date, symbol_partition_name,
};

const DEFAULT_NORMALIZED_OUTPUT_DIR: &str = "/mnt/burner-archive/ws_normalized";
const NORMALIZE_BATCH_SIZE: usize = 25_000;

#[derive(Debug, Parser)]
#[command(about = "Normalize raw websocket JSONL.zst files into daily Parquet datasets")]
struct NormalizeCli {
    #[command(flatten)]
    args: NormalizeArgs,
}

#[derive(Clone, Debug, Args)]
pub(crate) struct NormalizeArgs {
    /// UTC capture day to normalize, formatted as YYYY-MM-DD.
    #[arg(long, value_parser = parse_date)]
    pub(crate) date: NaiveDate,

    /// Raw websocket archive root.
    #[arg(short = 'i', long = "input-dir", default_value = DEFAULT_WS_OUTPUT_DIR)]
    pub(crate) input_dir: PathBuf,

    /// Normalized Parquet output root.
    #[arg(short = 'o', long = "output-dir", default_value = DEFAULT_NORMALIZED_OUTPUT_DIR)]
    pub(crate) output_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct RawWsEventOwned {
    received_at: String,
    received_mts: i64,
    exchange: String,
    symbol: String,
    channel: String,
    connection_id: String,
    payload_text: Option<String>,
}

#[derive(Debug, Clone)]
struct NormalizedEventFields {
    exchange: String,
    symbol: String,
    received_at: String,
    received_mts: i64,
    connection_id: String,
    channel: String,
}

impl NormalizedEventFields {
    fn from_raw(event: &RawWsEventOwned) -> Self {
        Self {
            exchange: event.exchange.clone(),
            symbol: event.symbol.clone(),
            received_at: event.received_at.clone(),
            received_mts: event.received_mts,
            connection_id: event.connection_id.clone(),
            channel: event.channel.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct DeribitInstrumentMeta {
    instrument_name: String,
    kind: Option<String>,
    state: Option<String>,
    base_currency: Option<String>,
    quote_currency: Option<String>,
    settlement_currency: Option<String>,
    counter_currency: Option<String>,
    instrument_type: Option<String>,
    future_type: Option<String>,
    settlement_period: Option<String>,
    option_type: Option<String>,
    price_index: Option<String>,
    strike: Option<String>,
    contract_size: Option<String>,
    tick_size: Option<String>,
    expiration_timestamp: Option<i64>,
    creation_timestamp: Option<i64>,
    instrument_id: Option<i64>,
    is_active: Option<bool>,
}

#[derive(Debug)]
struct DeribitQuoteRow {
    exchange: String,
    symbol: String,
    received_at: String,
    received_mts: i64,
    connection_id: String,
    subscription_channel: String,
    instrument_name: String,
    kind: Option<String>,
    expiration_timestamp: Option<i64>,
    strike: Option<String>,
    option_type: Option<String>,
    settlement_period: Option<String>,
    ticker_timestamp: Option<i64>,
    ticker_type: Option<String>,
    state: Option<String>,
    best_bid_price: Option<String>,
    best_bid_amount: Option<String>,
    best_ask_price: Option<String>,
    best_ask_amount: Option<String>,
    index_price: Option<String>,
    mark_price: Option<String>,
    last_price: Option<String>,
    underlying_price: Option<String>,
    underlying_index: Option<String>,
    open_interest: Option<String>,
    settlement_price: Option<String>,
    estimated_delivery_price: Option<String>,
    min_price: Option<String>,
    max_price: Option<String>,
    bid_iv: Option<String>,
    ask_iv: Option<String>,
    mark_iv: Option<String>,
    funding_8h: Option<String>,
    current_funding: Option<String>,
    stats_volume: Option<String>,
    stats_volume_usd: Option<String>,
    stats_volume_notional: Option<String>,
    stats_high: Option<String>,
    stats_low: Option<String>,
    stats_price_change: Option<String>,
    delta: Option<String>,
    gamma: Option<String>,
    theta: Option<String>,
    vega: Option<String>,
    rho: Option<String>,
}

#[derive(Debug)]
struct DeribitTradeRow {
    exchange: String,
    symbol: String,
    received_at: String,
    received_mts: i64,
    connection_id: String,
    subscription_channel: String,
    instrument_name: String,
    kind: Option<String>,
    expiration_timestamp: Option<i64>,
    strike: Option<String>,
    option_type: Option<String>,
    settlement_period: Option<String>,
    trade_timestamp: Option<i64>,
    trade_id: Option<String>,
    trade_seq: Option<i64>,
    direction: Option<String>,
    tick_direction: Option<i64>,
    price: Option<String>,
    amount: Option<String>,
    contracts: Option<String>,
    mark_price: Option<String>,
    index_price: Option<String>,
    iv: Option<String>,
    liquidation: Option<String>,
    block_trade_id: Option<String>,
    combo_id: Option<String>,
    combo_trade_id: Option<String>,
    block_rfq_id: Option<i64>,
}

#[derive(Debug)]
struct DeribitInstrumentStateRow {
    exchange: String,
    symbol: String,
    received_at: String,
    received_mts: i64,
    connection_id: String,
    subscription_channel: String,
    instrument_name: Option<String>,
    kind: Option<String>,
    timestamp: Option<i64>,
    state: Option<String>,
    raw_data: Option<String>,
}

#[derive(Debug)]
struct BitfinexTradeRow {
    base: NormalizedEventFields,
    bitfinex_channel_id: Option<i64>,
    event_type: String,
    is_final: bool,
    trade_id: Option<i64>,
    trade_mts: Option<i64>,
    side: Option<String>,
    amount: Option<String>,
    amount_abs: Option<String>,
    price: Option<String>,
}

#[derive(Debug)]
struct BitfinexBookLevelRow {
    base: NormalizedEventFields,
    bitfinex_channel_id: Option<i64>,
    event_type: String,
    level_index: Option<i64>,
    price: Option<String>,
    count: Option<i64>,
    amount: Option<String>,
    amount_abs: Option<String>,
    side: Option<String>,
}

#[derive(Debug)]
struct HibachiTradeRow {
    base: NormalizedEventFields,
    trade_timestamp: Option<i64>,
    taker_side: Option<String>,
    raw_taker_side: Option<String>,
    price: Option<String>,
    quantity: Option<String>,
}

#[derive(Debug)]
struct HibachiBookLevelRow {
    base: NormalizedEventFields,
    message_type: Option<String>,
    book_timestamp_ms: Option<i64>,
    depth: Option<i64>,
    granularity: Option<String>,
    side: String,
    level_index: i64,
    price: Option<String>,
    quantity: Option<String>,
    start_price: Option<String>,
    end_price: Option<String>,
}

#[derive(Debug)]
struct HibachiQuoteRow {
    base: NormalizedEventFields,
    bid_price: Option<String>,
    bid_size: Option<String>,
    ask_price: Option<String>,
    ask_size: Option<String>,
}

#[derive(Debug)]
struct HibachiPriceRow {
    base: NormalizedEventFields,
    price_type: String,
    price: Option<String>,
}

#[derive(Debug)]
struct HibachiFundingRow {
    base: NormalizedEventFields,
    estimated_funding_rate: Option<String>,
    next_funding_timestamp: Option<i64>,
}

#[derive(Debug)]
struct HyperliquidTradeRow {
    base: NormalizedEventFields,
    coin: Option<String>,
    side: Option<String>,
    raw_side: Option<String>,
    price: Option<String>,
    size: Option<String>,
    trade_timestamp: Option<i64>,
    hash: Option<String>,
    trade_id: Option<i64>,
    user_0: Option<String>,
    user_1: Option<String>,
}

#[derive(Debug)]
struct HyperliquidBookLevelRow {
    base: NormalizedEventFields,
    coin: Option<String>,
    book_timestamp: Option<i64>,
    snapshot: Option<bool>,
    side: String,
    level_index: i64,
    price: Option<String>,
    size: Option<String>,
    order_count: Option<i64>,
}

#[derive(Debug)]
struct HyperliquidControlRow {
    base: NormalizedEventFields,
    event: Option<String>,
    message_channel: Option<String>,
    method: Option<String>,
    subscription_type: Option<String>,
    coin: Option<String>,
    payload_json: String,
}

#[derive(Debug)]
struct NormalizeSummary {
    output_dir: PathBuf,
    tables: Vec<NormalizedTableSummary>,
}

#[derive(Debug)]
struct NormalizedTableSummary {
    venue: &'static str,
    symbol: String,
    dataset: &'static str,
    row_count: usize,
    path: PathBuf,
}

pub(crate) fn run_cli() -> Result<()> {
    let cli = NormalizeCli::parse();
    run_command(&cli.args)
}

pub(crate) fn run_command(args: &NormalizeArgs) -> Result<()> {
    let summary = normalize_day(args)?;
    eprintln!("normalized {} into:", args.date);
    for table in &summary.tables {
        eprintln!(
            "  {}/{}/{}: {} row(s) -> {}",
            table.venue,
            table.symbol,
            table.dataset,
            table.row_count,
            table.path.display()
        );
    }
    eprintln!(
        "wrote normalized Parquet under {}",
        summary.output_dir.display()
    );
    Ok(())
}

fn normalize_day(args: &NormalizeArgs) -> Result<NormalizeSummary> {
    let mut tables = Vec::new();
    tables.extend(normalize_deribit_day(args)?);
    tables.extend(normalize_bitfinex_day(args)?);
    tables.extend(normalize_hibachi_day(args)?);
    tables.extend(normalize_hyperliquid_day(args)?);

    if tables.is_empty() {
        bail!(
            "no supported non-Extended raw files found for {} under {}",
            args.date,
            args.input_dir.display()
        );
    }

    Ok(NormalizeSummary {
        output_dir: args.output_dir.clone(),
        tables,
    })
}

fn normalize_deribit_day(args: &NormalizeArgs) -> Result<Vec<NormalizedTableSummary>> {
    let mut summaries = Vec::new();
    for (symbol_name, symbol_dir) in venue_symbol_dirs(&args.input_dir, "deribit")? {
        let source_files = deribit_raw_files(&symbol_dir, &symbol_name, args.date)?;
        if !source_files.iter().any(|path| path.exists()) {
            continue;
        }

        let instruments = collect_deribit_instruments(&symbol_dir, &symbol_name, args.date)?;
        let output_dir = args.output_dir.join("deribit").join(&symbol_name);
        let mut instrument_rows = instruments.values().cloned().collect::<Vec<_>>();
        instrument_rows.sort_by(|left, right| left.instrument_name.cmp(&right.instrument_name));

        let instruments_path =
            normalized_parquet_path(&output_dir, &symbol_name, "instruments", args.date);
        write_deribit_instruments_parquet(&instruments_path, &symbol_name, &instrument_rows)?;
        summaries.push(NormalizedTableSummary {
            venue: "deribit",
            symbol: symbol_name.clone(),
            dataset: "instruments",
            row_count: instrument_rows.len(),
            path: instruments_path,
        });

        let quotes_path =
            normalized_parquet_path(&output_dir, &symbol_name, "incremental_ticker", args.date);
        let quote_count = write_deribit_quotes_from_raw(
            &symbol_dir,
            &symbol_name,
            args.date,
            &instruments,
            &quotes_path,
        )?;
        summaries.push(NormalizedTableSummary {
            venue: "deribit",
            symbol: symbol_name.clone(),
            dataset: "incremental_ticker",
            row_count: quote_count,
            path: quotes_path,
        });

        let trades_path = normalized_parquet_path(&output_dir, &symbol_name, "trades", args.date);
        let trade_count = write_deribit_trades_from_raw(
            &symbol_dir,
            &symbol_name,
            args.date,
            &instruments,
            &trades_path,
        )?;
        summaries.push(NormalizedTableSummary {
            venue: "deribit",
            symbol: symbol_name.clone(),
            dataset: "trades",
            row_count: trade_count,
            path: trades_path,
        });

        let states_path =
            normalized_parquet_path(&output_dir, &symbol_name, "instrument_state", args.date);
        if deribit_raw_file_path(&symbol_dir, &symbol_name, "instrument_state", args.date)?.exists()
        {
            let state_count =
                write_deribit_states_from_raw(&symbol_dir, &symbol_name, args.date, &states_path)?;
            summaries.push(NormalizedTableSummary {
                venue: "deribit",
                symbol: symbol_name.clone(),
                dataset: "instrument_state",
                row_count: state_count,
                path: states_path,
            });
        }
    }

    Ok(summaries)
}

fn normalize_bitfinex_day(args: &NormalizeArgs) -> Result<Vec<NormalizedTableSummary>> {
    let mut summaries = Vec::new();
    for (symbol_name, symbol_dir) in venue_symbol_dirs(&args.input_dir, "bitfinex")? {
        let output_dir = args.output_dir.join("bitfinex").join(&symbol_name);

        let book_raw_path =
            raw_channel_file_path(&symbol_dir, &symbol_name, "book_l25", args.date)?;
        if book_raw_path.exists() {
            let book_path =
                normalized_parquet_path(&output_dir, &symbol_name, "book_l25", args.date);
            let row_count = write_bitfinex_book_from_raw(&book_raw_path, &book_path)?;
            summaries.push(NormalizedTableSummary {
                venue: "bitfinex",
                symbol: symbol_name.clone(),
                dataset: "book_l25",
                row_count,
                path: book_path,
            });
        }

        let trades_raw_path =
            raw_channel_file_path(&symbol_dir, &symbol_name, "trades", args.date)?;
        if trades_raw_path.exists() {
            let trades_path =
                normalized_parquet_path(&output_dir, &symbol_name, "trades", args.date);
            let row_count = write_bitfinex_trades_from_raw(&trades_raw_path, &trades_path)?;
            summaries.push(NormalizedTableSummary {
                venue: "bitfinex",
                symbol: symbol_name.clone(),
                dataset: "trades",
                row_count,
                path: trades_path,
            });
        }
    }
    Ok(summaries)
}

fn write_bitfinex_book_from_raw(input_path: &Path, output_path: &Path) -> Result<usize> {
    let mut writer = StreamingParquetWriter::create(
        output_path,
        bitfinex_book_schema(),
        bitfinex_book_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(input_path, |event, text| {
        let message = serde_json::from_str::<Value>(text).context("invalid Bitfinex book JSON")?;
        for row in bitfinex_book_rows_from_message(&event, &message) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn write_bitfinex_trades_from_raw(input_path: &Path, output_path: &Path) -> Result<usize> {
    let mut writer = StreamingParquetWriter::create(
        output_path,
        bitfinex_trade_schema(),
        bitfinex_trade_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(input_path, |event, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Bitfinex trades JSON")?;
        for row in bitfinex_trade_rows_from_message(&event, &message) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn normalize_hibachi_day(args: &NormalizeArgs) -> Result<Vec<NormalizedTableSummary>> {
    let mut summaries = Vec::new();
    for (symbol_name, symbol_dir) in venue_symbol_dirs(&args.input_dir, "hibachi")? {
        let raw_path = raw_channel_file_path(&symbol_dir, &symbol_name, "market_data", args.date)?;
        if !raw_path.exists() {
            continue;
        }

        let output_dir = args.output_dir.join("hibachi").join(&symbol_name);
        let paths = HibachiOutputPaths {
            trades: normalized_parquet_path(&output_dir, &symbol_name, "trades", args.date),
            orderbook: normalized_parquet_path(&output_dir, &symbol_name, "orderbook", args.date),
            quotes: normalized_parquet_path(&output_dir, &symbol_name, "quotes", args.date),
            prices: normalized_parquet_path(&output_dir, &symbol_name, "prices", args.date),
            funding: normalized_parquet_path(&output_dir, &symbol_name, "funding", args.date),
        };
        let counts = write_hibachi_from_raw(&raw_path, &paths)?;
        summaries.extend([
            NormalizedTableSummary {
                venue: "hibachi",
                symbol: symbol_name.clone(),
                dataset: "trades",
                row_count: counts.trades,
                path: paths.trades,
            },
            NormalizedTableSummary {
                venue: "hibachi",
                symbol: symbol_name.clone(),
                dataset: "orderbook",
                row_count: counts.orderbook,
                path: paths.orderbook,
            },
            NormalizedTableSummary {
                venue: "hibachi",
                symbol: symbol_name.clone(),
                dataset: "quotes",
                row_count: counts.quotes,
                path: paths.quotes,
            },
            NormalizedTableSummary {
                venue: "hibachi",
                symbol: symbol_name.clone(),
                dataset: "prices",
                row_count: counts.prices,
                path: paths.prices,
            },
            NormalizedTableSummary {
                venue: "hibachi",
                symbol: symbol_name.clone(),
                dataset: "funding",
                row_count: counts.funding,
                path: paths.funding,
            },
        ]);
    }
    Ok(summaries)
}

struct HibachiOutputPaths {
    trades: PathBuf,
    orderbook: PathBuf,
    quotes: PathBuf,
    prices: PathBuf,
    funding: PathBuf,
}

struct HibachiOutputCounts {
    trades: usize,
    orderbook: usize,
    quotes: usize,
    prices: usize,
    funding: usize,
}

fn write_hibachi_from_raw(
    input_path: &Path,
    paths: &HibachiOutputPaths,
) -> Result<HibachiOutputCounts> {
    let mut trades = StreamingParquetWriter::create(
        &paths.trades,
        hibachi_trade_schema(),
        hibachi_trade_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    let mut orderbook = StreamingParquetWriter::create(
        &paths.orderbook,
        hibachi_orderbook_schema(),
        hibachi_orderbook_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    let mut quotes = StreamingParquetWriter::create(
        &paths.quotes,
        hibachi_quote_schema(),
        hibachi_quote_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    let mut prices = StreamingParquetWriter::create(
        &paths.prices,
        hibachi_price_schema(),
        hibachi_price_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    let mut funding = StreamingParquetWriter::create(
        &paths.funding,
        hibachi_funding_schema(),
        hibachi_funding_columns,
        NORMALIZE_BATCH_SIZE,
    )?;

    for_each_raw_text_event(input_path, |event, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Hibachi market data JSON")?;
        match message.get("topic").and_then(Value::as_str) {
            Some("trades") => {
                if let Some(row) = hibachi_trade_row_from_message(&event, &message) {
                    trades.write(row)?;
                }
            }
            Some("orderbook") => {
                for row in hibachi_orderbook_rows_from_message(&event, &message) {
                    orderbook.write(row)?;
                }
            }
            Some("ask_bid_price") => {
                if let Some(row) = hibachi_quote_row_from_message(&event, &message) {
                    quotes.write(row)?;
                }
            }
            Some("mark_price" | "spot_price") => {
                if let Some(row) = hibachi_price_row_from_message(&event, &message) {
                    prices.write(row)?;
                }
            }
            Some("funding_rate_estimation") => {
                if let Some(row) = hibachi_funding_row_from_message(&event, &message) {
                    funding.write(row)?;
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(HibachiOutputCounts {
        trades: trades.close()?,
        orderbook: orderbook.close()?,
        quotes: quotes.close()?,
        prices: prices.close()?,
        funding: funding.close()?,
    })
}

fn normalize_hyperliquid_day(args: &NormalizeArgs) -> Result<Vec<NormalizedTableSummary>> {
    let mut summaries = Vec::new();
    for (symbol_name, symbol_dir) in venue_symbol_dirs(&args.input_dir, "hyperliquid")? {
        let output_dir = args.output_dir.join("hyperliquid").join(&symbol_name);

        let book_raw_path = raw_channel_file_path(&symbol_dir, &symbol_name, "book", args.date)?;
        if book_raw_path.exists() {
            let book_path = normalized_parquet_path(&output_dir, &symbol_name, "book", args.date);
            let row_count = write_hyperliquid_book_from_raw(&book_raw_path, &book_path)?;
            summaries.push(NormalizedTableSummary {
                venue: "hyperliquid",
                symbol: symbol_name.clone(),
                dataset: "book",
                row_count,
                path: book_path,
            });
        }

        let trades_raw_path =
            raw_channel_file_path(&symbol_dir, &symbol_name, "trades", args.date)?;
        if trades_raw_path.exists() {
            let trades_path =
                normalized_parquet_path(&output_dir, &symbol_name, "trades", args.date);
            let row_count = write_hyperliquid_trades_from_raw(&trades_raw_path, &trades_path)?;
            summaries.push(NormalizedTableSummary {
                venue: "hyperliquid",
                symbol: symbol_name.clone(),
                dataset: "trades",
                row_count,
                path: trades_path,
            });
        }

        let control_raw_path =
            raw_channel_file_path(&symbol_dir, &symbol_name, "control", args.date)?;
        if control_raw_path.exists() {
            let control_path =
                normalized_parquet_path(&output_dir, &symbol_name, "control", args.date);
            let row_count = write_hyperliquid_control_from_raw(&control_raw_path, &control_path)?;
            summaries.push(NormalizedTableSummary {
                venue: "hyperliquid",
                symbol: symbol_name.clone(),
                dataset: "control",
                row_count,
                path: control_path,
            });
        }
    }
    Ok(summaries)
}

fn write_hyperliquid_book_from_raw(input_path: &Path, output_path: &Path) -> Result<usize> {
    let mut writer = StreamingParquetWriter::create(
        output_path,
        hyperliquid_book_schema(),
        hyperliquid_book_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(input_path, |event, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Hyperliquid book JSON")?;
        for row in hyperliquid_book_rows_from_message(&event, &message) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn write_hyperliquid_trades_from_raw(input_path: &Path, output_path: &Path) -> Result<usize> {
    let mut writer = StreamingParquetWriter::create(
        output_path,
        hyperliquid_trade_schema(),
        hyperliquid_trade_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(input_path, |event, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Hyperliquid trades JSON")?;
        for row in hyperliquid_trade_rows_from_message(&event, &message) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn write_hyperliquid_control_from_raw(input_path: &Path, output_path: &Path) -> Result<usize> {
    let mut writer = StreamingParquetWriter::create(
        output_path,
        hyperliquid_control_schema(),
        hyperliquid_control_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(input_path, |event, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Hyperliquid control JSON")?;
        writer.write(hyperliquid_control_row_from_message(&event, &message, text))?;
        Ok(())
    })?;
    writer.close()
}

fn collect_deribit_instruments(
    symbol_dir: &Path,
    symbol_name: &str,
    date: NaiveDate,
) -> Result<HashMap<String, DeribitInstrumentMeta>> {
    let mut instruments = HashMap::new();

    let control_path = deribit_raw_file_path(symbol_dir, symbol_name, "control", date)?;
    for_each_raw_text_event(&control_path, |_, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Deribit control JSON")?;
        if let Some(rows) = message.get("result").and_then(Value::as_array) {
            for row in rows {
                if let Some(meta) = deribit_instrument_meta_from_value(row) {
                    instruments.insert(meta.instrument_name.clone(), meta);
                }
            }
        }
        Ok(())
    })?;

    let creation_path =
        deribit_raw_file_path(symbol_dir, symbol_name, "instrument_creation", date)?;
    for_each_raw_text_event(&creation_path, |_, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Deribit creation JSON")?;
        if let Some(meta) =
            deribit_message_data(&message).and_then(deribit_instrument_meta_from_value)
        {
            instruments.insert(meta.instrument_name.clone(), meta);
        }
        Ok(())
    })?;

    Ok(instruments)
}

fn write_deribit_quotes_from_raw(
    symbol_dir: &Path,
    symbol_name: &str,
    date: NaiveDate,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
    output_path: &Path,
) -> Result<usize> {
    let path = deribit_raw_file_path(symbol_dir, symbol_name, "incremental_ticker", date)?;
    let mut writer = StreamingParquetWriter::create(
        output_path,
        deribit_quote_schema(),
        deribit_quote_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(&path, |event, text| {
        let message = serde_json::from_str::<Value>(text).context("invalid Deribit ticker JSON")?;
        if let Some(row) = deribit_quote_row_from_message(&event, &message, instruments) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn write_deribit_trades_from_raw(
    symbol_dir: &Path,
    symbol_name: &str,
    date: NaiveDate,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
    output_path: &Path,
) -> Result<usize> {
    let path = deribit_raw_file_path(symbol_dir, symbol_name, "trades", date)?;
    let mut writer = StreamingParquetWriter::create(
        output_path,
        deribit_trade_schema(),
        deribit_trade_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(&path, |event, text| {
        let message = serde_json::from_str::<Value>(text).context("invalid Deribit trades JSON")?;
        for row in deribit_trade_rows_from_message(&event, &message, instruments) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn write_deribit_states_from_raw(
    symbol_dir: &Path,
    symbol_name: &str,
    date: NaiveDate,
    output_path: &Path,
) -> Result<usize> {
    let path = deribit_raw_file_path(symbol_dir, symbol_name, "instrument_state", date)?;
    let mut writer = StreamingParquetWriter::create(
        output_path,
        deribit_instrument_state_schema(),
        deribit_instrument_state_columns,
        NORMALIZE_BATCH_SIZE,
    )?;
    for_each_raw_text_event(&path, |event, text| {
        let message =
            serde_json::from_str::<Value>(text).context("invalid Deribit instrument state JSON")?;
        if let Some(row) = deribit_instrument_state_row_from_message(&event, &message) {
            writer.write(row)?;
        }
        Ok(())
    })?;
    writer.close()
}

fn deribit_instrument_meta_from_value(value: &Value) -> Option<DeribitInstrumentMeta> {
    let instrument_name = value.get("instrument_name")?.as_str()?.to_owned();
    Some(DeribitInstrumentMeta {
        instrument_name,
        kind: json_field_string(value, "kind"),
        state: json_field_string(value, "state"),
        base_currency: json_field_string(value, "base_currency"),
        quote_currency: json_field_string(value, "quote_currency"),
        settlement_currency: json_field_string(value, "settlement_currency"),
        counter_currency: json_field_string(value, "counter_currency"),
        instrument_type: json_field_string(value, "instrument_type"),
        future_type: json_field_string(value, "future_type"),
        settlement_period: json_field_string(value, "settlement_period"),
        option_type: json_field_string(value, "option_type"),
        price_index: json_field_string(value, "price_index"),
        strike: json_field_string(value, "strike"),
        contract_size: json_field_string(value, "contract_size"),
        tick_size: json_field_string(value, "tick_size"),
        expiration_timestamp: json_field_i64(value, "expiration_timestamp"),
        creation_timestamp: json_field_i64(value, "creation_timestamp"),
        instrument_id: json_field_i64(value, "instrument_id"),
        is_active: value.get("is_active").and_then(Value::as_bool),
    })
}

fn deribit_quote_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
) -> Option<DeribitQuoteRow> {
    let data = deribit_message_data(message)?;
    let instrument_name = data.get("instrument_name")?.as_str()?.to_owned();
    let meta = instruments.get(&instrument_name);

    Some(DeribitQuoteRow {
        exchange: event.exchange.clone(),
        symbol: event.symbol.clone(),
        received_at: event.received_at.clone(),
        received_mts: event.received_mts,
        connection_id: event.connection_id.clone(),
        subscription_channel: deribit_message_channel(message)
            .unwrap_or(&event.channel)
            .to_owned(),
        instrument_name,
        kind: meta_string(meta, |meta| &meta.kind),
        expiration_timestamp: meta.and_then(|meta| meta.expiration_timestamp),
        strike: meta_string(meta, |meta| &meta.strike),
        option_type: meta_string(meta, |meta| &meta.option_type),
        settlement_period: meta_string(meta, |meta| &meta.settlement_period),
        ticker_timestamp: json_field_i64(data, "timestamp"),
        ticker_type: json_field_string(data, "type"),
        state: json_field_string(data, "state"),
        best_bid_price: json_field_string(data, "best_bid_price"),
        best_bid_amount: json_field_string(data, "best_bid_amount"),
        best_ask_price: json_field_string(data, "best_ask_price"),
        best_ask_amount: json_field_string(data, "best_ask_amount"),
        index_price: json_field_string(data, "index_price"),
        mark_price: json_field_string(data, "mark_price"),
        last_price: json_field_string(data, "last_price"),
        underlying_price: json_field_string(data, "underlying_price"),
        underlying_index: json_field_string(data, "underlying_index"),
        open_interest: json_field_string(data, "open_interest"),
        settlement_price: json_field_string(data, "settlement_price"),
        estimated_delivery_price: json_field_string(data, "estimated_delivery_price"),
        min_price: json_field_string(data, "min_price"),
        max_price: json_field_string(data, "max_price"),
        bid_iv: json_field_string(data, "bid_iv"),
        ask_iv: json_field_string(data, "ask_iv"),
        mark_iv: json_field_string(data, "mark_iv"),
        funding_8h: json_field_string(data, "funding_8h"),
        current_funding: json_field_string(data, "current_funding"),
        stats_volume: json_pointer_string(data, "/stats/volume"),
        stats_volume_usd: json_pointer_string(data, "/stats/volume_usd"),
        stats_volume_notional: json_pointer_string(data, "/stats/volume_notional"),
        stats_high: json_pointer_string(data, "/stats/high"),
        stats_low: json_pointer_string(data, "/stats/low"),
        stats_price_change: json_pointer_string(data, "/stats/price_change"),
        delta: json_pointer_string(data, "/greeks/delta"),
        gamma: json_pointer_string(data, "/greeks/gamma"),
        theta: json_pointer_string(data, "/greeks/theta"),
        vega: json_pointer_string(data, "/greeks/vega"),
        rho: json_pointer_string(data, "/greeks/rho"),
    })
}

fn deribit_trade_rows_from_message(
    event: &RawWsEventOwned,
    message: &Value,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
) -> Vec<DeribitTradeRow> {
    let Some(data) = deribit_message_data(message) else {
        return Vec::new();
    };

    match data {
        Value::Array(trades) => trades
            .iter()
            .filter_map(|trade| deribit_trade_row_from_value(event, message, trade, instruments))
            .collect(),
        Value::Object(_) => deribit_trade_row_from_value(event, message, data, instruments)
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

fn deribit_trade_row_from_value(
    event: &RawWsEventOwned,
    message: &Value,
    trade: &Value,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
) -> Option<DeribitTradeRow> {
    let instrument_name = trade.get("instrument_name")?.as_str()?.to_owned();
    let meta = instruments.get(&instrument_name);

    Some(DeribitTradeRow {
        exchange: event.exchange.clone(),
        symbol: event.symbol.clone(),
        received_at: event.received_at.clone(),
        received_mts: event.received_mts,
        connection_id: event.connection_id.clone(),
        subscription_channel: deribit_message_channel(message)
            .unwrap_or(&event.channel)
            .to_owned(),
        instrument_name,
        kind: meta_string(meta, |meta| &meta.kind),
        expiration_timestamp: meta.and_then(|meta| meta.expiration_timestamp),
        strike: meta_string(meta, |meta| &meta.strike),
        option_type: meta_string(meta, |meta| &meta.option_type),
        settlement_period: meta_string(meta, |meta| &meta.settlement_period),
        trade_timestamp: json_field_i64(trade, "timestamp"),
        trade_id: json_field_string(trade, "trade_id"),
        trade_seq: json_field_i64(trade, "trade_seq"),
        direction: json_field_string(trade, "direction")
            .as_deref()
            .map(canonical_buy_sell_side),
        tick_direction: json_field_i64(trade, "tick_direction"),
        price: json_field_string(trade, "price"),
        amount: json_field_string(trade, "amount"),
        contracts: json_field_string(trade, "contracts"),
        mark_price: json_field_string(trade, "mark_price"),
        index_price: json_field_string(trade, "index_price"),
        iv: json_field_string(trade, "iv"),
        liquidation: json_field_string(trade, "liquidation"),
        block_trade_id: json_field_string(trade, "block_trade_id"),
        combo_id: json_field_string(trade, "combo_id"),
        combo_trade_id: json_field_string(trade, "combo_trade_id"),
        block_rfq_id: json_field_i64(trade, "block_rfq_id"),
    })
}

fn deribit_instrument_state_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Option<DeribitInstrumentStateRow> {
    let data = deribit_message_data(message)?;
    Some(DeribitInstrumentStateRow {
        exchange: event.exchange.clone(),
        symbol: event.symbol.clone(),
        received_at: event.received_at.clone(),
        received_mts: event.received_mts,
        connection_id: event.connection_id.clone(),
        subscription_channel: deribit_message_channel(message)
            .unwrap_or(&event.channel)
            .to_owned(),
        instrument_name: json_field_string(data, "instrument_name"),
        kind: json_field_string(data, "kind"),
        timestamp: json_field_i64(data, "timestamp"),
        state: json_field_string(data, "state"),
        raw_data: serde_json::to_string(data).ok(),
    })
}

fn bitfinex_trade_rows_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Vec<BitfinexTradeRow> {
    let Value::Array(values) = message else {
        return Vec::new();
    };
    let channel_id = values.first().and_then(json_value_i64);
    let Some(payload) = values.get(1) else {
        return Vec::new();
    };

    if payload.as_str() == Some("hb") {
        return Vec::new();
    }

    if let Some(trades) = payload.as_array().filter(|items| {
        items
            .first()
            .is_some_and(|first| matches!(first, Value::Array(_)))
    }) {
        return trades
            .iter()
            .map(|trade| bitfinex_trade_row_from_value(event, channel_id, "snapshot", trade))
            .collect();
    }

    let Some(event_type) = payload.as_str() else {
        return Vec::new();
    };
    let Some(trade) = values.get(2) else {
        return Vec::new();
    };
    vec![bitfinex_trade_row_from_value(
        event, channel_id, event_type, trade,
    )]
}

fn bitfinex_trade_row_from_value(
    event: &RawWsEventOwned,
    channel_id: Option<i64>,
    event_type: &str,
    trade: &Value,
) -> BitfinexTradeRow {
    let amount = json_array_string(trade, 2);
    BitfinexTradeRow {
        base: NormalizedEventFields::from_raw(event),
        bitfinex_channel_id: channel_id,
        event_type: event_type.to_owned(),
        is_final: matches!(event_type, "snapshot" | "tu"),
        trade_id: json_array_i64(trade, 0),
        trade_mts: json_array_i64(trade, 1),
        side: signed_amount_side(amount.as_deref(), "buy", "sell"),
        amount_abs: amount.as_deref().map(abs_decimal_string),
        amount,
        price: json_array_string(trade, 3),
    }
}

fn bitfinex_book_rows_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Vec<BitfinexBookLevelRow> {
    let Value::Array(values) = message else {
        return Vec::new();
    };
    let channel_id = values.first().and_then(json_value_i64);
    let Some(payload) = values.get(1) else {
        return Vec::new();
    };

    if payload.as_str() == Some("hb") {
        return Vec::new();
    }

    if let Some(levels) = payload.as_array().filter(|items| {
        items
            .first()
            .is_some_and(|first| matches!(first, Value::Array(_)))
    }) {
        return levels
            .iter()
            .enumerate()
            .map(|(index, level)| {
                let level_index = i64::try_from(index).ok();
                bitfinex_book_row_from_value(event, channel_id, "snapshot", level_index, level)
            })
            .collect();
    }

    vec![bitfinex_book_row_from_value(
        event,
        channel_id,
        "update",
        Some(0),
        payload,
    )]
}

fn bitfinex_book_row_from_value(
    event: &RawWsEventOwned,
    channel_id: Option<i64>,
    event_type: &str,
    level_index: Option<i64>,
    level: &Value,
) -> BitfinexBookLevelRow {
    let amount = json_array_string(level, 2);
    BitfinexBookLevelRow {
        base: NormalizedEventFields::from_raw(event),
        bitfinex_channel_id: channel_id,
        event_type: event_type.to_owned(),
        level_index,
        price: json_array_string(level, 0),
        count: json_array_i64(level, 1),
        side: signed_amount_side(amount.as_deref(), "bid", "ask"),
        amount_abs: amount.as_deref().map(abs_decimal_string),
        amount,
    }
}

fn hibachi_trade_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Option<HibachiTradeRow> {
    let trade = message.pointer("/data/trade")?;
    let raw_taker_side = json_field_string(trade, "takerSide");
    Some(HibachiTradeRow {
        base: NormalizedEventFields::from_raw(event),
        trade_timestamp: json_field_i64(trade, "timestamp"),
        taker_side: raw_taker_side.as_deref().map(canonical_buy_sell_side),
        raw_taker_side,
        price: json_field_string(trade, "price"),
        quantity: json_field_string(trade, "quantity"),
    })
}

fn hibachi_orderbook_rows_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Vec<HibachiBookLevelRow> {
    let mut rows = Vec::new();
    for (side, pointer) in [("bid", "/data/bid"), ("ask", "/data/ask")] {
        let Some(book_side) = message.pointer(pointer) else {
            continue;
        };
        let start_price = json_field_string(book_side, "startPrice");
        let end_price = json_field_string(book_side, "endPrice");
        let Some(levels) = book_side.get("levels").and_then(Value::as_array) else {
            continue;
        };
        for (index, level) in levels.iter().enumerate() {
            let Some(level_index) = i64::try_from(index).ok() else {
                continue;
            };
            rows.push(HibachiBookLevelRow {
                base: NormalizedEventFields::from_raw(event),
                message_type: json_field_string(message, "messageType"),
                book_timestamp_ms: json_field_i64(message, "timestamp_ms"),
                depth: json_field_i64(message, "depth"),
                granularity: json_field_string(message, "granularity"),
                side: side.to_owned(),
                level_index,
                price: json_field_string(level, "price"),
                quantity: json_field_string(level, "quantity"),
                start_price: start_price.clone(),
                end_price: end_price.clone(),
            });
        }
    }
    rows
}

fn hibachi_quote_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Option<HibachiQuoteRow> {
    let data = message.get("data")?;
    Some(HibachiQuoteRow {
        base: NormalizedEventFields::from_raw(event),
        bid_price: json_field_string(data, "bidPrice"),
        bid_size: json_field_string(data, "bidSize"),
        ask_price: json_field_string(data, "askPrice"),
        ask_size: json_field_string(data, "askSize"),
    })
}

fn hibachi_price_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Option<HibachiPriceRow> {
    let topic = message.get("topic")?.as_str()?;
    let data = message.get("data")?;
    let price = match topic {
        "mark_price" => json_field_string(data, "markPrice"),
        "spot_price" => json_field_string(data, "spotPrice"),
        _ => None,
    };
    Some(HibachiPriceRow {
        base: NormalizedEventFields::from_raw(event),
        price_type: topic.to_owned(),
        price,
    })
}

fn hibachi_funding_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Option<HibachiFundingRow> {
    let funding = message.pointer("/data/fundingRateEstimation")?;
    Some(HibachiFundingRow {
        base: NormalizedEventFields::from_raw(event),
        estimated_funding_rate: json_field_string(funding, "estimatedFundingRate"),
        next_funding_timestamp: json_field_i64(funding, "nextFundingTimestamp"),
    })
}

fn hyperliquid_trade_rows_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Vec<HyperliquidTradeRow> {
    let Some(trades) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    trades
        .iter()
        .map(|trade| {
            let users = trade.get("users");
            let raw_side = json_field_string(trade, "side");
            HyperliquidTradeRow {
                base: NormalizedEventFields::from_raw(event),
                coin: json_field_string(trade, "coin"),
                side: raw_side.as_deref().map(canonical_buy_sell_side),
                raw_side,
                price: json_field_string(trade, "px"),
                size: json_field_string(trade, "sz"),
                trade_timestamp: json_field_i64(trade, "time"),
                hash: json_field_string(trade, "hash"),
                trade_id: json_field_i64(trade, "tid"),
                user_0: users.and_then(|users| json_array_string(users, 0)),
                user_1: users.and_then(|users| json_array_string(users, 1)),
            }
        })
        .collect()
}

fn hyperliquid_book_rows_from_message(
    event: &RawWsEventOwned,
    message: &Value,
) -> Vec<HyperliquidBookLevelRow> {
    let Some(data) = message.get("data") else {
        return Vec::new();
    };
    let Some(sides) = data.get("levels").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (side_index, side_name) in ["bid", "ask"].iter().enumerate() {
        let Some(levels) = sides.get(side_index).and_then(Value::as_array) else {
            continue;
        };
        for (level_index, level) in levels.iter().enumerate() {
            let Some(level_index) = i64::try_from(level_index).ok() else {
                continue;
            };
            rows.push(HyperliquidBookLevelRow {
                base: NormalizedEventFields::from_raw(event),
                coin: json_field_string(data, "coin"),
                book_timestamp: json_field_i64(data, "time"),
                snapshot: data.get("snapshot").and_then(Value::as_bool),
                side: (*side_name).to_owned(),
                level_index,
                price: json_field_string(level, "px"),
                size: json_field_string(level, "sz"),
                order_count: json_field_i64(level, "n"),
            });
        }
    }
    rows
}

fn hyperliquid_control_row_from_message(
    event: &RawWsEventOwned,
    message: &Value,
    text: &str,
) -> HyperliquidControlRow {
    HyperliquidControlRow {
        base: NormalizedEventFields::from_raw(event),
        event: json_field_string(message, "event"),
        message_channel: json_field_string(message, "channel"),
        method: json_pointer_string(message, "/data/method"),
        subscription_type: json_pointer_string(message, "/data/subscription/type"),
        coin: json_pointer_string(message, "/data/subscription/coin"),
        payload_json: text.to_owned(),
    }
}

fn meta_string(
    meta: Option<&DeribitInstrumentMeta>,
    field: fn(&DeribitInstrumentMeta) -> &Option<String>,
) -> Option<String> {
    meta.and_then(|meta| field(meta).clone())
}

fn json_field_i64(value: &Value, field: &str) -> Option<i64> {
    json_value_i64(value.get(field)?)
}

fn json_value_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_u64()
            .and_then(|unsigned| i64::try_from(unsigned).ok())
    })
}

fn json_field_string(value: &Value, field: &str) -> Option<String> {
    json_value_string(value.get(field)?)
}

fn json_pointer_string(value: &Value, pointer: &str) -> Option<String> {
    json_value_string(value.pointer(pointer)?)
}

fn json_array_i64(value: &Value, index: usize) -> Option<i64> {
    value.as_array()?.get(index).and_then(json_value_i64)
}

fn json_array_string(value: &Value, index: usize) -> Option<String> {
    value.as_array()?.get(index).and_then(json_value_string)
}

fn json_value_string(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
    }
}

fn signed_amount_side(
    amount: Option<&str>,
    positive_side: &str,
    negative_side: &str,
) -> Option<String> {
    let amount = amount?;
    if amount.trim_start().starts_with('-') {
        Some(negative_side.to_owned())
    } else {
        Some(positive_side.to_owned())
    }
}

fn canonical_buy_sell_side(side: &str) -> String {
    match side.trim().to_ascii_lowercase().as_str() {
        "a" | "ask" | "s" | "sell" => "sell".to_owned(),
        "b" | "bid" | "buy" => "buy".to_owned(),
        other => other.to_owned(),
    }
}

fn abs_decimal_string(value: &str) -> String {
    value
        .trim_start()
        .strip_prefix('-')
        .unwrap_or_else(|| value.trim_start())
        .to_owned()
}

fn deribit_raw_files(
    symbol_dir: &Path,
    symbol_name: &str,
    date: NaiveDate,
) -> Result<Vec<PathBuf>> {
    [
        "control",
        "instrument_creation",
        "instrument_state",
        "incremental_ticker",
        "trades",
    ]
    .into_iter()
    .map(|channel| deribit_raw_file_path(symbol_dir, symbol_name, channel, date))
    .collect()
}

fn deribit_raw_file_path(
    symbol_dir: &Path,
    symbol_name: &str,
    channel: &str,
    date: NaiveDate,
) -> Result<PathBuf> {
    let channel_name = symbol_partition_name(channel)?;
    Ok(daily_stream_file_path(
        &symbol_dir.join(&channel_name),
        symbol_name,
        &channel_name,
        date,
    ))
}

fn venue_symbol_dirs(input_dir: &Path, venue: &str) -> Result<Vec<(String, PathBuf)>> {
    let venue_dir = input_dir.join(venue);
    if !venue_dir.exists() {
        return Ok(Vec::new());
    }

    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(&venue_dir)
        .with_context(|| format!("failed to read {}", venue_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", venue_dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect {}", entry.path().display()))?;
        if !file_type.is_dir() {
            continue;
        }
        let symbol_name = entry
            .file_name()
            .into_string()
            .map_err(|name| anyhow::anyhow!("non-UTF-8 symbol directory name: {name:?}"))?;
        dirs.push((symbol_name, entry.path()));
    }
    dirs.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(dirs)
}

fn raw_channel_file_path(
    symbol_dir: &Path,
    symbol_name: &str,
    channel: &str,
    date: NaiveDate,
) -> Result<PathBuf> {
    let channel_name = symbol_partition_name(channel)?;
    Ok(daily_stream_file_path(
        &symbol_dir.join(&channel_name),
        symbol_name,
        &channel_name,
        date,
    ))
}

fn for_each_raw_text_event<F>(path: &Path, mut handle: F) -> Result<usize>
where
    F: FnMut(RawWsEventOwned, &str) -> Result<()>,
{
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("failed to create zstd decoder for {}", path.display()))?;
    let reader = BufReader::new(decoder);
    let mut count = 0_usize;

    for (line_index, line) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line = match line {
            Ok(line) => line,
            Err(error) if is_recoverable_zstd_read_error(&error) => {
                eprintln!(
                    "stopped reading {} at line {line_number}: {error}",
                    path.display(),
                );
                break;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to read {} line {line_number}", path.display())
                });
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        let mut event = serde_json::from_str::<RawWsEventOwned>(&line).with_context(|| {
            format!(
                "invalid raw websocket envelope at {}:{line_number}",
                path.display()
            )
        })?;
        if let Some(text) = event.payload_text.take() {
            handle(event, &text)
                .with_context(|| format!("failed to normalize {}:{line_number}", path.display()))?;
            count = count.saturating_add(1);
        }
    }

    Ok(count)
}

fn is_recoverable_zstd_read_error(error: &std::io::Error) -> bool {
    let message = error.to_string();
    error.kind() == std::io::ErrorKind::UnexpectedEof
        || message.contains("incomplete frame")
        || message.contains("Data corruption detected")
}

fn write_deribit_instruments_parquet(
    path: &Path,
    symbol_name: &str,
    rows: &[DeribitInstrumentMeta],
) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("instrument_name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("base_currency", DataType::Utf8, true),
        Field::new("quote_currency", DataType::Utf8, true),
        Field::new("settlement_currency", DataType::Utf8, true),
        Field::new("counter_currency", DataType::Utf8, true),
        Field::new("instrument_type", DataType::Utf8, true),
        Field::new("future_type", DataType::Utf8, true),
        Field::new("settlement_period", DataType::Utf8, true),
        Field::new("option_type", DataType::Utf8, true),
        Field::new("price_index", DataType::Utf8, true),
        Field::new("strike", DataType::Utf8, true),
        Field::new("contract_size", DataType::Utf8, true),
        Field::new("tick_size", DataType::Utf8, true),
        Field::new("expiration_timestamp", DataType::Int64, true),
        Field::new("creation_timestamp", DataType::Int64, true),
        Field::new("instrument_id", DataType::Int64, true),
        Field::new("is_active", DataType::Boolean, true),
    ]));

    write_parquet_batch(
        path,
        schema,
        vec![
            required_string_array(rows.iter().map(|_| "deribit")),
            required_string_array(rows.iter().map(|_| symbol_name)),
            required_string_array(rows.iter().map(|row| row.instrument_name.as_str())),
            optional_string_array(rows.iter().map(|row| row.kind.as_deref())),
            optional_string_array(rows.iter().map(|row| row.state.as_deref())),
            optional_string_array(rows.iter().map(|row| row.base_currency.as_deref())),
            optional_string_array(rows.iter().map(|row| row.quote_currency.as_deref())),
            optional_string_array(rows.iter().map(|row| row.settlement_currency.as_deref())),
            optional_string_array(rows.iter().map(|row| row.counter_currency.as_deref())),
            optional_string_array(rows.iter().map(|row| row.instrument_type.as_deref())),
            optional_string_array(rows.iter().map(|row| row.future_type.as_deref())),
            optional_string_array(rows.iter().map(|row| row.settlement_period.as_deref())),
            optional_string_array(rows.iter().map(|row| row.option_type.as_deref())),
            optional_string_array(rows.iter().map(|row| row.price_index.as_deref())),
            optional_string_array(rows.iter().map(|row| row.strike.as_deref())),
            optional_string_array(rows.iter().map(|row| row.contract_size.as_deref())),
            optional_string_array(rows.iter().map(|row| row.tick_size.as_deref())),
            optional_i64_array(rows.iter().map(|row| row.expiration_timestamp)),
            optional_i64_array(rows.iter().map(|row| row.creation_timestamp)),
            optional_i64_array(rows.iter().map(|row| row.instrument_id)),
            optional_bool_array(rows.iter().map(|row| row.is_active)),
        ],
    )
}

fn deribit_quote_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("received_at", DataType::Utf8, false),
        Field::new("received_mts", DataType::Int64, false),
        Field::new("connection_id", DataType::Utf8, false),
        Field::new("subscription_channel", DataType::Utf8, false),
        Field::new("instrument_name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, true),
        Field::new("expiration_timestamp", DataType::Int64, true),
        Field::new("strike", DataType::Utf8, true),
        Field::new("option_type", DataType::Utf8, true),
        Field::new("settlement_period", DataType::Utf8, true),
        Field::new("ticker_timestamp", DataType::Int64, true),
        Field::new("ticker_type", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("best_bid_price", DataType::Utf8, true),
        Field::new("best_bid_amount", DataType::Utf8, true),
        Field::new("best_ask_price", DataType::Utf8, true),
        Field::new("best_ask_amount", DataType::Utf8, true),
        Field::new("index_price", DataType::Utf8, true),
        Field::new("mark_price", DataType::Utf8, true),
        Field::new("last_price", DataType::Utf8, true),
        Field::new("underlying_price", DataType::Utf8, true),
        Field::new("underlying_index", DataType::Utf8, true),
        Field::new("open_interest", DataType::Utf8, true),
        Field::new("settlement_price", DataType::Utf8, true),
        Field::new("estimated_delivery_price", DataType::Utf8, true),
        Field::new("min_price", DataType::Utf8, true),
        Field::new("max_price", DataType::Utf8, true),
        Field::new("bid_iv", DataType::Utf8, true),
        Field::new("ask_iv", DataType::Utf8, true),
        Field::new("mark_iv", DataType::Utf8, true),
        Field::new("funding_8h", DataType::Utf8, true),
        Field::new("current_funding", DataType::Utf8, true),
        Field::new("stats_volume", DataType::Utf8, true),
        Field::new("stats_volume_usd", DataType::Utf8, true),
        Field::new("stats_volume_notional", DataType::Utf8, true),
        Field::new("stats_high", DataType::Utf8, true),
        Field::new("stats_low", DataType::Utf8, true),
        Field::new("stats_price_change", DataType::Utf8, true),
        Field::new("delta", DataType::Utf8, true),
        Field::new("gamma", DataType::Utf8, true),
        Field::new("theta", DataType::Utf8, true),
        Field::new("vega", DataType::Utf8, true),
        Field::new("rho", DataType::Utf8, true),
    ]))
}

fn deribit_quote_columns(rows: &[DeribitQuoteRow]) -> Vec<ArrayRef> {
    vec![
        required_string_array(rows.iter().map(|row| row.exchange.as_str())),
        required_string_array(rows.iter().map(|row| row.symbol.as_str())),
        required_string_array(rows.iter().map(|row| row.received_at.as_str())),
        required_i64_array(rows.iter().map(|row| row.received_mts)),
        required_string_array(rows.iter().map(|row| row.connection_id.as_str())),
        required_string_array(rows.iter().map(|row| row.subscription_channel.as_str())),
        required_string_array(rows.iter().map(|row| row.instrument_name.as_str())),
        optional_string_array(rows.iter().map(|row| row.kind.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.expiration_timestamp)),
        optional_string_array(rows.iter().map(|row| row.strike.as_deref())),
        optional_string_array(rows.iter().map(|row| row.option_type.as_deref())),
        optional_string_array(rows.iter().map(|row| row.settlement_period.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.ticker_timestamp)),
        optional_string_array(rows.iter().map(|row| row.ticker_type.as_deref())),
        optional_string_array(rows.iter().map(|row| row.state.as_deref())),
        optional_string_array(rows.iter().map(|row| row.best_bid_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.best_bid_amount.as_deref())),
        optional_string_array(rows.iter().map(|row| row.best_ask_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.best_ask_amount.as_deref())),
        optional_string_array(rows.iter().map(|row| row.index_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.mark_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.last_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.underlying_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.underlying_index.as_deref())),
        optional_string_array(rows.iter().map(|row| row.open_interest.as_deref())),
        optional_string_array(rows.iter().map(|row| row.settlement_price.as_deref())),
        optional_string_array(
            rows.iter()
                .map(|row| row.estimated_delivery_price.as_deref()),
        ),
        optional_string_array(rows.iter().map(|row| row.min_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.max_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.bid_iv.as_deref())),
        optional_string_array(rows.iter().map(|row| row.ask_iv.as_deref())),
        optional_string_array(rows.iter().map(|row| row.mark_iv.as_deref())),
        optional_string_array(rows.iter().map(|row| row.funding_8h.as_deref())),
        optional_string_array(rows.iter().map(|row| row.current_funding.as_deref())),
        optional_string_array(rows.iter().map(|row| row.stats_volume.as_deref())),
        optional_string_array(rows.iter().map(|row| row.stats_volume_usd.as_deref())),
        optional_string_array(rows.iter().map(|row| row.stats_volume_notional.as_deref())),
        optional_string_array(rows.iter().map(|row| row.stats_high.as_deref())),
        optional_string_array(rows.iter().map(|row| row.stats_low.as_deref())),
        optional_string_array(rows.iter().map(|row| row.stats_price_change.as_deref())),
        optional_string_array(rows.iter().map(|row| row.delta.as_deref())),
        optional_string_array(rows.iter().map(|row| row.gamma.as_deref())),
        optional_string_array(rows.iter().map(|row| row.theta.as_deref())),
        optional_string_array(rows.iter().map(|row| row.vega.as_deref())),
        optional_string_array(rows.iter().map(|row| row.rho.as_deref())),
    ]
}

fn deribit_trade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("received_at", DataType::Utf8, false),
        Field::new("received_mts", DataType::Int64, false),
        Field::new("connection_id", DataType::Utf8, false),
        Field::new("subscription_channel", DataType::Utf8, false),
        Field::new("instrument_name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, true),
        Field::new("expiration_timestamp", DataType::Int64, true),
        Field::new("strike", DataType::Utf8, true),
        Field::new("option_type", DataType::Utf8, true),
        Field::new("settlement_period", DataType::Utf8, true),
        Field::new("trade_timestamp", DataType::Int64, true),
        Field::new("trade_id", DataType::Utf8, true),
        Field::new("trade_seq", DataType::Int64, true),
        Field::new("direction", DataType::Utf8, true),
        Field::new("tick_direction", DataType::Int64, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new("contracts", DataType::Utf8, true),
        Field::new("mark_price", DataType::Utf8, true),
        Field::new("index_price", DataType::Utf8, true),
        Field::new("iv", DataType::Utf8, true),
        Field::new("liquidation", DataType::Utf8, true),
        Field::new("block_trade_id", DataType::Utf8, true),
        Field::new("combo_id", DataType::Utf8, true),
        Field::new("combo_trade_id", DataType::Utf8, true),
        Field::new("block_rfq_id", DataType::Int64, true),
    ]))
}

fn deribit_trade_columns(rows: &[DeribitTradeRow]) -> Vec<ArrayRef> {
    vec![
        required_string_array(rows.iter().map(|row| row.exchange.as_str())),
        required_string_array(rows.iter().map(|row| row.symbol.as_str())),
        required_string_array(rows.iter().map(|row| row.received_at.as_str())),
        required_i64_array(rows.iter().map(|row| row.received_mts)),
        required_string_array(rows.iter().map(|row| row.connection_id.as_str())),
        required_string_array(rows.iter().map(|row| row.subscription_channel.as_str())),
        required_string_array(rows.iter().map(|row| row.instrument_name.as_str())),
        optional_string_array(rows.iter().map(|row| row.kind.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.expiration_timestamp)),
        optional_string_array(rows.iter().map(|row| row.strike.as_deref())),
        optional_string_array(rows.iter().map(|row| row.option_type.as_deref())),
        optional_string_array(rows.iter().map(|row| row.settlement_period.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.trade_timestamp)),
        optional_string_array(rows.iter().map(|row| row.trade_id.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.trade_seq)),
        optional_string_array(rows.iter().map(|row| row.direction.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.tick_direction)),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.amount.as_deref())),
        optional_string_array(rows.iter().map(|row| row.contracts.as_deref())),
        optional_string_array(rows.iter().map(|row| row.mark_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.index_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.iv.as_deref())),
        optional_string_array(rows.iter().map(|row| row.liquidation.as_deref())),
        optional_string_array(rows.iter().map(|row| row.block_trade_id.as_deref())),
        optional_string_array(rows.iter().map(|row| row.combo_id.as_deref())),
        optional_string_array(rows.iter().map(|row| row.combo_trade_id.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.block_rfq_id)),
    ]
}

fn deribit_instrument_state_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("received_at", DataType::Utf8, false),
        Field::new("received_mts", DataType::Int64, false),
        Field::new("connection_id", DataType::Utf8, false),
        Field::new("subscription_channel", DataType::Utf8, false),
        Field::new("instrument_name", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("raw_data", DataType::Utf8, true),
    ]))
}

fn deribit_instrument_state_columns(rows: &[DeribitInstrumentStateRow]) -> Vec<ArrayRef> {
    vec![
        required_string_array(rows.iter().map(|row| row.exchange.as_str())),
        required_string_array(rows.iter().map(|row| row.symbol.as_str())),
        required_string_array(rows.iter().map(|row| row.received_at.as_str())),
        required_i64_array(rows.iter().map(|row| row.received_mts)),
        required_string_array(rows.iter().map(|row| row.connection_id.as_str())),
        required_string_array(rows.iter().map(|row| row.subscription_channel.as_str())),
        optional_string_array(rows.iter().map(|row| row.instrument_name.as_deref())),
        optional_string_array(rows.iter().map(|row| row.kind.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.timestamp)),
        optional_string_array(rows.iter().map(|row| row.state.as_deref())),
        optional_string_array(rows.iter().map(|row| row.raw_data.as_deref())),
    ]
}

fn bitfinex_trade_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("bitfinex_channel_id", DataType::Int64, true),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("is_final", DataType::Boolean, false),
        Field::new("trade_id", DataType::Int64, true),
        Field::new("trade_mts", DataType::Int64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new("amount_abs", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn bitfinex_trade_columns(rows: &[BitfinexTradeRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_i64_array(rows.iter().map(|row| row.bitfinex_channel_id)),
        required_string_array(rows.iter().map(|row| row.event_type.as_str())),
        required_bool_array(rows.iter().map(|row| row.is_final)),
        optional_i64_array(rows.iter().map(|row| row.trade_id)),
        optional_i64_array(rows.iter().map(|row| row.trade_mts)),
        optional_string_array(rows.iter().map(|row| row.side.as_deref())),
        optional_string_array(rows.iter().map(|row| row.amount.as_deref())),
        optional_string_array(rows.iter().map(|row| row.amount_abs.as_deref())),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
    ]);
    columns
}

fn bitfinex_book_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("bitfinex_channel_id", DataType::Int64, true),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("level_index", DataType::Int64, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new("amount_abs", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn bitfinex_book_columns(rows: &[BitfinexBookLevelRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_i64_array(rows.iter().map(|row| row.bitfinex_channel_id)),
        required_string_array(rows.iter().map(|row| row.event_type.as_str())),
        optional_i64_array(rows.iter().map(|row| row.level_index)),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.count)),
        optional_string_array(rows.iter().map(|row| row.amount.as_deref())),
        optional_string_array(rows.iter().map(|row| row.amount_abs.as_deref())),
        optional_string_array(rows.iter().map(|row| row.side.as_deref())),
    ]);
    columns
}

fn hibachi_trade_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("trade_timestamp", DataType::Int64, true),
        Field::new("taker_side", DataType::Utf8, true),
        Field::new("raw_taker_side", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("quantity", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hibachi_trade_columns(rows: &[HibachiTradeRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_i64_array(rows.iter().map(|row| row.trade_timestamp)),
        optional_string_array(rows.iter().map(|row| row.taker_side.as_deref())),
        optional_string_array(rows.iter().map(|row| row.raw_taker_side.as_deref())),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.quantity.as_deref())),
    ]);
    columns
}

fn hibachi_orderbook_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("message_type", DataType::Utf8, true),
        Field::new("book_timestamp_ms", DataType::Int64, true),
        Field::new("depth", DataType::Int64, true),
        Field::new("granularity", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, false),
        Field::new("level_index", DataType::Int64, false),
        Field::new("price", DataType::Utf8, true),
        Field::new("quantity", DataType::Utf8, true),
        Field::new("start_price", DataType::Utf8, true),
        Field::new("end_price", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hibachi_orderbook_columns(rows: &[HibachiBookLevelRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_string_array(rows.iter().map(|row| row.message_type.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.book_timestamp_ms)),
        optional_i64_array(rows.iter().map(|row| row.depth)),
        optional_string_array(rows.iter().map(|row| row.granularity.as_deref())),
        required_string_array(rows.iter().map(|row| row.side.as_str())),
        required_i64_array(rows.iter().map(|row| row.level_index)),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.quantity.as_deref())),
        optional_string_array(rows.iter().map(|row| row.start_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.end_price.as_deref())),
    ]);
    columns
}

fn hibachi_quote_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("bid_price", DataType::Utf8, true),
        Field::new("bid_size", DataType::Utf8, true),
        Field::new("ask_price", DataType::Utf8, true),
        Field::new("ask_size", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hibachi_quote_columns(rows: &[HibachiQuoteRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_string_array(rows.iter().map(|row| row.bid_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.bid_size.as_deref())),
        optional_string_array(rows.iter().map(|row| row.ask_price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.ask_size.as_deref())),
    ]);
    columns
}

fn hibachi_price_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("price_type", DataType::Utf8, false),
        Field::new("price", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hibachi_price_columns(rows: &[HibachiPriceRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        required_string_array(rows.iter().map(|row| row.price_type.as_str())),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
    ]);
    columns
}

fn hibachi_funding_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("estimated_funding_rate", DataType::Utf8, true),
        Field::new("next_funding_timestamp", DataType::Int64, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hibachi_funding_columns(rows: &[HibachiFundingRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_string_array(rows.iter().map(|row| row.estimated_funding_rate.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.next_funding_timestamp)),
    ]);
    columns
}

fn hyperliquid_trade_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("coin", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("raw_side", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
        Field::new("trade_timestamp", DataType::Int64, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new("trade_id", DataType::Int64, true),
        Field::new("user_0", DataType::Utf8, true),
        Field::new("user_1", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hyperliquid_trade_columns(rows: &[HyperliquidTradeRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_string_array(rows.iter().map(|row| row.coin.as_deref())),
        optional_string_array(rows.iter().map(|row| row.side.as_deref())),
        optional_string_array(rows.iter().map(|row| row.raw_side.as_deref())),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.size.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.trade_timestamp)),
        optional_string_array(rows.iter().map(|row| row.hash.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.trade_id)),
        optional_string_array(rows.iter().map(|row| row.user_0.as_deref())),
        optional_string_array(rows.iter().map(|row| row.user_1.as_deref())),
    ]);
    columns
}

fn hyperliquid_book_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("coin", DataType::Utf8, true),
        Field::new("book_timestamp", DataType::Int64, true),
        Field::new("snapshot", DataType::Boolean, true),
        Field::new("side", DataType::Utf8, false),
        Field::new("level_index", DataType::Int64, false),
        Field::new("price", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
        Field::new("order_count", DataType::Int64, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn hyperliquid_book_columns(rows: &[HyperliquidBookLevelRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_string_array(rows.iter().map(|row| row.coin.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.book_timestamp)),
        optional_bool_array(rows.iter().map(|row| row.snapshot)),
        required_string_array(rows.iter().map(|row| row.side.as_str())),
        required_i64_array(rows.iter().map(|row| row.level_index)),
        optional_string_array(rows.iter().map(|row| row.price.as_deref())),
        optional_string_array(rows.iter().map(|row| row.size.as_deref())),
        optional_i64_array(rows.iter().map(|row| row.order_count)),
    ]);
    columns
}

fn hyperliquid_control_schema() -> Arc<Schema> {
    let mut fields = base_event_fields();
    fields.extend([
        Field::new("event", DataType::Utf8, true),
        Field::new("message_channel", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("subscription_type", DataType::Utf8, true),
        Field::new("coin", DataType::Utf8, true),
        Field::new("payload_json", DataType::Utf8, false),
    ]);
    Arc::new(Schema::new(fields))
}

fn hyperliquid_control_columns(rows: &[HyperliquidControlRow]) -> Vec<ArrayRef> {
    let mut columns = base_event_columns(rows, |row| &row.base);
    columns.extend([
        optional_string_array(rows.iter().map(|row| row.event.as_deref())),
        optional_string_array(rows.iter().map(|row| row.message_channel.as_deref())),
        optional_string_array(rows.iter().map(|row| row.method.as_deref())),
        optional_string_array(rows.iter().map(|row| row.subscription_type.as_deref())),
        optional_string_array(rows.iter().map(|row| row.coin.as_deref())),
        required_string_array(rows.iter().map(|row| row.payload_json.as_str())),
    ]);
    columns
}

struct StreamingParquetWriter<R> {
    path: PathBuf,
    tmp_path: PathBuf,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    buffer: Vec<R>,
    row_count: usize,
    batch_size: usize,
    columns: fn(&[R]) -> Vec<ArrayRef>,
}

impl<R> StreamingParquetWriter<R> {
    fn create(
        path: &Path,
        schema: Arc<Schema>,
        columns: fn(&[R]) -> Vec<ArrayRef>,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            bail!("normalizer batch size must be greater than zero");
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let tmp_path = parquet_tmp_path(path);
        let file = File::create(&tmp_path)
            .with_context(|| format!("failed to create {}", tmp_path.display()))?;
        let properties = normalized_writer_properties()?;
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .context("failed to create normalized Parquet writer")?;
        Ok(Self {
            path: path.to_path_buf(),
            tmp_path,
            writer: Some(writer),
            schema,
            buffer: Vec::with_capacity(batch_size),
            row_count: 0,
            batch_size,
            columns,
        })
    }

    fn write(&mut self, row: R) -> Result<()> {
        self.buffer.push(row);
        self.row_count = self.row_count.saturating_add(1);
        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let columns = (self.columns)(&self.buffer);
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .context("failed to build normalized Arrow batch")?;
        self.writer
            .as_mut()
            .context("normalized Parquet writer was already closed")?
            .write(&batch)
            .context("failed to write normalized Parquet batch")?;
        self.buffer.clear();
        Ok(())
    }

    fn close(mut self) -> Result<usize> {
        self.flush()?;
        let writer = self
            .writer
            .take()
            .context("normalized Parquet writer was already closed")?;
        writer
            .close()
            .context("failed to close normalized Parquet writer")?;
        std::fs::rename(&self.tmp_path, &self.path).with_context(|| {
            format!(
                "failed to replace {} with {}",
                self.path.display(),
                self.tmp_path.display()
            )
        })?;
        Ok(self.row_count)
    }
}

fn write_parquet_batch(path: &Path, schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .context("failed to build normalized Arrow batch")?;
    let tmp_path = parquet_tmp_path(path);
    let file = File::create(&tmp_path)
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    let properties = normalized_writer_properties()?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))
        .context("failed to create normalized Parquet writer")?;
    writer
        .write(&batch)
        .context("failed to write normalized Parquet batch")?;
    writer
        .close()
        .context("failed to close normalized Parquet writer")?;
    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to replace {} with {}",
            path.display(),
            tmp_path.display()
        )
    })?;
    Ok(())
}

fn normalized_writer_properties() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).context("invalid zstd compression level")?,
        ))
        .build())
}

fn parquet_tmp_path(path: &Path) -> PathBuf {
    path.with_extension(
        path.extension()
            .and_then(|extension| extension.to_str())
            .map_or_else(|| "tmp".to_owned(), |extension| format!("{extension}.tmp")),
    )
}

fn required_string_array<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(values.collect::<StringArray>())
}

fn required_i64_array(values: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

fn optional_i64_array(values: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(values.collect::<Int64Array>())
}

fn required_bool_array(values: impl Iterator<Item = bool>) -> ArrayRef {
    Arc::new(values.collect::<BooleanArray>())
}

fn optional_bool_array(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(values.collect::<BooleanArray>())
}

fn base_event_fields() -> Vec<Field> {
    vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("received_at", DataType::Utf8, false),
        Field::new("received_mts", DataType::Int64, false),
        Field::new("connection_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
    ]
}

fn base_event_columns<R>(rows: &[R], base: impl Fn(&R) -> &NormalizedEventFields) -> Vec<ArrayRef> {
    vec![
        required_string_array(rows.iter().map(|row| base(row).exchange.as_str())),
        required_string_array(rows.iter().map(|row| base(row).symbol.as_str())),
        required_string_array(rows.iter().map(|row| base(row).received_at.as_str())),
        required_i64_array(rows.iter().map(|row| base(row).received_mts)),
        required_string_array(rows.iter().map(|row| base(row).connection_id.as_str())),
        required_string_array(rows.iter().map(|row| base(row).channel.as_str())),
    ]
}

fn normalized_parquet_path(
    output_dir: &Path,
    symbol_name: &str,
    dataset: &str,
    date: NaiveDate,
) -> PathBuf {
    output_dir.join(dataset).join(format!(
        "{}_{}_{}.parquet",
        symbol_name,
        dataset,
        date.format("%y-%m-%d")
    ))
}

#[cfg(test)]
mod tests {
    use std::{io::BufWriter, path::Path};

    use super::*;

    fn temp_test_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "modl-normalizer-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("current time should be after Unix epoch")
                .as_nanos()
        ))
    }

    fn test_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 29).expect("valid test date")
    }

    fn sample_raw_event(channel: &str, payload_text: &str) -> Value {
        sample_raw_event_for("deribit", "BTC", channel, payload_text)
    }

    fn sample_raw_event_for(
        exchange: &str,
        symbol: &str,
        channel: &str,
        payload_text: &str,
    ) -> Value {
        serde_json::json!({
            "received_at": "2026-06-29T22:17:08.192Z",
            "received_mts": 1_782_771_428_192_i64,
            "exchange": exchange,
            "symbol": symbol,
            "channel": channel,
            "connection_id": format!("{exchange}-{symbol}-{channel}"),
            "payload_text": payload_text
        })
    }

    fn sample_event_owned(exchange: &str, symbol: &str, channel: &str) -> RawWsEventOwned {
        RawWsEventOwned {
            received_at: "2026-06-29T22:17:08.192Z".to_owned(),
            received_mts: 1_782_771_428_192,
            exchange: exchange.to_owned(),
            symbol: symbol.to_owned(),
            channel: channel.to_owned(),
            connection_id: format!("{exchange}-{symbol}-{channel}"),
            payload_text: None,
        }
    }

    fn write_raw_zstd_file(path: &Path, events: &[Value]) -> Result<()> {
        std::fs::create_dir_all(path.parent().expect("raw path has parent"))?;
        let file = std::fs::File::create(path)?;
        let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), 1)?;
        for event in events {
            serde_json::to_writer(&mut encoder, event)?;
            std::io::Write::write_all(&mut encoder, b"\n")?;
        }
        encoder.finish()?;
        Ok(())
    }

    fn parquet_num_rows(path: &Path) -> Result<i64> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let file = std::fs::File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        Ok(reader.metadata().file_metadata().num_rows())
    }

    #[test]
    fn normalizes_deribit_quote_and_trade_metadata() {
        let instrument = serde_json::json!({
            "instrument_name": "BTC-31JUL26-60000-P",
            "kind": "option",
            "base_currency": "BTC",
            "quote_currency": "BTC",
            "settlement_currency": "BTC",
            "expiration_timestamp": 1_785_484_800_000_i64,
            "creation_timestamp": 1_777_993_440_000_i64,
            "strike": 60000.0,
            "option_type": "put",
            "settlement_period": "month",
            "is_active": true
        });
        let meta = deribit_instrument_meta_from_value(&instrument).expect("instrument metadata");
        let instruments = HashMap::from([(meta.instrument_name.clone(), meta)]);
        let event = RawWsEventOwned {
            received_at: "2026-06-29T22:17:08.192Z".to_owned(),
            received_mts: 1_782_771_428_192,
            exchange: "deribit".to_owned(),
            symbol: "BTC".to_owned(),
            channel: "incremental_ticker".to_owned(),
            connection_id: "deribit-BTC-instruments".to_owned(),
            payload_text: None,
        };

        let ticker = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "incremental_ticker.BTC-31JUL26-60000-P",
                "data": {
                    "timestamp": 1_782_771_427_066_i64,
                    "type": "snapshot",
                    "instrument_name": "BTC-31JUL26-60000-P",
                    "mark_price": 0.043_449_2,
                    "stats": {"volume": 5.0, "volume_usd": 300_000.0}
                }
            }
        });
        let quote =
            deribit_quote_row_from_message(&event, &ticker, &instruments).expect("ticker row");
        assert_eq!(quote.kind.as_deref(), Some("option"));
        assert_eq!(quote.strike.as_deref(), Some("60000.0"));
        assert_eq!(quote.option_type.as_deref(), Some("put"));
        assert_eq!(quote.mark_price.as_deref(), Some("0.0434492"));
        assert_eq!(quote.stats_volume.as_deref(), Some("5.0"));

        let trades = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "trades.BTC-31JUL26-60000-P.100ms",
                "data": [{
                    "timestamp": 1_782_772_067_807_i64,
                    "iv": 41.51,
                    "price": 0.044,
                    "amount": 0.9,
                    "direction": "buy",
                    "index_price": 60377.86,
                    "instrument_name": "BTC-31JUL26-60000-P",
                    "trade_seq": 2265,
                    "mark_price": 0.043_449_2,
                    "tick_direction": 2,
                    "contracts": 0.9,
                    "trade_id": "436176324"
                }]
            }
        });
        let trade_rows = deribit_trade_rows_from_message(&event, &trades, &instruments);
        assert_eq!(trade_rows.len(), 1);
        assert_eq!(trade_rows[0].kind.as_deref(), Some("option"));
        assert_eq!(trade_rows[0].strike.as_deref(), Some("60000.0"));
        assert_eq!(trade_rows[0].price.as_deref(), Some("0.044"));
        assert_eq!(trade_rows[0].trade_id.as_deref(), Some("436176324"));

        let state = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "instrument.state.BTC-PERPETUAL.raw",
                "data": {
                    "timestamp": 1_782_772_068_000_i64,
                    "instrument_name": "BTC-PERPETUAL",
                    "kind": "future",
                    "state": "open"
                }
            }
        });
        let state_row =
            deribit_instrument_state_row_from_message(&event, &state).expect("state row");
        assert_eq!(state_row.instrument_name.as_deref(), Some("BTC-PERPETUAL"));
        assert_eq!(state_row.kind.as_deref(), Some("future"));
        assert_eq!(state_row.timestamp, Some(1_782_772_068_000));
        assert_eq!(state_row.state.as_deref(), Some("open"));
    }

    #[test]
    fn normalizes_bitfinex_trade_and_book_messages() {
        let event = sample_event_owned("bitfinex", "tBTCUSD", "trades");
        let snapshot = serde_json::json!([
            278_551,
            [
                [1_942_614_150_i64, 1_782_761_937_606_i64, -0.0002, 60357],
                [1_942_614_136_i64, 1_782_761_920_198_i64, 0.00598, 60376]
            ]
        ]);
        let rows = bitfinex_trade_rows_from_message(&event, &snapshot);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].event_type, "snapshot");
        assert!(rows[0].is_final);
        assert_eq!(rows[0].side.as_deref(), Some("sell"));
        assert_eq!(rows[0].amount_abs.as_deref(), Some("0.0002"));
        assert_eq!(rows[1].side.as_deref(), Some("buy"));

        let execution = serde_json::json!([
            278_551,
            "te",
            [1_942_614_151_i64, 1_782_761_937_607_i64, 0.1000, 60358]
        ]);
        let execution_rows = bitfinex_trade_rows_from_message(&event, &execution);
        assert_eq!(execution_rows.len(), 1);
        assert_eq!(execution_rows[0].event_type, "te");
        assert!(!execution_rows[0].is_final);

        let update = serde_json::json!([
            278_551,
            "tu",
            [1_942_614_151_i64, 1_782_761_937_607_i64, -0.1000, 60358]
        ]);
        let update_rows = bitfinex_trade_rows_from_message(&event, &update);
        assert_eq!(update_rows.len(), 1);
        assert_eq!(update_rows[0].event_type, "tu");
        assert!(update_rows[0].is_final);
        assert_eq!(update_rows[0].side.as_deref(), Some("sell"));

        let book_event = sample_event_owned("bitfinex", "tBTCUSD", "book_l25");
        let book_snapshot = serde_json::json!([
            1_372_681,
            [[60360, 1, 0.082_836_31], [60368, 1, -0.000_220_01]]
        ]);
        let book_rows = bitfinex_book_rows_from_message(&book_event, &book_snapshot);
        assert_eq!(book_rows.len(), 2);
        assert_eq!(book_rows[0].event_type, "snapshot");
        assert_eq!(book_rows[0].level_index, Some(0));
        assert_eq!(book_rows[0].side.as_deref(), Some("bid"));
        assert_eq!(book_rows[1].side.as_deref(), Some("ask"));

        let book_update = serde_json::json!([1_372_681, [60361, 1, -0.108_815_5]]);
        let update_rows = bitfinex_book_rows_from_message(&book_event, &book_update);
        assert_eq!(update_rows.len(), 1);
        assert_eq!(update_rows[0].event_type, "update");
        assert_eq!(update_rows[0].side.as_deref(), Some("ask"));
    }

    #[test]
    fn normalizes_hibachi_market_data_topics() {
        let event = sample_event_owned("hibachi", "BTC/USDT-P", "market_data");
        let trade = serde_json::json!({
            "data": {
                "trade": {
                    "price": "60351.00000",
                    "quantity": "0.0000222946",
                    "takerSide": "Buy",
                    "timestamp": 1_782_761_952_i64
                }
            },
            "symbol": "BTC/USDT-P",
            "topic": "trades"
        });
        let trade_row = hibachi_trade_row_from_message(&event, &trade).expect("trade row");
        assert_eq!(trade_row.trade_timestamp, Some(1_782_761_952));
        assert_eq!(trade_row.taker_side.as_deref(), Some("buy"));
        assert_eq!(trade_row.raw_taker_side.as_deref(), Some("Buy"));
        assert_eq!(trade_row.price.as_deref(), Some("60351.00000"));

        let orderbook = serde_json::json!({
            "data": {
                "ask": {
                    "endPrice": "60357.4",
                    "levels": [{"price": "60339.9", "quantity": "0.0074449901"}],
                    "startPrice": "60339.9"
                },
                "bid": {
                    "endPrice": "60323.5",
                    "levels": [
                        {"price": "60339.8", "quantity": "0.0166066700"},
                        {"price": "60339.7", "quantity": "0.0100000000"}
                    ],
                    "startPrice": "60339.8"
                }
            },
            "depth": 20,
            "granularity": "0.1",
            "messageType": "Snapshot",
            "timestamp_ms": 1_782_761_945_377_i64,
            "topic": "orderbook"
        });
        let book_rows = hibachi_orderbook_rows_from_message(&event, &orderbook);
        assert_eq!(book_rows.len(), 3);
        assert_eq!(book_rows[0].side, "bid");
        assert_eq!(book_rows[0].level_index, 0);
        assert_eq!(book_rows[1].side, "bid");
        assert_eq!(book_rows[1].level_index, 1);
        assert_eq!(book_rows[2].side, "ask");
        assert_eq!(book_rows[2].price.as_deref(), Some("60339.9"));

        let quote = serde_json::json!({
            "data": {
                "askPrice": "60339.90000",
                "askSize": "0.0074449901",
                "bidPrice": "60339.80000",
                "bidSize": "0.0166066700"
            },
            "topic": "ask_bid_price"
        });
        let quote_row = hibachi_quote_row_from_message(&event, &quote).expect("quote row");
        assert_eq!(quote_row.bid_price.as_deref(), Some("60339.80000"));
        assert_eq!(quote_row.ask_size.as_deref(), Some("0.0074449901"));

        let mark = serde_json::json!({"data":{"markPrice":"60339.90000"},"topic":"mark_price"});
        let mark_row = hibachi_price_row_from_message(&event, &mark).expect("mark row");
        assert_eq!(mark_row.price_type, "mark_price");
        assert_eq!(mark_row.price.as_deref(), Some("60339.90000"));

        let funding = serde_json::json!({
            "data": {
                "fundingRateEstimation": {
                    "estimatedFundingRate": "0.000036",
                    "nextFundingTimestamp": 1_782_763_200_i64
                }
            },
            "topic": "funding_rate_estimation"
        });
        let funding_row = hibachi_funding_row_from_message(&event, &funding).expect("funding row");
        assert_eq!(
            funding_row.estimated_funding_rate.as_deref(),
            Some("0.000036")
        );
        assert_eq!(funding_row.next_funding_timestamp, Some(1_782_763_200));
    }

    #[test]
    fn normalizes_hyperliquid_messages() {
        let event = sample_event_owned("hyperliquid", "UBTC/USDC", "trades");
        let trades = serde_json::json!({
            "channel": "trades",
            "data": [
                {
                    "coin": "@142",
                    "side": "B",
                    "px": "60196.0",
                    "sz": "0.00126",
                    "time": 1_782_767_091_000_i64,
                    "hash": "0x0",
                    "tid": 1_034_424_858_715_071_i64,
                    "users": ["0xa", "0xb"]
                },
                {
                    "coin": "@142",
                    "side": "A",
                    "px": "60187.0",
                    "sz": "0.00026",
                    "time": 1_782_767_137_928_i64,
                    "hash": "0x1",
                    "tid": 1_111_024_960_583_881_i64,
                    "users": ["0xc", "0xd"]
                }
            ]
        });
        let trade_rows = hyperliquid_trade_rows_from_message(&event, &trades);
        assert_eq!(trade_rows.len(), 2);
        assert_eq!(trade_rows[0].side.as_deref(), Some("buy"));
        assert_eq!(trade_rows[0].raw_side.as_deref(), Some("B"));
        assert_eq!(trade_rows[0].user_0.as_deref(), Some("0xa"));
        assert_eq!(trade_rows[1].side.as_deref(), Some("sell"));
        assert_eq!(trade_rows[1].raw_side.as_deref(), Some("A"));

        let book_event = sample_event_owned("hyperliquid", "UBTC/USDC", "book");
        let book = serde_json::json!({
            "channel": "l2Book",
            "data": {
                "coin": "@142",
                "time": 1_782_767_204_732_i64,
                "snapshot": false,
                "levels": [
                    [{"px": "60212.0", "sz": "0.83902", "n": 2}],
                    [{"px": "60213.0", "sz": "0.67412", "n": 8}]
                ]
            }
        });
        let book_rows = hyperliquid_book_rows_from_message(&book_event, &book);
        assert_eq!(book_rows.len(), 2);
        assert_eq!(book_rows[0].side, "bid");
        assert_eq!(book_rows[0].order_count, Some(2));
        assert_eq!(book_rows[1].side, "ask");
        assert_eq!(book_rows[1].price.as_deref(), Some("60213.0"));

        let control_event = sample_event_owned("hyperliquid", "UBTC/USDC", "control");
        let control_text = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"@142"}}}"#;
        let control = serde_json::from_str::<Value>(control_text).expect("control JSON");
        let control_row =
            hyperliquid_control_row_from_message(&control_event, &control, control_text);
        assert_eq!(
            control_row.message_channel.as_deref(),
            Some("subscriptionResponse")
        );
        assert_eq!(control_row.method.as_deref(), Some("subscribe"));
        assert_eq!(control_row.subscription_type.as_deref(), Some("trades"));
        assert_eq!(control_row.coin.as_deref(), Some("@142"));
        assert_eq!(control_row.payload_json, control_text);
    }

    #[test]
    fn writes_deribit_normalized_parquet_files() -> Result<()> {
        let date = test_date();
        let dir = temp_test_dir();
        let input_dir = dir.join("raw");
        let output_dir = dir.join("normalized");
        let deribit_symbol_dir = input_dir.join("deribit").join("btc");

        let control_payload = r#"{"jsonrpc":"2.0","id":2,"result":[{"instrument_name":"BTC-31JUL26-60000-P","kind":"option","base_currency":"BTC","quote_currency":"BTC","settlement_currency":"BTC","expiration_timestamp":1785484800000,"creation_timestamp":1777993440000,"strike":60000.0,"option_type":"put","settlement_period":"month","is_active":true}]}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&deribit_symbol_dir, "btc", "control", date)?,
            &[sample_raw_event("control", control_payload)],
        )?;

        let ticker_payload = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"incremental_ticker.BTC-31JUL26-60000-P","data":{"timestamp":1782771427066,"type":"snapshot","instrument_name":"BTC-31JUL26-60000-P","mark_price":0.0434492,"stats":{"volume":5.0}}}}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&deribit_symbol_dir, "btc", "incremental_ticker", date)?,
            &[sample_raw_event("incremental_ticker", ticker_payload)],
        )?;

        let trades_payload = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-31JUL26-60000-P.100ms","data":[{"timestamp":1782772067807,"iv":41.51,"price":0.044,"amount":0.9,"direction":"buy","index_price":60377.86,"instrument_name":"BTC-31JUL26-60000-P","trade_seq":2265,"mark_price":0.0434492,"tick_direction":2,"contracts":0.9,"trade_id":"436176324"}]}}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&deribit_symbol_dir, "btc", "trades", date)?,
            &[sample_raw_event("trades", trades_payload)],
        )?;

        let instrument_state_payload = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"instrument.state.BTC-PERPETUAL.raw","data":{"timestamp":1782772068000,"instrument_name":"BTC-PERPETUAL","kind":"future","state":"open"}}}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&deribit_symbol_dir, "btc", "instrument_state", date)?,
            &[sample_raw_event(
                "instrument_state",
                instrument_state_payload,
            )],
        )?;

        run_command(&NormalizeArgs {
            date,
            input_dir,
            output_dir: output_dir.clone(),
        })?;

        let normalized_dir = output_dir.join("deribit").join("btc");
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &normalized_dir,
                "btc",
                "instruments",
                date
            ))?,
            1
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &normalized_dir,
                "btc",
                "incremental_ticker",
                date
            ))?,
            1
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &normalized_dir,
                "btc",
                "trades",
                date
            ))?,
            1
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &normalized_dir,
                "btc",
                "instrument_state",
                date
            ))?,
            1
        );

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn writes_non_extended_normalized_parquet_files() -> Result<()> {
        let date = test_date();
        let dir = temp_test_dir();
        let input_dir = dir.join("raw");
        let output_dir = dir.join("normalized");

        let bitfinex_dir = input_dir.join("bitfinex").join("tbtcusd");
        write_raw_zstd_file(
            &raw_channel_file_path(&bitfinex_dir, "tbtcusd", "trades", date)?,
            &[sample_raw_event_for(
                "bitfinex",
                "tBTCUSD",
                "trades",
                r"[278551,[[1942614150,1782761937606,-0.0002,60357],[1942614136,1782761920198,0.00598,60376]]]",
            )],
        )?;
        write_raw_zstd_file(
            &raw_channel_file_path(&bitfinex_dir, "tbtcusd", "book_l25", date)?,
            &[
                sample_raw_event_for(
                    "bitfinex",
                    "tBTCUSD",
                    "book_l25",
                    r"[1372681,[[60360,1,0.08283631],[60368,1,-0.00022001]]]",
                ),
                sample_raw_event_for(
                    "bitfinex",
                    "tBTCUSD",
                    "book_l25",
                    r"[1372681,[60361,1,0.1088155]]",
                ),
            ],
        )?;

        let hibachi_dir = input_dir.join("hibachi").join("btc_usdt-p");
        write_raw_zstd_file(
            &raw_channel_file_path(&hibachi_dir, "btc_usdt-p", "market_data", date)?,
            &[
                sample_raw_event_for(
                    "hibachi",
                    "BTC/USDT-P",
                    "market_data",
                    r#"{"data":{"trade":{"price":"60351.00000","quantity":"0.0000222946","takerSide":"Buy","timestamp":1782761952}},"symbol":"BTC/USDT-P","topic":"trades"}"#,
                ),
                sample_raw_event_for(
                    "hibachi",
                    "BTC/USDT-P",
                    "market_data",
                    r#"{"data":{"ask":{"endPrice":"60357.4","levels":[{"price":"60339.9","quantity":"0.0074449901"}],"startPrice":"60339.9"},"bid":{"endPrice":"60323.5","levels":[{"price":"60339.8","quantity":"0.0166066700"}],"startPrice":"60339.8"}},"depth":20,"granularity":"0.1","messageType":"Snapshot","symbol":"BTC/USDT-P","timestamp_ms":1782761945377,"topic":"orderbook"}"#,
                ),
                sample_raw_event_for(
                    "hibachi",
                    "BTC/USDT-P",
                    "market_data",
                    r#"{"data":{"askPrice":"60339.90000","askSize":"0.0074449901","bidPrice":"60339.80000","bidSize":"0.0166066700"},"symbol":"BTC/USDT-P","topic":"ask_bid_price"}"#,
                ),
                sample_raw_event_for(
                    "hibachi",
                    "BTC/USDT-P",
                    "market_data",
                    r#"{"data":{"markPrice":"60339.90000"},"symbol":"BTC/USDT-P","topic":"mark_price"}"#,
                ),
                sample_raw_event_for(
                    "hibachi",
                    "BTC/USDT-P",
                    "market_data",
                    r#"{"data":{"spotPrice":"60325.66597"},"symbol":"BTC/USDT-P","topic":"spot_price"}"#,
                ),
                sample_raw_event_for(
                    "hibachi",
                    "BTC/USDT-P",
                    "market_data",
                    r#"{"data":{"fundingRateEstimation":{"estimatedFundingRate":"0.000036","nextFundingTimestamp":1782763200}},"symbol":"BTC/USDT-P","topic":"funding_rate_estimation"}"#,
                ),
            ],
        )?;

        let hyperliquid_dir = input_dir.join("hyperliquid").join("ubtc_usdc");
        write_raw_zstd_file(
            &raw_channel_file_path(&hyperliquid_dir, "ubtc_usdc", "trades", date)?,
            &[sample_raw_event_for(
                "hyperliquid",
                "UBTC/USDC",
                "trades",
                r#"{"channel":"trades","data":[{"coin":"@142","side":"B","px":"60196.0","sz":"0.00126","time":1782767091000,"hash":"0x0","tid":1034424858715071,"users":["0xa","0xb"]},{"coin":"@142","side":"A","px":"60187.0","sz":"0.00026","time":1782767137928,"hash":"0x1","tid":1111024960583881,"users":["0xc","0xd"]}]}"#,
            )],
        )?;
        write_raw_zstd_file(
            &raw_channel_file_path(&hyperliquid_dir, "ubtc_usdc", "book", date)?,
            &[sample_raw_event_for(
                "hyperliquid",
                "UBTC/USDC",
                "book",
                r#"{"channel":"l2Book","data":{"coin":"@142","time":1782767204732,"snapshot":false,"levels":[[{"px":"60212.0","sz":"0.83902","n":2}],[{"px":"60213.0","sz":"0.67412","n":8}]]}}"#,
            )],
        )?;
        write_raw_zstd_file(
            &raw_channel_file_path(&hyperliquid_dir, "ubtc_usdc", "control", date)?,
            &[sample_raw_event_for(
                "hyperliquid",
                "UBTC/USDC",
                "control",
                r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"@142"}}}"#,
            )],
        )?;

        run_command(&NormalizeArgs {
            date,
            input_dir,
            output_dir: output_dir.clone(),
        })?;

        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("bitfinex").join("tbtcusd"),
                "tbtcusd",
                "trades",
                date
            ))?,
            2
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("bitfinex").join("tbtcusd"),
                "tbtcusd",
                "book_l25",
                date
            ))?,
            3
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("hibachi").join("btc_usdt-p"),
                "btc_usdt-p",
                "orderbook",
                date
            ))?,
            2
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("hibachi").join("btc_usdt-p"),
                "btc_usdt-p",
                "prices",
                date
            ))?,
            2
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("hyperliquid").join("ubtc_usdc"),
                "ubtc_usdc",
                "book",
                date
            ))?,
            2
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("hyperliquid").join("ubtc_usdc"),
                "ubtc_usdc",
                "trades",
                date
            ))?,
            2
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &output_dir.join("hyperliquid").join("ubtc_usdc"),
                "ubtc_usdc",
                "control",
                date
            ))?,
            1
        );

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }
}
