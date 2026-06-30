use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use hypersdk::hypercore::{
    self as hypercore,
    types::{Incoming as HyperliquidIncoming, Subscription as HyperliquidSubscription},
    ws::Event as HyperliquidEvent,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use reqwest::{Client, StatusCode, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, Interval, MissedTickBehavior, interval, interval_at, sleep},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Error as WsError, Message,
        client::IntoClientRequest,
        error::ProtocolError,
        http::{HeaderValue, header::USER_AGENT},
        protocol::CloseFrame,
    },
};

const API_BASE: &str = "https://api-pub.bitfinex.com/v2/";
const DEFAULT_SYMBOL: &str = "tBTCUSD";
const MAX_LIMIT: u16 = 10_000;
const RATE_LIMIT_PAUSE: Duration = Duration::from_secs(65);
const MAX_ATTEMPTS: u8 = 6;
const CHECKPOINT_FILE: &str = ".checkpoint.json";
const CHECKPOINT_TMP_FILE: &str = ".checkpoint.json.tmp";
const CHECKPOINT_VERSION: u8 = 1;
const DEFAULT_WS_HEARTBEAT_SECS: u64 = 20;
const DEFAULT_WS_OUTPUT_DIR: &str = "/mnt/burner-archive/ws_raw";
const DEFAULT_NORMALIZED_OUTPUT_DIR: &str = "/mnt/burner-archive/ws_normalized";
const DEFAULT_EXTENDED_MARKET: &str = "BTC-USD";
const DEFAULT_EXTENDED_SPOT_MARKET: &str = "BTCSPOT-USD";
const DEFAULT_HIBACHI_SYMBOL: &str = "BTC/USDT-P";
const DEFAULT_DERIBIT_TRADES_INTERVAL: &str = "100ms";
const DERIBIT_SUBSCRIBE_CHUNK_SIZE: usize = 200;
const DERIBIT_SUBSCRIBE_CHUNK_DELAY: Duration = Duration::from_millis(250);
const DEFAULT_DERIBIT_WS_URL: &str = "wss://www.deribit.com/ws/api/v2";
const DEFAULT_HIBACHI_MARKET_WS_URL: &str = concat!(
    "wss://data-api.hibachi.xyz/ws/market?hibachiClient=",
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION")
);

#[derive(Debug, Parser)]
#[command(about = "Pull and record public crypto market data")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    history: HistoryArgs,
}

#[derive(Debug, Parser)]
#[command(about = "Normalize raw websocket JSONL.zst files into daily Parquet datasets")]
struct NormalizeCli {
    #[command(flatten)]
    args: NormalizeArgs,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Record stable BTC websocket venues to daily compressed JSONL.
    Btc(BtcArgs),

    /// Normalize raw websocket JSONL.zst files into daily Parquet datasets.
    Normalize(NormalizeArgs),

    /// Record realtime websocket market data as daily compressed raw JSONL.
    Stream(StreamArgs),
}

#[derive(Clone, Debug, Args)]
struct HistoryArgs {
    /// Bitfinex symbol to pull.
    #[arg(long, default_value = DEFAULT_SYMBOL)]
    symbol: String,

    /// Inclusive start time as Unix milliseconds or RFC3339. Enables forward paging.
    #[arg(long, value_parser = parse_mts)]
    start: Option<i64>,

    /// Inclusive end time as Unix milliseconds or RFC3339.
    #[arg(long, value_parser = parse_mts)]
    end: Option<i64>,

    /// Records per REST request.
    #[arg(long, default_value_t = MAX_LIMIT, value_parser = clap::value_parser!(u16).range(1..=i64::from(MAX_LIMIT)))]
    limit: u16,

    /// Process-wide REST request budget. Bitfinex limits vary by endpoint; 10 rpm is conservative.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..=90))]
    rpm: u32,

    /// Stop after writing this many trades.
    #[arg(long)]
    max_trades: Option<usize>,

    /// Output root directory. Files are written under <output-dir>/<symbol>/.
    #[arg(short = 'o', long = "output-dir", default_value = ".")]
    output_dir: PathBuf,

    /// Number of trades to buffer per Parquet row group.
    #[arg(long, default_value_t = usize::from(MAX_LIMIT), value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Start from --start even if a checkpoint exists.
    #[arg(long)]
    ignore_checkpoint: bool,
}

#[derive(Clone, Debug, Args)]
struct StreamArgs {
    /// Venue preset(s) to record. Repeat the flag or pass comma-separated values.
    #[arg(
        long = "venue",
        value_enum,
        value_delimiter = ',',
        default_value = "bitfinex"
    )]
    venues: Vec<StreamVenue>,

    /// Output root directory. Files are written under <output-dir>/<exchange>/<symbol>/<channel>/.
    #[arg(short = 'o', long = "output-dir", default_value = ".")]
    output_dir: PathBuf,

    /// Bitfinex public websocket symbol.
    #[arg(long, default_value = DEFAULT_SYMBOL)]
    bitfinex_symbol: String,

    /// Extended market name.
    #[arg(long, default_value = DEFAULT_EXTENDED_MARKET)]
    extended_market: String,

    /// Extended spot BTC market name.
    #[arg(long, default_value = DEFAULT_EXTENDED_SPOT_MARKET)]
    extended_spot_market: String,

    /// Hibachi market symbol.
    #[arg(long, default_value = DEFAULT_HIBACHI_SYMBOL)]
    hibachi_symbol: String,

    /// Hibachi websocket URL.
    #[arg(long, env = "HIBACHI_WS_URL", default_value = DEFAULT_HIBACHI_MARKET_WS_URL)]
    hibachi_url: String,

    /// Deribit websocket URL.
    #[arg(long, env = "DERIBIT_WS_URL", default_value = DEFAULT_DERIBIT_WS_URL)]
    deribit_url: String,

    /// Deribit BTC instrument kinds to track. Repeat the flag or pass comma-separated values.
    #[arg(
        long = "deribit-kind",
        value_enum,
        value_delimiter = ',',
        default_value = "future,option"
    )]
    deribit_kinds: Vec<DeribitInstrumentKind>,

    /// Deribit trade notification interval for dynamically-created instruments.
    #[arg(long, default_value = DEFAULT_DERIBIT_TRADES_INTERVAL, value_parser = parse_deribit_trades_interval)]
    deribit_trades_interval: String,

    /// Hyperliquid spot market coin to subscribe. Omit to resolve BTC spot as UBTC/USDC.
    #[arg(long)]
    hyperliquid_spot_coin: Option<String>,

    /// Zstd compression level for raw JSONL files.
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(i32).range(1..=22))]
    zstd_level: i32,

    /// Stop each feed after this many data messages. Mostly useful for smoke tests.
    #[arg(long)]
    max_messages: Option<usize>,

    /// Delay before reconnecting a failed websocket.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=300))]
    reconnect_delay_secs: u64,

    /// Websocket protocol ping interval in seconds. Set to 0 to disable client pings.
    #[arg(long = "heartbeat-secs", default_value_t = DEFAULT_WS_HEARTBEAT_SECS, value_parser = clap::value_parser!(u64).range(0..=300))]
    heartbeat_secs: u64,
}

#[derive(Clone, Debug, Args)]
struct BtcArgs {
    /// Output root directory. Files are written under <output-dir>/<exchange>/<symbol>/<channel>/.
    #[arg(short = 'o', long = "output-dir", default_value = DEFAULT_WS_OUTPUT_DIR)]
    output_dir: PathBuf,

    /// Zstd compression level for raw JSONL files.
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(i32).range(1..=22))]
    zstd_level: i32,

    /// Stop each feed after this many data messages. Mostly useful for smoke tests.
    #[arg(long)]
    max_messages: Option<usize>,

    /// Delay before reconnecting a failed websocket.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=300))]
    reconnect_delay_secs: u64,

    /// Websocket protocol ping interval in seconds. Set to 0 to disable client pings.
    #[arg(long = "heartbeat-secs", default_value_t = DEFAULT_WS_HEARTBEAT_SECS, value_parser = clap::value_parser!(u64).range(0..=300))]
    heartbeat_secs: u64,

    /// Hibachi websocket URL.
    #[arg(long, env = "HIBACHI_WS_URL", default_value = DEFAULT_HIBACHI_MARKET_WS_URL)]
    hibachi_url: String,

    /// Deribit websocket URL.
    #[arg(long, env = "DERIBIT_WS_URL", default_value = DEFAULT_DERIBIT_WS_URL)]
    deribit_url: String,

    /// Hyperliquid spot market coin to subscribe. Omit to resolve BTC spot as UBTC/USDC.
    #[arg(long)]
    hyperliquid_spot_coin: Option<String>,
}

#[derive(Clone, Debug, Args)]
struct NormalizeArgs {
    /// UTC capture day to normalize, formatted as YYYY-MM-DD.
    #[arg(long, value_parser = parse_date)]
    date: NaiveDate,

    /// Raw websocket archive root.
    #[arg(short = 'i', long = "input-dir", default_value = DEFAULT_WS_OUTPUT_DIR)]
    input_dir: PathBuf,

    /// Normalized Parquet output root.
    #[arg(short = 'o', long = "output-dir", default_value = DEFAULT_NORMALIZED_OUTPUT_DIR)]
    output_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum StreamVenue {
    Bitfinex,
    Extended,
    Hibachi,
    Deribit,
    Hyperliquid,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
enum DeribitInstrumentKind {
    Future,
    Option,
}

impl DeribitInstrumentKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Future => "future",
            Self::Option => "option",
        }
    }
}

#[derive(Debug, Clone)]
struct Trade {
    exchange: &'static str,
    symbol: String,
    id: i64,
    mts: i64,
    timestamp: DateTime<Utc>,
    side: TradeSide,
    amount: Decimal,
    amount_abs: Decimal,
    price: Decimal,
}

#[derive(Debug, Clone, Copy)]
enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

impl TradeSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
struct RateGate {
    permits: Arc<Semaphore>,
    _refill_task: JoinHandle<()>,
}

impl RateGate {
    fn new(rpm: u32) -> Result<Self> {
        if rpm == 0 {
            bail!("rpm must be greater than zero");
        }

        let interval_duration = Duration::from_secs_f64(60.0 / f64::from(rpm));
        let permits = Arc::new(Semaphore::new(1));
        let refill_permits = Arc::clone(&permits);
        let refill_task = tokio::spawn(async move {
            let mut ticks = interval(interval_duration);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticks.tick().await;

            loop {
                ticks.tick().await;
                if refill_permits.available_permits() == 0 {
                    refill_permits.add_permits(1);
                }
            }
        });

        Ok(Self {
            permits,
            _refill_task: refill_task,
        })
    }

    async fn wait(&self) -> Result<()> {
        let permit = self
            .permits
            .acquire()
            .await
            .context("rate limiter was closed")?;
        permit.forget();
        Ok(())
    }
}

#[derive(Debug)]
struct BitfinexClient {
    http: Client,
    rate_gate: Arc<RateGate>,
}

impl BitfinexClient {
    fn new(rpm: u32) -> Result<Self> {
        let http = Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            http,
            rate_gate: Arc::new(RateGate::new(rpm)?),
        })
    }

    async fn trades(&self, query: &TradeQuery<'_>) -> Result<Vec<Trade>> {
        let mut attempt = 0;

        loop {
            self.rate_gate.wait().await?;
            attempt += 1;

            let result = self.fetch_trades_once(query).await;
            match result {
                Ok(response) => return Ok(response),
                Err(error) if is_retryable(&error) && attempt < MAX_ATTEMPTS => {
                    let delay = retry_delay(attempt);
                    eprintln!(
                        "request failed on attempt {attempt}/{MAX_ATTEMPTS}: {error:#}. retrying in {}s",
                        delay.as_secs()
                    );
                    sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn fetch_trades_once(&self, query: &TradeQuery<'_>) -> Result<Vec<Trade>> {
        let url = query.url()?;
        let response = self
            .http
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("failed to request {url}"))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read Bitfinex response body")?;

        if status == StatusCode::TOO_MANY_REQUESTS || is_bitfinex_rate_limit_body(&body) {
            eprintln!(
                "Bitfinex returned a rate-limit response. Pausing for {}s before retrying.",
                RATE_LIMIT_PAUSE.as_secs()
            );
            sleep(RATE_LIMIT_PAUSE).await;
            return Err(anyhow!("retryable: Bitfinex rate limit"));
        }

        if status.is_server_error() {
            return Err(anyhow!(
                "retryable: Bitfinex returned HTTP {status}: {body}"
            ));
        }

        if !status.is_success() {
            bail!("Bitfinex returned HTTP {status}: {body}");
        }

        let value: Value =
            serde_json::from_str(&body).with_context(|| format!("invalid JSON: {body}"))?;
        parse_trades(query.symbol, &value)
    }
}

#[derive(Debug)]
struct TradeQuery<'a> {
    symbol: &'a str,
    start: Option<i64>,
    end: Option<i64>,
    limit: u16,
    sort: SortOrder,
}

impl TradeQuery<'_> {
    fn url(&self) -> Result<Url> {
        let path = format!("trades/{}/hist", self.symbol);
        let mut url = Url::parse(API_BASE)
            .context("invalid Bitfinex base URL")?
            .join(&path)
            .with_context(|| format!("invalid Bitfinex trades path: {path}"))?;

        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", &self.limit.to_string());
            query.append_pair("sort", self.sort.as_query_value());
            if let Some(start) = self.start {
                query.append_pair("start", &start.to_string());
            }
            if let Some(end) = self.end {
                query.append_pair("end", &end.to_string());
            }
        }

        Ok(url)
    }
}

#[derive(Debug, Clone, Copy)]
enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    const fn as_query_value(self) -> &'static str {
        match self {
            Self::Ascending => "1",
            Self::Descending => "-1",
        }
    }
}

/// Runs the main recorder/history command-line interface.
///
/// # Errors
///
/// Returns an error when CLI validation fails, a network request fails, a websocket feed fails, or
/// output files cannot be written.
pub async fn run_cli() -> Result<()> {
    install_default_crypto_provider();

    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Btc(btc_args)) => run_stream_command(btc_stream_args(btc_args)).await,
        Some(Commands::Normalize(normalize_args)) => run_normalize_command(&normalize_args),
        Some(Commands::Stream(stream_args)) => run_stream_command(stream_args).await,
        None => run_history_command(cli.history).await,
    }
}

/// Runs the standalone raw websocket normalizer command-line interface.
///
/// # Errors
///
/// Returns an error when CLI validation fails, raw input files cannot be decoded, or normalized
/// Parquet files cannot be written.
pub fn run_normalize_cli() -> Result<()> {
    let cli = NormalizeCli::parse();
    run_normalize_command(&cli.args)
}

fn install_default_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap_or_else(drop);
}

async fn run_history_command(cli: HistoryArgs) -> Result<()> {
    validate_args(&cli)?;

    let client = BitfinexClient::new(cli.rpm)?;
    let mut writer = DailyParquetTradeWriter::create(&cli.output_dir, &cli.symbol, cli.batch_size)?;
    let checkpoint = if cli.ignore_checkpoint {
        None
    } else {
        writer.load_checkpoint()?
    };
    let start = resolve_start(cli.start, checkpoint.as_ref());

    if let Some(checkpoint) = &checkpoint {
        if start == Some(checkpoint.next_start_mts) {
            eprintln!(
                "resuming from checkpoint at {} ({})",
                checkpoint.next_start_mts, checkpoint.last_completed_date
            );
        }
    }

    let written = if let Some(start) = start {
        if cli.end.is_some_and(|end| start > end) {
            eprintln!("resume point {start} is after --end; nothing to pull");
            0
        } else {
            pull_forward(&client, &cli, start, &mut writer).await?
        }
    } else {
        pull_recent_page(&client, &cli, &mut writer).await?
    };

    let output_dir = writer.partition_dir().to_path_buf();
    writer.close()?;
    eprintln!("wrote {written} trades to {}", output_dir.display());
    Ok(())
}

async fn run_stream_command(args: StreamArgs) -> Result<()> {
    validate_stream_args(&args)?;
    let specs = stream_specs(&args);
    if specs.is_empty() {
        bail!("no websocket feeds selected");
    }

    eprintln!(
        "starting {} websocket feed(s); writing compressed JSONL under {}",
        specs.len(),
        args.output_dir.display()
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut join_set = JoinSet::new();
    for spec in specs {
        let run_args = args.clone();
        let feed_shutdown = shutdown_rx.clone();
        join_set.spawn(async move { run_feed_loop(spec, run_args, feed_shutdown).await });
    }

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received Ctrl+C; stopping websocket recorders");
                let _ = shutdown_tx.send(true);
                while let Some(result) = join_set.join_next().await {
                    result.context("stream task panicked")??;
                }
                return Ok(());
            }
            result = join_set.join_next() => {
                match result {
                    Some(result) => result.context("stream task panicked")??,
                    None => return Ok(()),
                }
            }
        }
    }
}

fn btc_stream_args(args: BtcArgs) -> StreamArgs {
    StreamArgs {
        venues: vec![
            StreamVenue::Bitfinex,
            StreamVenue::Hibachi,
            StreamVenue::Deribit,
            StreamVenue::Hyperliquid,
        ],
        output_dir: args.output_dir,
        bitfinex_symbol: DEFAULT_SYMBOL.to_owned(),
        extended_market: DEFAULT_EXTENDED_MARKET.to_owned(),
        extended_spot_market: DEFAULT_EXTENDED_SPOT_MARKET.to_owned(),
        hibachi_symbol: DEFAULT_HIBACHI_SYMBOL.to_owned(),
        hibachi_url: args.hibachi_url,
        deribit_url: args.deribit_url,
        deribit_kinds: vec![DeribitInstrumentKind::Future, DeribitInstrumentKind::Option],
        deribit_trades_interval: DEFAULT_DERIBIT_TRADES_INTERVAL.to_owned(),
        hyperliquid_spot_coin: args.hyperliquid_spot_coin,
        zstd_level: args.zstd_level,
        max_messages: args.max_messages,
        reconnect_delay_secs: args.reconnect_delay_secs,
        heartbeat_secs: args.heartbeat_secs,
    }
}

async fn run_feed_loop(
    spec: FeedSpec,
    args: StreamArgs,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut total_messages = 0_usize;
    let mut runtime_state = FeedRuntimeState::default();
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        match run_feed_once(
            &spec,
            &args,
            &mut total_messages,
            &mut shutdown,
            &mut runtime_state,
        )
        .await
        {
            Ok(FeedRunStatus::Complete) => return Ok(()),
            Ok(FeedRunStatus::Reconnect) => {}
            Err(error) => {
                eprintln!(
                    "{}:{}:{} websocket error: {error:#}",
                    spec.exchange, spec.symbol, spec.channel
                );
            }
        }

        if args
            .max_messages
            .is_some_and(|max_messages| total_messages >= max_messages)
        {
            return Ok(());
        }

        tokio::select! {
            result = shutdown.changed() => {
                if result.is_ok() && !*shutdown.borrow() {
                    continue;
                }
                return Ok(());
            }
            () = sleep(Duration::from_secs(args.reconnect_delay_secs)) => {}
        }
    }
}

async fn run_feed_once(
    spec: &FeedSpec,
    args: &StreamArgs,
    total_messages: &mut usize,
    shutdown: &mut watch::Receiver<bool>,
    runtime_state: &mut FeedRuntimeState,
) -> Result<FeedRunStatus> {
    match spec.behavior {
        FeedBehavior::Static => run_static_feed_once(spec, args, total_messages, shutdown).await,
        FeedBehavior::DeribitInstrumentDiscovery => {
            run_deribit_feed_once(spec, args, total_messages, shutdown, runtime_state).await
        }
        FeedBehavior::HyperliquidSpot => {
            run_hyperliquid_spot_feed_once(args, total_messages, shutdown).await
        }
    }
}

async fn run_static_feed_once(
    spec: &FeedSpec,
    args: &StreamArgs,
    total_messages: &mut usize,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<FeedRunStatus> {
    eprintln!(
        "connecting {}:{}:{} -> {}",
        spec.exchange, spec.symbol, spec.channel, spec.url
    );

    let mut request = spec
        .url
        .as_str()
        .into_client_request()
        .with_context(|| format!("failed to build websocket request for {}", spec.url))?;
    request.headers_mut().insert(
        USER_AGENT,
        HeaderValue::from_static(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        )),
    );

    let (socket, _) = connect_async(request)
        .await
        .with_context(|| format!("failed to connect {}", spec.url))?;
    let (mut socket_write, mut socket_read) = socket.split();
    send_subscriptions(&mut socket_write, spec).await?;

    let mut event_writer = DailyCompressedEventWriter::create(
        &args.output_dir,
        spec.exchange,
        &spec.symbol,
        spec.channel,
        args.zstd_level,
    )?;
    let mut heartbeat = HeartbeatState::new(args.heartbeat_secs, spec.heartbeat_policy);

    let status = loop {
        match next_websocket_event(
            &mut socket_read,
            &mut socket_write,
            &mut heartbeat,
            &spec.connection_id,
            shutdown,
        )
        .await?
        {
            NextWebsocketEvent::HeartbeatSent => continue,
            NextWebsocketEvent::Shutdown => break FeedRunStatus::Complete,
            NextWebsocketEvent::Closed => break FeedRunStatus::Reconnect,
            NextWebsocketEvent::Message(message) => {
                let Some(message) = websocket_message_or_reconnect(message, &spec.connection_id)?
                else {
                    break FeedRunStatus::Reconnect;
                };
                let message_status = handle_websocket_message(
                    message,
                    spec,
                    &mut socket_write,
                    &mut event_writer,
                    &mut heartbeat,
                    total_messages,
                )
                .await?;
                if message_status == FeedMessageStatus::Reconnect {
                    break FeedRunStatus::Reconnect;
                }
            }
        }

        if args
            .max_messages
            .is_some_and(|max_messages| *total_messages >= max_messages)
        {
            eprintln!(
                "{} reached max message count ({})",
                spec.connection_id, total_messages
            );
            break FeedRunStatus::Complete;
        }
    };

    event_writer.close()?;
    Ok(status)
}

async fn run_deribit_feed_once(
    spec: &FeedSpec,
    args: &StreamArgs,
    total_messages: &mut usize,
    shutdown: &mut watch::Receiver<bool>,
    runtime_state: &mut FeedRuntimeState,
) -> Result<FeedRunStatus> {
    eprintln!(
        "connecting {}:{}:{} -> {}",
        spec.exchange, spec.symbol, spec.channel, spec.url
    );

    let mut request = spec
        .url
        .as_str()
        .into_client_request()
        .with_context(|| format!("failed to build websocket request for {}", spec.url))?;
    request.headers_mut().insert(
        USER_AGENT,
        HeaderValue::from_static(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        )),
    );

    let (socket, _) = connect_async(request)
        .await
        .with_context(|| format!("failed to connect {}", spec.url))?;
    let (mut socket_write, mut socket_read) = socket.split();

    send_deribit_set_heartbeat(
        &mut socket_write,
        runtime_state,
        &spec.connection_id,
        args.heartbeat_secs,
    )
    .await?;

    let mut initial_channels = deribit_lifecycle_channels(&args.deribit_kinds);
    initial_channels.extend(runtime_state.instruments.iter().flat_map(|instrument| {
        deribit_instrument_channels(instrument, &args.deribit_trades_interval)
    }));
    send_deribit_subscribe(
        &mut socket_write,
        runtime_state,
        &spec.connection_id,
        initial_channels,
    )
    .await?;
    send_deribit_get_instruments_requests(
        &mut socket_write,
        runtime_state,
        &spec.connection_id,
        &args.deribit_kinds,
    )
    .await?;

    let mut event_writers =
        DeribitCompressedEventWriters::create(&args.output_dir, args.zstd_level);
    let mut heartbeat = HeartbeatState::new(args.heartbeat_secs, spec.heartbeat_policy);

    let status = loop {
        match next_websocket_event(
            &mut socket_read,
            &mut socket_write,
            &mut heartbeat,
            &spec.connection_id,
            shutdown,
        )
        .await?
        {
            NextWebsocketEvent::HeartbeatSent => continue,
            NextWebsocketEvent::Shutdown => break FeedRunStatus::Complete,
            NextWebsocketEvent::Closed => break FeedRunStatus::Reconnect,
            NextWebsocketEvent::Message(message) => {
                let Some(message) = websocket_message_or_reconnect(message, &spec.connection_id)?
                else {
                    break FeedRunStatus::Reconnect;
                };
                let mut context = DeribitMessageContext {
                    spec,
                    args,
                    socket_write: &mut socket_write,
                    event_writers: &mut event_writers,
                    heartbeat: &mut heartbeat,
                    total_messages,
                    runtime_state,
                };
                let message_status =
                    handle_deribit_websocket_message(message, &mut context).await?;
                if message_status == FeedMessageStatus::Reconnect {
                    break FeedRunStatus::Reconnect;
                }
            }
        }

        if args
            .max_messages
            .is_some_and(|max_messages| *total_messages >= max_messages)
        {
            eprintln!(
                "{} reached max message count ({})",
                spec.connection_id, total_messages
            );
            break FeedRunStatus::Complete;
        }
    };

    event_writers.close()?;
    Ok(status)
}

async fn send_deribit_set_heartbeat<S>(
    socket_write: &mut S,
    runtime_state: &mut FeedRuntimeState,
    connection_id: &str,
    heartbeat_secs: u64,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    if heartbeat_secs == 0 {
        return Ok(());
    }

    let interval = heartbeat_secs.max(10);
    let id = runtime_state.next_deribit_request_id();
    let text = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "public/set_heartbeat",
        "id": id,
        "params": {
            "interval": interval,
        }
    })
    .to_string();

    socket_write
        .send(Message::Text(text.into()))
        .await
        .with_context(|| format!("failed to enable Deribit heartbeat on {connection_id}"))?;
    eprintln!("{connection_id} enabled Deribit API heartbeat every {interval}s");
    Ok(())
}

async fn send_deribit_subscribe<S>(
    socket_write: &mut S,
    runtime_state: &mut FeedRuntimeState,
    connection_id: &str,
    channels: Vec<String>,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    if channels.is_empty() {
        return Ok(());
    }

    let count = channels.len();
    let chunk_count = channels.chunks(DERIBIT_SUBSCRIBE_CHUNK_SIZE).len();
    for (index, channel_chunk) in channels.chunks(DERIBIT_SUBSCRIBE_CHUNK_SIZE).enumerate() {
        let id = runtime_state.next_deribit_request_id();
        let text = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": id,
            "params": {
                "channels": channel_chunk,
            }
        })
        .to_string();

        socket_write
            .send(Message::Text(text.into()))
            .await
            .with_context(|| format!("failed to send Deribit subscription on {connection_id}"))?;
        socket_write
            .flush()
            .await
            .with_context(|| format!("failed to flush Deribit subscription on {connection_id}"))?;

        if index + 1 < chunk_count {
            sleep(DERIBIT_SUBSCRIBE_CHUNK_DELAY).await;
        }
    }
    eprintln!(
        "{connection_id} subscribed to {count} Deribit channel(s) in {chunk_count} request(s)"
    );
    Ok(())
}

async fn send_deribit_get_instruments_requests<S>(
    socket_write: &mut S,
    runtime_state: &mut FeedRuntimeState,
    connection_id: &str,
    kinds: &[DeribitInstrumentKind],
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    for (index, kind) in kinds.iter().copied().enumerate() {
        let id = runtime_state.next_deribit_request_id();
        runtime_state.get_instruments_request_ids.insert(id);
        let text = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "public/get_instruments",
            "id": id,
            "params": {
                "currency": "BTC",
                "kind": kind.as_str(),
                "expired": false,
            }
        })
        .to_string();

        socket_write
            .send(Message::Text(text.into()))
            .await
            .with_context(|| {
                format!("failed to send Deribit get_instruments request on {connection_id}")
            })?;
        eprintln!(
            "{connection_id} requested current BTC {} instruments",
            kind.as_str()
        );

        if index + 1 < kinds.len() {
            sleep(Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

async fn send_deribit_public_test<S>(
    socket_write: &mut S,
    runtime_state: &mut FeedRuntimeState,
    connection_id: &str,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let id = runtime_state.next_deribit_request_id();
    let text = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "public/test",
        "id": id,
        "params": {}
    })
    .to_string();

    socket_write
        .send(Message::Text(text.into()))
        .await
        .with_context(|| {
            format!("failed to answer Deribit heartbeat test_request on {connection_id}")
        })
}

async fn run_hyperliquid_spot_feed_once(
    args: &StreamArgs,
    total_messages: &mut usize,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<FeedRunStatus> {
    let target = resolve_hyperliquid_spot_target(args).await?;
    eprintln!(
        "connecting hyperliquid:{}:spot_market_data via hypersdk",
        target.output_symbol
    );

    let mut ws = hypercore::mainnet_ws();
    ws.subscribe(HyperliquidSubscription::Trades {
        coin: target.subscription_coin.clone(),
    });
    ws.subscribe(HyperliquidSubscription::L2Book {
        coin: target.subscription_coin.clone(),
        n_sig_figs: None,
        mantissa: None,
        fast: false,
    });
    eprintln!(
        "hyperliquid-{} subscribed to trades and l2Book for {}",
        target.output_symbol, target.subscription_coin
    );

    let mut event_writers = HyperliquidCompressedEventWriters::create(
        &args.output_dir,
        &target.output_symbol,
        args.zstd_level,
    );

    loop {
        tokio::select! {
            result = shutdown.changed() => {
                let shutdown_event = shutdown_event(result.is_err(), shutdown);
                if matches!(shutdown_event, NextWebsocketEvent::Shutdown) {
                    break;
                }
            }
            event = ws.next() => {
                let Some(event) = event else {
                    break;
                };
                handle_hyperliquid_event(event, &mut event_writers)?;
                *total_messages += 1;

                if args.max_messages.is_some_and(|max_messages| *total_messages >= max_messages) {
                    eprintln!(
                        "hyperliquid-{} reached max message count ({})",
                        target.output_symbol, total_messages
                    );
                    break;
                }
            }
        }
    }

    event_writers.close()?;
    Ok(FeedRunStatus::Complete)
}

fn handle_hyperliquid_event(
    event: HyperliquidEvent,
    event_writers: &mut HyperliquidCompressedEventWriters,
) -> Result<()> {
    match event {
        HyperliquidEvent::Connected => {
            event_writers.write_control_event(&serde_json::json!({"event": "connected"}))
        }
        HyperliquidEvent::Disconnected => {
            event_writers.write_control_event(&serde_json::json!({"event": "disconnected"}))
        }
        HyperliquidEvent::Message(message) => {
            let channel = hyperliquid_output_channel(&message);
            let text = serde_json::to_string(&message)
                .context("failed to encode Hyperliquid websocket event")?;
            event_writers.write_text_event(channel, &text)
        }
    }
}

async fn resolve_hyperliquid_spot_target(args: &StreamArgs) -> Result<HyperliquidSpotTarget> {
    if let Some(coin) = &args.hyperliquid_spot_coin {
        return Ok(HyperliquidSpotTarget {
            subscription_coin: coin.clone(),
            output_symbol: coin.clone(),
        });
    }

    let client = hypercore::mainnet();
    let spot_markets = client
        .spot()
        .await
        .context("failed to fetch Hyperliquid spot markets")?;
    let market = spot_markets
        .iter()
        .find(|market| market.base().name == "UBTC" && market.quote().name == "USDC")
        .or_else(|| {
            spot_markets
                .iter()
                .find(|market| market.symbol().eq_ignore_ascii_case("BTC/USDC"))
        })
        .or_else(|| {
            spot_markets
                .iter()
                .find(|market| market.symbol().contains("BTC/USDC"))
        })
        .context("failed to resolve Hyperliquid BTC spot market")?;

    Ok(HyperliquidSpotTarget {
        subscription_coin: market.name.clone(),
        output_symbol: market.symbol(),
    })
}

fn hyperliquid_output_channel(message: &HyperliquidIncoming) -> &'static str {
    match message {
        HyperliquidIncoming::Trades(_) => "trades",
        HyperliquidIncoming::L2Book(_) => "book",
        _ => "control",
    }
}

async fn send_subscriptions<S>(socket_write: &mut S, spec: &FeedSpec) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    for subscription in &spec.subscribe_messages {
        let text = subscription.to_string();
        socket_write
            .send(Message::Text(text.into()))
            .await
            .with_context(|| format!("failed to send subscription on {}", spec.connection_id))?;
    }
    Ok(())
}

async fn next_websocket_event<R, S>(
    socket_read: &mut R,
    socket_write: &mut S,
    heartbeat: &mut HeartbeatState,
    connection_id: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<NextWebsocketEvent>
where
    R: Stream<Item = Result<Message, WsError>> + Unpin,
    S: Sink<Message, Error = WsError> + Unpin,
{
    if heartbeat.is_enabled() {
        tokio::select! {
            result = shutdown.changed() => Ok(shutdown_event(result.is_err(), shutdown)),
            message = socket_read.next() => Ok(websocket_message_event(message)),
            result = heartbeat.wait_and_send_ping(socket_write, connection_id) => {
                result?;
                Ok(NextWebsocketEvent::HeartbeatSent)
            }
        }
    } else {
        tokio::select! {
            result = shutdown.changed() => Ok(shutdown_event(result.is_err(), shutdown)),
            message = socket_read.next() => Ok(websocket_message_event(message)),
        }
    }
}

fn shutdown_event(shutdown_closed: bool, shutdown: &watch::Receiver<bool>) -> NextWebsocketEvent {
    if !shutdown_closed && !*shutdown.borrow() {
        return NextWebsocketEvent::HeartbeatSent;
    }
    NextWebsocketEvent::Shutdown
}

fn websocket_message_event(message: Option<Result<Message, WsError>>) -> NextWebsocketEvent {
    match message {
        Some(message) => NextWebsocketEvent::Message(message),
        None => NextWebsocketEvent::Closed,
    }
}

fn websocket_message_or_reconnect(
    message: Result<Message, WsError>,
    connection_id: &str,
) -> Result<Option<Message>> {
    match message {
        Ok(message) => Ok(Some(message)),
        Err(error) if is_expected_websocket_reconnect_error(&error) => {
            eprintln!("{connection_id} websocket connection reset; reconnecting");
            Ok(None)
        }
        Err(error) => Err(error).with_context(|| format!("failed reading {connection_id}")),
    }
}

fn is_expected_websocket_reconnect_error(error: &WsError) -> bool {
    match error {
        WsError::ConnectionClosed
        | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        WsError::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

async fn handle_websocket_message<S>(
    message: Message,
    spec: &FeedSpec,
    socket_write: &mut S,
    event_writer: &mut DailyCompressedEventWriter,
    heartbeat: &mut HeartbeatState,
    total_messages: &mut usize,
) -> Result<FeedMessageStatus>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    match message {
        Message::Text(text) => {
            let text = text.as_str();
            event_writer.write_text_event(spec, text)?;
            *total_messages += 1;
        }
        Message::Binary(bytes) => {
            event_writer.write_binary_event(spec, &bytes)?;
            *total_messages += 1;
        }
        Message::Ping(_) => flush_queued_pong(socket_write, &spec.connection_id).await?,
        Message::Pong(payload) => heartbeat.observe_pong(payload.as_ref()),
        Message::Frame(_) => {}
        Message::Close(frame) => {
            log_websocket_close(&spec.connection_id, frame.as_ref());
            return Ok(FeedMessageStatus::Reconnect);
        }
    }
    Ok(FeedMessageStatus::Continue)
}

struct DeribitMessageContext<'a, S> {
    spec: &'a FeedSpec,
    args: &'a StreamArgs,
    socket_write: &'a mut S,
    event_writers: &'a mut DeribitCompressedEventWriters,
    heartbeat: &'a mut HeartbeatState,
    total_messages: &'a mut usize,
    runtime_state: &'a mut FeedRuntimeState,
}

async fn handle_deribit_websocket_message<S>(
    message: Message,
    context: &mut DeribitMessageContext<'_, S>,
) -> Result<FeedMessageStatus>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    match message {
        Message::Text(text) => {
            let text = text.as_str();
            context
                .event_writers
                .write_text_event(deribit_output_channel(text), text)?;
            maybe_answer_deribit_heartbeat_test_request(
                text,
                context.socket_write,
                context.runtime_state,
                &context.spec.connection_id,
            )
            .await?;
            maybe_subscribe_deribit_get_instruments(text, context).await?;
            maybe_subscribe_deribit_created_instrument(
                text,
                context.args,
                context.socket_write,
                context.runtime_state,
                &context.spec.connection_id,
            )
            .await?;
            *context.total_messages += 1;
        }
        Message::Binary(bytes) => {
            context
                .event_writers
                .write_binary_event("control", &bytes)?;
            *context.total_messages += 1;
        }
        Message::Ping(_) => {
            flush_queued_pong(context.socket_write, &context.spec.connection_id).await?;
        }
        Message::Pong(payload) => context.heartbeat.observe_pong(payload.as_ref()),
        Message::Frame(_) => {}
        Message::Close(frame) => {
            log_websocket_close(&context.spec.connection_id, frame.as_ref());
            return Ok(FeedMessageStatus::Reconnect);
        }
    }
    Ok(FeedMessageStatus::Continue)
}

async fn maybe_answer_deribit_heartbeat_test_request<S>(
    text: &str,
    socket_write: &mut S,
    runtime_state: &mut FeedRuntimeState,
    connection_id: &str,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return Ok(());
    };
    if deribit_heartbeat_type(&message) != Some("test_request") {
        return Ok(());
    }

    send_deribit_public_test(socket_write, runtime_state, connection_id).await
}

fn deribit_heartbeat_type(message: &Value) -> Option<&str> {
    (message.get("method").and_then(Value::as_str) == Some("heartbeat"))
        .then(|| message.pointer("/params/type").and_then(Value::as_str))
        .flatten()
}

async fn maybe_subscribe_deribit_get_instruments<S>(
    text: &str,
    context: &mut DeribitMessageContext<'_, S>,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return Ok(());
    };
    let Some(response_id) = deribit_response_id(&message) else {
        return Ok(());
    };
    if !context
        .runtime_state
        .get_instruments_request_ids
        .remove(&response_id)
    {
        return Ok(());
    }

    if let Some(error) = message.get("error") {
        bail!("Deribit get_instruments failed: {error}");
    }

    let instrument_names = deribit_instrument_names_from_get_instruments_response(
        &message,
        &context.args.deribit_kinds,
    );
    let mut channels = Vec::new();
    let mut new_instruments = 0usize;
    for instrument_name in instrument_names {
        if context
            .runtime_state
            .instruments
            .insert(instrument_name.clone())
        {
            new_instruments = new_instruments.saturating_add(1);
            channels.extend(deribit_instrument_channels(
                &instrument_name,
                &context.args.deribit_trades_interval,
            ));
        }
    }

    if !channels.is_empty() {
        eprintln!(
            "{} discovered {new_instruments} current Deribit BTC instrument(s)",
            context.spec.connection_id
        );
        send_deribit_subscribe(
            context.socket_write,
            context.runtime_state,
            &context.spec.connection_id,
            channels,
        )
        .await?;
    }
    Ok(())
}

async fn flush_queued_pong<S>(socket_write: &mut S, connection_id: &str) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    socket_write
        .flush()
        .await
        .with_context(|| format!("failed to flush queued pong on {connection_id}"))
}

fn log_websocket_close(connection_id: &str, frame: Option<&CloseFrame>) {
    match frame {
        Some(frame) if is_remote_ping_timeout_close(frame) => {
            eprintln!(
                "{connection_id} websocket closed: code={} reason=\"{}\"; remote server did not receive our pong before its ping timeout",
                frame.code, frame.reason
            );
        }
        Some(frame) => {
            eprintln!(
                "{connection_id} websocket closed: code={} reason=\"{}\"",
                frame.code, frame.reason
            );
        }
        None => {
            eprintln!("{connection_id} websocket closed without a close frame");
        }
    }
}

fn is_remote_ping_timeout_close(frame: &CloseFrame) -> bool {
    frame.reason.as_str().eq_ignore_ascii_case("Ping timeout")
}

fn deribit_response_id(message: &Value) -> Option<u64> {
    message.get("id").and_then(Value::as_u64)
}

async fn maybe_subscribe_deribit_created_instrument<S>(
    text: &str,
    args: &StreamArgs,
    socket_write: &mut S,
    runtime_state: &mut FeedRuntimeState,
    connection_id: &str,
) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return Ok(());
    };
    let Some(instrument_name) = deribit_created_instrument_name(&message, &args.deribit_kinds)
    else {
        return Ok(());
    };

    if runtime_state.instruments.insert(instrument_name.clone()) {
        let channels = deribit_instrument_channels(&instrument_name, &args.deribit_trades_interval);
        send_deribit_subscribe(socket_write, runtime_state, connection_id, channels).await?;
    }
    Ok(())
}

fn deribit_created_instrument_name(
    message: &Value,
    allowed_kinds: &[DeribitInstrumentKind],
) -> Option<String> {
    let channel = deribit_message_channel(message)?;
    if !channel.starts_with("instrument.creation.") {
        return None;
    }

    let data = deribit_message_data(message)?;
    if data
        .get("base_currency")
        .and_then(Value::as_str)
        .is_some_and(|currency| currency != "BTC")
    {
        return None;
    }

    if let Some(kind) = data.get("kind").and_then(Value::as_str) {
        let allowed = allowed_kinds
            .iter()
            .any(|allowed_kind| allowed_kind.as_str() == kind);
        if !allowed {
            return None;
        }
    }

    data.get("instrument_name")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn deribit_instrument_names_from_get_instruments_response(
    message: &Value,
    allowed_kinds: &[DeribitInstrumentKind],
) -> Vec<String> {
    let Some(instruments) = message.get("result").and_then(Value::as_array) else {
        return Vec::new();
    };

    instruments
        .iter()
        .filter_map(|instrument| {
            let kind = instrument.get("kind").and_then(Value::as_str)?;
            if !allowed_kinds
                .iter()
                .any(|allowed_kind| allowed_kind.as_str() == kind)
            {
                return None;
            }

            let is_btc = instrument
                .get("base_currency")
                .and_then(Value::as_str)
                .is_some_and(|currency| currency == "BTC")
                || instrument
                    .get("product_group")
                    .and_then(Value::as_str)
                    .is_some_and(|product_group| product_group == "BTC");
            if !is_btc {
                return None;
            }

            if instrument
                .get("is_active")
                .and_then(Value::as_bool)
                .is_some_and(|is_active| !is_active)
            {
                return None;
            }

            instrument
                .get("instrument_name")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn deribit_message_data(message: &Value) -> Option<&Value> {
    message
        .pointer("/params/data")
        .or_else(|| message.get("data"))
}

fn deribit_output_channel(text: &str) -> &'static str {
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return "control";
    };
    match deribit_message_channel(&message) {
        Some(channel) if channel.starts_with("instrument.creation.") => "instrument_creation",
        Some(channel) if channel.starts_with("instrument.state.") => "instrument_state",
        Some(channel) if channel.starts_with("incremental_ticker.") => "incremental_ticker",
        Some(channel) if channel.starts_with("trades.") => "trades",
        _ => "control",
    }
}

fn deribit_message_channel(message: &Value) -> Option<&str> {
    message.pointer("/params/channel").and_then(Value::as_str)
}

fn deribit_lifecycle_channels(kinds: &[DeribitInstrumentKind]) -> Vec<String> {
    kinds
        .iter()
        .flat_map(|kind| {
            [
                format!("instrument.creation.{}.BTC", kind.as_str()),
                format!("instrument.state.{}.BTC", kind.as_str()),
            ]
        })
        .collect()
}

fn deribit_instrument_channels(instrument_name: &str, trades_interval: &str) -> Vec<String> {
    vec![
        format!("incremental_ticker.{instrument_name}"),
        format!("trades.{instrument_name}.{trades_interval}"),
    ]
}

fn websocket_heartbeat_duration(heartbeat_secs: u64) -> Option<Duration> {
    (heartbeat_secs > 0).then(|| Duration::from_secs(heartbeat_secs))
}

fn websocket_heartbeat_interval(heartbeat_secs: u64) -> Option<Interval> {
    websocket_heartbeat_duration(heartbeat_secs).map(|duration| {
        let mut ticks = interval_at(Instant::now() + duration, duration);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticks
    })
}

fn heartbeat_payload(sequence: u64) -> Vec<u8> {
    format!("modl:{sequence:016x}").into_bytes()
}

#[derive(Debug)]
struct HeartbeatState {
    ticks: Option<Interval>,
    sequence: u64,
    awaiting_pong: Option<Vec<u8>>,
    interval_secs: u64,
    policy: HeartbeatPolicy,
}

impl HeartbeatState {
    fn new(interval_secs: u64, policy: HeartbeatPolicy) -> Self {
        Self {
            ticks: websocket_heartbeat_interval(interval_secs),
            sequence: 0,
            awaiting_pong: None,
            interval_secs,
            policy,
        }
    }

    const fn is_enabled(&self) -> bool {
        self.ticks.is_some()
    }

    async fn wait_and_send_ping<S>(
        &mut self,
        socket_write: &mut S,
        connection_id: &str,
    ) -> Result<()>
    where
        S: Sink<Message, Error = WsError> + Unpin,
    {
        let ticks = self
            .ticks
            .as_mut()
            .context("heartbeat tick requested while heartbeat is disabled")?;
        ticks.tick().await;

        self.clear_stale_pong_or_timeout(connection_id)?;

        self.sequence = self.sequence.wrapping_add(1);
        let payload = heartbeat_payload(self.sequence);
        socket_write
            .send(Message::Ping(payload.clone().into()))
            .await
            .with_context(|| format!("failed to send heartbeat ping on {connection_id}"))?;
        self.awaiting_pong = Some(payload);
        Ok(())
    }

    fn clear_stale_pong_or_timeout(&mut self, connection_id: &str) -> Result<()> {
        if self.awaiting_pong.is_none() {
            return Ok(());
        }

        match self.policy {
            HeartbeatPolicy::Required => bail!(
                "{connection_id} heartbeat pong timeout after {} seconds",
                self.interval_secs
            ),
            HeartbeatPolicy::BestEffort => {
                self.awaiting_pong = None;
                Ok(())
            }
        }
    }

    fn observe_pong(&mut self, payload: &[u8]) {
        if self
            .awaiting_pong
            .as_deref()
            .is_some_and(|expected| expected == payload)
        {
            self.awaiting_pong = None;
        }
    }
}

#[derive(Debug)]
enum NextWebsocketEvent {
    Message(Result<Message, WsError>),
    HeartbeatSent,
    Shutdown,
    Closed,
}

#[derive(Debug, Eq, PartialEq)]
enum FeedMessageStatus {
    Continue,
    Reconnect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeedRunStatus {
    Complete,
    Reconnect,
}

#[derive(Default)]
struct FeedRuntimeState {
    instruments: HashSet<String>,
    get_instruments_request_ids: HashSet<u64>,
    request_id: u64,
}

impl FeedRuntimeState {
    fn next_deribit_request_id(&mut self) -> u64 {
        self.request_id = self.request_id.saturating_add(1);
        self.request_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeedBehavior {
    Static,
    DeribitInstrumentDiscovery,
    HyperliquidSpot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatPolicy {
    Required,
    BestEffort,
}

#[derive(Clone, Debug)]
struct FeedSpec {
    exchange: &'static str,
    symbol: String,
    channel: &'static str,
    connection_id: String,
    url: String,
    subscribe_messages: Vec<Value>,
    behavior: FeedBehavior,
    heartbeat_policy: HeartbeatPolicy,
}

fn stream_specs(args: &StreamArgs) -> Vec<FeedSpec> {
    let mut specs = Vec::new();
    for venue in &args.venues {
        match venue {
            StreamVenue::Bitfinex => specs.extend(bitfinex_stream_specs(&args.bitfinex_symbol)),
            StreamVenue::Extended => {
                specs.extend(extended_stream_specs(&args.extended_market));
                specs.extend(extended_spot_stream_specs(&args.extended_spot_market));
            }
            StreamVenue::Hibachi => specs.push(hibachi_stream_spec(args)),
            StreamVenue::Deribit => specs.push(deribit_stream_spec(args)),
            StreamVenue::Hyperliquid => specs.push(hyperliquid_stream_spec()),
        }
    }
    specs
}

fn bitfinex_stream_specs(symbol: &str) -> Vec<FeedSpec> {
    let url = "wss://api-pub.bitfinex.com/ws/2".to_owned();
    vec![
        FeedSpec {
            exchange: "bitfinex",
            symbol: symbol.to_owned(),
            channel: "book_l25",
            connection_id: format!("bitfinex-{symbol}-book_l25"),
            url: url.clone(),
            subscribe_messages: vec![serde_json::json!({
                "event": "subscribe",
                "channel": "book",
                "symbol": symbol,
                "prec": "P0",
                "freq": "F0",
                "len": "25"
            })],
            behavior: FeedBehavior::Static,
            heartbeat_policy: HeartbeatPolicy::Required,
        },
        FeedSpec {
            exchange: "bitfinex",
            symbol: symbol.to_owned(),
            channel: "trades",
            connection_id: format!("bitfinex-{symbol}-trades"),
            url,
            subscribe_messages: vec![serde_json::json!({
                "event": "subscribe",
                "channel": "trades",
                "symbol": symbol
            })],
            behavior: FeedBehavior::Static,
            heartbeat_policy: HeartbeatPolicy::Required,
        },
    ]
}

fn extended_stream_specs(market: &str) -> Vec<FeedSpec> {
    const EXTENDED_WS_BASE: &str = "wss://api.starknet.extended.exchange";
    [
        (
            "orderbook",
            format!("/stream.extended.exchange/v1/orderbooks/{market}"),
        ),
        (
            "trades",
            format!("/stream.extended.exchange/v1/publicTrades/{market}"),
        ),
        (
            "funding",
            format!("/stream.extended.exchange/v1/funding/{market}"),
        ),
        (
            "mark_price",
            format!("/stream.extended.exchange/v1/prices/mark/{market}"),
        ),
        (
            "index_price",
            format!("/stream.extended.exchange/v1/prices/index/{market}"),
        ),
    ]
    .into_iter()
    .map(|(channel, path)| FeedSpec {
        exchange: "extended",
        symbol: market.to_owned(),
        channel,
        connection_id: format!("extended-{market}-{channel}"),
        url: format!("{EXTENDED_WS_BASE}{path}"),
        subscribe_messages: Vec::new(),
        behavior: FeedBehavior::Static,
        heartbeat_policy: HeartbeatPolicy::BestEffort,
    })
    .collect()
}

fn extended_spot_stream_specs(market: &str) -> Vec<FeedSpec> {
    const EXTENDED_WS_BASE: &str = "wss://api.starknet.extended.exchange";
    [
        (
            "orderbook",
            format!("/stream.extended.exchange/v1/orderbooks/{market}"),
        ),
        (
            "trades",
            format!("/stream.extended.exchange/v1/publicTrades/{market}"),
        ),
    ]
    .into_iter()
    .map(|(channel, path)| FeedSpec {
        exchange: "extended",
        symbol: market.to_owned(),
        channel,
        connection_id: format!("extended-{market}-{channel}"),
        url: format!("{EXTENDED_WS_BASE}{path}"),
        subscribe_messages: Vec::new(),
        behavior: FeedBehavior::Static,
        heartbeat_policy: HeartbeatPolicy::BestEffort,
    })
    .collect()
}

fn hibachi_stream_spec(args: &StreamArgs) -> FeedSpec {
    let url = args.hibachi_url.clone();
    let symbol = args.hibachi_symbol.clone();
    FeedSpec {
        exchange: "hibachi",
        symbol: symbol.clone(),
        channel: "market_data",
        connection_id: format!("hibachi-{symbol}-market_data"),
        url,
        subscribe_messages: vec![serde_json::json!({
            "method": "subscribe",
            "parameters": {
                "subscriptions": [
                    {"symbol": symbol, "topic": "mark_price"},
                    {"symbol": symbol, "topic": "spot_price"},
                    {"symbol": symbol, "topic": "funding_rate_estimation"},
                    {"symbol": symbol, "topic": "trades"},
                    {"symbol": symbol, "topic": "orderbook"},
                    {"symbol": symbol, "topic": "ask_bid_price"}
                ]
            }
        })],
        behavior: FeedBehavior::Static,
        heartbeat_policy: HeartbeatPolicy::Required,
    }
}

fn deribit_stream_spec(args: &StreamArgs) -> FeedSpec {
    FeedSpec {
        exchange: "deribit",
        symbol: "BTC".to_owned(),
        channel: "instrument_lifecycle",
        connection_id: "deribit-BTC-instruments".to_owned(),
        url: args.deribit_url.clone(),
        subscribe_messages: Vec::new(),
        behavior: FeedBehavior::DeribitInstrumentDiscovery,
        heartbeat_policy: HeartbeatPolicy::BestEffort,
    }
}

fn hyperliquid_stream_spec() -> FeedSpec {
    FeedSpec {
        exchange: "hyperliquid",
        symbol: "spot_btc".to_owned(),
        channel: "spot_market_data",
        connection_id: "hyperliquid-spot-btc".to_owned(),
        url: String::new(),
        subscribe_messages: Vec::new(),
        behavior: FeedBehavior::HyperliquidSpot,
        heartbeat_policy: HeartbeatPolicy::Required,
    }
}

fn validate_stream_args(args: &StreamArgs) -> Result<()> {
    if args.venues.is_empty() {
        bail!("at least one --venue is required");
    }
    if args.bitfinex_symbol.is_empty() {
        bail!("bitfinex symbol cannot be empty");
    }
    if args.extended_market.is_empty() {
        bail!("extended market cannot be empty");
    }
    if args.extended_spot_market.is_empty() {
        bail!("extended spot market cannot be empty");
    }
    if args.hibachi_symbol.is_empty() {
        bail!("hibachi symbol cannot be empty");
    }
    if args.deribit_url.is_empty() {
        bail!("deribit URL cannot be empty");
    }
    if args.deribit_kinds.is_empty() {
        bail!("at least one --deribit-kind is required");
    }
    if args
        .hyperliquid_spot_coin
        .as_deref()
        .is_some_and(str::is_empty)
    {
        bail!("hyperliquid spot coin cannot be empty");
    }
    if args.max_messages == Some(0) {
        bail!("max-messages must be greater than zero");
    }
    Ok(())
}

fn parse_deribit_trades_interval(value: &str) -> Result<String, String> {
    match value {
        "raw" | "100ms" | "agg2" => Ok(value.to_owned()),
        _ => Err("Deribit trades interval must be one of: raw, 100ms, agg2".to_owned()),
    }
}

struct DailyCompressedEventWriter {
    partition_dir: PathBuf,
    symbol_name: String,
    channel_name: String,
    zstd_level: i32,
    current_date: Option<NaiveDate>,
    current_writer: Option<zstd::stream::write::Encoder<'static, BufWriter<File>>>,
}

impl DailyCompressedEventWriter {
    fn create(
        output_dir: &Path,
        exchange: &str,
        symbol: &str,
        channel: &str,
        zstd_level: i32,
    ) -> Result<Self> {
        let exchange_name = symbol_partition_name(exchange)?;
        let symbol_name = symbol_partition_name(symbol)?;
        let channel_name = symbol_partition_name(channel)?;
        let partition_dir = output_dir
            .join(exchange_name)
            .join(&symbol_name)
            .join(&channel_name);
        std::fs::create_dir_all(&partition_dir)
            .with_context(|| format!("failed to create {}", partition_dir.display()))?;

        Ok(Self {
            partition_dir,
            symbol_name,
            channel_name,
            zstd_level,
            current_date: None,
            current_writer: None,
        })
    }

    fn write_text_event(&mut self, spec: &FeedSpec, text: &str) -> Result<()> {
        let received_at = Utc::now();
        let event = RawWsEvent {
            received_at: received_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            received_mts: received_at.timestamp_millis(),
            exchange: spec.exchange,
            symbol: &spec.symbol,
            channel: spec.channel,
            connection_id: &spec.connection_id,
            payload_text: Some(text),
            payload_base64: None,
        };

        self.write_event(received_at.date_naive(), &event)
    }

    fn write_binary_event(&mut self, spec: &FeedSpec, bytes: &[u8]) -> Result<()> {
        let received_at = Utc::now();
        let encoded = BASE64_STANDARD.encode(bytes);
        let event = RawWsEvent {
            received_at: received_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            received_mts: received_at.timestamp_millis(),
            exchange: spec.exchange,
            symbol: &spec.symbol,
            channel: spec.channel,
            connection_id: &spec.connection_id,
            payload_text: None,
            payload_base64: Some(&encoded),
        };
        self.write_event(received_at.date_naive(), &event)
    }

    fn write_event(&mut self, date: NaiveDate, event: &RawWsEvent<'_>) -> Result<()> {
        if self.current_date != Some(date) {
            self.close_current_writer()?;
            let path = daily_stream_file_path(
                &self.partition_dir,
                &self.symbol_name,
                &self.channel_name,
                date,
            );
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("failed to open {}", path.display()))?;
            let writer = BufWriter::new(file);
            self.current_writer = Some(
                zstd::stream::write::Encoder::new(writer, self.zstd_level).with_context(|| {
                    format!("failed to create zstd writer for {}", path.display())
                })?,
            );
            self.current_date = Some(date);
        }

        let writer = self
            .current_writer
            .as_mut()
            .context("compressed event writer was not opened")?;
        serde_json::to_writer(&mut *writer, event)
            .context("failed to encode raw websocket event")?;
        writer
            .write_all(b"\n")
            .context("failed to write raw websocket newline")?;
        Ok(())
    }

    fn close_current_writer(&mut self) -> Result<()> {
        if let Some(writer) = self.current_writer.take() {
            writer.finish().context("failed to finish zstd stream")?;
            self.current_date = None;
        }
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.close_current_writer()
    }
}

struct DeribitCompressedEventWriters {
    output_dir: PathBuf,
    zstd_level: i32,
    writers: HashMap<&'static str, DailyCompressedEventWriter>,
}

impl DeribitCompressedEventWriters {
    fn create(output_dir: &Path, zstd_level: i32) -> Self {
        Self {
            output_dir: output_dir.to_path_buf(),
            zstd_level,
            writers: HashMap::new(),
        }
    }

    fn write_text_event(&mut self, channel: &'static str, text: &str) -> Result<()> {
        let spec = deribit_output_spec(channel);
        self.writer(channel)?
            .write_text_event(&spec, text)
            .with_context(|| format!("failed to write Deribit {channel} text event"))
    }

    fn write_binary_event(&mut self, channel: &'static str, bytes: &[u8]) -> Result<()> {
        let spec = deribit_output_spec(channel);
        self.writer(channel)?
            .write_binary_event(&spec, bytes)
            .with_context(|| format!("failed to write Deribit {channel} binary event"))
    }

    fn writer(&mut self, channel: &'static str) -> Result<&mut DailyCompressedEventWriter> {
        if !self.writers.contains_key(channel) {
            let writer = DailyCompressedEventWriter::create(
                &self.output_dir,
                "deribit",
                "BTC",
                channel,
                self.zstd_level,
            )?;
            self.writers.insert(channel, writer);
        }

        self.writers
            .get_mut(channel)
            .context("Deribit compressed writer was not initialized")
    }

    fn close(mut self) -> Result<()> {
        for (_, writer) in self.writers.drain() {
            writer.close()?;
        }
        Ok(())
    }
}

fn deribit_output_spec(channel: &'static str) -> FeedSpec {
    FeedSpec {
        exchange: "deribit",
        symbol: "BTC".to_owned(),
        channel,
        connection_id: "deribit-BTC-instruments".to_owned(),
        url: String::new(),
        subscribe_messages: Vec::new(),
        behavior: FeedBehavior::Static,
        heartbeat_policy: HeartbeatPolicy::Required,
    }
}

struct HyperliquidSpotTarget {
    subscription_coin: String,
    output_symbol: String,
}

struct HyperliquidCompressedEventWriters {
    output_dir: PathBuf,
    symbol: String,
    zstd_level: i32,
    writers: HashMap<&'static str, DailyCompressedEventWriter>,
}

impl HyperliquidCompressedEventWriters {
    fn create(output_dir: &Path, symbol: &str, zstd_level: i32) -> Self {
        Self {
            output_dir: output_dir.to_path_buf(),
            symbol: symbol.to_owned(),
            zstd_level,
            writers: HashMap::new(),
        }
    }

    fn write_control_event(&mut self, payload: &Value) -> Result<()> {
        let text = serde_json::to_string(&payload)
            .context("failed to encode Hyperliquid control event")?;
        self.write_text_event("control", &text)
    }

    fn write_text_event(&mut self, channel: &'static str, text: &str) -> Result<()> {
        let spec = self.output_spec(channel);
        self.writer(channel)?
            .write_text_event(&spec, text)
            .with_context(|| format!("failed to write Hyperliquid {channel} text event"))
    }

    fn writer(&mut self, channel: &'static str) -> Result<&mut DailyCompressedEventWriter> {
        if !self.writers.contains_key(channel) {
            let writer = DailyCompressedEventWriter::create(
                &self.output_dir,
                "hyperliquid",
                &self.symbol,
                channel,
                self.zstd_level,
            )?;
            self.writers.insert(channel, writer);
        }

        self.writers
            .get_mut(channel)
            .context("Hyperliquid compressed writer was not initialized")
    }

    fn output_spec(&self, channel: &'static str) -> FeedSpec {
        FeedSpec {
            exchange: "hyperliquid",
            symbol: self.symbol.clone(),
            channel,
            connection_id: "hyperliquid-spot-btc".to_owned(),
            url: String::new(),
            subscribe_messages: Vec::new(),
            behavior: FeedBehavior::Static,
            heartbeat_policy: HeartbeatPolicy::Required,
        }
    }

    fn close(mut self) -> Result<()> {
        for (_, writer) in self.writers.drain() {
            writer.close()?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct RawWsEvent<'a> {
    received_at: String,
    received_mts: i64,
    exchange: &'a str,
    symbol: &'a str,
    channel: &'a str,
    connection_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_base64: Option<&'a str>,
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
struct NormalizeSummary {
    output_dir: PathBuf,
    instrument_count: usize,
    quote_count: usize,
    trade_count: usize,
}

fn run_normalize_command(args: &NormalizeArgs) -> Result<()> {
    let summary = normalize_deribit_day(args)?;
    eprintln!(
        "normalized Deribit BTC {}: {} instruments, {} ticker rows, {} trade rows",
        args.date, summary.instrument_count, summary.quote_count, summary.trade_count
    );
    eprintln!(
        "wrote normalized Parquet under {}",
        summary.output_dir.display()
    );
    Ok(())
}

fn normalize_deribit_day(args: &NormalizeArgs) -> Result<NormalizeSummary> {
    let source_files = deribit_raw_files(&args.input_dir, args.date)?;
    if !source_files.iter().any(|path| path.exists()) {
        bail!(
            "no Deribit BTC raw files found for {} under {}",
            args.date,
            args.input_dir.display()
        );
    }

    let instruments = collect_deribit_instruments(&args.input_dir, args.date)?;
    let quotes = collect_deribit_quotes(&args.input_dir, args.date, &instruments)?;
    let trades = collect_deribit_trades(&args.input_dir, args.date, &instruments)?;

    let output_dir = args.output_dir.join("deribit").join("btc");
    let mut instrument_rows = instruments.into_values().collect::<Vec<_>>();
    instrument_rows.sort_by(|left, right| left.instrument_name.cmp(&right.instrument_name));

    write_deribit_instruments_parquet(
        &normalized_parquet_path(&output_dir, "instruments", args.date),
        &instrument_rows,
    )?;
    write_deribit_quotes_parquet(
        &normalized_parquet_path(&output_dir, "incremental_ticker", args.date),
        &quotes,
    )?;
    write_deribit_trades_parquet(
        &normalized_parquet_path(&output_dir, "trades", args.date),
        &trades,
    )?;

    Ok(NormalizeSummary {
        output_dir,
        instrument_count: instrument_rows.len(),
        quote_count: quotes.len(),
        trade_count: trades.len(),
    })
}

fn collect_deribit_instruments(
    input_dir: &Path,
    date: NaiveDate,
) -> Result<HashMap<String, DeribitInstrumentMeta>> {
    let mut instruments = HashMap::new();

    let control_path = deribit_raw_file_path(input_dir, "control", date)?;
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

    let creation_path = deribit_raw_file_path(input_dir, "instrument_creation", date)?;
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

fn collect_deribit_quotes(
    input_dir: &Path,
    date: NaiveDate,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
) -> Result<Vec<DeribitQuoteRow>> {
    let mut rows = Vec::new();
    let path = deribit_raw_file_path(input_dir, "incremental_ticker", date)?;
    for_each_raw_text_event(&path, |event, text| {
        let message = serde_json::from_str::<Value>(text).context("invalid Deribit ticker JSON")?;
        if let Some(row) = deribit_quote_row_from_message(&event, &message, instruments) {
            rows.push(row);
        }
        Ok(())
    })?;
    Ok(rows)
}

fn collect_deribit_trades(
    input_dir: &Path,
    date: NaiveDate,
    instruments: &HashMap<String, DeribitInstrumentMeta>,
) -> Result<Vec<DeribitTradeRow>> {
    let mut rows = Vec::new();
    let path = deribit_raw_file_path(input_dir, "trades", date)?;
    for_each_raw_text_event(&path, |event, text| {
        let message = serde_json::from_str::<Value>(text).context("invalid Deribit trades JSON")?;
        rows.extend(deribit_trade_rows_from_message(
            &event,
            &message,
            instruments,
        ));
        Ok(())
    })?;
    Ok(rows)
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
        direction: json_field_string(trade, "direction"),
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

fn json_value_string(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
    }
}

fn deribit_raw_files(input_dir: &Path, date: NaiveDate) -> Result<Vec<PathBuf>> {
    [
        "control",
        "instrument_creation",
        "incremental_ticker",
        "trades",
    ]
    .into_iter()
    .map(|channel| deribit_raw_file_path(input_dir, channel, date))
    .collect()
}

fn deribit_raw_file_path(input_dir: &Path, channel: &str, date: NaiveDate) -> Result<PathBuf> {
    let symbol_name = symbol_partition_name("BTC")?;
    let channel_name = symbol_partition_name(channel)?;
    Ok(daily_stream_file_path(
        &input_dir
            .join("deribit")
            .join(&symbol_name)
            .join(&channel_name),
        &symbol_name,
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
            Err(error) if is_incomplete_zstd_frame(&error) => {
                eprintln!(
                    "stopped reading {} at line {line_number}: incomplete zstd tail",
                    path.display()
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

fn is_incomplete_zstd_frame(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::UnexpectedEof
        || error.to_string().contains("incomplete frame")
}

fn write_deribit_instruments_parquet(path: &Path, rows: &[DeribitInstrumentMeta]) -> Result<()> {
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
            required_string_array(rows.iter().map(|_| "BTC")),
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

fn write_deribit_quotes_parquet(path: &Path, rows: &[DeribitQuoteRow]) -> Result<()> {
    write_parquet_batch(path, deribit_quote_schema(), deribit_quote_columns(rows))
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

fn write_deribit_trades_parquet(path: &Path, rows: &[DeribitTradeRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
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
    ]));

    write_parquet_batch(
        path,
        schema,
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
        ],
    )
}

fn write_parquet_batch(path: &Path, schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .context("failed to build normalized Arrow batch")?;
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).context("invalid zstd compression level")?,
        ))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))
        .context("failed to create normalized Parquet writer")?;
    writer
        .write(&batch)
        .context("failed to write normalized Parquet batch")?;
    writer
        .close()
        .context("failed to close normalized Parquet writer")?;
    Ok(())
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

fn optional_bool_array(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(values.collect::<BooleanArray>())
}

fn resolve_start(cli_start: Option<i64>, checkpoint: Option<&Checkpoint>) -> Option<i64> {
    match (cli_start, checkpoint) {
        (Some(start), Some(checkpoint)) => Some(start.max(checkpoint.next_start_mts)),
        (None, Some(checkpoint)) => Some(checkpoint.next_start_mts),
        (start, None) => start,
    }
}

async fn pull_recent_page(
    client: &BitfinexClient,
    cli: &HistoryArgs,
    writer: &mut DailyParquetTradeWriter,
) -> Result<usize> {
    let query = TradeQuery {
        symbol: &cli.symbol,
        start: None,
        end: cli.end,
        limit: cli.limit,
        sort: SortOrder::Descending,
    };

    let mut trades = client.trades(&query).await?;
    trades.sort_unstable_by_key(|trade| (trade.mts, trade.id));
    writer.write_trades(trades.into_iter().take(limit_remaining(cli.max_trades, 0)))
}

async fn pull_forward(
    client: &BitfinexClient,
    cli: &HistoryArgs,
    start: i64,
    writer: &mut DailyParquetTradeWriter,
) -> Result<usize> {
    let mut cursor = start;
    let mut written = 0;

    loop {
        let query = TradeQuery {
            symbol: &cli.symbol,
            start: Some(cursor),
            end: cli.end,
            limit: cli.limit,
            sort: SortOrder::Ascending,
        };

        let mut trades = client.trades(&query).await?;
        if trades.is_empty() {
            break;
        }

        trades.sort_unstable_by_key(|trade| (trade.mts, trade.id));

        let max_mts = trades
            .iter()
            .map(|trade| trade.mts)
            .max()
            .context("non-empty trade page had no max timestamp")?;
        let page_len = trades.len();
        let remaining = limit_remaining(cli.max_trades, written);
        written += writer.write_trades(trades.into_iter().take(remaining))?;

        if cli.max_trades.is_some_and(|max| written >= max) || page_len < usize::from(cli.limit) {
            break;
        }

        let next_cursor = max_mts
            .checked_add(1)
            .context("trade timestamp overflow while advancing cursor")?;
        if next_cursor <= cursor {
            bail!("cursor did not advance past {cursor}");
        }
        cursor = next_cursor;
    }

    Ok(written)
}

fn parse_trades(symbol: &str, value: &Value) -> Result<Vec<Trade>> {
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("expected Bitfinex trade response array, got {value}"))?;

    rows.iter()
        .enumerate()
        .map(|(index, row)| parse_trade(symbol, index, row))
        .collect()
}

fn parse_trade(symbol: &str, index: usize, row: &Value) -> Result<Trade> {
    let fields = row
        .as_array()
        .ok_or_else(|| anyhow!("trade row {index} is not an array: {row}"))?;

    if fields.len() < 4 {
        bail!("trade row {index} has {} fields, expected 4", fields.len());
    }

    let id = parse_i64(&fields[0], "ID")?;
    let mts = parse_i64(&fields[1], "MTS")?;
    let amount = parse_decimal(&fields[2], "AMOUNT")?;
    let price = parse_decimal(&fields[3], "PRICE")?;
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(mts).ok_or_else(|| anyhow!("invalid MTS {mts}"))?;

    Ok(Trade {
        exchange: "bitfinex",
        symbol: symbol.to_owned(),
        id,
        mts,
        timestamp,
        side: trade_side(amount),
        amount,
        amount_abs: amount.abs(),
        price,
    })
}

fn parse_i64(value: &Value, field: &str) -> Result<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| anyhow!("{field} is not an i64: {value}")),
        Value::String(text) => text
            .parse::<i64>()
            .with_context(|| format!("{field} is not an i64: {text}")),
        _ => bail!("{field} has unexpected JSON type: {value}"),
    }
}

fn parse_decimal(value: &Value, field: &str) -> Result<Decimal> {
    let text = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => bail!("{field} has unexpected JSON type: {value}"),
    };

    Decimal::from_str(&text).with_context(|| format!("{field} is not a decimal: {text}"))
}

fn trade_side(amount: Decimal) -> TradeSide {
    if amount.is_sign_positive() {
        TradeSide::Buy
    } else if amount.is_sign_negative() {
        TradeSide::Sell
    } else {
        TradeSide::Unknown
    }
}

fn validate_args(cli: &HistoryArgs) -> Result<()> {
    if cli.symbol.is_empty() {
        bail!("symbol cannot be empty");
    }

    if let (Some(start), Some(end)) = (cli.start, cli.end) {
        if start > end {
            bail!("start must be less than or equal to end");
        }
    }

    if cli.max_trades == Some(0) {
        bail!("max-trades must be greater than zero");
    }

    Ok(())
}

struct DailyParquetTradeWriter {
    partition_dir: PathBuf,
    symbol_name: String,
    checkpoint_store: CheckpointStore,
    batch_size: usize,
    current_date: Option<NaiveDate>,
    current_last_trade: Option<CheckpointTrade>,
    current_writer: Option<ParquetTradeFileWriter>,
}

impl DailyParquetTradeWriter {
    fn create(output_dir: &Path, symbol: &str, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            bail!("batch size must be greater than zero");
        }

        let symbol_name = symbol_partition_name(symbol)?;
        let partition_dir = output_dir.join(&symbol_name);
        std::fs::create_dir_all(&partition_dir)
            .with_context(|| format!("failed to create {}", partition_dir.display()))?;
        let checkpoint_store =
            CheckpointStore::new(&partition_dir, symbol.to_owned(), symbol_name.clone());

        Ok(Self {
            partition_dir,
            symbol_name,
            checkpoint_store,
            batch_size,
            current_date: None,
            current_last_trade: None,
            current_writer: None,
        })
    }

    fn partition_dir(&self) -> &Path {
        &self.partition_dir
    }

    fn load_checkpoint(&self) -> Result<Option<Checkpoint>> {
        self.checkpoint_store.load()
    }

    fn write_trades<I>(&mut self, trades: I) -> Result<usize>
    where
        I: IntoIterator<Item = Trade>,
    {
        let mut written = 0;
        for trade in trades {
            self.write_trade(trade)?;
            written += 1;
        }
        Ok(written)
    }

    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        let trade_date = trade.timestamp.date_naive();
        if self.current_date != Some(trade_date) {
            self.close_current_writer(true)?;
            let path = daily_file_path(&self.partition_dir, &self.symbol_name, trade_date);
            self.current_writer = Some(ParquetTradeFileWriter::create(&path, self.batch_size)?);
            self.current_date = Some(trade_date);
        }

        let checkpoint_trade = CheckpointTrade {
            mts: trade.mts,
            id: trade.id,
        };
        self.current_writer
            .as_mut()
            .context("daily Parquet writer was not opened")?
            .write_trade(trade)?;
        self.current_last_trade = Some(checkpoint_trade);
        Ok(())
    }

    fn close_current_writer(&mut self, checkpoint_completed_day: bool) -> Result<()> {
        if let Some(writer) = self.current_writer.take() {
            let completed_date = self
                .current_date
                .take()
                .context("daily Parquet writer had no active date")?;
            let last_trade = self
                .current_last_trade
                .take()
                .context("daily Parquet writer had no last trade")?;
            writer.close()?;
            if checkpoint_completed_day {
                self.checkpoint_store
                    .save_completed_day(completed_date, last_trade)?;
            }
        }
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.close_current_writer(false)
    }
}

#[derive(Clone, Copy, Debug)]
struct CheckpointTrade {
    mts: i64,
    id: i64,
}

#[derive(Debug)]
struct CheckpointStore {
    path: PathBuf,
    tmp_path: PathBuf,
    symbol: String,
    symbol_name: String,
}

impl CheckpointStore {
    fn new(partition_dir: &Path, symbol: String, symbol_name: String) -> Self {
        Self {
            path: partition_dir.join(CHECKPOINT_FILE),
            tmp_path: partition_dir.join(CHECKPOINT_TMP_FILE),
            symbol,
            symbol_name,
        }
    }

    fn load(&self) -> Result<Option<Checkpoint>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(&self.path)
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        let checkpoint: Checkpoint = serde_json::from_reader(file)
            .with_context(|| format!("failed to parse {}", self.path.display()))?;
        checkpoint.validate(&self.symbol, &self.symbol_name)?;
        Ok(Some(checkpoint))
    }

    fn save_completed_day(
        &self,
        completed_date: NaiveDate,
        last_trade: CheckpointTrade,
    ) -> Result<()> {
        let checkpoint =
            Checkpoint::new(&self.symbol, &self.symbol_name, completed_date, last_trade)?;
        let bytes =
            serde_json::to_vec_pretty(&checkpoint).context("failed to encode checkpoint")?;

        {
            let mut file = std::fs::File::create(&self.tmp_path)
                .with_context(|| format!("failed to create {}", self.tmp_path.display()))?;
            file.write_all(&bytes)
                .with_context(|| format!("failed to write {}", self.tmp_path.display()))?;
            file.write_all(b"\n")
                .with_context(|| format!("failed to write {}", self.tmp_path.display()))?;
            file.sync_all()
                .with_context(|| format!("failed to sync {}", self.tmp_path.display()))?;
        }

        std::fs::rename(&self.tmp_path, &self.path).with_context(|| {
            format!(
                "failed to replace {} with {}",
                self.path.display(),
                self.tmp_path.display()
            )
        })?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Checkpoint {
    version: u8,
    exchange: String,
    symbol: String,
    symbol_partition: String,
    last_completed_date: String,
    last_trade_mts: i64,
    last_trade_id: i64,
    next_start_mts: i64,
    updated_at: String,
}

impl Checkpoint {
    fn new(
        symbol: &str,
        symbol_name: &str,
        completed_date: NaiveDate,
        last_trade: CheckpointTrade,
    ) -> Result<Self> {
        Ok(Self {
            version: CHECKPOINT_VERSION,
            exchange: "bitfinex".to_owned(),
            symbol: symbol.to_owned(),
            symbol_partition: symbol_name.to_owned(),
            last_completed_date: completed_date.to_string(),
            last_trade_mts: last_trade.mts,
            last_trade_id: last_trade.id,
            next_start_mts: last_trade
                .mts
                .checked_add(1)
                .context("checkpoint timestamp overflow")?,
            updated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        })
    }

    fn validate(&self, symbol: &str, symbol_name: &str) -> Result<()> {
        if self.version != CHECKPOINT_VERSION {
            bail!(
                "unsupported checkpoint version {} in {}",
                self.version,
                CHECKPOINT_FILE
            );
        }
        if self.exchange != "bitfinex" {
            bail!("checkpoint exchange mismatch: {}", self.exchange);
        }
        if self.symbol != symbol {
            bail!("checkpoint symbol mismatch: {} != {symbol}", self.symbol);
        }
        if self.symbol_partition != symbol_name {
            bail!(
                "checkpoint symbol partition mismatch: {} != {symbol_name}",
                self.symbol_partition
            );
        }
        if self.next_start_mts <= self.last_trade_mts {
            bail!("checkpoint next_start_mts must be after last_trade_mts");
        }
        Ok(())
    }
}

struct ParquetTradeFileWriter {
    writer: Option<ArrowWriter<std::fs::File>>,
    schema: Arc<Schema>,
    buffer: Vec<Trade>,
    batch_size: usize,
}

impl ParquetTradeFileWriter {
    fn create(path: &Path, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            bail!("batch size must be greater than zero");
        }

        let file = std::fs::File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        let schema = parquet_schema();
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(3).context("invalid zstd compression level")?,
            ))
            .build();
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .context("failed to create Parquet writer")?;

        Ok(Self {
            writer: Some(writer),
            schema,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
        })
    }

    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        self.buffer.push(trade);
        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = trades_to_record_batch(&self.schema, &self.buffer)?;
        self.writer
            .as_mut()
            .context("Parquet writer was already closed")?
            .write(&batch)
            .context("failed to write Parquet batch")?;
        self.buffer.clear();
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.flush()?;
        let writer = self
            .writer
            .take()
            .context("Parquet writer was already closed")?;
        writer.close().context("failed to close Parquet writer")?;
        Ok(())
    }
}

fn parquet_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("mts", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
        Field::new("amount_abs", DataType::Utf8, false),
        Field::new("price", DataType::Utf8, false),
    ]))
}

fn trades_to_record_batch(schema: &Arc<Schema>, trades: &[Trade]) -> Result<RecordBatch> {
    let exchange = StringArray::from_iter_values(trades.iter().map(|trade| trade.exchange));
    let symbol = StringArray::from_iter_values(trades.iter().map(|trade| trade.symbol.as_str()));
    let id = Int64Array::from_iter_values(trades.iter().map(|trade| trade.id));
    let mts = Int64Array::from_iter_values(trades.iter().map(|trade| trade.mts));
    let timestamp = StringArray::from_iter_values(
        trades
            .iter()
            .map(|trade| trade.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)),
    );
    let side = StringArray::from_iter_values(trades.iter().map(|trade| trade.side.as_str()));
    let amount =
        StringArray::from_iter_values(trades.iter().map(|trade| decimal_string(&trade.amount)));
    let amount_abs =
        StringArray::from_iter_values(trades.iter().map(|trade| decimal_string(&trade.amount_abs)));
    let price =
        StringArray::from_iter_values(trades.iter().map(|trade| decimal_string(&trade.price)));

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(exchange) as ArrayRef,
            Arc::new(symbol),
            Arc::new(id),
            Arc::new(mts),
            Arc::new(timestamp),
            Arc::new(side),
            Arc::new(amount),
            Arc::new(amount_abs),
            Arc::new(price),
        ],
    )
    .context("failed to build Arrow record batch")
}

fn decimal_string(value: &Decimal) -> String {
    value.normalize().to_string()
}

fn symbol_partition_name(symbol: &str) -> Result<String> {
    let normalized: String = symbol
        .chars()
        .map(|char| {
            if char.is_ascii_alphanumeric() || char == '-' || char == '_' {
                char.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();

    if normalized.is_empty() {
        bail!("symbol cannot be empty after path normalization");
    }

    Ok(normalized)
}

fn daily_file_path(partition_dir: &Path, symbol_name: &str, date: NaiveDate) -> PathBuf {
    partition_dir.join(format!(
        "{}_{}.parquet",
        symbol_name,
        date.format("%y-%m-%d")
    ))
}

fn daily_stream_file_path(
    partition_dir: &Path,
    symbol_name: &str,
    channel_name: &str,
    date: NaiveDate,
) -> PathBuf {
    partition_dir.join(format!(
        "{}_{}_{}.jsonl.zst",
        symbol_name,
        channel_name,
        date.format("%y-%m-%d")
    ))
}

fn normalized_parquet_path(output_dir: &Path, dataset: &str, date: NaiveDate) -> PathBuf {
    output_dir.join(dataset).join(format!(
        "btc_{}_{}.parquet",
        dataset,
        date.format("%y-%m-%d")
    ))
}

fn parse_mts(value: &str) -> Result<i64, String> {
    if value.chars().all(|char| char.is_ascii_digit()) {
        return value
            .parse::<i64>()
            .map_err(|error| format!("invalid Unix millisecond timestamp: {error}"));
    }

    DateTime::parse_from_rfc3339(value)
        .map(|date_time| date_time.timestamp_millis())
        .map_err(|error| format!("expected Unix milliseconds or RFC3339 timestamp: {error}"))
}

fn parse_date(value: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| format!("expected YYYY-MM-DD date: {error}"))
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("expected positive integer: {error}"))?;
    if parsed == 0 {
        return Err("expected positive integer greater than zero".to_owned());
    }
    Ok(parsed)
}

fn is_bitfinex_rate_limit_body(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return false;
    };

    value
        .get("error")
        .and_then(Value::as_str)
        .is_some_and(|error| error == "ERR_RATE_LIMIT")
}

fn is_retryable(error: &anyhow::Error) -> bool {
    error.to_string().contains("retryable:")
}

fn retry_delay(attempt: u8) -> Duration {
    let seconds = 2_u64.pow(u32::from(attempt)).min(30);
    Duration::from_secs(seconds)
}

const fn limit_remaining(max_trades: Option<usize>, written: usize) -> usize {
    match max_trades {
        Some(max) => max.saturating_sub(written),
        None => usize::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_trade() -> Trade {
        sample_trade_at(1_781_472_895_818)
    }

    fn sample_trade_at(mts: i64) -> Trade {
        let row = serde_json::json!([1_936_324_137_i64, mts, "-0.00012155", "64883"]);
        parse_trade("tBTCUSD", 0, &row).expect("valid trade")
    }

    fn temp_test_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "modl-bitfinex-test-{}-{}",
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
        serde_json::json!({
            "received_at": "2026-06-29T22:17:08.192Z",
            "received_mts": 1_782_771_428_192_i64,
            "exchange": "deribit",
            "symbol": "BTC",
            "channel": channel,
            "connection_id": "deribit-BTC-instruments",
            "payload_text": payload_text
        })
    }

    fn write_raw_zstd_file(path: &Path, events: &[Value]) -> Result<()> {
        std::fs::create_dir_all(path.parent().expect("raw path has parent"))?;
        let file = std::fs::File::create(path)?;
        let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), 1)?;
        for event in events {
            serde_json::to_writer(&mut encoder, event)?;
            encoder.write_all(b"\n")?;
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
    fn parses_rfc3339_as_milliseconds() {
        let parsed = parse_mts("2026-06-14T00:00:00Z").expect("valid timestamp");
        assert_eq!(parsed, 1_781_395_200_000);
    }

    #[test]
    fn parses_trade_row_without_float_conversion() {
        let trade = sample_trade();

        assert_eq!(trade.id, 1_936_324_137);
        assert_eq!(trade.mts, 1_781_472_895_818);
        assert!(matches!(trade.side, TradeSide::Sell));
        assert_eq!(trade.amount.to_string(), "-0.00012155");
        assert_eq!(trade.amount_abs.to_string(), "0.00012155");
        assert_eq!(trade.price.to_string(), "64883");
    }

    #[test]
    fn detects_bitfinex_rate_limit_body() {
        assert!(is_bitfinex_rate_limit_body(r#"{"error":"ERR_RATE_LIMIT"}"#));
        assert!(!is_bitfinex_rate_limit_body(r"[]"));
    }

    #[test]
    fn writes_parquet_file() -> Result<()> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = temp_test_dir();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("trades.parquet");

        let mut writer = ParquetTradeFileWriter::create(&path, 2)?;
        writer.write_trade(sample_trade())?;
        writer.write_trade(sample_trade())?;
        writer.close()?;

        let file = std::fs::File::open(&path)?;
        let reader = SerializedFileReader::new(file)?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn writes_daily_partitioned_files() -> Result<()> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = temp_test_dir();
        let mut writer = DailyParquetTradeWriter::create(&dir, "tBTCUSD", 1)?;
        assert_eq!(
            writer.write_trades(vec![
                sample_trade_at(1_781_452_800_000),
                sample_trade_at(1_781_539_200_000),
            ])?,
            2
        );
        writer.close()?;

        let partition_dir = dir.join("tbtcusd");
        let day_one = partition_dir.join("tbtcusd_26-06-14.parquet");
        let day_two = partition_dir.join("tbtcusd_26-06-15.parquet");
        let checkpoint_path = partition_dir.join(CHECKPOINT_FILE);
        assert!(day_one.exists());
        assert!(day_two.exists());
        assert!(checkpoint_path.exists());

        for path in [&day_one, &day_two] {
            let file = std::fs::File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        }

        let checkpoint: Checkpoint =
            serde_json::from_reader(std::fs::File::open(checkpoint_path)?)?;
        assert_eq!(checkpoint.symbol, "tBTCUSD");
        assert_eq!(checkpoint.symbol_partition, "tbtcusd");
        assert_eq!(checkpoint.last_completed_date, "2026-06-14");
        assert_eq!(checkpoint.last_trade_mts, 1_781_452_800_000);
        assert_eq!(checkpoint.next_start_mts, 1_781_452_800_001);

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn normalizes_symbol_for_partition_paths() -> Result<()> {
        assert_eq!(symbol_partition_name("tBTCUSD")?, "tbtcusd");
        assert_eq!(symbol_partition_name("tBTC:F0:USTF0")?, "tbtc_f0_ustf0");
        Ok(())
    }

    #[test]
    fn resolves_start_from_checkpoint() {
        let checkpoint = Checkpoint {
            version: CHECKPOINT_VERSION,
            exchange: "bitfinex".to_owned(),
            symbol: "tBTCUSD".to_owned(),
            symbol_partition: "tbtcusd".to_owned(),
            last_completed_date: "2026-06-14".to_owned(),
            last_trade_mts: 1_781_452_800_000,
            last_trade_id: 1,
            next_start_mts: 1_781_452_800_001,
            updated_at: "2026-06-14T00:00:00Z".to_owned(),
        };

        assert_eq!(
            resolve_start(None, Some(&checkpoint)),
            Some(1_781_452_800_001)
        );
        assert_eq!(
            resolve_start(Some(1_700_000_000_000), Some(&checkpoint)),
            Some(1_781_452_800_001)
        );
        assert_eq!(
            resolve_start(Some(1_900_000_000_000), Some(&checkpoint)),
            Some(1_900_000_000_000)
        );
        assert_eq!(resolve_start(Some(42), None), Some(42));
    }

    #[test]
    fn builds_stream_presets() {
        let bitfinex = bitfinex_stream_specs("tBTCUSD");
        assert_eq!(bitfinex.len(), 2);
        assert_eq!(bitfinex[0].channel, "book_l25");
        assert_eq!(bitfinex[1].channel, "trades");

        let extended = extended_stream_specs("BTC-USD");
        assert_eq!(extended.len(), 5);
        assert!(extended.iter().any(|spec| spec.channel == "orderbook"));
        assert!(extended.iter().any(|spec| spec.channel == "mark_price"));
        assert!(
            extended
                .iter()
                .all(|spec| spec.heartbeat_policy == HeartbeatPolicy::BestEffort)
        );

        let extended_spot = extended_spot_stream_specs("BTCSPOT-USD");
        assert_eq!(extended_spot.len(), 2);
        assert!(extended_spot.iter().any(|spec| spec.channel == "orderbook"));
        assert!(extended_spot.iter().any(|spec| spec.channel == "trades"));
        assert!(
            extended_spot
                .iter()
                .all(|spec| spec.heartbeat_policy == HeartbeatPolicy::BestEffort)
        );

        let deribit = deribit_stream_spec(&StreamArgs {
            venues: vec![StreamVenue::Deribit],
            output_dir: PathBuf::from("/tmp/modl-ws"),
            bitfinex_symbol: DEFAULT_SYMBOL.to_owned(),
            extended_market: DEFAULT_EXTENDED_MARKET.to_owned(),
            extended_spot_market: DEFAULT_EXTENDED_SPOT_MARKET.to_owned(),
            hibachi_symbol: DEFAULT_HIBACHI_SYMBOL.to_owned(),
            hibachi_url: DEFAULT_HIBACHI_MARKET_WS_URL.to_owned(),
            deribit_url: DEFAULT_DERIBIT_WS_URL.to_owned(),
            deribit_kinds: vec![DeribitInstrumentKind::Future, DeribitInstrumentKind::Option],
            deribit_trades_interval: DEFAULT_DERIBIT_TRADES_INTERVAL.to_owned(),
            hyperliquid_spot_coin: None,
            zstd_level: 6,
            max_messages: None,
            reconnect_delay_secs: 5,
            heartbeat_secs: DEFAULT_WS_HEARTBEAT_SECS,
        });
        assert_eq!(deribit.exchange, "deribit");
        assert_eq!(deribit.heartbeat_policy, HeartbeatPolicy::BestEffort);

        let hyperliquid = hyperliquid_stream_spec();
        assert_eq!(hyperliquid.exchange, "hyperliquid");
        assert_eq!(hyperliquid.behavior, FeedBehavior::HyperliquidSpot);
    }

    #[test]
    fn parses_btc_shortcut_command() {
        let cli = Cli::try_parse_from([
            "modl",
            "btc",
            "--output-dir",
            "/tmp/modl-btc",
            "--max-messages",
            "1",
        ])
        .expect("btc shortcut should parse");

        match cli.command {
            Some(Commands::Btc(args)) => {
                assert_eq!(args.output_dir, PathBuf::from("/tmp/modl-btc"));
                assert_eq!(args.max_messages, Some(1));
            }
            command => panic!("expected btc command, got {command:?}"),
        }
    }

    #[test]
    fn parses_normalize_command() {
        let cli = Cli::try_parse_from([
            "modl",
            "normalize",
            "--date",
            "2026-06-29",
            "--input-dir",
            "/tmp/modl-ws-raw",
            "--output-dir",
            "/tmp/modl-normalized",
        ])
        .expect("normalize command should parse");

        match cli.command {
            Some(Commands::Normalize(args)) => {
                assert_eq!(args.date, test_date());
                assert_eq!(args.input_dir, PathBuf::from("/tmp/modl-ws-raw"));
                assert_eq!(args.output_dir, PathBuf::from("/tmp/modl-normalized"));
            }
            command => panic!("expected normalize command, got {command:?}"),
        }
    }

    #[test]
    fn btc_shortcut_expands_to_all_btc_streams() {
        let args = btc_stream_args(BtcArgs {
            output_dir: PathBuf::from("/tmp/modl-btc"),
            zstd_level: 6,
            max_messages: Some(1),
            reconnect_delay_secs: 5,
            heartbeat_secs: DEFAULT_WS_HEARTBEAT_SECS,
            hibachi_url: DEFAULT_HIBACHI_MARKET_WS_URL.to_owned(),
            deribit_url: DEFAULT_DERIBIT_WS_URL.to_owned(),
            hyperliquid_spot_coin: None,
        });

        assert_eq!(
            args.venues,
            vec![
                StreamVenue::Bitfinex,
                StreamVenue::Hibachi,
                StreamVenue::Deribit,
                StreamVenue::Hyperliquid,
            ]
        );
        assert_eq!(args.output_dir, PathBuf::from("/tmp/modl-btc"));
        assert_eq!(args.extended_spot_market, DEFAULT_EXTENDED_SPOT_MARKET);
        assert_eq!(stream_specs(&args).len(), 5);
    }

    #[test]
    fn builds_deribit_subscription_channels() {
        let channels = deribit_lifecycle_channels(&[
            DeribitInstrumentKind::Future,
            DeribitInstrumentKind::Option,
        ]);
        assert_eq!(
            channels,
            vec![
                "instrument.creation.future.BTC",
                "instrument.state.future.BTC",
                "instrument.creation.option.BTC",
                "instrument.state.option.BTC",
            ]
        );

        assert_eq!(
            deribit_instrument_channels("BTC-PERPETUAL", "100ms"),
            vec![
                "incremental_ticker.BTC-PERPETUAL",
                "trades.BTC-PERPETUAL.100ms",
            ]
        );
    }

    #[test]
    fn routes_deribit_messages_to_output_channels() {
        assert_eq!(
            deribit_output_channel(
                r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"instrument.creation.future.BTC","data":{}}}"#
            ),
            "instrument_creation"
        );
        assert_eq!(
            deribit_output_channel(
                r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"instrument.state.option.BTC","data":{}}}"#
            ),
            "instrument_state"
        );
        assert_eq!(
            deribit_output_channel(
                r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"incremental_ticker.BTC-PERPETUAL","data":{}}}"#
            ),
            "incremental_ticker"
        );
        assert_eq!(
            deribit_output_channel(
                r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[]}}"#
            ),
            "trades"
        );
        assert_eq!(
            deribit_output_channel(r#"{"jsonrpc":"2.0","id":1}"#),
            "control"
        );
    }

    #[test]
    fn detects_deribit_created_btc_future_or_option() {
        let allowed = [DeribitInstrumentKind::Future, DeribitInstrumentKind::Option];
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "instrument.creation.option.BTC",
                "data": {
                    "kind": "option",
                    "base_currency": "BTC",
                    "instrument_name": "BTC-13JAN23-16000-P"
                }
            }
        });
        assert_eq!(
            deribit_created_instrument_name(&message, &allowed).as_deref(),
            Some("BTC-13JAN23-16000-P")
        );

        let eth_message = serde_json::json!({
            "params": {
                "channel": "instrument.creation.option.ETH",
                "data": {
                    "kind": "option",
                    "base_currency": "ETH",
                    "instrument_name": "ETH-13JAN23-16000-P"
                }
            }
        });
        assert_eq!(
            deribit_created_instrument_name(&eth_message, &allowed),
            None
        );
    }

    #[test]
    fn extracts_current_deribit_btc_instruments() {
        let allowed = [DeribitInstrumentKind::Future, DeribitInstrumentKind::Option];
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": [
                {
                    "kind": "future",
                    "base_currency": "BTC",
                    "instrument_name": "BTC-PERPETUAL",
                    "is_active": true
                },
                {
                    "kind": "option",
                    "base_currency": "BTC",
                    "instrument_name": "BTC-13JAN23-16000-P",
                    "is_active": true
                },
                {
                    "kind": "option",
                    "base_currency": "ETH",
                    "instrument_name": "ETH-13JAN23-16000-P",
                    "is_active": true
                },
                {
                    "kind": "future",
                    "base_currency": "BTC",
                    "instrument_name": "BTC-INACTIVE",
                    "is_active": false
                },
                {
                    "kind": "spot",
                    "base_currency": "BTC",
                    "instrument_name": "BTC_USDC",
                    "is_active": true
                }
            ]
        });

        assert_eq!(
            deribit_instrument_names_from_get_instruments_response(&message, &allowed),
            vec!["BTC-PERPETUAL", "BTC-13JAN23-16000-P"]
        );
        assert_eq!(deribit_response_id(&message), Some(2));
    }

    #[test]
    fn detects_deribit_heartbeat_test_request() {
        let test_request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "heartbeat",
            "params": {
                "type": "test_request"
            }
        });
        assert_eq!(deribit_heartbeat_type(&test_request), Some("test_request"));

        let heartbeat = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "heartbeat",
            "params": {
                "type": "heartbeat"
            }
        });
        assert_eq!(deribit_heartbeat_type(&heartbeat), Some("heartbeat"));

        let subscription = serde_json::json!({
            "method": "subscription",
            "params": {
                "channel": "trades.BTC-PERPETUAL.100ms"
            }
        });
        assert_eq!(deribit_heartbeat_type(&subscription), None);
    }

    #[test]
    fn treats_websocket_reset_without_close_as_reconnect() {
        assert!(is_expected_websocket_reconnect_error(&WsError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake
        )));
        assert!(is_expected_websocket_reconnect_error(&WsError::Io(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset)
        )));
        assert!(!is_expected_websocket_reconnect_error(&WsError::Protocol(
            ProtocolError::MaskedFrameFromServer
        )));
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
    }

    #[test]
    fn writes_deribit_normalized_parquet_files() -> Result<()> {
        let date = test_date();
        let dir = temp_test_dir();
        let input_dir = dir.join("raw");
        let output_dir = dir.join("normalized");

        let control_payload = r#"{"jsonrpc":"2.0","id":2,"result":[{"instrument_name":"BTC-31JUL26-60000-P","kind":"option","base_currency":"BTC","quote_currency":"BTC","settlement_currency":"BTC","expiration_timestamp":1785484800000,"creation_timestamp":1777993440000,"strike":60000.0,"option_type":"put","settlement_period":"month","is_active":true}]}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&input_dir, "control", date)?,
            &[sample_raw_event("control", control_payload)],
        )?;

        let ticker_payload = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"incremental_ticker.BTC-31JUL26-60000-P","data":{"timestamp":1782771427066,"type":"snapshot","instrument_name":"BTC-31JUL26-60000-P","mark_price":0.0434492,"stats":{"volume":5.0}}}}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&input_dir, "incremental_ticker", date)?,
            &[sample_raw_event("incremental_ticker", ticker_payload)],
        )?;

        let trades_payload = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-31JUL26-60000-P.100ms","data":[{"timestamp":1782772067807,"iv":41.51,"price":0.044,"amount":0.9,"direction":"buy","index_price":60377.86,"instrument_name":"BTC-31JUL26-60000-P","trade_seq":2265,"mark_price":0.0434492,"tick_direction":2,"contracts":0.9,"trade_id":"436176324"}]}}"#;
        write_raw_zstd_file(
            &deribit_raw_file_path(&input_dir, "trades", date)?,
            &[sample_raw_event("trades", trades_payload)],
        )?;

        run_normalize_command(&NormalizeArgs {
            date,
            input_dir,
            output_dir: output_dir.clone(),
        })?;

        let normalized_dir = output_dir.join("deribit").join("btc");
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &normalized_dir,
                "instruments",
                date
            ))?,
            1
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(
                &normalized_dir,
                "incremental_ticker",
                date
            ))?,
            1
        );
        assert_eq!(
            parquet_num_rows(&normalized_parquet_path(&normalized_dir, "trades", date))?,
            1
        );

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn routes_hyperliquid_messages_to_output_channels() {
        assert_eq!(
            hyperliquid_output_channel(&HyperliquidIncoming::Trades(Vec::new())),
            "trades"
        );
    }

    #[test]
    fn classifies_remote_ping_timeout_close_frames() {
        let ping_timeout = CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error,
            reason: "Ping timeout".into(),
        };
        assert!(is_remote_ping_timeout_close(&ping_timeout));

        let normal = CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
            reason: "Normal closure".into(),
        };
        assert!(!is_remote_ping_timeout_close(&normal));
    }

    #[test]
    fn builds_websocket_heartbeat_config() {
        assert_eq!(
            websocket_heartbeat_duration(DEFAULT_WS_HEARTBEAT_SECS),
            Some(Duration::from_secs(DEFAULT_WS_HEARTBEAT_SECS))
        );
        assert_eq!(websocket_heartbeat_duration(0), None);

        let payload = heartbeat_payload(42);
        assert_eq!(payload.as_slice(), b"modl:000000000000002a");
        assert!(payload.len() <= 125);

        let mut required = HeartbeatState {
            ticks: None,
            sequence: 0,
            awaiting_pong: Some(heartbeat_payload(1)),
            interval_secs: DEFAULT_WS_HEARTBEAT_SECS,
            policy: HeartbeatPolicy::Required,
        };
        assert!(required.clear_stale_pong_or_timeout("required").is_err());

        let mut best_effort = HeartbeatState {
            ticks: None,
            sequence: 0,
            awaiting_pong: Some(heartbeat_payload(1)),
            interval_secs: DEFAULT_WS_HEARTBEAT_SECS,
            policy: HeartbeatPolicy::BestEffort,
        };
        assert!(best_effort.clear_stale_pong_or_timeout("extended").is_ok());
        assert_eq!(best_effort.awaiting_pong, None);
    }

    #[test]
    fn writes_compressed_raw_events() -> Result<()> {
        let dir = temp_test_dir();
        let spec = FeedSpec {
            exchange: "bitfinex",
            symbol: "tBTCUSD".to_owned(),
            channel: "trades",
            connection_id: "bitfinex-tBTCUSD-trades".to_owned(),
            url: "wss://example.invalid".to_owned(),
            subscribe_messages: Vec::new(),
            behavior: FeedBehavior::Static,
            heartbeat_policy: HeartbeatPolicy::Required,
        };

        let mut writer =
            DailyCompressedEventWriter::create(&dir, spec.exchange, &spec.symbol, spec.channel, 1)?;
        writer.write_text_event(&spec, r#"{"event":"test"}"#)?;
        writer.close()?;

        let mut writer =
            DailyCompressedEventWriter::create(&dir, spec.exchange, &spec.symbol, spec.channel, 1)?;
        writer.write_text_event(&spec, r#"{"event":"second"}"#)?;
        writer.close()?;

        let partition_dir = dir.join("bitfinex").join("tbtcusd").join("trades");
        let entries = std::fs::read_dir(&partition_dir)?.collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 1);

        let file = std::fs::File::open(entries[0].path())?;
        let decoded =
            zstd::stream::decode_all(file).context("failed to decode compressed event file")?;
        let text = String::from_utf8(decoded).context("event file was not UTF-8")?;
        assert!(text.contains(r#""exchange":"bitfinex""#));
        assert!(text.contains(r#""payload_text":"{\"event\":\"test\"}""#));
        assert!(text.contains(r#""payload_text":"{\"event\":\"second\"}""#));

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }
}
