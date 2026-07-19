use std::{
    env,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};

const DEFAULT_OUTPUT_DIR: &str = "/mnt/burner-archive/ws_raw";
const DEFAULT_BITFINEX_SYMBOL: &str = "tBTCUSD";
const DEFAULT_EXTENDED_MARKET: &str = "BTC-USD";
const DEFAULT_EXTENDED_SPOT_MARKET: &str = "BTCSPOT-USD";
const DEFAULT_HIBACHI_SYMBOL: &str = "BTC/USDT-P";
const DEFAULT_DERIBIT_CURRENCY: &str = "BTC";
const DEFAULT_HEARTBEAT_SECS: u64 = 20;
const DEFAULT_PROGRESS_EVERY: usize = 100;

fn main() -> Result<()> {
    run_menu()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VenueId {
    Bitfinex,
    Extended,
    Hibachi,
    Deribit,
    Hyperliquid,
}

impl VenueId {
    const fn cli_value(self) -> &'static str {
        match self {
            Self::Bitfinex => "bitfinex",
            Self::Extended => "extended",
            Self::Hibachi => "hibachi",
            Self::Deribit => "deribit",
            Self::Hyperliquid => "hyperliquid",
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Bitfinex => "Bitfinex",
            Self::Extended => "Extended",
            Self::Hibachi => "Hibachi",
            Self::Deribit => "Deribit",
            Self::Hyperliquid => "Hyperliquid",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct VenueSelection {
    id: VenueId,
    enabled: bool,
}

#[derive(Debug)]
struct TuiState {
    output_dir: String,
    bitfinex_symbol: String,
    extended_market: String,
    extended_spot_market: String,
    hibachi_symbol: String,
    deribit_currency: String,
    hyperliquid_spot_coin: String,
    heartbeat_secs: u64,
    progress_every: usize,
    venues: [VenueSelection; 5],
}

impl Default for TuiState {
    fn default() -> Self {
        Self {
            output_dir: DEFAULT_OUTPUT_DIR.to_owned(),
            bitfinex_symbol: DEFAULT_BITFINEX_SYMBOL.to_owned(),
            extended_market: DEFAULT_EXTENDED_MARKET.to_owned(),
            extended_spot_market: DEFAULT_EXTENDED_SPOT_MARKET.to_owned(),
            hibachi_symbol: DEFAULT_HIBACHI_SYMBOL.to_owned(),
            deribit_currency: DEFAULT_DERIBIT_CURRENCY.to_owned(),
            hyperliquid_spot_coin: String::new(),
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
            progress_every: DEFAULT_PROGRESS_EVERY,
            venues: [
                VenueSelection {
                    id: VenueId::Bitfinex,
                    enabled: true,
                },
                VenueSelection {
                    id: VenueId::Extended,
                    enabled: false,
                },
                VenueSelection {
                    id: VenueId::Hibachi,
                    enabled: false,
                },
                VenueSelection {
                    id: VenueId::Deribit,
                    enabled: true,
                },
                VenueSelection {
                    id: VenueId::Hyperliquid,
                    enabled: true,
                },
            ],
        }
    }
}

impl TuiState {
    fn render(&self) -> Result<()> {
        clear_screen()?;
        println!("MODL Pi recorder");
        println!("=================");
        println!("Output dir: {}", self.output_dir);
        println!("Heartbeat: {}s", self.heartbeat_secs);
        println!("Progress: every {} messages per feed", self.progress_every);
        println!();
        println!("Venues");
        for (index, venue) in self.venues.iter().enumerate() {
            println!(
                "  {}) [{}] {:<12} {}",
                index + 1,
                if venue.enabled { "x" } else { " " },
                venue.id.name(),
                self.venue_detail(venue.id)
            );
        }
        println!();
        println!("Commands");
        println!("  1-5 toggle venue   a all venues on   n all venues off");
        println!("  m edit market symbols   o edit output dir   h edit heartbeat");
        println!("  r run recorder      q quit");
        println!();
        Ok(())
    }

    fn venue_detail(&self, venue: VenueId) -> String {
        match venue {
            VenueId::Bitfinex => format!("symbol={}", self.bitfinex_symbol),
            VenueId::Extended => format!(
                "perp={} spot={}",
                self.extended_market, self.extended_spot_market
            ),
            VenueId::Hibachi => format!("symbol={}", self.hibachi_symbol),
            VenueId::Deribit => format!("currency={}", self.deribit_currency),
            VenueId::Hyperliquid if self.hyperliquid_spot_coin.trim().is_empty() => {
                "spot=auto BTC/USDC".to_owned()
            }
            VenueId::Hyperliquid => format!("spot_coin={}", self.hyperliquid_spot_coin),
        }
    }

    fn toggle_venue(&mut self, menu_index: usize) {
        if let Some(venue) = self.venues.get_mut(menu_index) {
            venue.enabled = !venue.enabled;
        }
    }

    fn set_all_venues(&mut self, enabled: bool) {
        for venue in &mut self.venues {
            venue.enabled = enabled;
        }
    }

    fn enabled_venues(&self) -> Vec<VenueId> {
        self.venues
            .iter()
            .filter(|venue| venue.enabled)
            .map(|venue| venue.id)
            .collect()
    }

    fn recorder_args(&self) -> Result<Vec<String>> {
        let venues = self.enabled_venues();
        if venues.is_empty() {
            bail!("select at least one venue before running the recorder");
        }

        let mut args = vec![
            "stream".to_owned(),
            "--venue".to_owned(),
            venues
                .iter()
                .map(|venue| venue.cli_value())
                .collect::<Vec<_>>()
                .join(","),
            "--output-dir".to_owned(),
            self.output_dir.clone(),
            "--bitfinex-symbol".to_owned(),
            self.bitfinex_symbol.clone(),
            "--extended-market".to_owned(),
            self.extended_market.clone(),
            "--extended-spot-market".to_owned(),
            self.extended_spot_market.clone(),
            "--hibachi-symbol".to_owned(),
            self.hibachi_symbol.clone(),
            "--deribit-currency".to_owned(),
            self.deribit_currency.trim().to_ascii_uppercase(),
            "--heartbeat-secs".to_owned(),
            self.heartbeat_secs.to_string(),
            "--progress-every".to_owned(),
            self.progress_every.to_string(),
        ];

        let hyperliquid_spot_coin = self.hyperliquid_spot_coin.trim();
        if !hyperliquid_spot_coin.is_empty() {
            args.push("--hyperliquid-spot-coin".to_owned());
            args.push(hyperliquid_spot_coin.to_owned());
        }

        Ok(args)
    }
}

fn run_menu() -> Result<()> {
    let mut state = TuiState::default();
    loop {
        state.render()?;
        let command = prompt("command")?;
        match command.trim() {
            "1" => state.toggle_venue(0),
            "2" => state.toggle_venue(1),
            "3" => state.toggle_venue(2),
            "4" => state.toggle_venue(3),
            "5" => state.toggle_venue(4),
            "a" | "A" => state.set_all_venues(true),
            "n" | "N" => state.set_all_venues(false),
            "m" | "M" => edit_markets(&mut state)?,
            "o" | "O" => {
                state.output_dir = prompt_default("output dir", &state.output_dir)?;
            }
            "h" | "H" => {
                state.heartbeat_secs =
                    prompt_u64_default("heartbeat seconds", state.heartbeat_secs)?;
            }
            "r" | "R" => run_recorder(&state)?,
            "q" | "Q" => break,
            "" => {}
            other => pause(&format!("unknown command: {other}"))?,
        }
    }
    clear_screen()?;
    Ok(())
}

fn edit_markets(state: &mut TuiState) -> Result<()> {
    clear_screen()?;
    println!("Edit market symbols");
    println!("Leave a field blank to keep its current value.");
    println!();
    state.bitfinex_symbol = prompt_default("Bitfinex symbol", &state.bitfinex_symbol)?;
    state.extended_market = prompt_default("Extended perp market", &state.extended_market)?;
    state.extended_spot_market =
        prompt_default("Extended spot market", &state.extended_spot_market)?;
    state.hibachi_symbol = prompt_default("Hibachi symbol", &state.hibachi_symbol)?;
    state.deribit_currency =
        prompt_default("Deribit currency", &state.deribit_currency)?.to_ascii_uppercase();
    state.hyperliquid_spot_coin = prompt_default(
        "Hyperliquid spot coin override",
        &state.hyperliquid_spot_coin,
    )?;
    Ok(())
}

fn run_recorder(state: &TuiState) -> Result<()> {
    let recorder = recorder_binary()?;
    let args = state.recorder_args()?;
    clear_screen()?;
    println!("Starting MODL recorder");
    println!("Command:");
    println!("  {}", display_command(&recorder, &args));
    println!();
    println!("Recorder output follows. Progress lines show captured message counts.");
    println!("Press Enter to stop the recorder cleanly and return to the menu.");
    println!();

    let mut child = Command::new(&recorder)
        .args(&args)
        .spawn()
        .with_context(|| {
            format!(
                "failed to start {}; run `cargo build --release --bins` first or set MODL_RECORDER_BIN",
                recorder.display()
            )
        })?;

    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .context("failed to read stop request")?;
    stop_child_cleanly(&mut child)?;
    pause("recorder stopped")
}

fn recorder_binary() -> Result<PathBuf> {
    if let Ok(path) = env::var("MODL_RECORDER_BIN") {
        return Ok(PathBuf::from(path));
    }

    let current_exe = env::current_exe().context("failed to locate current executable")?;
    let binary_name = if cfg!(windows) { "modl.exe" } else { "modl" };
    let sibling = current_exe.with_file_name(binary_name);
    if sibling.exists() {
        return Ok(sibling);
    }

    Ok(PathBuf::from(binary_name))
}

fn stop_child_cleanly(child: &mut Child) -> Result<ExitStatus> {
    if let Some(status) = child.try_wait().context("failed to inspect recorder")? {
        return Ok(status);
    }

    send_interrupt(child.id())?;
    for _ in 0..40 {
        if let Some(status) = child.try_wait().context("failed to inspect recorder")? {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(250));
    }

    child
        .kill()
        .context("failed to kill recorder after timeout")?;
    child.wait().context("failed waiting for killed recorder")
}

fn send_interrupt(pid: u32) -> Result<()> {
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
        .with_context(|| format!("failed to send SIGINT to recorder pid {pid}"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("failed to send SIGINT to recorder pid {pid}: kill exited with {status}");
    }
}

fn display_command(program: &Path, args: &[String]) -> String {
    std::iter::once(shell_quote(&program.display().to_string()))
        .chain(args.iter().map(|arg| shell_quote(arg)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(value: &str) -> String {
    if value.chars().all(|ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ',' | ':' | '=')
    }) {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}> ");
    io::stdout().flush().context("failed to flush prompt")?;
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read terminal input")?;
    Ok(input.trim().to_owned())
}

fn prompt_default(label: &str, current: &str) -> Result<String> {
    print!("{label} [{current}]> ");
    io::stdout().flush().context("failed to flush prompt")?;
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read terminal input")?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(current.to_owned())
    } else {
        Ok(trimmed.to_owned())
    }
}

fn prompt_u64_default(label: &str, current: u64) -> Result<u64> {
    loop {
        let value = prompt_default(label, &current.to_string())?;
        match value.parse() {
            Ok(parsed) => return Ok(parsed),
            Err(error) => {
                pause(&format!("invalid number: {error}"))?;
            }
        }
    }
}

fn pause(message: &str) -> Result<()> {
    println!();
    println!("{message}");
    println!("Press Enter to continue.");
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read terminal input")?;
    Ok(())
}

fn clear_screen() -> Result<()> {
    print!("\x1b[2J\x1b[H");
    io::stdout().flush().context("failed to flush terminal")
}
