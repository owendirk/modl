from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
from scipy.special import logsumexp
from scipy.stats import norm, skewnorm

MINUTES_PER_YEAR = 365 * 24 * 60
DEFAULT_BAR_MINUTES = 5
DEFAULT_HAWKES_BSI_KAPPAS = (0.1, 0.3, 0.5)
FEATURE_DATASET_KEYS = frozenset(
    [
        "bitfinex/tbtcusd/book_l25",
        "bitfinex/tbtcusd/trades",
        "deribit/btc/incremental_ticker",
        "deribit/btc/instrument_state",
        "deribit/btc/instruments",
        "deribit/btc/trades",
        "hibachi/btc_usdt-p/funding",
        "hibachi/btc_usdt-p/orderbook",
        "hibachi/btc_usdt-p/prices",
        "hibachi/btc_usdt-p/quotes",
        "hibachi/btc_usdt-p/trades",
        "hyperliquid/ubtc_usdc/book",
        "hyperliquid/ubtc_usdc/control",
        "hyperliquid/ubtc_usdc/trades",
    ]
)


@dataclass(frozen=True)
class AdvancedFeatureResult:
    feature_matrix: pd.DataFrame
    targets: pd.DataFrame
    model_table: pd.DataFrame
    ic_table: pd.DataFrame
    term_structure: pd.DataFrame
    rv_features: pd.DataFrame
    correlation_matrix: pd.DataFrame


@dataclass(frozen=True)
class FeatureSet:
    feature_matrix: pd.DataFrame
    base_feature_matrix: pd.DataFrame
    trade_features: pl.DataFrame
    book_features: pl.DataFrame
    deribit_option_features: pd.DataFrame
    term_structure: pd.DataFrame
    option_smile: pd.DataFrame
    futures_basis: pd.DataFrame
    funding_features: pd.DataFrame
    rv_features: pd.DataFrame
    hawkes_bsi_features: pd.DataFrame
    reference_price: pd.Series


@dataclass(frozen=True)
class FirTargetSpec:
    entry_minutes: int
    wait_minutes: int
    exit_minutes: int
    name: str | None = None


@dataclass(frozen=True)
class StateEmission:
    state: int
    distribution: str
    params: tuple[float, ...]
    n: int


@dataclass(frozen=True)
class HMMFitResult:
    labels: pd.DataFrame
    returns: pd.Series
    emissions: tuple[StateEmission, ...]
    transition_matrix: pd.DataFrame
    start_probability: pd.Series
    state_probability: pd.DataFrame
    states: pd.Series
    diagnostics: dict[str, pd.DataFrame]


def scan_dataset(datasets: dict[str, str], key: str) -> pl.LazyFrame:
    return pl.scan_parquet(datasets[key])


def ts_ms(column: str) -> pl.Expr:
    return pl.from_epoch(pl.col(column), time_unit="ms")


def float_col(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False)


def bar_interval(bar_minutes: int) -> str:
    if bar_minutes <= 0:
        raise ValueError("bar_minutes must be positive")
    return f"{bar_minutes}m"


def minutes_to_periods(minutes: int, bar_minutes: int) -> int:
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    return max(1, int(round(minutes / bar_minutes)))


def discover_datasets(root: Path, date_tag: str) -> dict[str, str]:
    datasets = {}
    for path in sorted(root.rglob(f"*_{date_tag}.parquet")):
        rel = path.relative_to(root)
        if len(rel.parts) < 4:
            continue
        exchange, symbol, dataset = rel.parts[:3]
        datasets[f"{exchange}/{symbol}/{dataset}"] = str(path)
    return datasets


def available_dataset_dates(root: Path) -> dict[str, set[str]]:
    dates: dict[str, set[str]] = {}
    for path in sorted(root.rglob("*.parquet")):
        rel = path.relative_to(root)
        if len(rel.parts) < 4:
            continue
        stem = path.stem
        try:
            date_tag = stem.rsplit("_", 1)[1]
            date = datetime.strptime(date_tag, "%y-%m-%d").strftime("%Y-%m-%d")
        except (IndexError, ValueError):
            continue
        exchange, symbol, dataset = rel.parts[:3]
        dates.setdefault(date, set()).add(f"{exchange}/{symbol}/{dataset}")
    return dates


def latest_dataset_date(root: Path, required_keys: set[str] | frozenset[str] | None = None) -> str:
    dates = available_dataset_dates(root)
    if not dates:
        raise FileNotFoundError(f"No normalized Parquet files found under {root}")
    for date in sorted(dates, reverse=True):
        if required_keys is None or set(required_keys).issubset(dates[date]):
            return date
    raise FileNotFoundError(f"No normalized Parquet date under {root} has the required dataset set")


def latest_feature_date(root: Path) -> str:
    return latest_dataset_date(root, FEATURE_DATASET_KEYS)


def empty_frame(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame({column: [] for column in columns})


def signed_qty_expr(side_col: str = "side", qty_col: str = "qty") -> pl.Expr:
    return (
        pl.when(pl.col(side_col) == "buy")
        .then(pl.col(qty_col))
        .when(pl.col(side_col) == "sell")
        .then(-pl.col(qty_col))
        .otherwise(0.0)
    )


def minute_trade_features(
    datasets: dict[str, str],
    venue: str,
    key: str,
    ts_col: str,
    price_col: str,
    qty_col: str,
    side_col: str = "side",
    ts_unit: str = "ms",
    extra_filter: pl.Expr | None = None,
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pl.DataFrame:
    if key not in datasets:
        return empty_frame(
            [
                "minute",
                "venue",
                "trade_count",
                "volume",
                "notional",
                "signed_volume",
                "buy_count",
                "sell_count",
                "vwap",
                "flow_imbalance",
            ]
        )

    lf = scan_dataset(datasets, key)
    if extra_filter is not None:
        lf = lf.filter(extra_filter)

    return (
        lf.with_columns(
            [
                pl.from_epoch(pl.col(ts_col), time_unit=ts_unit).alias("ts"),
                float_col(price_col).alias("price_f"),
                float_col(qty_col).alias("qty"),
                pl.col(side_col).cast(pl.String).str.to_lowercase().alias("side_norm"),
            ]
        )
        .filter(pl.col("ts").is_not_null() & pl.col("price_f").is_not_null() & pl.col("qty").is_not_null())
        .with_columns(
            [
                (pl.col("price_f") * pl.col("qty")).alias("notional"),
                signed_qty_expr("side_norm", "qty").alias("signed_qty"),
                (pl.col("side_norm") == "buy").cast(pl.Int64).alias("buy_count"),
                (pl.col("side_norm") == "sell").cast(pl.Int64).alias("sell_count"),
            ]
        )
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes))
        .agg(
            [
                pl.len().alias("trade_count"),
                pl.col("qty").sum().alias("volume"),
                pl.col("notional").sum().alias("notional"),
                pl.col("signed_qty").sum().alias("signed_volume"),
                pl.col("buy_count").sum().alias("buy_count"),
                pl.col("sell_count").sum().alias("sell_count"),
            ]
        )
        .with_columns(
            [
                pl.lit(venue).alias("venue"),
                (pl.col("notional") / pl.col("volume")).alias("vwap"),
                (pl.col("signed_volume") / pl.col("volume")).alias("flow_imbalance"),
            ]
        )
        .rename({"ts": "minute"})
        .collect()
    )


def build_trade_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pl.DataFrame:
    frames = [
        minute_trade_features(
            datasets,
            "bitfinex",
            "bitfinex/tbtcusd/trades",
            ts_col="trade_mts",
            price_col="price",
            qty_col="amount_abs",
            extra_filter=pl.col("is_final"),
            bar_minutes=bar_minutes,
        ),
        minute_trade_features(
            datasets,
            "hibachi",
            "hibachi/btc_usdt-p/trades",
            ts_col="trade_timestamp",
            price_col="price",
            qty_col="quantity",
            side_col="taker_side",
            ts_unit="s",
            bar_minutes=bar_minutes,
        ),
        minute_trade_features(
            datasets,
            "hyperliquid",
            "hyperliquid/ubtc_usdc/trades",
            ts_col="trade_timestamp",
            price_col="price",
            qty_col="size",
            bar_minutes=bar_minutes,
        ),
    ]
    frames = [frame for frame in frames if frame.height > 0]
    if not frames:
        return empty_frame(["minute", "venue"])
    return pl.concat(frames, how="diagonal_relaxed").sort(["venue", "minute"])


def hibachi_quote_minute_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pl.DataFrame:
    key = "hibachi/btc_usdt-p/quotes"
    if key not in datasets:
        return empty_frame(["minute", "venue", "mid", "spread", "spread_bps", "top_imbalance", "quote_count"])

    return (
        scan_dataset(datasets, key)
        .with_columns(
            [
                ts_ms("received_mts").alias("ts"),
                float_col("bid_price").alias("bid"),
                float_col("ask_price").alias("ask"),
                float_col("bid_size").alias("bid_size_f"),
                float_col("ask_size").alias("ask_size_f"),
            ]
        )
        .filter(pl.col("bid").is_not_null() & pl.col("ask").is_not_null())
        .with_columns(
            [
                ((pl.col("bid") + pl.col("ask")) / 2).alias("mid"),
                (pl.col("ask") - pl.col("bid")).alias("spread"),
                ((pl.col("bid_size_f") - pl.col("ask_size_f")) / (pl.col("bid_size_f") + pl.col("ask_size_f"))).alias(
                    "top_imbalance"
                ),
            ]
        )
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes))
        .agg(
            [
                pl.col("mid").mean().alias("mid"),
                pl.col("spread").mean().alias("spread"),
                pl.col("top_imbalance").mean().alias("top_imbalance"),
                pl.len().alias("quote_count"),
            ]
        )
        .with_columns([pl.lit("hibachi").alias("venue"), (pl.col("spread") / pl.col("mid") * 10_000).alias("spread_bps")])
        .rename({"ts": "minute"})
        .collect()
    )


def book_minute_features(
    datasets: dict[str, str],
    venue: str,
    key: str,
    size_col: str,
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pl.DataFrame:
    if key not in datasets:
        return empty_frame(["minute", "venue", "mid", "spread", "spread_bps", "depth_imbalance", "level_rows"])

    return (
        scan_dataset(datasets, key)
        .with_columns([ts_ms("received_mts").alias("ts"), float_col("price").alias("price_f"), float_col(size_col).alias("size_f")])
        .filter(pl.col("price_f").is_not_null())
        .group_by(["received_mts", "ts"])
        .agg(
            [
                pl.col("price_f").filter(pl.col("side") == "bid").max().alias("bid"),
                pl.col("price_f").filter(pl.col("side") == "ask").min().alias("ask"),
                pl.col("size_f").filter(pl.col("side") == "bid").sum().alias("bid_depth"),
                pl.col("size_f").filter(pl.col("side") == "ask").sum().alias("ask_depth"),
                pl.len().alias("level_rows"),
            ]
        )
        .with_columns(
            [
                ((pl.col("bid") + pl.col("ask")) / 2).alias("mid"),
                (pl.col("ask") - pl.col("bid")).alias("spread"),
                ((pl.col("bid_depth") - pl.col("ask_depth")) / (pl.col("bid_depth") + pl.col("ask_depth"))).alias(
                    "depth_imbalance"
                ),
            ]
        )
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes))
        .agg(
            [
                pl.col("mid").mean().alias("mid"),
                pl.col("spread").mean().alias("spread"),
                pl.col("depth_imbalance").mean().alias("depth_imbalance"),
                pl.col("level_rows").sum().alias("level_rows"),
            ]
        )
        .with_columns([pl.lit(venue).alias("venue"), (pl.col("spread") / pl.col("mid") * 10_000).alias("spread_bps")])
        .rename({"ts": "minute"})
        .collect()
    )


def build_book_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pl.DataFrame:
    frames = [
        hibachi_quote_minute_features(datasets, bar_minutes=bar_minutes),
        book_minute_features(
            datasets,
            "hyperliquid",
            "hyperliquid/ubtc_usdc/book",
            size_col="size",
            bar_minutes=bar_minutes,
        ),
        book_minute_features(
            datasets,
            "bitfinex_updates",
            "bitfinex/tbtcusd/book_l25",
            size_col="amount_abs",
            bar_minutes=bar_minutes,
        ),
    ]
    frames = [frame for frame in frames if frame.height > 0]
    if not frames:
        return empty_frame(["minute", "venue"])
    return pl.concat(frames, how="diagonal_relaxed").sort(["venue", "minute"])


def build_deribit_option_minute_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    key = "deribit/btc/incremental_ticker"
    if key not in datasets:
        return pd.DataFrame()

    return (
        scan_dataset(datasets, key)
        .filter(pl.col("kind") == "option")
        .with_columns(
            [
                ts_ms("received_mts").alias("ts"),
                float_col("mark_iv").alias("mark_iv_f"),
                float_col("index_price").alias("index_price_f"),
                float_col("open_interest").alias("open_interest_f"),
            ]
        )
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes))
        .agg(
            [
                pl.len().alias("option_tick_count"),
                pl.col("instrument_name").n_unique().alias("active_options"),
                pl.col("mark_iv_f").median().alias("median_mark_iv"),
                pl.col("index_price_f").median().alias("median_index_price"),
                pl.col("open_interest_f").sum().alias("open_interest_sum"),
            ]
        )
        .rename({"ts": "minute"})
        .collect()
        .to_pandas()
        .set_index("minute")
    )


def build_hibachi_funding_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    key = "hibachi/btc_usdt-p/funding"
    if key not in datasets:
        return pd.DataFrame()

    return (
        scan_dataset(datasets, key)
        .with_columns([ts_ms("received_mts").alias("ts"), float_col("estimated_funding_rate").alias("estimated_funding_rate")])
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes))
        .agg([pl.col("estimated_funding_rate").last().alias("estimated_funding_rate"), pl.len().alias("funding_updates")])
        .rename({"ts": "minute"})
        .collect()
        .to_pandas()
        .set_index("minute")
    )


def build_reference_price(feature_matrix: pd.DataFrame) -> pd.Series:
    candidates = [
        "book_mid_hibachi",
        "book_mid_hyperliquid",
        "book_mid_bitfinex_updates",
        "trade_vwap_bitfinex",
        "trade_vwap_hyperliquid",
        "trade_vwap_hibachi",
        "median_index_price",
    ]
    available = [column for column in candidates if column in feature_matrix.columns]
    if not available:
        raise ValueError("no usable price column found for reference price")

    reference = feature_matrix[available[0]].copy()
    for column in available[1:]:
        reference = reference.combine_first(feature_matrix[column])
    return reference.astype(float).ffill()


def rolling_realized_vol(
    log_return: pd.Series,
    window_periods: int,
    bar_minutes: int,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    bars_per_year = MINUTES_PER_YEAR / bar_minutes
    sq = log_return.pow(2)
    rv_var = sq.rolling(window_periods, min_periods=window_periods).sum() / window_periods * bars_per_year

    abs_prod = log_return.abs() * log_return.shift(1).abs()
    bpv_var = (np.pi / 2.0) * (
        abs_prod.rolling(window_periods, min_periods=window_periods).sum() / max(window_periods - 1, 1)
    ) * bars_per_year

    jump_var = (rv_var - bpv_var).clip(lower=0)
    return np.sqrt(rv_var), np.sqrt(bpv_var), jump_var


def future_realized_vol(
    log_return: pd.Series,
    horizon_periods: int,
    bar_minutes: int,
) -> pd.Series:
    bars_per_year = MINUTES_PER_YEAR / bar_minutes
    future_sq_sum = (
        log_return.pow(2)
        .shift(-1)
        .iloc[::-1]
        .rolling(horizon_periods, min_periods=horizon_periods)
        .sum()
        .iloc[::-1]
    )
    future_var = future_sq_sum / horizon_periods * bars_per_year
    return np.sqrt(future_var)


def default_fir_target_specs(bar_minutes: int = DEFAULT_BAR_MINUTES) -> tuple[FirTargetSpec, ...]:
    if bar_minutes <= 0:
        raise ValueError("bar_minutes must be positive")
    return (
        FirTargetSpec(entry_minutes=bar_minutes, wait_minutes=0, exit_minutes=3 * bar_minutes),
        FirTargetSpec(entry_minutes=bar_minutes, wait_minutes=0, exit_minutes=6 * bar_minutes),
        FirTargetSpec(entry_minutes=bar_minutes, wait_minutes=0, exit_minutes=12 * bar_minutes),
        FirTargetSpec(entry_minutes=3 * bar_minutes, wait_minutes=3 * bar_minutes, exit_minutes=6 * bar_minutes),
        FirTargetSpec(entry_minutes=6 * bar_minutes, wait_minutes=6 * bar_minutes, exit_minutes=12 * bar_minutes),
    )


def fir_target_name(spec: FirTargetSpec) -> str:
    if spec.name:
        return spec.name
    return f"entry{spec.entry_minutes}m_wait{spec.wait_minutes}m_exit{spec.exit_minutes}m"


def make_fir_execution_weights(entry_periods: int, wait_periods: int, exit_periods: int) -> np.ndarray:
    if entry_periods <= 0:
        raise ValueError("entry_periods must be positive")
    if wait_periods < 0:
        raise ValueError("wait_periods cannot be negative")
    if exit_periods <= 0:
        raise ValueError("exit_periods must be positive")

    entry = np.full(entry_periods, -1.0 / entry_periods)
    wait = np.zeros(wait_periods)
    exit_ = np.full(exit_periods, 1.0 / exit_periods)
    weights = np.concatenate([entry, wait, exit_])
    weights[np.abs(weights) < 1e-15] = 0.0
    return weights


def fir_position_path(weights: np.ndarray) -> np.ndarray:
    weights = np.asarray(weights, dtype=float)
    return -np.cumsum(weights)


def future_weighted_log_return(
    log_price: pd.Series,
    weights: np.ndarray,
    entry_offset_periods: int = 1,
) -> pd.Series:
    if entry_offset_periods < 0:
        raise ValueError("entry_offset_periods cannot be negative")

    weights = np.asarray(weights, dtype=float)
    if weights.ndim != 1 or weights.size == 0:
        raise ValueError("weights must be a non-empty 1D array")
    if not np.isclose(weights.sum(), 0.0):
        raise ValueError("FIR return weights must sum to zero")

    values = pd.Series(log_price, copy=False).astype(float).to_numpy()
    out = np.full(values.shape[0], np.nan)
    future_values = values[entry_offset_periods:]
    if weights.size > future_values.shape[0]:
        return pd.Series(out, index=log_price.index)

    windows = np.lib.stride_tricks.sliding_window_view(future_values, weights.size)
    convolved = windows @ weights
    convolved[~np.isfinite(windows).all(axis=1)] = np.nan
    out[: convolved.shape[0]] = convolved
    return pd.Series(out, index=log_price.index)


def build_fir_execution_targets(
    reference_price: pd.Series,
    specs: tuple[FirTargetSpec, ...] | None = None,
    bar_minutes: int = DEFAULT_BAR_MINUTES,
    entry_offset_periods: int = 1,
) -> pd.DataFrame:
    if specs is None:
        specs = default_fir_target_specs(bar_minutes)

    log_price = np.log(reference_price.astype(float)).replace([np.inf, -np.inf], np.nan)
    out = pd.DataFrame(index=reference_price.index)
    for spec in specs:
        entry_periods = minutes_to_periods(spec.entry_minutes, bar_minutes)
        wait_periods = 0 if spec.wait_minutes == 0 else minutes_to_periods(spec.wait_minutes, bar_minutes)
        exit_periods = minutes_to_periods(spec.exit_minutes, bar_minutes)
        weights = make_fir_execution_weights(entry_periods, wait_periods, exit_periods)
        name = fir_target_name(spec)
        out[f"target_fir_return_{name}"] = future_weighted_log_return(
            log_price,
            weights,
            entry_offset_periods=entry_offset_periods,
        )
    return out


def build_realized_features(
    reference_price: pd.Series,
    horizons: tuple[int, ...],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    out = pd.DataFrame(index=reference_price.index)
    out["reference_price"] = reference_price
    out["log_price"] = np.log(reference_price)
    out[f"ret_{bar_minutes}m"] = out["log_price"].diff()

    lag_minutes = sorted({bar_minutes, 15, 30, 60})
    for lag in [lag for lag in lag_minutes if lag >= bar_minutes]:
        out[f"ret_{lag}m"] = out["log_price"].diff(minutes_to_periods(lag, bar_minutes))

    for window in horizons:
        window_periods = minutes_to_periods(window, bar_minutes)
        rv, bpv, jump_var = rolling_realized_vol(out[f"ret_{bar_minutes}m"], window_periods, bar_minutes)
        out[f"rv_{window}m"] = rv
        out[f"bpv_{window}m"] = bpv
        out[f"jump_var_{window}m"] = jump_var
        out[f"jump_share_{window}m"] = jump_var / out[f"rv_{window}m"].pow(2)

    if "rv_5m" in out.columns and "rv_30m" in out.columns:
        out["rv_5m_over_30m"] = out["rv_5m"] / out["rv_30m"]
    if "bpv_5m" in out.columns and "rv_5m" in out.columns:
        out["bpv_5m_over_rv_5m"] = out["bpv_5m"] / out["rv_5m"]

    return out


def build_term_structure_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    key = "deribit/btc/incremental_ticker"
    if key not in datasets:
        return pd.DataFrame()

    base = (
        scan_dataset(datasets, key)
        .filter(pl.col("kind") == "option")
        .with_columns(
            [
                ts_ms("received_mts").alias("ts"),
                ((pl.col("expiration_timestamp") - pl.col("received_mts")) / 86_400_000.0).alias(
                    "dte_days"
                ),
                float_col("mark_iv").alias("mark_iv_f"),
                float_col("strike").alias("strike_f"),
                float_col("index_price").alias("index_price_f"),
                float_col("open_interest").alias("open_interest_f"),
            ]
        )
        .filter(
            pl.col("ts").is_not_null()
            & pl.col("dte_days").is_not_null()
            & (pl.col("dte_days") > 0)
            & pl.col("mark_iv_f").is_not_null()
        )
        .with_columns(
            [
                (pl.col("strike_f") / pl.col("index_price_f") - 1.0).alias("moneyness"),
                pl.when(pl.col("dte_days") <= 7)
                .then(pl.lit("iv_0_7d"))
                .when(pl.col("dte_days") <= 30)
                .then(pl.lit("iv_7_30d"))
                .when(pl.col("dte_days") <= 90)
                .then(pl.lit("iv_30_90d"))
                .otherwise(pl.lit("iv_90d_plus"))
                .alias("tenor_bucket"),
            ]
        )
    )

    bucket = (
        base.filter(pl.col("moneyness").abs() <= 0.20)
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes), group_by="tenor_bucket")
        .agg(
            [
                pl.col("mark_iv_f").median().alias("median_iv"),
                pl.col("instrument_name").n_unique().alias("option_count"),
            ]
        )
        .collect()
        .to_pandas()
    )
    if bucket.empty:
        return pd.DataFrame()

    iv_wide = bucket.pivot(index="ts", columns="tenor_bucket", values="median_iv").sort_index()
    count_wide = bucket.pivot(index="ts", columns="tenor_bucket", values="option_count").sort_index()
    count_wide.columns = [f"{column}_count" for column in count_wide.columns]

    skew = (
        base.filter(pl.col("moneyness").abs() <= 0.25)
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes), group_by="option_type")
        .agg(pl.col("mark_iv_f").median().alias("median_iv"))
        .collect()
        .to_pandas()
    )
    if not skew.empty:
        skew_wide = skew.pivot(index="ts", columns="option_type", values="median_iv").sort_index()
        if "put" in skew_wide.columns and "call" in skew_wide.columns:
            skew_wide["put_call_iv_spread"] = skew_wide["put"] - skew_wide["call"]
        skew_wide = skew_wide.add_prefix("atm_")
    else:
        skew_wide = pd.DataFrame(index=iv_wide.index)

    term = pd.concat([iv_wide, count_wide, skew_wide], axis=1).sort_index()
    term.index.name = "minute"
    term = term.ffill()

    term["term_slope_30_90_minus_0_7"] = term.get("iv_30_90d") - term.get("iv_0_7d")
    term["term_slope_90_plus_minus_7_30"] = term.get("iv_90d_plus") - term.get("iv_7_30d")
    if {"iv_0_7d", "iv_30_90d", "iv_90d_plus"}.issubset(term.columns):
        term["term_curvature"] = term["iv_30_90d"] - (term["iv_0_7d"] + term["iv_90d_plus"]) / 2
    if "iv_7_30d" in term.columns:
        term["short_iv_decimal"] = term["iv_7_30d"] / 100.0
    elif "iv_0_7d" in term.columns:
        term["short_iv_decimal"] = term["iv_0_7d"] / 100.0
    else:
        term["short_iv_decimal"] = np.nan

    return term


def build_option_smile_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    key = "deribit/btc/incremental_ticker"
    if key not in datasets:
        return pd.DataFrame()

    base = (
        scan_dataset(datasets, key)
        .filter(pl.col("kind") == "option")
        .with_columns(
            [
                ts_ms("received_mts").alias("ts"),
                float_col("mark_iv").alias("mark_iv_f"),
                float_col("strike").alias("strike_f"),
                float_col("index_price").alias("index_price_f"),
            ]
        )
        .filter(pl.col("mark_iv_f").is_not_null() & pl.col("strike_f").is_not_null() & pl.col("index_price_f").is_not_null())
        .with_columns((pl.col("strike_f") / pl.col("index_price_f") - 1.0).alias("moneyness"))
        .with_columns(
            pl.when((pl.col("option_type") == "put") & (pl.col("moneyness") >= -0.25) & (pl.col("moneyness") <= -0.05))
            .then(pl.lit("otm_put_iv"))
            .when((pl.col("option_type") == "put") & (pl.col("moneyness").abs() <= 0.05))
            .then(pl.lit("atm_put_iv"))
            .when((pl.col("option_type") == "call") & (pl.col("moneyness").abs() <= 0.05))
            .then(pl.lit("atm_call_iv"))
            .when((pl.col("option_type") == "call") & (pl.col("moneyness") >= 0.05) & (pl.col("moneyness") <= 0.25))
            .then(pl.lit("otm_call_iv"))
            .otherwise(None)
            .alias("smile_bucket")
        )
        .filter(pl.col("smile_bucket").is_not_null())
    )

    smile_long = (
        base.sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes), group_by="smile_bucket")
        .agg([pl.col("mark_iv_f").median().alias("median_iv"), pl.len().alias("option_count")])
        .collect()
        .to_pandas()
    )
    if smile_long.empty:
        return pd.DataFrame()

    smile = smile_long.pivot(index="ts", columns="smile_bucket", values="median_iv").sort_index()
    counts = smile_long.pivot(index="ts", columns="smile_bucket", values="option_count").sort_index()
    counts.columns = [f"{column}_count" for column in counts.columns]
    out = pd.concat([smile, counts], axis=1).ffill()
    out.index.name = "minute"

    atm_mean = out[[column for column in ["atm_call_iv", "atm_put_iv"] if column in out.columns]].mean(axis=1)
    if "otm_put_iv" in out.columns:
        out["put_wing_minus_atm"] = out["otm_put_iv"] - atm_mean
    if "otm_call_iv" in out.columns:
        out["call_wing_minus_atm"] = out["otm_call_iv"] - atm_mean
    if {"otm_call_iv", "otm_put_iv"}.issubset(out.columns):
        out["risk_reversal_proxy"] = out["otm_call_iv"] - out["otm_put_iv"]
        out["butterfly_proxy"] = (out["otm_call_iv"] + out["otm_put_iv"]) / 2 - atm_mean

    return out.add_prefix("smile_")


def build_deribit_futures_basis_features(
    datasets: dict[str, str],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    key = "deribit/btc/incremental_ticker"
    if key not in datasets:
        return pd.DataFrame()

    basis_long = (
        scan_dataset(datasets, key)
        .filter(pl.col("kind") == "future")
        .with_columns(
            [
                ts_ms("received_mts").alias("ts"),
                ((pl.col("expiration_timestamp") - pl.col("received_mts")) / 86_400_000.0).alias("dte_days"),
                float_col("mark_price").alias("mark_price_f"),
                float_col("index_price").alias("index_price_f"),
            ]
        )
        .filter(pl.col("mark_price_f").is_not_null() & pl.col("index_price_f").is_not_null())
        .with_columns(
            [
                ((pl.col("mark_price_f") / pl.col("index_price_f") - 1.0) * 10_000).alias("basis_bps"),
                pl.when(pl.col("settlement_period") == "perpetual")
                .then(pl.lit("fut_perp"))
                .when(pl.col("dte_days") <= 7)
                .then(pl.lit("fut_0_7d"))
                .when(pl.col("dte_days") <= 30)
                .then(pl.lit("fut_7_30d"))
                .when(pl.col("dte_days") <= 90)
                .then(pl.lit("fut_30_90d"))
                .otherwise(pl.lit("fut_90d_plus"))
                .alias("future_bucket"),
            ]
        )
        .with_columns(
            pl.when(pl.col("future_bucket") == "fut_perp")
            .then(None)
            .otherwise(pl.col("basis_bps") * 365.0 / pl.col("dte_days"))
            .alias("annualized_basis_bps")
        )
        .sort("ts")
        .group_by_dynamic("ts", every=bar_interval(bar_minutes), group_by="future_bucket")
        .agg(
            [
                pl.col("basis_bps").median().alias("basis_bps"),
                pl.col("annualized_basis_bps").median().alias("annualized_basis_bps"),
                pl.col("instrument_name").n_unique().alias("future_count"),
            ]
        )
        .collect()
        .to_pandas()
    )
    if basis_long.empty:
        return pd.DataFrame()

    basis = basis_long.pivot(index="ts", columns="future_bucket", values="basis_bps").sort_index()
    ann = basis_long.pivot(index="ts", columns="future_bucket", values="annualized_basis_bps").sort_index()
    ann.columns = [f"{column}_annualized" for column in ann.columns]
    counts = basis_long.pivot(index="ts", columns="future_bucket", values="future_count").sort_index()
    counts.columns = [f"{column}_count" for column in counts.columns]

    out = pd.concat([basis, ann, counts], axis=1).ffill()
    out.index.name = "minute"
    if {"fut_30_90d", "fut_0_7d"}.issubset(out.columns):
        out["future_basis_slope_30_90_minus_0_7"] = out["fut_30_90d"] - out["fut_0_7d"]
    if {"fut_90d_plus", "fut_7_30d"}.issubset(out.columns):
        out["future_basis_slope_90_plus_minus_7_30"] = out["fut_90d_plus"] - out["fut_7_30d"]
    return out.add_prefix("basis_")


def add_cross_venue_features(feature_matrix: pd.DataFrame) -> pd.DataFrame:
    out = feature_matrix.copy()

    if {"book_mid_hibachi", "book_mid_hyperliquid"}.issubset(out.columns):
        out["mid_hibachi_minus_hyperliquid"] = out["book_mid_hibachi"] - out["book_mid_hyperliquid"]
        out["mid_hibachi_minus_hyperliquid_bps"] = (
            out["mid_hibachi_minus_hyperliquid"] / out["book_mid_hibachi"] * 10_000
        )

    if {"book_spread_bps_hibachi", "book_spread_bps_hyperliquid"}.issubset(out.columns):
        out["spread_bps_hibachi_minus_hyperliquid"] = (
            out["book_spread_bps_hibachi"] - out["book_spread_bps_hyperliquid"]
        )

    flow_cols = [column for column in out.columns if column.startswith("trade_flow_imbalance_")]
    if flow_cols:
        out["cross_venue_flow_imbalance_mean"] = out[flow_cols].mean(axis=1)
        out["cross_venue_flow_imbalance_std"] = out[flow_cols].std(axis=1)

    volume_cols = [column for column in out.columns if column.startswith("trade_volume_")]
    if volume_cols:
        out["cross_venue_volume_sum"] = out[volume_cols].sum(axis=1)

    spread_cols = [column for column in out.columns if column.startswith("book_spread_bps_")]
    if spread_cols:
        out["cross_venue_spread_bps_mean"] = out[spread_cols].mean(axis=1)
        out["cross_venue_spread_bps_std"] = out[spread_cols].std(axis=1)

    return out


def wide_by_venue(frame: pl.DataFrame, values: list[str], prefix: str) -> pd.DataFrame:
    pdf = frame.to_pandas()
    if pdf.empty:
        return pd.DataFrame()
    wide = pdf.pivot_table(index="minute", columns="venue", values=values, aggfunc="last")
    wide.columns = [f"{prefix}_{metric}_{venue}" for metric, venue in wide.columns]
    return wide.sort_index()


def hawkes_kappa_label(kappa: float) -> str:
    return f"k{int(round(kappa * 100)):03d}"


def hawkes_decay(values: pd.Series, kappa: float) -> pd.Series:
    alpha = float(np.exp(-kappa))
    state = 0.0
    out = np.empty(len(values), dtype=float)
    for idx, value in enumerate(pd.Series(values).fillna(0.0).to_numpy(dtype=float)):
        state = state * alpha + value
        out[idx] = state
    return pd.Series(out, index=values.index)


def build_hawkes_bsi_features(
    trade_features: pl.DataFrame,
    kappas: tuple[float, ...] = DEFAULT_HAWKES_BSI_KAPPAS,
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> pd.DataFrame:
    pdf = trade_features.to_pandas()
    if pdf.empty or not {"minute", "venue", "signed_volume", "volume"}.issubset(pdf.columns):
        return pd.DataFrame()

    pdf = pdf.copy()
    pdf["minute"] = pd.to_datetime(pdf["minute"])
    pdf = pdf.sort_values(["venue", "minute"])
    start = pdf["minute"].min()
    end = pdf["minute"].max()
    if pd.isna(start) or pd.isna(end):
        return pd.DataFrame()

    index = pd.date_range(start=start, end=end, freq=f"{bar_minutes}min")
    out = pd.DataFrame(index=index)
    norm_columns_by_kappa: dict[str, list[str]] = {}

    for venue, venue_df in pdf.groupby("venue", sort=True):
        bars = (
            venue_df.set_index("minute")[["signed_volume", "volume"]]
            .apply(pd.to_numeric, errors="coerce")
            .reindex(index)
            .fillna(0.0)
        )
        for kappa in kappas:
            label = hawkes_kappa_label(kappa)
            bsi = hawkes_decay(bars["signed_volume"], kappa)
            volume_memory = hawkes_decay(bars["volume"], kappa).replace(0.0, np.nan)
            normalized = bsi / volume_memory

            raw_col = f"hawkes_bsi_{label}_{venue}"
            norm_col = f"hawkes_bsi_norm_{label}_{venue}"
            out[raw_col] = bsi
            out[norm_col] = normalized
            out[f"hawkes_bsi_norm_diff_{label}_{venue}"] = normalized.diff()
            norm_columns_by_kappa.setdefault(label, []).append(norm_col)

    for label, columns in norm_columns_by_kappa.items():
        normalized = out[columns]
        out[f"hawkes_bsi_norm_mean_{label}"] = normalized.mean(axis=1)
        out[f"hawkes_bsi_norm_std_{label}"] = normalized.std(axis=1)
        out[f"hawkes_bsi_norm_range_{label}"] = normalized.max(axis=1) - normalized.min(axis=1)
        out[f"hawkes_bsi_norm_positive_share_{label}"] = (normalized > 0).mean(axis=1)

    out.index.name = "minute"
    return out


def add_rolling_transforms(
    frame: pd.DataFrame,
    windows: tuple[int, ...] = (5, 15, 30),
    bar_minutes: int = DEFAULT_BAR_MINUTES,
    max_columns: int = 48,
) -> pd.DataFrame:
    out = frame.copy()
    candidate_prefixes = (
        "trade_",
        "book_",
        "cross_",
        "mid_",
        "spread_",
        "rv_",
        "bpv_",
        "jump_",
        "iv_",
        "term_",
        "smile_",
        "basis_",
        "estimated_funding_rate",
    )
    numeric = out.select_dtypes(include=[np.number])
    candidates = [
        column
        for column in numeric.columns
        if column.startswith(candidate_prefixes) and not column.endswith("_count") and not column.endswith("_updates")
    ][:max_columns]

    additions = {}
    for column in candidates:
        additions[f"{column}_diff_{bar_minutes}m"] = numeric[column].diff()
        for window_minutes in windows:
            window_periods = minutes_to_periods(window_minutes, bar_minutes)
            min_periods = max(1, min(window_periods, 3))
            mean = numeric[column].rolling(window_periods, min_periods=min_periods).mean()
            std = numeric[column].rolling(window_periods, min_periods=min_periods).std()
            additions[f"{column}_mean_{window_minutes}m"] = mean
            additions[f"{column}_z_{window_minutes}m"] = (numeric[column] - mean) / std.replace(0, np.nan)
    if additions:
        out = pd.concat([out, pd.DataFrame(additions, index=out.index)], axis=1)
    return out.copy()


def build_feature_set(
    datasets: dict[str, str],
    horizons: tuple[int, ...] = (5, 15, 30),
    rolling_windows: tuple[int, ...] = (5, 15, 30),
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> FeatureSet:
    trade_features = build_trade_features(datasets, bar_minutes=bar_minutes)
    book_features = build_book_features(datasets, bar_minutes=bar_minutes)
    deribit_option_features = build_deribit_option_minute_features(datasets, bar_minutes=bar_minutes)
    funding_features = build_hibachi_funding_features(datasets, bar_minutes=bar_minutes)
    term_structure = build_term_structure_features(datasets, bar_minutes=bar_minutes)
    option_smile = build_option_smile_features(datasets, bar_minutes=bar_minutes)
    futures_basis = build_deribit_futures_basis_features(datasets, bar_minutes=bar_minutes)
    hawkes_bsi_features = build_hawkes_bsi_features(trade_features, bar_minutes=bar_minutes)

    trade_wide = wide_by_venue(trade_features, ["vwap", "volume", "trade_count", "flow_imbalance"], "trade")
    book_wide = wide_by_venue(book_features, ["mid", "spread_bps", "top_imbalance", "depth_imbalance"], "book")
    base_feature_matrix = pd.concat([trade_wide, book_wide, deribit_option_features, funding_features], axis=1).sort_index()
    base_feature_matrix = base_feature_matrix.loc[:, ~base_feature_matrix.columns.duplicated()]

    base_with_cross = add_cross_venue_features(base_feature_matrix)
    reference_price = build_reference_price(base_with_cross)
    rv_features = build_realized_features(reference_price, horizons, bar_minutes=bar_minutes)

    feature_matrix = pd.concat(
        [base_with_cross, rv_features, term_structure, option_smile, futures_basis, hawkes_bsi_features],
        axis=1,
    ).sort_index()
    feature_matrix = feature_matrix.loc[:, ~feature_matrix.columns.duplicated()]
    feature_matrix = add_rolling_transforms(
        feature_matrix,
        windows=rolling_windows,
        bar_minutes=bar_minutes,
    )

    return FeatureSet(
        feature_matrix=feature_matrix,
        base_feature_matrix=base_feature_matrix,
        trade_features=trade_features,
        book_features=book_features,
        deribit_option_features=deribit_option_features,
        term_structure=term_structure,
        option_smile=option_smile,
        futures_basis=futures_basis,
        funding_features=funding_features,
        rv_features=rv_features,
        hawkes_bsi_features=hawkes_bsi_features,
        reference_price=reference_price,
    )


def build_targets(
    reference_price: pd.Series,
    term_structure: pd.DataFrame,
    horizons: tuple[int, ...],
    bar_minutes: int = DEFAULT_BAR_MINUTES,
    fir_specs: tuple[FirTargetSpec, ...] | None = None,
    fir_entry_offset_periods: int = 1,
) -> pd.DataFrame:
    out = pd.DataFrame(index=reference_price.index)
    log_price = np.log(reference_price)
    log_return = log_price.diff()

    short_iv = term_structure.get("short_iv_decimal", pd.Series(index=out.index, dtype=float))
    short_iv = short_iv.reindex(out.index).ffill()

    for horizon in horizons:
        horizon_periods = minutes_to_periods(horizon, bar_minutes)
        out[f"target_future_return_{horizon}m"] = log_price.shift(-horizon_periods) - log_price
        future_rv = future_realized_vol(log_return, horizon_periods, bar_minutes)
        out[f"target_future_rv_{horizon}m"] = future_rv
        out[f"target_vrp_{horizon}m"] = short_iv.pow(2) - future_rv.pow(2)

    fir_targets = build_fir_execution_targets(
        reference_price,
        specs=fir_specs,
        bar_minutes=bar_minutes,
        entry_offset_periods=fir_entry_offset_periods,
    )
    return pd.concat([out, fir_targets], axis=1)


def compute_ic_table(
    model_table: pd.DataFrame,
    feature_columns: list[str],
    target_columns: list[str],
    min_obs: int = 20,
) -> pd.DataFrame:
    rows = []
    for target in target_columns:
        for feature in feature_columns:
            pair = model_table[[feature, target]].replace([np.inf, -np.inf], np.nan).dropna()
            if len(pair) < min_obs:
                continue
            if pair[feature].nunique() < 2 or pair[target].nunique() < 2:
                continue
            rows.append(
                {
                    "feature": feature,
                    "target": target,
                    "n": len(pair),
                    "spearman_ic": pair[feature].corr(pair[target], method="spearman"),
                    "pearson_corr": pair[feature].corr(pair[target], method="pearson"),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["feature", "target", "n", "spearman_ic", "pearson_corr", "abs_ic"])
    out = pd.DataFrame(rows)
    out["abs_ic"] = out["spearman_ic"].abs()
    return out.sort_values(["target", "abs_ic"], ascending=[True, False]).reset_index(drop=True)


def _apply_label(labels: np.ndarray, start: int, end: int, direction: float) -> None:
    if labels.size == 0:
        return
    start = max(0, start)
    end = min(labels.size - 1, end)
    if end >= start:
        labels[start : end + 1] = direction


def _amplitude_label_pass(cum_bps: np.ndarray, minamp_bps: float, inactive_bars: int) -> np.ndarray:
    labels = np.zeros(cum_bps.shape[0], dtype=float)
    if cum_bps.size == 0:
        return labels

    start = 0
    cursor = 0
    idx_min = 0
    idx_max = 0
    val_min = cum_bps[0]
    val_max = cum_bps[0]

    while cursor < cum_bps.size:
        value = cum_bps[cursor]
        amplitude = val_max - val_min

        if amplitude >= minamp_bps and idx_min > idx_max and (value - val_min) >= minamp_bps:
            _apply_label(labels, start, idx_max - 1, 0.0)
            _apply_label(labels, idx_max, idx_min, -1.0)
            start = idx_min
            idx_max = cursor
            val_max = value
        elif amplitude >= minamp_bps and idx_max > idx_min and (val_max - value) >= minamp_bps:
            _apply_label(labels, start, idx_min - 1, 0.0)
            _apply_label(labels, idx_min, idx_max, 1.0)
            start = idx_max
            idx_min = cursor
            val_min = value
        elif idx_max > idx_min and (cursor - idx_max) >= inactive_bars and value <= val_max:
            if amplitude >= minamp_bps:
                _apply_label(labels, start, idx_min - 1, 0.0)
                _apply_label(labels, idx_min, idx_max, 1.0)
                _apply_label(labels, idx_max + 1, cursor, 0.0)
            else:
                _apply_label(labels, start, cursor, 0.0)
            start = cursor
            idx_max = cursor
            idx_min = cursor
            val_max = value
            val_min = value
        elif idx_min > idx_max and (cursor - idx_min) >= inactive_bars and value >= val_min:
            if amplitude >= minamp_bps:
                _apply_label(labels, start, idx_max - 1, 0.0)
                _apply_label(labels, idx_max, idx_min, -1.0)
                _apply_label(labels, idx_min + 1, cursor, 0.0)
            else:
                _apply_label(labels, start, cursor, 0.0)
            start = cursor
            idx_max = cursor
            idx_min = cursor
            val_max = value
            val_min = value

        if value >= val_max:
            idx_max = cursor
            val_max = value
        if value <= val_min:
            idx_min = cursor
            val_min = value

        cursor += 1

    amplitude = val_max - val_min
    if amplitude >= minamp_bps and idx_min > idx_max:
        _apply_label(labels, start, idx_max - 1, 0.0)
        _apply_label(labels, idx_max, idx_min, -1.0)
        _apply_label(labels, idx_min + 1, cursor - 1, 0.0)
    elif amplitude >= minamp_bps and idx_max > idx_min:
        _apply_label(labels, start, idx_min - 1, 0.0)
        _apply_label(labels, idx_min, idx_max, 1.0)
        _apply_label(labels, idx_max + 1, cursor - 1, 0.0)
    else:
        _apply_label(labels, start, cursor - 1, 0.0)

    return labels


def _amplitude_ols_filter(cum_bps: np.ndarray, labels: np.ndarray, minamp_bps: float) -> np.ndarray:
    labels = labels.copy()
    pos = 0
    n = labels.size

    while pos < n:
        direction = labels[pos]
        if direction == 0.0:
            pos += 1
            continue

        start = pos
        end = pos
        while end < n and labels[end] == direction:
            end += 1
        end -= 1

        max_fwd_idx = start
        max_back_idx = end
        max_fwd = 0.0
        max_back = 0.0

        exy = exx = ex = ey = 0.0
        for idx in range(start, end + 1):
            x = float(idx - start)
            y = float(cum_bps[idx])
            exy += x * y
            exx += x * x
            ex += x
            ey += y
            if x <= 0.0:
                continue
            denom = exx - ex * ex / (x + 1.0)
            if abs(denom) < 1e-15:
                continue
            beta = (exy - ex * ey / (x + 1.0)) / denom
            distance = direction * beta * x
            if distance > max_fwd:
                max_fwd = distance
                max_fwd_idx = idx

        exy = exx = ex = ey = 0.0
        for idx in range(end, start - 1, -1):
            x = float(end - idx)
            y = float(cum_bps[idx])
            exy += x * y
            exx += x * x
            ex += x
            ey += y
            if x <= 0.0:
                continue
            denom = exx - ex * ex / (x + 1.0)
            if abs(denom) < 1e-15:
                continue
            beta = (exy - ex * ey / (x + 1.0)) / denom
            distance = -direction * beta * x
            if distance > max_back:
                max_back = distance
                max_back_idx = idx

        if max_fwd < minamp_bps and max_back < minamp_bps:
            _apply_label(labels, start, end, 0.0)
        else:
            if max_fwd >= minamp_bps:
                _apply_label(labels, start, max_fwd_idx, direction)
                _apply_label(labels, max_fwd_idx + 1, max_back_idx - 1, 0.0)
            else:
                _apply_label(labels, start, max_back_idx, 0.0)

            if max_back >= minamp_bps:
                _apply_label(labels, max_back_idx, end, direction)
            else:
                _apply_label(labels, max(max_back_idx, max_fwd_idx + 1), end, 0.0)

        pos = end + 1

    return labels


def amplitude_based_labels(
    reference_price: pd.Series,
    minamp_bps: float = 100.0,
    inactive_bars: int = 10,
    apply_ols_filter: bool = True,
) -> pd.DataFrame:
    if minamp_bps <= 0:
        raise ValueError("minamp_bps must be positive")
    if inactive_bars <= 0:
        raise ValueError("inactive_bars must be positive")

    price = pd.Series(reference_price, copy=False).astype(float).ffill()
    first_valid = price.dropna()
    if first_valid.empty:
        return pd.DataFrame(index=price.index, columns=["price", "cum_bps", "label"])

    cum_bps = np.log(price / first_valid.iloc[0]) * 10_000.0
    finite = np.isfinite(cum_bps.to_numpy())
    labels = np.zeros(price.shape[0], dtype=float)
    if finite.any():
        finite_values = cum_bps.to_numpy()[finite]
        finite_labels = _amplitude_label_pass(finite_values, minamp_bps, inactive_bars)
        if apply_ols_filter:
            finite_labels = _amplitude_ols_filter(finite_values, finite_labels, minamp_bps)
        labels[finite] = finite_labels

    return pd.DataFrame(
        {
            "price": price,
            "cum_bps": cum_bps,
            "label": labels.astype(int),
        },
        index=price.index,
    )


def fit_transition_matrix(
    labels: pd.Series,
    state_order: tuple[int, ...] = (-1, 0, 1),
    smoothing: float = 1.0,
) -> pd.DataFrame:
    states = list(state_order)
    counts = pd.DataFrame(smoothing, index=states, columns=states, dtype=float)
    clean = pd.Series(labels).dropna().astype(int)
    for prev, current in zip(clean.iloc[:-1], clean.iloc[1:]):
        if prev in counts.index and current in counts.columns:
            counts.loc[prev, current] += 1.0
    return counts.div(counts.sum(axis=1), axis=0)


def fit_start_probability(
    labels: pd.Series,
    state_order: tuple[int, ...] = (-1, 0, 1),
    smoothing: float = 1.0,
) -> pd.Series:
    counts = pd.Series(smoothing, index=list(state_order), dtype=float)
    clean = pd.Series(labels).dropna().astype(int)
    if not clean.empty and clean.iloc[0] in counts.index:
        counts.loc[clean.iloc[0]] += 1.0
    return counts / counts.sum()


def _fallback_normal_params(values: pd.Series) -> tuple[float, float]:
    clean = pd.Series(values).replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    if clean.empty:
        return 0.0, 1e-6
    sigma = float(clean.std(ddof=0))
    if not np.isfinite(sigma) or sigma <= 0:
        sigma = max(abs(float(clean.mean())) * 0.1, 1e-6)
    return float(clean.mean()), sigma


def fit_state_emissions(
    returns: pd.Series,
    labels: pd.Series,
    state_order: tuple[int, ...] = (-1, 0, 1),
    min_samples: int = 8,
) -> tuple[StateEmission, ...]:
    aligned = pd.concat(
        [pd.Series(returns, name="ret"), pd.Series(labels, name="label")],
        axis=1,
    ).replace([np.inf, -np.inf], np.nan).dropna()
    if aligned.empty:
        overall = pd.Series([0.0])
    else:
        overall = aligned["ret"].astype(float)

    fallback_mu, fallback_sigma = _fallback_normal_params(overall)
    emissions = []
    for state in state_order:
        samples = aligned.loc[aligned["label"].astype(int) == state, "ret"].astype(float)
        if state == 0 or samples.shape[0] < min_samples:
            mu, sigma = _fallback_normal_params(samples if not samples.empty else overall)
            emissions.append(StateEmission(state=state, distribution="norm", params=(mu, sigma), n=int(samples.shape[0])))
            continue

        try:
            shape, loc, scale = skewnorm.fit(samples.to_numpy())
            if not all(np.isfinite([shape, loc, scale])) or scale <= 0:
                raise ValueError("invalid skew-normal fit")
            emissions.append(
                StateEmission(
                    state=state,
                    distribution="skewnorm",
                    params=(float(shape), float(loc), float(scale)),
                    n=int(samples.shape[0]),
                )
            )
        except Exception:
            mu = fallback_mu if samples.empty else float(samples.mean())
            sigma = fallback_sigma if samples.empty else float(samples.std(ddof=0))
            sigma = sigma if np.isfinite(sigma) and sigma > 0 else fallback_sigma
            emissions.append(StateEmission(state=state, distribution="norm", params=(mu, sigma), n=int(samples.shape[0])))

    return tuple(emissions)


def emission_parameter_table(emissions: tuple[StateEmission, ...]) -> pd.DataFrame:
    rows = []
    for emission in emissions:
        row = {"state": emission.state, "distribution": emission.distribution, "n": emission.n}
        if emission.distribution == "skewnorm":
            row.update({"shape": emission.params[0], "loc": emission.params[1], "scale": emission.params[2]})
        else:
            row.update({"mean": emission.params[0], "std": emission.params[1]})
        rows.append(row)
    return pd.DataFrame(rows)


def emission_log_likelihood(
    returns: pd.Series,
    emissions: tuple[StateEmission, ...],
) -> pd.DataFrame:
    values = pd.Series(returns, copy=False).astype(float)
    matrix = {}
    for emission in emissions:
        if emission.distribution == "skewnorm":
            ll = skewnorm.logpdf(values.to_numpy(), *emission.params)
        elif emission.distribution == "norm":
            ll = norm.logpdf(values.to_numpy(), *emission.params)
        else:
            raise ValueError(f"unsupported emission distribution: {emission.distribution}")
        ll = np.asarray(ll, dtype=float)
        ll[~np.isfinite(ll)] = np.nan
        matrix[emission.state] = ll
    return pd.DataFrame(matrix, index=values.index)


def hmm_filter_probabilities(
    returns: pd.Series,
    emissions: tuple[StateEmission, ...],
    transition_matrix: pd.DataFrame,
    start_probability: pd.Series,
) -> pd.DataFrame:
    states = [emission.state for emission in emissions]
    log_emit = emission_log_likelihood(returns, emissions).reindex(columns=states)
    log_emit = log_emit.fillna(0.0).to_numpy()
    trans = transition_matrix.reindex(index=states, columns=states).fillna(0.0).to_numpy(dtype=float)
    start = start_probability.reindex(states).fillna(0.0).to_numpy(dtype=float)

    trans = np.clip(trans, 1e-300, None)
    trans = trans / trans.sum(axis=1, keepdims=True)
    start = np.clip(start, 1e-300, None)
    start = start / start.sum()

    log_trans = np.log(trans)
    log_start = np.log(start)
    log_alpha = np.full(log_emit.shape, np.nan)

    for row in range(log_emit.shape[0]):
        if row == 0:
            predicted = log_start
        else:
            predicted = logsumexp(log_alpha[row - 1][:, None] + log_trans, axis=0)
        alpha = predicted + log_emit[row]
        normalizer = logsumexp(alpha)
        if np.isfinite(normalizer):
            log_alpha[row] = alpha - normalizer
        else:
            log_alpha[row] = -np.log(len(states))

    probabilities = np.exp(log_alpha)
    return pd.DataFrame(probabilities, index=pd.Series(returns).index, columns=[f"state_{state}" for state in states])


def hmm_state_from_probabilities(state_probability: pd.DataFrame) -> pd.Series:
    if state_probability.empty:
        return pd.Series(dtype="Int64")
    states = [int(column.removeprefix("state_")) for column in state_probability.columns]
    best = np.nanargmax(state_probability.to_numpy(), axis=1)
    return pd.Series([states[idx] for idx in best], index=state_probability.index, name="hmm_state")


def hmm_confusion_matrix(
    true_labels: pd.Series,
    predicted_states: pd.Series,
    state_order: tuple[int, ...] = (-1, 0, 1),
    normalize: bool = False,
) -> pd.DataFrame:
    pair = pd.concat(
        [pd.Series(true_labels, name="offline_label"), pd.Series(predicted_states, name="hmm_state")],
        axis=1,
    ).dropna()
    if pair.empty:
        return pd.DataFrame(index=list(state_order), columns=list(state_order)).fillna(0)
    confusion = pd.crosstab(pair["offline_label"].astype(int), pair["hmm_state"].astype(int))
    confusion = confusion.reindex(index=list(state_order), columns=list(state_order), fill_value=0)
    if normalize:
        row_sum = confusion.sum(axis=1).replace(0, np.nan)
        confusion = confusion.div(row_sum, axis=0)
    return confusion


def classification_report_table(
    true_labels: pd.Series,
    predicted_states: pd.Series,
    state_order: tuple[int, ...] = (-1, 0, 1),
) -> pd.DataFrame:
    confusion = hmm_confusion_matrix(true_labels, predicted_states, state_order=state_order, normalize=False)
    rows = []
    for state in state_order:
        tp = float(confusion.loc[state, state])
        fp = float(confusion[state].sum() - tp)
        fn = float(confusion.loc[state].sum() - tp)
        precision = tp / (tp + fp) if (tp + fp) else np.nan
        recall = tp / (tp + fn) if (tp + fn) else np.nan
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else np.nan
        rows.append(
            {
                "state": state,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "support": int(confusion.loc[state].sum()),
            }
        )
    return pd.DataFrame(rows)


def fit_amplitude_hmm_labeller(
    reference_price: pd.Series,
    minamp_bps: float = 100.0,
    inactive_bars: int = 10,
    train_fraction: float = 0.7,
    state_order: tuple[int, ...] = (-1, 0, 1),
    transition_smoothing: float = 1.0,
) -> HMMFitResult:
    if not 0.0 < train_fraction <= 1.0:
        raise ValueError("train_fraction must be in (0, 1]")

    labels = amplitude_based_labels(
        reference_price,
        minamp_bps=minamp_bps,
        inactive_bars=inactive_bars,
        apply_ols_filter=True,
    )
    returns = np.log(labels["price"]).diff().replace([np.inf, -np.inf], np.nan).rename("ret")
    aligned = pd.concat([returns, labels["label"]], axis=1).dropna()
    if aligned.empty:
        empty_prob = pd.DataFrame(columns=[f"state_{state}" for state in state_order])
        empty_states = pd.Series(dtype="Int64", name="hmm_state")
        empty_transition = pd.DataFrame(index=list(state_order), columns=list(state_order), dtype=float)
        empty_start = pd.Series(index=list(state_order), dtype=float)
        return HMMFitResult(
            labels=labels,
            returns=returns,
            emissions=tuple(),
            transition_matrix=empty_transition,
            start_probability=empty_start,
            state_probability=empty_prob,
            states=empty_states,
            diagnostics={},
        )

    train_len = max(1, int(np.floor(aligned.shape[0] * train_fraction)))
    train = aligned.iloc[:train_len]

    emissions = fit_state_emissions(train["ret"], train["label"], state_order=state_order)
    transition_matrix = fit_transition_matrix(
        train["label"],
        state_order=state_order,
        smoothing=transition_smoothing,
    )
    start_probability = fit_start_probability(
        train["label"],
        state_order=state_order,
        smoothing=transition_smoothing,
    )
    state_probability = hmm_filter_probabilities(aligned["ret"], emissions, transition_matrix, start_probability)
    states = hmm_state_from_probabilities(state_probability)

    train_index = train.index
    test_index = aligned.index.difference(train_index)
    diagnostics = {
        "emissions": emission_parameter_table(emissions),
        "label_counts": aligned["label"].value_counts().reindex(list(state_order), fill_value=0).rename("count").to_frame(),
        "train_label_counts": train["label"].value_counts().reindex(list(state_order), fill_value=0).rename("count").to_frame(),
        "confusion_all": hmm_confusion_matrix(aligned["label"], states, state_order=state_order, normalize=False),
        "confusion_all_normalized": hmm_confusion_matrix(aligned["label"], states, state_order=state_order, normalize=True),
        "classification_all": classification_report_table(aligned["label"], states, state_order=state_order),
    }
    if len(test_index) > 0:
        diagnostics["test_label_counts"] = (
            aligned.loc[test_index, "label"].value_counts().reindex(list(state_order), fill_value=0).rename("count").to_frame()
        )
        diagnostics["confusion_test"] = hmm_confusion_matrix(
            aligned.loc[test_index, "label"],
            states.loc[test_index],
            state_order=state_order,
            normalize=False,
        )
        diagnostics["classification_test"] = classification_report_table(
            aligned.loc[test_index, "label"],
            states.loc[test_index],
            state_order=state_order,
        )

    return HMMFitResult(
        labels=labels,
        returns=returns,
        emissions=emissions,
        transition_matrix=transition_matrix,
        start_probability=start_probability,
        state_probability=state_probability,
        states=states,
        diagnostics=diagnostics,
    )


def build_advanced_feature_result(
    feature_matrix: pd.DataFrame,
    datasets: dict[str, str],
    horizons: tuple[int, ...] = (5, 15, 30),
    min_ic_obs: int = 20,
    bar_minutes: int = DEFAULT_BAR_MINUTES,
) -> AdvancedFeatureResult:
    base = add_cross_venue_features(feature_matrix)
    reference_price = build_reference_price(base)
    rv_features = build_realized_features(reference_price, horizons, bar_minutes=bar_minutes)
    term_structure = build_term_structure_features(datasets, bar_minutes=bar_minutes)

    model_features = pd.concat([base, rv_features, term_structure], axis=1).sort_index()
    model_features = model_features.loc[:, ~model_features.columns.duplicated()]
    targets = build_targets(reference_price, term_structure, horizons, bar_minutes=bar_minutes)
    model_table = pd.concat([model_features, targets], axis=1).sort_index()

    target_columns = [column for column in targets.columns if column in model_table.columns]
    feature_columns = [
        column
        for column in model_features.select_dtypes(include=[np.number]).columns
        if not column.startswith("target_")
    ]
    ic_table = compute_ic_table(model_table, feature_columns, target_columns, min_obs=min_ic_obs)

    corr_columns = feature_columns + target_columns
    correlation_matrix = model_table[corr_columns].replace([np.inf, -np.inf], np.nan).corr(
        method="spearman"
    )

    return AdvancedFeatureResult(
        feature_matrix=model_features,
        targets=targets,
        model_table=model_table,
        ic_table=ic_table,
        term_structure=term_structure,
        rv_features=rv_features,
        correlation_matrix=correlation_matrix,
    )


def plot_term_structure(term_structure: pd.DataFrame) -> None:
    columns = [column for column in ["iv_0_7d", "iv_7_30d", "iv_30_90d", "iv_90d_plus"] if column in term_structure]
    if term_structure.empty or not columns:
        print("No term-structure rows to plot")
        return
    fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=True)
    term_structure[columns].plot(ax=axes[0], marker="o")
    axes[0].set_title("Deribit ATM-ish IV Term Structure")
    axes[0].set_ylabel("IV")
    slope_cols = [column for column in term_structure.columns if column.startswith("term_") or column.endswith("_iv_spread")]
    term_structure[slope_cols].plot(ax=axes[1], marker="o")
    axes[1].axhline(0, color="black", linewidth=1)
    axes[1].set_title("Term Structure Slopes / Curvature / Skew")
    fig.autofmt_xdate()
    plt.tight_layout()


def plot_realized_vol(rv_features: pd.DataFrame) -> None:
    columns = [column for column in rv_features.columns if column.startswith("rv_") or column.startswith("bpv_")]
    columns = [column for column in columns if not column.endswith("_over_30m") and not column.endswith("_over_rv_5m")]
    if rv_features.empty or not columns:
        print("No realized-vol rows to plot")
        return
    fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=True)
    rv_features[[column for column in columns if column.startswith("rv_")]].plot(ax=axes[0], marker="o")
    axes[0].set_title("Realized Volatility")
    rv_features[[column for column in columns if column.startswith("bpv_")]].plot(ax=axes[1], marker="o")
    axes[1].set_title("Bipower Variation Volatility")
    fig.autofmt_xdate()
    plt.tight_layout()


def plot_ic_heatmap(ic_table: pd.DataFrame, top_n: int = 40) -> None:
    if ic_table.empty:
        print("No IC rows to plot")
        return
    top_features = ic_table.groupby("feature")["abs_ic"].max().nlargest(top_n).index
    plot_data = (
        ic_table[ic_table["feature"].isin(top_features)]
        .pivot_table(index="feature", columns="target", values="spearman_ic", aggfunc="first")
        .fillna(0)
    )
    height = max(8, min(24, 0.35 * len(plot_data)))
    plt.figure(figsize=(18, height))
    sns.heatmap(plot_data, cmap="vlag", center=0, annot=False)
    plt.title("Spearman IC: Features vs Forward Targets")
    plt.tight_layout()


def plot_top_ic_bars(ic_table: pd.DataFrame, target: str, top_n: int = 20) -> None:
    subset = ic_table[ic_table["target"] == target].copy()
    if subset.empty:
        print(f"No IC rows for target: {target}")
        return
    subset = subset.reindex(subset["spearman_ic"].abs().sort_values(ascending=False).index).head(top_n)
    subset = subset.sort_values("spearman_ic")
    plt.figure(figsize=(12, max(6, 0.35 * len(subset))))
    colors = np.where(subset["spearman_ic"] >= 0, "#2f6fdd", "#cc4c4c")
    plt.barh(subset["feature"], subset["spearman_ic"], color=colors)
    plt.axvline(0, color="black", linewidth=1)
    plt.title(f"Top Spearman IC for {target}")
    plt.xlabel("Spearman IC")
    plt.tight_layout()
