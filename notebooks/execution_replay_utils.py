from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl


def snap_limit_price(side: str, raw_price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return float(raw_price)
    ticks = raw_price / tick_size
    if side == "bid":
        return float(np.floor(ticks) * tick_size)
    if side == "ask":
        return float(np.ceil(ticks) * tick_size)
    raise ValueError(f"unknown side: {side}")


def load_hibachi_l2_book(root: Path) -> pd.DataFrame:
    paths = sorted((root / "hibachi" / "btc_usdt-p" / "orderbook").glob("*.parquet"))
    if not paths:
        return pd.DataFrame(columns=["received_mts", "side", "price", "qty"])

    frames = [
        pl.scan_parquet(path).select(
            [
                pl.col("received_mts").cast(pl.Int64),
                pl.col("side").cast(pl.Utf8).str.to_lowercase().alias("side"),
                pl.col("price").cast(pl.Float64, strict=False).alias("price"),
                pl.col("quantity").cast(pl.Float64, strict=False).fill_null(0.0).alias("qty"),
            ]
        )
        for path in paths
    ]
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .filter(pl.col("received_mts").is_not_null() & pl.col("side").is_not_null() & pl.col("price").is_not_null())
        .sort(["received_mts", "side", "price"])
        .collect()
        .to_pandas()
    )


@dataclass(frozen=True)
class L2DepthLookup:
    snapshot_mts: np.ndarray
    starts: np.ndarray
    counts: np.ndarray
    side: np.ndarray
    price: np.ndarray
    qty: np.ndarray

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> "L2DepthLookup":
        if frame.empty:
            empty_int = np.array([], dtype=np.int64)
            empty_float = np.array([], dtype=float)
            empty_obj = np.array([], dtype=object)
            return cls(empty_int, empty_int, empty_int, empty_obj, empty_float, empty_float)

        ordered = frame.sort_values(["received_mts", "side", "price"]).reset_index(drop=True)
        ts = ordered["received_mts"].to_numpy(dtype=np.int64)
        snapshot_mts, starts, counts = np.unique(ts, return_index=True, return_counts=True)
        return cls(
            snapshot_mts=snapshot_mts,
            starts=starts.astype(np.int64),
            counts=counts.astype(np.int64),
            side=ordered["side"].astype(str).to_numpy(),
            price=ordered["price"].to_numpy(dtype=float),
            qty=ordered["qty"].fillna(0.0).to_numpy(dtype=float),
        )

    def queue_ahead(self, side: str, order_price: float, live_mts: int) -> dict[str, float | bool]:
        snapshot_idx = int(np.searchsorted(self.snapshot_mts, live_mts, side="right") - 1)
        if snapshot_idx < 0:
            return {
                "l2_snapshot_found": False,
                "l2_price_covered": False,
                "l2_snapshot_mts": np.nan,
                "l2_snapshot_age_ms": np.nan,
                "l2_queue_ahead_qty": np.nan,
                "l2_levels_ahead": 0,
                "l2_best_bid": np.nan,
                "l2_best_ask": np.nan,
                "l2_min_bid": np.nan,
                "l2_max_ask": np.nan,
            }

        start = self.starts[snapshot_idx]
        end = start + self.counts[snapshot_idx]
        side_slice = self.side[start:end]
        price_slice = self.price[start:end]
        qty_slice = self.qty[start:end]

        bid_mask = side_slice == "bid"
        ask_mask = side_slice == "ask"
        bid_prices = price_slice[bid_mask]
        ask_prices = price_slice[ask_mask]
        bid_qty = qty_slice[bid_mask]
        ask_qty = qty_slice[ask_mask]

        best_bid = float(np.nanmax(bid_prices)) if len(bid_prices) else np.nan
        best_ask = float(np.nanmin(ask_prices)) if len(ask_prices) else np.nan
        min_bid = float(np.nanmin(bid_prices)) if len(bid_prices) else np.nan
        max_ask = float(np.nanmax(ask_prices)) if len(ask_prices) else np.nan

        eps = 1e-9
        if side == "bid" and len(bid_prices):
            ahead = bid_prices >= order_price - eps
            covered = bool(order_price >= min_bid - eps)
            queue = float(np.nansum(np.where(ahead, bid_qty, 0.0)))
            levels = int(ahead.sum())
        elif side == "ask" and len(ask_prices):
            ahead = ask_prices <= order_price + eps
            covered = bool(order_price <= max_ask + eps)
            queue = float(np.nansum(np.where(ahead, ask_qty, 0.0)))
            levels = int(ahead.sum())
        else:
            covered = False
            queue = np.nan
            levels = 0

        snapshot_mts = int(self.snapshot_mts[snapshot_idx])
        return {
            "l2_snapshot_found": True,
            "l2_price_covered": covered,
            "l2_snapshot_mts": snapshot_mts,
            "l2_snapshot_age_ms": int(live_mts - snapshot_mts),
            "l2_queue_ahead_qty": queue,
            "l2_levels_ahead": levels,
            "l2_best_bid": best_bid,
            "l2_best_ask": best_ask,
            "l2_min_bid": min_bid,
            "l2_max_ask": max_ask,
        }
