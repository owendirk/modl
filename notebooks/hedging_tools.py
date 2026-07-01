from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from sklearn.covariance import LedoitWolf


@dataclass(frozen=True)
class HedgeSolveResult:
    hedge_trade: pd.Series
    residual: pd.Series
    diagnostics: pd.DataFrame


def first_available(frame: pd.DataFrame, columns: list[str], min_obs: int = 3) -> pd.Series | None:
    for column in columns:
        if column in frame and frame[column].notna().sum() >= min_obs:
            return frame[column].astype(float)
    return None


def build_hedge_universe(
    frame: pd.DataFrame,
    reference: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    specs = [
        {
            "instrument": "bitfinex_spot",
            "venue": "bitfinex",
            "kind": "spot",
            "price_cols": ["book_mid_bitfinex_updates", "trade_vwap_bitfinex"],
            "spread_col": "book_spread_bps_bitfinex_updates",
            "volume_col": "trade_volume_bitfinex",
            "funding_col": None,
        },
        {
            "instrument": "hibachi_perp",
            "venue": "hibachi",
            "kind": "perp",
            "price_cols": ["book_mid_hibachi", "trade_vwap_hibachi"],
            "spread_col": "book_spread_bps_hibachi",
            "volume_col": "trade_volume_hibachi",
            "funding_col": "estimated_funding_rate",
        },
        {
            "instrument": "hyperliquid_ubtc",
            "venue": "hyperliquid",
            "kind": "spot_or_perp_proxy",
            "price_cols": ["book_mid_hyperliquid", "trade_vwap_hyperliquid"],
            "spread_col": "book_spread_bps_hyperliquid",
            "volume_col": "trade_volume_hyperliquid",
            "funding_col": None,
        },
    ]

    prices: dict[str, pd.Series] = {}
    rows = []
    for spec in specs:
        price = first_available(frame, spec["price_cols"])
        if price is None:
            continue
        price_source = next(
            column
            for column in spec["price_cols"]
            if column in frame and frame[column].notna().sum() >= 3
        )
        prices[spec["instrument"]] = price
        spread = frame.get(spec["spread_col"], pd.Series(index=frame.index, dtype=float)).astype(float)
        volume = frame.get(spec["volume_col"], pd.Series(index=frame.index, dtype=float)).astype(float)
        funding = (
            frame.get(spec["funding_col"], pd.Series(index=frame.index, dtype=float)).astype(float)
            if spec["funding_col"]
            else pd.Series(index=frame.index, dtype=float)
        )
        rows.append(
            {
                "instrument": spec["instrument"],
                "venue": spec["venue"],
                "kind": spec["kind"],
                "price_source": price_source,
                "spread_bps_median": spread.replace([np.inf, -np.inf], np.nan).median(),
                "volume_median": volume.replace([np.inf, -np.inf], np.nan).median(),
                "funding_rate_median": funding.replace([np.inf, -np.inf], np.nan).median(),
            }
        )

    if "basis_fut_perp" in frame and reference.notna().sum() >= 3:
        basis = frame["basis_fut_perp"].astype(float).reindex(reference.index).ffill()
        deribit_price = reference.astype(float).reindex(frame.index).ffill() * (1.0 + basis / 10_000.0)
        if deribit_price.notna().sum() >= 3:
            prices["deribit_perp_proxy"] = deribit_price
            rows.append(
                {
                    "instrument": "deribit_perp_proxy",
                    "venue": "deribit",
                    "kind": "perp_proxy",
                    "price_source": "reference_price * (1 + basis_fut_perp / 10000)",
                    "spread_bps_median": np.nan,
                    "volume_median": np.nan,
                    "funding_rate_median": np.nan,
                }
            )

    price_panel = pd.DataFrame(prices).sort_index().ffill()
    universe = pd.DataFrame(rows).set_index("instrument") if rows else pd.DataFrame()
    return price_panel, universe


def build_return_panel(prices: pd.DataFrame, min_obs: int = 5) -> pd.DataFrame:
    cleaned = prices.replace([np.inf, -np.inf], np.nan).ffill().dropna(how="all")
    returns = np.log(cleaned).diff().replace([np.inf, -np.inf], np.nan)
    keep = returns.notna().sum() >= min_obs
    returns = returns.loc[:, keep]
    return returns.dropna(how="any")


def shrink_covariance(returns: pd.DataFrame) -> pd.DataFrame:
    if returns.empty:
        return pd.DataFrame()
    clean = returns.replace([np.inf, -np.inf], np.nan).dropna()
    columns = clean.columns
    if clean.shape[0] >= max(4, clean.shape[1] + 1) and clean.shape[1] > 1:
        cov = LedoitWolf().fit(clean.to_numpy()).covariance_
    else:
        sample = clean.cov().fillna(0.0).to_numpy()
        diag = np.diag(np.diag(sample))
        cov = 0.50 * sample + 0.50 * diag
    cov = np.asarray(cov, dtype=float)
    cov = (cov + cov.T) / 2.0
    cov += np.eye(cov.shape[0]) * 1e-12
    return pd.DataFrame(cov, index=columns, columns=columns)


def pca_from_cov(cov: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if cov.empty:
        return pd.DataFrame(), pd.DataFrame()
    values, vectors = np.linalg.eigh(cov.to_numpy())
    order = np.argsort(values)[::-1]
    values = values[order]
    vectors = vectors[:, order]
    explained = values / values.sum() if values.sum() > 0 else np.zeros_like(values)
    components = pd.DataFrame(vectors, index=cov.index, columns=[f"pc{i + 1}" for i in range(len(values))])
    summary = pd.DataFrame(
        {
            "eigenvalue": values,
            "explained_variance": explained,
            "cumulative_explained": np.cumsum(explained),
        },
        index=components.columns,
    )
    return summary, components


def pair_spread_betas(columns: list[str]) -> pd.DataFrame:
    betas = []
    names = []
    for i, left in enumerate(columns):
        for right in columns[i + 1 :]:
            beta = pd.Series(0.0, index=columns)
            beta[left] = 1.0
            beta[right] = -1.0
            betas.append(beta)
            names.append(f"{left}_minus_{right}")
    if not betas:
        return pd.DataFrame(index=columns)
    return pd.DataFrame(betas, index=names).T


def reduce_by_spread_vectors(
    inventory: pd.Series,
    cov: pd.DataFrame,
    ridge: float = 1e-6,
) -> tuple[pd.Series, pd.Series]:
    if cov.empty or inventory.empty:
        return pd.Series(dtype=float), inventory.copy()
    betas = pair_spread_betas(list(cov.columns))
    if betas.empty:
        return pd.Series(dtype=float), inventory.copy()
    sigma = cov.reindex(index=inventory.index, columns=inventory.index).to_numpy(dtype=float)
    b = betas.reindex(index=inventory.index).to_numpy(dtype=float)
    q = inventory.to_numpy(dtype=float)
    lhs = b.T @ sigma @ b + np.eye(b.shape[1]) * ridge
    rhs = b.T @ sigma @ q
    weights = np.linalg.solve(lhs, rhs)
    hedgeable = b @ weights
    residual = q - hedgeable
    return pd.Series(weights, index=betas.columns, name="spread_weight"), pd.Series(
        residual,
        index=inventory.index,
        name="residual_btc",
    )


def portfolio_variance(exposure: pd.Series, cov: pd.DataFrame) -> float:
    if cov.empty:
        return np.nan
    aligned = exposure.reindex(cov.index).fillna(0.0).to_numpy(dtype=float)
    sigma = cov.to_numpy(dtype=float)
    return float(aligned @ sigma @ aligned)


def latest_or_median(series: pd.Series, fallback: float = np.nan) -> float:
    clean = pd.Series(series).replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return fallback
    return float(clean.tail(min(12, len(clean))).median())


def estimate_hedge_costs(
    frame: pd.DataFrame,
    universe: pd.DataFrame,
    horizon_minutes: int,
) -> pd.DataFrame:
    rows = []
    for instrument, row in universe.iterrows():
        spread_bps = row.get("spread_bps_median")
        if not np.isfinite(spread_bps):
            spread_bps = 2.0
        half_spread_bps = max(float(spread_bps) / 2.0, 0.0)

        funding_rate = row.get("funding_rate_median")
        funding_carry_bps = 0.0
        if np.isfinite(funding_rate):
            funding_carry_bps = abs(float(funding_rate)) * 10_000.0 * horizon_minutes / (8 * 60)

        basis_carry_bps = 0.0
        if instrument == "deribit_perp_proxy" and "basis_fut_perp" in frame:
            basis_carry_bps = abs(latest_or_median(frame["basis_fut_perp"], fallback=0.0)) * 0.10

        liquidity_penalty_bps = 0.0
        volume = row.get("volume_median")
        if np.isfinite(volume) and volume > 0:
            liquidity_penalty_bps = min(2.0, 1.0 / np.sqrt(float(volume)))
        elif row.get("kind") == "perp_proxy":
            liquidity_penalty_bps = 1.0

        total = half_spread_bps + funding_carry_bps + basis_carry_bps + liquidity_penalty_bps
        rows.append(
            {
                "instrument": instrument,
                "half_spread_bps": half_spread_bps,
                "funding_carry_bps": funding_carry_bps,
                "basis_penalty_bps": basis_carry_bps,
                "liquidity_penalty_bps": liquidity_penalty_bps,
                "total_cost_bps": total,
            }
        )
    return pd.DataFrame(rows).set_index("instrument").sort_values("total_cost_bps")


def solve_cost_aware_hedge(
    inventory: pd.Series,
    cov: pd.DataFrame,
    costs: pd.Series,
    max_abs_btc: float = 2.0,
    cost_aversion: float = 1.0,
) -> HedgeSolveResult:
    columns = list(cov.columns)
    q = inventory.reindex(columns).fillna(0.0).to_numpy(dtype=float)
    sigma = cov.reindex(index=columns, columns=columns).to_numpy(dtype=float)
    fallback_cost = costs.median() if len(costs) else 1.0
    cost = costs.reindex(columns).fillna(fallback_cost).to_numpy(dtype=float)

    def smooth_abs(x: np.ndarray) -> np.ndarray:
        return np.sqrt(x * x + 1e-8)

    def objective(x: np.ndarray) -> float:
        residual = q + x
        risk_bps2 = float(residual @ sigma @ residual) * 1e8
        trade_cost_bps = float(np.sum(cost * smooth_abs(x)))
        return risk_bps2 + cost_aversion * trade_cost_bps

    bounds = [(-max_abs_btc, max_abs_btc) for _ in columns]
    result = minimize(objective, x0=np.zeros(len(columns), dtype=float), method="L-BFGS-B", bounds=bounds)
    hedge = pd.Series(result.x, index=columns, name="hedge_trade_btc")
    residual = pd.Series(q + result.x, index=columns, name="residual_btc")
    diagnostics = pd.DataFrame(
        [
            {"metric": "success", "value": bool(result.success)},
            {"metric": "objective", "value": float(result.fun)},
            {"metric": "iterations", "value": int(result.nit)},
            {"metric": "message", "value": str(result.message)},
        ]
    )
    return HedgeSolveResult(hedge_trade=hedge, residual=residual, diagnostics=diagnostics)


def rolling_hedge_simulation(
    returns: pd.DataFrame,
    inventory: pd.Series,
    costs: pd.Series,
    window_bars: int,
    horizon_periods: int,
    max_abs_btc: float,
    cost_aversion: float,
) -> pd.DataFrame:
    if window_bars < 2:
        raise ValueError("window_bars must be at least 2")
    clean = returns.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.shape[0] <= window_bars:
        return pd.DataFrame()

    prev_hedge = pd.Series(0.0, index=clean.columns)
    rows = []
    for idx in range(window_bars, clean.shape[0]):
        timestamp = clean.index[idx]
        train = clean.iloc[idx - window_bars : idx]
        realized = clean.iloc[idx]
        cov_horizon = shrink_covariance(train) * horizon_periods
        solve = solve_cost_aware_hedge(
            inventory.reindex(cov_horizon.columns).fillna(0.0),
            cov_horizon,
            costs.reindex(cov_horizon.columns),
            max_abs_btc=max_abs_btc,
            cost_aversion=cost_aversion,
        )
        aligned_inventory = inventory.reindex(cov_horizon.columns).fillna(0.0)
        aligned_returns = realized.reindex(cov_horizon.columns).fillna(0.0)
        turnover = (solve.hedge_trade - prev_hedge.reindex(solve.hedge_trade.index).fillna(0.0)).abs()
        turnover_cost_bps = float((turnover * costs.reindex(turnover.index).fillna(costs.median())).sum())
        unhedged_pnl_bps = float(aligned_inventory @ aligned_returns * 10_000.0)
        hedged_pnl_bps = float(solve.residual @ aligned_returns * 10_000.0 - turnover_cost_bps)
        risk_before = portfolio_variance(aligned_inventory, cov_horizon)
        risk_after = portfolio_variance(solve.residual, cov_horizon)
        rows.append(
            {
                "timestamp": timestamp,
                "unhedged_pnl_bps": unhedged_pnl_bps,
                "hedged_pnl_bps_after_cost": hedged_pnl_bps,
                "turnover_cost_bps": turnover_cost_bps,
                "turnover_btc": float(turnover.sum()),
                "risk_before_vol_bps": np.sqrt(risk_before) * 10_000.0 if risk_before >= 0 else np.nan,
                "risk_after_vol_bps": np.sqrt(risk_after) * 10_000.0 if risk_after >= 0 else np.nan,
            }
        )
        prev_hedge = solve.hedge_trade

    return pd.DataFrame(rows).set_index("timestamp")
