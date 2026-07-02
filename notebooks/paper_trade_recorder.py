from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


EVENT_TYPES = {
    "decision",
    "replay_projection",
    "market_snapshot",
    "order_submit",
    "order_ack",
    "order_cancel_request",
    "order_cancel_ack",
    "order_fill",
    "order_reject",
    "heartbeat",
}

REQUIRED_EVENT_COLUMNS = [
    "event_id",
    "event_hash",
    "prev_hash",
    "run_id",
    "event_type",
    "event_mts",
    "ingest_mts",
    "source",
    "venue",
    "symbol",
    "strategy",
    "paper_order_id",
]


def utc_mts() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000)


def utc_date_from_mts(mts: int) -> str:
    return datetime.fromtimestamp(int(mts) / 1_000, tz=timezone.utc).date().isoformat()


def decimal_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        value = value.strip()
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return None
    try:
        return format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError):
        return str(value)


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


def canonical_event_json(event: dict[str, Any]) -> str:
    material = {}
    for key, value in event.items():
        if key == "event_hash":
            continue
        safe_value = json_safe(value)
        if safe_value is not None:
            material[key] = safe_value
    return json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def event_hash(event: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_event_json(event).encode("utf-8")).hexdigest()


def stable_order_id(row: pd.Series, run_id: str) -> str:
    fields = [
        run_id,
        row.get("policy"),
        row.get("date"),
        row.get("minute"),
        row.get("side"),
        row.get("quote_distance_bps"),
        row.get("latency_ms"),
        row.get("ttl_ms"),
        row.get("maker_rebate_bps"),
        row.get("decision_mts"),
    ]
    digest = hashlib.sha256("|".join("" if value is None else str(value) for value in fields).encode("utf-8")).hexdigest()
    return f"paper-{digest[:20]}"


def make_event_id(run_id: str, index: int, event_type: str, paper_order_id: str | None) -> str:
    material = f"{run_id}|{index:012d}|{event_type}|{paper_order_id or ''}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


@dataclass
class PaperTradeRecorder:
    root: Path
    run_id: str
    venue: str = "hibachi"
    symbol: str = "BTC/USDT-P"
    strategy: str = "bid_inventory_score"

    def __post_init__(self) -> None:
        self.root = Path(self.root).expanduser()
        self.path = self.root / f"run_id={self.run_id}" / "paper_events.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._event_index = 0
        self._prev_hash = "GENESIS"
        if self.path.exists():
            existing = read_event_log(self.path)
            if not existing.empty:
                self._event_index = len(existing)
                self._prev_hash = str(existing.iloc[-1]["event_hash"])

    def append(self, event_type: str, event_mts: int, paper_order_id: str | None = None, **fields: Any) -> dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unknown paper event_type: {event_type}")

        event = {
            "event_id": make_event_id(self.run_id, self._event_index, event_type, paper_order_id),
            "event_hash": None,
            "prev_hash": self._prev_hash,
            "run_id": self.run_id,
            "event_type": event_type,
            "event_mts": int(event_mts),
            "event_time": datetime.fromtimestamp(int(event_mts) / 1_000, tz=timezone.utc).isoformat(),
            "ingest_mts": utc_mts(),
            "source": fields.pop("source", "paper_recorder"),
            "venue": fields.pop("venue", self.venue),
            "symbol": fields.pop("symbol", self.symbol),
            "strategy": fields.pop("strategy", self.strategy),
            "paper_order_id": paper_order_id,
        }
        event.update({key: json_safe(value) for key, value in fields.items()})
        event["event_hash"] = event_hash(event)

        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, ensure_ascii=True) + "\n")

        self._event_index += 1
        self._prev_hash = str(event["event_hash"])
        return event


def read_event_log(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    frame = pd.DataFrame(rows)
    frame.attrs["raw_events"] = rows
    return frame


def validate_event_log(events: pd.DataFrame) -> pd.DataFrame:
    checks: list[dict[str, Any]] = []
    missing = [column for column in REQUIRED_EVENT_COLUMNS if column not in events.columns]
    checks.append({"check": "required_columns", "passed": not missing, "detail": ",".join(missing)})

    if events.empty:
        checks.append({"check": "non_empty", "passed": False, "detail": "no events"})
        return pd.DataFrame(checks)

    checks.append({"check": "event_id_unique", "passed": bool(events["event_id"].is_unique), "detail": ""})
    checks.append({"check": "known_event_types", "passed": bool(events["event_type"].isin(EVENT_TYPES).all()), "detail": ""})
    checks.append({"check": "monotonic_ingest_mts", "passed": bool(events["ingest_mts"].is_monotonic_increasing), "detail": ""})

    raw_events = events.attrs.get("raw_events")
    if not raw_events:
        raw_events = [row.dropna().to_dict() for _, row in events.iterrows()]

    previous_hash = "GENESIS"
    hash_chain_ok = True
    event_hash_ok = True
    for row_event in raw_events:
        if row_event.get("prev_hash") != previous_hash:
            hash_chain_ok = False
        expected_hash = event_hash(row_event)
        if row_event.get("event_hash") != expected_hash:
            event_hash_ok = False
        previous_hash = str(row_event.get("event_hash"))

    checks.append({"check": "prev_hash_chain", "passed": hash_chain_ok, "detail": ""})
    checks.append({"check": "event_hash_recomputes", "passed": event_hash_ok, "detail": ""})
    return pd.DataFrame(checks)


def lifecycle_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty or "paper_order_id" not in events.columns:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    order_events = events.dropna(subset=["paper_order_id"]).sort_values(["paper_order_id", "event_mts"])
    for paper_order_id, group in order_events.groupby("paper_order_id", sort=False):
        first_by_type = group.groupby("event_type")["event_mts"].min()
        if "fill_qty" in group.columns:
            fill_qty = group.loc[group["event_type"].eq("order_fill"), "fill_qty"]
        else:
            fill_qty = pd.Series(dtype=float)
        filled_qty = pd.to_numeric(fill_qty, errors="coerce").sum()
        remaining_qty = pd.to_numeric(group.get("remaining_qty", pd.Series(dtype=float)), errors="coerce").dropna()
        last_remaining = float(remaining_qty.iloc[-1]) if len(remaining_qty) else np.nan
        status = "open"
        if "order_reject" in first_by_type:
            status = "rejected"
        elif "order_fill" in first_by_type and filled_qty > 0 and (not np.isfinite(last_remaining) or last_remaining <= 0.0):
            status = "filled"
        elif "order_cancel_ack" in first_by_type and filled_qty > 0:
            status = "partial_cancelled"
        elif "order_cancel_ack" in first_by_type:
            status = "cancelled"
        elif "order_fill" in first_by_type and filled_qty > 0:
            status = "partial_open"

        row = {
            "paper_order_id": paper_order_id,
            "status": status,
            "events": int(len(group)),
            "decision_mts": first_by_type.get("decision", np.nan),
            "submit_mts": first_by_type.get("order_submit", np.nan),
            "ack_mts": first_by_type.get("order_ack", np.nan),
            "fill_mts": first_by_type.get("order_fill", np.nan),
            "cancel_request_mts": first_by_type.get("order_cancel_request", np.nan),
            "cancel_ack_mts": first_by_type.get("order_cancel_ack", np.nan),
            "filled_qty": float(filled_qty),
            "last_remaining_qty": last_remaining,
        }
        row["decision_to_submit_ms"] = row["submit_mts"] - row["decision_mts"]
        row["submit_to_ack_ms"] = row["ack_mts"] - row["submit_mts"]
        row["submit_to_fill_ms"] = row["fill_mts"] - row["submit_mts"]
        row["cancel_latency_ms"] = row["cancel_ack_mts"] - row["cancel_request_mts"]
        rows.append(row)

    return pd.DataFrame(rows)


def select_replay_orders(
    replay: pd.DataFrame,
    policy: str,
    latency_ms: int,
    ttl_ms: int,
    maker_rebate_bps: float,
    max_orders: int | None = None,
) -> pd.DataFrame:
    selected = replay[
        replay["policy"].eq(policy)
        & replay["latency_ms"].eq(latency_ms)
        & replay["ttl_ms"].eq(ttl_ms)
        & np.isclose(replay["maker_rebate_bps"].astype(float), float(maker_rebate_bps))
    ].copy()
    selected = selected.sort_values(["decision_mts", "side", "quote_distance_bps"]).reset_index(drop=True)
    if max_orders is not None:
        selected = selected.head(max_orders).copy()
    return selected


def build_order_plan(selected: pd.DataFrame, run_id: str, order_qty: float) -> pd.DataFrame:
    plan = selected.copy()
    plan["paper_order_id"] = [stable_order_id(row, run_id) for _, row in plan.iterrows()]
    plan["order_qty"] = float(order_qty)
    plan["order_qty_str"] = plan["order_qty"].map(decimal_string)
    plan["order_price_str"] = plan["order_price"].map(decimal_string)
    plan["paper_status_expected"] = np.where(plan["event_full_fill"], "filled", "cancelled")
    return plan


def record_replay_projection(
    recorder: PaperTradeRecorder,
    plan: pd.DataFrame,
    *,
    ack_latency_ms: int = 50,
    cancel_ack_latency_ms: int = 100,
) -> pd.DataFrame:
    for _, order in plan.sort_values(["decision_mts", "paper_order_id"]).iterrows():
        order_id = str(order["paper_order_id"])
        decision_mts = int(order["decision_mts"])
        live_mts = int(order["live_mts"])
        ttl_ms = int(order["ttl_ms"])
        cancel_request_mts = live_mts + ttl_ms
        cancel_ack_mts = cancel_request_mts + int(cancel_ack_latency_ms)
        order_qty = float(order["order_qty"])
        fill_qty = float(order["event_partial_qty"]) if np.isfinite(order["event_partial_qty"]) else 0.0
        remaining_after_fill = max(0.0, order_qty - fill_qty)

        recorder.append(
            "decision",
            decision_mts,
            order_id,
            source="quote_policy",
            policy=order["policy"],
            side=order["side"],
            order_qty=decimal_string(order_qty),
            order_price=order["order_price_str"],
            quote_distance_bps=float(order["quote_distance_bps"]),
            model_quote_score_bps=float(order["model_quote_score_bps"]),
            predicted_standalone_value_bps=float(order["predicted_standalone_value_bps"]),
            expected_event_ev_after_rebate_bps=float(order["event_value_after_rebate_bps"]),
            paper_ttl_ms=ttl_ms,
        )
        recorder.append(
            "replay_projection",
            decision_mts,
            order_id,
            source="historical_replay",
            event_full_fill=bool(order["event_full_fill"]),
            event_partial_qty=decimal_string(fill_qty),
            event_markout_bps=order.get("event_markout_bps"),
            event_value_after_rebate_bps=order.get("event_value_after_rebate_bps"),
            trade_through_full_fill_no_queue=bool(order["trade_through_full_fill_no_queue"]),
            l2_price_covered=bool(order["l2_price_covered"]),
        )
        recorder.append(
            "market_snapshot",
            live_mts,
            order_id,
            source="hibachi_l2",
            live_bid=decimal_string(order.get("live_bid")),
            live_ask=decimal_string(order.get("live_ask")),
            live_mid=decimal_string(order.get("live_mid")),
            l2_snapshot_mts=order.get("l2_snapshot_mts"),
            l2_snapshot_age_ms=order.get("l2_snapshot_age_ms"),
            l2_queue_ahead_qty=decimal_string(order.get("l2_queue_ahead_qty")),
            l2_price_covered=bool(order["l2_price_covered"]),
        )
        recorder.append(
            "order_submit",
            live_mts,
            order_id,
            source="paper_order_manager",
            side=order["side"],
            order_type="post_only_limit",
            order_qty=decimal_string(order_qty),
            order_price=order["order_price_str"],
            remaining_qty=decimal_string(order_qty),
        )
        recorder.append(
            "order_ack",
            live_mts + int(ack_latency_ms),
            order_id,
            source="paper_order_manager",
            ack_latency_ms=int(ack_latency_ms),
            venue_order_id=f"paper-venue-{order_id[-12:]}",
            remaining_qty=decimal_string(order_qty),
        )

        if bool(order["event_full_fill"]):
            fill_mts = int(order["event_fill_mts"])
            recorder.append(
                "order_fill",
                fill_mts,
                order_id,
                source="historical_replay",
                fill_qty=decimal_string(fill_qty),
                fill_price=order["order_price_str"],
                remaining_qty=decimal_string(remaining_after_fill),
                event_markout_bps=order.get("event_markout_bps"),
                event_value_after_rebate_bps=order.get("event_value_after_rebate_bps"),
            )
        else:
            recorder.append(
                "order_cancel_request",
                cancel_request_mts,
                order_id,
                source="paper_order_manager",
                remaining_qty=decimal_string(order_qty),
                reason="ttl_expired",
            )
            recorder.append(
                "order_cancel_ack",
                cancel_ack_mts,
                order_id,
                source="paper_order_manager",
                cancel_latency_ms=int(cancel_ack_latency_ms),
                remaining_qty=decimal_string(0),
                reason="ttl_expired",
            )

    return read_event_log(recorder.path)


def replay_comparison(plan: pd.DataFrame, lifecycle: pd.DataFrame) -> pd.DataFrame:
    if plan.empty or lifecycle.empty:
        return pd.DataFrame()
    expected = plan[
        [
            "paper_order_id",
            "policy",
            "side",
            "decision_mts",
            "live_mts",
            "ttl_ms",
            "event_full_fill",
            "event_partial_qty",
            "event_value_after_rebate_bps",
            "l2_price_covered",
            "l2_queue_ahead_qty",
        ]
    ].copy()
    merged = expected.merge(lifecycle, on="paper_order_id", how="left")
    merged["recorded_full_fill"] = merged["status"].eq("filled")
    merged["fill_match"] = merged["event_full_fill"].eq(merged["recorded_full_fill"])
    merged["qty_gap"] = pd.to_numeric(merged["event_partial_qty"], errors="coerce").fillna(0.0) - merged["filled_qty"].fillna(0.0)
    return merged
