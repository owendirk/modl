from __future__ import annotations

import heapq
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from paper_trade_recorder import PaperTradeRecorder, decimal_string, read_event_log


@dataclass(frozen=True)
class PaperRuntimeConfig:
    ack_latency_ms: int = 50
    cancel_ack_latency_ms: int = 100
    replace_existing_by_side: bool = False
    fill_during_cancel_pending: bool = True
    heartbeat_interval_ms: int = 300_000


@dataclass
class PaperOrderState:
    paper_order_id: str
    side: str
    order_price: Decimal
    order_qty: Decimal
    remaining_qty: Decimal
    queue_ahead_qty: Decimal
    decision_mts: int
    submit_mts: int
    ack_mts: int
    ttl_ms: int
    status: str = "submitted"
    cancel_request_mts: int | None = None
    cancel_ack_mts: int | None = None
    filled_qty: Decimal = Decimal("0")

    @property
    def is_active(self) -> bool:
        return self.status in {"submitted", "open", "cancel_pending"}


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    text = decimal_string(value)
    if text is None:
        text = default
    return Decimal(text)


def _finite_or_none(value: Any) -> Any:
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return None
    return value


def _event_priority(event_type: str) -> int:
    priorities = {
        "decision": 0,
        "submit": 1,
        "ack": 2,
        "trade": 3,
        "cancel_request": 4,
        "cancel_ack": 5,
        "heartbeat": 9,
    }
    return priorities[event_type]


def prepare_public_trades(trades: pd.DataFrame) -> pd.DataFrame:
    required = {"received_mts", "taker_side", "price", "qty"}
    missing = sorted(required.difference(trades.columns))
    if missing:
        raise ValueError(f"trades missing required columns: {missing}")

    out = trades[list(required)].copy()
    out["received_mts"] = pd.to_numeric(out["received_mts"], errors="coerce").astype("Int64")
    out["taker_side"] = out["taker_side"].astype(str).str.lower()
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce")
    out = out.dropna(subset=["received_mts", "price", "qty"])
    out = out[out["qty"] > 0].copy()
    return out.sort_values("received_mts").reset_index(drop=True)


def _trade_matches_order(order: PaperOrderState, trade_side: str, trade_price: Decimal) -> bool:
    if order.side == "bid":
        return trade_side == "sell" and trade_price <= order.order_price
    return trade_side == "buy" and trade_price >= order.order_price


def _order_priority(order: PaperOrderState) -> tuple[Decimal, int]:
    if order.side == "bid":
        return (-order.order_price, order.ack_mts)
    return (order.order_price, order.ack_mts)


class PaperOrderManager:
    def __init__(self, recorder: PaperTradeRecorder, config: PaperRuntimeConfig | None = None) -> None:
        self.recorder = recorder
        self.config = config or PaperRuntimeConfig()
        self.orders: dict[str, PaperOrderState] = {}
        self._dynamic_sequence = 10_000_000_000

    def record_decision(self, order: pd.Series) -> None:
        self.recorder.append(
            "decision",
            int(order["decision_mts"]),
            str(order["paper_order_id"]),
            source="quote_policy",
            policy=order.get("policy"),
            side=order.get("side"),
            order_qty=decimal_string(order.get("order_qty")),
            order_price=decimal_string(order.get("order_price")),
            quote_distance_bps=_finite_or_none(order.get("quote_distance_bps")),
            model_quote_score_bps=_finite_or_none(order.get("model_quote_score_bps")),
            predicted_standalone_value_bps=_finite_or_none(order.get("predicted_standalone_value_bps")),
            expected_event_ev_after_rebate_bps=_finite_or_none(order.get("event_value_after_rebate_bps")),
            paper_ttl_ms=int(order.get("ttl_ms")),
        )
        self.recorder.append(
            "replay_projection",
            int(order["decision_mts"]),
            str(order["paper_order_id"]),
            source="historical_replay",
            event_full_fill=bool(order.get("event_full_fill", False)),
            event_partial_qty=decimal_string(order.get("event_partial_qty")),
            event_markout_bps=_finite_or_none(order.get("event_markout_bps")),
            event_value_after_rebate_bps=_finite_or_none(order.get("event_value_after_rebate_bps")),
            trade_through_full_fill_no_queue=bool(order.get("trade_through_full_fill_no_queue", False)),
            l2_price_covered=bool(order.get("l2_price_covered", False)),
        )

    def submit_order(self, order: pd.Series, event_heap: list[tuple[int, int, int, str, Any]]) -> None:
        order_id = str(order["paper_order_id"])
        event_mts = int(order["live_mts"])
        side = str(order["side"])

        if self.config.replace_existing_by_side:
            for existing in list(self.orders.values()):
                if existing.side == side and existing.is_active and existing.paper_order_id != order_id:
                    self.request_cancel(existing.paper_order_id, event_mts, "replace_quote", event_heap)

        state = PaperOrderState(
            paper_order_id=order_id,
            side=side,
            order_price=_to_decimal(order.get("order_price")),
            order_qty=_to_decimal(order.get("order_qty")),
            remaining_qty=_to_decimal(order.get("order_qty")),
            queue_ahead_qty=_to_decimal(order.get("l2_queue_ahead_qty")),
            decision_mts=int(order["decision_mts"]),
            submit_mts=event_mts,
            ack_mts=event_mts + int(self.config.ack_latency_ms),
            ttl_ms=int(order["ttl_ms"]),
        )
        self.orders[order_id] = state

        self.recorder.append(
            "market_snapshot",
            event_mts,
            order_id,
            source="paper_runtime_market_snapshot",
            live_bid=decimal_string(order.get("live_bid")),
            live_ask=decimal_string(order.get("live_ask")),
            live_mid=decimal_string(order.get("live_mid")),
            l2_snapshot_mts=_finite_or_none(order.get("l2_snapshot_mts")),
            l2_snapshot_age_ms=_finite_or_none(order.get("l2_snapshot_age_ms")),
            l2_queue_ahead_qty=decimal_string(order.get("l2_queue_ahead_qty")),
            l2_price_covered=bool(order.get("l2_price_covered", False)),
        )
        self.recorder.append(
            "order_submit",
            event_mts,
            order_id,
            source="paper_runtime_order_manager",
            side=state.side,
            order_type="post_only_limit",
            order_qty=decimal_string(state.order_qty),
            order_price=decimal_string(state.order_price),
            remaining_qty=decimal_string(state.remaining_qty),
            queue_ahead_qty=decimal_string(state.queue_ahead_qty),
        )

    def ack_order(self, order_id: str, event_mts: int) -> None:
        state = self.orders.get(order_id)
        if state is None or state.status != "submitted":
            return
        state.status = "open"
        self.recorder.append(
            "order_ack",
            event_mts,
            order_id,
            source="paper_runtime_order_manager",
            ack_latency_ms=int(event_mts - state.submit_mts),
            venue_order_id=f"paper-venue-{order_id[-12:]}",
            remaining_qty=decimal_string(state.remaining_qty),
        )

    def request_cancel(
        self,
        order_id: str,
        event_mts: int,
        reason: str,
        event_heap: list[tuple[int, int, int, str, Any]],
    ) -> None:
        state = self.orders.get(order_id)
        if state is None or state.status not in {"submitted", "open"}:
            return
        state.status = "cancel_pending"
        state.cancel_request_mts = int(event_mts)
        state.cancel_ack_mts = int(event_mts) + int(self.config.cancel_ack_latency_ms)
        self.recorder.append(
            "order_cancel_request",
            event_mts,
            order_id,
            source="paper_runtime_order_manager",
            remaining_qty=decimal_string(state.remaining_qty),
            reason=reason,
        )
        heapq.heappush(event_heap, (state.cancel_ack_mts, _event_priority("cancel_ack"), self._dynamic_sequence, "cancel_ack", order_id))
        self._dynamic_sequence += 1

    def ack_cancel(self, order_id: str, event_mts: int, reason: str = "cancel_ack") -> None:
        state = self.orders.get(order_id)
        if state is None or state.status != "cancel_pending":
            return
        state.status = "cancelled"
        cancelled_qty = state.remaining_qty
        state.remaining_qty = Decimal("0")
        self.recorder.append(
            "order_cancel_ack",
            event_mts,
            order_id,
            source="paper_runtime_order_manager",
            cancel_latency_ms=int(event_mts - (state.cancel_request_mts or event_mts)),
            cancelled_qty=decimal_string(cancelled_qty),
            remaining_qty=decimal_string(state.remaining_qty),
            reason=reason,
        )

    def process_trade(self, trade: pd.Series) -> None:
        trade_mts = int(trade["received_mts"])
        trade_side = str(trade["taker_side"]).lower()
        trade_price = _to_decimal(trade["price"])
        available_qty = _to_decimal(trade["qty"])
        if available_qty <= 0:
            return

        active_orders = []
        for state in self.orders.values():
            if state.status not in {"open", "cancel_pending"}:
                continue
            if state.status == "cancel_pending" and not self.config.fill_during_cancel_pending:
                continue
            if state.ack_mts > trade_mts:
                continue
            if state.cancel_ack_mts is not None and trade_mts > state.cancel_ack_mts:
                continue
            if _trade_matches_order(state, trade_side, trade_price):
                active_orders.append(state)

        active_orders.sort(key=_order_priority)
        for state in active_orders:
            if available_qty <= 0:
                break
            if state.queue_ahead_qty > 0:
                consumed_queue = min(state.queue_ahead_qty, available_qty)
                state.queue_ahead_qty -= consumed_queue
                available_qty -= consumed_queue
            if available_qty <= 0 or state.remaining_qty <= 0:
                continue
            fill_qty = min(state.remaining_qty, available_qty)
            state.remaining_qty -= fill_qty
            state.filled_qty += fill_qty
            available_qty -= fill_qty
            self.recorder.append(
                "order_fill",
                trade_mts,
                state.paper_order_id,
                source="paper_runtime_public_trade_match",
                fill_qty=decimal_string(fill_qty),
                fill_price=decimal_string(state.order_price),
                trade_price=decimal_string(trade_price),
                trade_qty=decimal_string(trade["qty"]),
                remaining_qty=decimal_string(state.remaining_qty),
                queue_ahead_qty=decimal_string(state.queue_ahead_qty),
                maker_taker="maker",
            )
            if state.remaining_qty <= 0:
                state.status = "filled"

    def heartbeat(self, event_mts: int) -> None:
        open_orders = sum(1 for order in self.orders.values() if order.status in {"submitted", "open", "cancel_pending"})
        filled_orders = sum(1 for order in self.orders.values() if order.status == "filled")
        cancelled_orders = sum(1 for order in self.orders.values() if order.status == "cancelled")
        self.recorder.append(
            "heartbeat",
            event_mts,
            None,
            source="paper_runtime_order_manager",
            open_orders=open_orders,
            filled_orders=filled_orders,
            cancelled_orders=cancelled_orders,
        )


def run_historical_paper_runtime(
    recorder: PaperTradeRecorder,
    order_plan: pd.DataFrame,
    trades: pd.DataFrame,
    config: PaperRuntimeConfig | None = None,
) -> pd.DataFrame:
    if order_plan.empty:
        return read_event_log(recorder.path)

    config = config or PaperRuntimeConfig()
    manager = PaperOrderManager(recorder, config)
    public_trades = prepare_public_trades(trades)

    min_mts = int(order_plan["decision_mts"].min())
    max_mts = int((order_plan["live_mts"] + order_plan["ttl_ms"] + config.cancel_ack_latency_ms).max())
    public_trades = public_trades[(public_trades["received_mts"] >= min_mts) & (public_trades["received_mts"] <= max_mts)].copy()

    event_heap: list[tuple[int, int, int, str, Any]] = []
    sequence = 0
    for idx, order in order_plan.reset_index(drop=True).iterrows():
        decision_mts = int(order["decision_mts"])
        live_mts = int(order["live_mts"])
        heapq.heappush(event_heap, (decision_mts, _event_priority("decision"), sequence, "decision", idx))
        sequence += 1
        heapq.heappush(event_heap, (live_mts, _event_priority("submit"), sequence, "submit", idx))
        sequence += 1
        heapq.heappush(event_heap, (live_mts + config.ack_latency_ms, _event_priority("ack"), sequence, "ack", idx))
        sequence += 1
        heapq.heappush(event_heap, (live_mts + int(order["ttl_ms"]), _event_priority("cancel_request"), sequence, "cancel_request", idx))
        sequence += 1

    for idx, _ in public_trades.reset_index(drop=True).iterrows():
        trade_mts = int(public_trades.iloc[idx]["received_mts"])
        heapq.heappush(event_heap, (trade_mts, _event_priority("trade"), sequence, "trade", idx))
        sequence += 1

    if config.heartbeat_interval_ms > 0:
        heartbeat_mts = min_mts
        while heartbeat_mts <= max_mts:
            heapq.heappush(event_heap, (heartbeat_mts, _event_priority("heartbeat"), sequence, "heartbeat", None))
            sequence += 1
            heartbeat_mts += config.heartbeat_interval_ms

    order_plan_reset = order_plan.reset_index(drop=True)
    trades_reset = public_trades.reset_index(drop=True)
    while event_heap:
        event_mts, _, _, event_type, payload = heapq.heappop(event_heap)
        if event_type == "decision":
            manager.record_decision(order_plan_reset.iloc[int(payload)])
        elif event_type == "submit":
            manager.submit_order(order_plan_reset.iloc[int(payload)], event_heap)
        elif event_type == "ack":
            manager.ack_order(str(order_plan_reset.iloc[int(payload)]["paper_order_id"]), int(event_mts))
        elif event_type == "cancel_request":
            manager.request_cancel(str(order_plan_reset.iloc[int(payload)]["paper_order_id"]), int(event_mts), "ttl_expired", event_heap)
        elif event_type == "cancel_ack":
            manager.ack_cancel(str(payload), int(event_mts))
        elif event_type == "trade":
            manager.process_trade(trades_reset.iloc[int(payload)])
        elif event_type == "heartbeat":
            manager.heartbeat(int(event_mts))

    return read_event_log(recorder.path)


def runtime_summary(events: pd.DataFrame, lifecycle: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    order_count = lifecycle["paper_order_id"].nunique() if not lifecycle.empty else 0
    filled_qty = lifecycle["filled_qty"].sum() if "filled_qty" in lifecycle else 0.0
    return pd.DataFrame(
        [
            {
                "events": int(len(events)),
                "orders": int(order_count),
                "fills": int(events["event_type"].eq("order_fill").sum()),
                "filled_orders": int(lifecycle["status"].eq("filled").sum()) if not lifecycle.empty else 0,
                "partial_cancelled_orders": int(lifecycle["status"].eq("partial_cancelled").sum()) if not lifecycle.empty else 0,
                "cancelled_orders": int(lifecycle["status"].eq("cancelled").sum()) if not lifecycle.empty else 0,
                "filled_qty": float(filled_qty),
                "first_event_mts": int(events["event_mts"].min()),
                "last_event_mts": int(events["event_mts"].max()),
            }
        ]
    )
