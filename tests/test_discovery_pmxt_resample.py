from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from helpers import CONDITION, START_NS, gamma_payload, market, pmxt_rows

from canonical_data.discovery import GammaClient, discover
from canonical_data.errors import (
    IdentityError,
    MissingInitialSnapshotError,
    ReconstructionError,
    ResourceLimitError,
    SourceError,
)
from canonical_data.models import Asset, BookEvent, EventType
from canonical_data.pmxt import (
    PMXT_COLUMNS,
    BookReconstructor,
    decode_rows,
    order_and_deduplicate,
    read_pmxt_parquet,
)
from canonical_data.resample import resample_200ms
from canonical_data.sources import ProductionSourceLoader


class DiscoveryTests(unittest.TestCase):
    def test_gamma_client_builds_exact_slug_lookup(self) -> None:
        seen: list[tuple[str, int]] = []

        def fetch(url: str, limit: int) -> bytes:
            seen.append((url, limit))
            return gamma_payload()

        found, payload, url = GammaClient(fetch, 12345).fetch_market(
            market().asset, START_NS // 1_000_000_000
        )
        self.assertEqual(found.condition_id, CONDITION)
        self.assertEqual(payload, gamma_payload())
        self.assertTrue(
            url.endswith("/events/slug/dogecoin-up-or-down-april-13-2026-3pm-et")
        )
        self.assertEqual(seen, [(url, 12345)])

    def test_binds_identity_rules_tokens_and_official_outcome(self) -> None:
        found = discover([gamma_payload()])
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].condition_id, CONDITION)
        self.assertEqual((found[0].token_up, found[0].token_down), ("1", "2"))
        self.assertEqual(found[0].official_outcome.value, "UP")
        self.assertEqual(found[0].market_end_ns - found[0].market_start_ns, 3_600_000_000_000)

    def test_all_seven_asset_profiles_bind_exact_1h_semantics(self) -> None:
        for asset in Asset:
            with self.subTest(asset=asset.value):
                found = discover([gamma_payload(asset)])[0]
                self.assertIs(found.asset, asset)
                self.assertEqual(found.timeframe, "1h")
                self.assertEqual(found.market_start_ns % 3_600_000_000_000, 0)
                self.assertEqual(found.market_end_ns - found.market_start_ns, 3_600_000_000_000)

    def test_rejects_wrong_rule_authority(self) -> None:
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["resolutionSource"] = "https://example.test/not-authority"
        with self.assertRaises(IdentityError):
            discover([json.dumps(raw).encode()])

    def test_requires_exact_binance_resolution_source(self) -> None:
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["resolutionSource"] = ""
        with self.assertRaises(IdentityError):
            discover([json.dumps(raw).encode()])

    def test_binds_named_binance_candle_across_official_fields(self) -> None:
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["description"] = (
            "This market resolves Up if the close price is greater than or equal to the open "
            "price for the DOGE/USDT 1 hour candle; otherwise Down. The source is Binance."
        )
        found = discover([json.dumps(raw).encode()])
        self.assertEqual(found[0].official_outcome.value, "UP")

    def test_exact_resolution_source_and_asset_named_rules_bind_candle(self) -> None:
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["description"] = (
            "This market resolves Up if the DOGE/USDT 1 hour candle close price is greater "
            "than or equal to its open price; otherwise Down according to Binance."
        )
        found = discover([json.dumps(raw).encode()])
        self.assertEqual(
            found[0].resolution_source_url,
            "https://www.binance.com/en/trade/DOGE_USDT",
        )

        raw[0]["markets"][0]["description"] = (
            "This market resolves Up if the BTC/USDT 1 hour candle close price is greater "
            "than or equal to its open price; otherwise Down according to Binance."
        )
        with self.assertRaises(IdentityError):
            discover([json.dumps(raw).encode()])

    def test_official_end_must_match_exact_one_hour_slug_window(self) -> None:
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["endDate"] = "2026-04-13T19:15:00Z"
        with self.assertRaises(IdentityError):
            discover([json.dumps(raw).encode()])

    def test_rejects_unresolved_and_ambiguous_outcome(self) -> None:
        raw = json.loads(gamma_payload(outcome_prices=["0.7", "0.3"]))
        with self.assertRaises(IdentityError):
            discover([json.dumps(raw).encode()])
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["closed"] = False
        with self.assertRaises(IdentityError):
            discover([json.dumps(raw).encode()])

    def test_conflicting_duplicate_identity_is_rejected(self) -> None:
        first = gamma_payload()
        raw = json.loads(first)
        raw[0]["markets"][0]["id"] = "different"
        with self.assertRaises(IdentityError):
            discover([first, json.dumps(raw).encode()])


class PmxtTests(unittest.TestCase):
    def test_parquet_reader_filters_identity_and_caps_rows(self) -> None:
        timestamp = datetime.fromtimestamp(START_NS / 1_000_000_000, UTC)
        base = pmxt_rows(False)[0]
        rows = []
        other = "0x" + "f" * 64
        for condition, token in (
            (CONDITION, "1"),
            (CONDITION, "1"),
            (CONDITION, "3"),
            (other, "3"),
        ):
            row = {key: base.get(key) for key in PMXT_COLUMNS}
            row["timestamp_received"] = timestamp
            row["timestamp"] = timestamp
            row["market"] = condition.encode()
            row["asset_id"] = token
            rows.append(row)
        schema = pa.schema(
            [
                ("timestamp_received", pa.timestamp("ms", tz="UTC")),
                ("timestamp", pa.timestamp("ms", tz="UTC")),
                ("market", pa.binary(66)),
                ("event_type", pa.string()),
                ("asset_id", pa.string()),
                ("bids", pa.string()),
                ("asks", pa.string()),
                ("price", pa.decimal128(9, 4)),
                ("size", pa.decimal128(18, 6)),
                ("side", pa.string()),
                ("best_bid", pa.decimal128(9, 4)),
                ("best_ask", pa.decimal128(9, 4)),
                ("fee_rate_bps", pa.uint16()),
                ("transaction_hash", pa.string()),
                ("old_tick_size", pa.decimal128(9, 4)),
                ("new_tick_size", pa.decimal128(9, 4)),
            ]
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pmxt.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, row_group_size=1)
            found = read_pmxt_parquet(path, {CONDITION}, {"1"}, "fixture", max_scanned_rows=3)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].condition_id, CONDITION)
            separate_other = read_pmxt_parquet(
                path, {other}, {"3"}, "fixture", max_scanned_rows=2
            )
            combined = read_pmxt_parquet(
                path,
                {CONDITION, other},
                {"1", "3"},
                "fixture",
                max_scanned_rows=3,
                source_row_partition_by_condition={CONDITION: "DOGE", other: "BTC"},
                token_ids_by_source_row_partition={"DOGE": {"1"}, "BTC": {"3"}},
            )
            self.assertEqual(separate_other[0].source_row, 0)
            self.assertEqual(
                {event.condition_id: event.source_row for event in combined},
                {CONDITION: found[0].source_row, other: separate_other[0].source_row},
            )
            self.assertEqual(len(combined), 2)
            with self.assertRaisesRegex(ResourceLimitError, "partition cap"):
                read_pmxt_parquet(
                    path,
                    {CONDITION, other},
                    {"1", "3"},
                    "fixture",
                    max_scanned_rows=3,
                    max_output_rows=4,
                    source_row_partition_by_condition={CONDITION: "DOGE", other: "BTC"},
                    token_ids_by_source_row_partition={"DOGE": {"1"}, "BTC": {"3"}},
                    max_output_rows_per_source_row_partition=1,
                )
            with self.assertRaises(ResourceLimitError):
                read_pmxt_parquet(path, {CONDITION}, {"1"}, "fixture", max_scanned_rows=0)
            downloaded = ProductionSourceLoader(GammaClient(), 7).load_downloaded_pmxt(
                path, "https://example.test/hour.parquet", (market(),), '"etag"'
            )
            self.assertEqual(len(downloaded.events), 1)
            batches: list[BookEvent] = []
            streamed = ProductionSourceLoader(GammaClient(), 7).load_downloaded_pmxt(
                path,
                "https://example.test/hour.parquet",
                (market(),),
                '"etag"',
                event_batch_consumer=batches.extend,
            )
            self.assertEqual(streamed.events, ())
            self.assertEqual(order_and_deduplicate(batches), list(downloaded.events))
            self.assertEqual(
                downloaded.provenance[0].transformations,
                (
                    "bounded_whole_object_fallback",
                    "condition_token_filter",
                    "market_receive_window_filter",
                ),
            )

    def test_downloaded_reader_retains_only_causal_market_inventory(self) -> None:
        base = pmxt_rows(False)[0]
        before_ms = START_NS // 1_000_000 - 3_600_001
        end_ms = market().market_end_ns // 1_000_000
        timestamps_ms = (
            *(before_ms - offset for offset in range(10)),
            START_NS // 1_000_000 - 3_600_000,
            START_NS // 1_000_000,
            *(end_ms + offset for offset in range(10)),
        )
        rows = []
        for timestamp_ms in timestamps_ms:
            row = {key: base.get(key) for key in PMXT_COLUMNS}
            row["timestamp_received"] = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
            row["timestamp"] = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
            row["market"] = CONDITION.encode()
            if timestamp_ms < START_NS // 1_000_000 - 3_600_000 or timestamp_ms >= end_ms:
                row["event_type"] = "malformed_outside_inventory"
            rows.append(row)
        schema = pa.schema(
            [
                ("timestamp_received", pa.timestamp("ms", tz="UTC")),
                ("timestamp", pa.timestamp("ms", tz="UTC")),
                ("market", pa.binary(66)),
                ("event_type", pa.string()),
                ("asset_id", pa.string()),
                ("bids", pa.string()),
                ("asks", pa.string()),
                ("price", pa.decimal128(9, 4)),
                ("size", pa.decimal128(18, 6)),
                ("side", pa.string()),
                ("best_bid", pa.decimal128(9, 4)),
                ("best_ask", pa.decimal128(9, 4)),
                ("fee_rate_bps", pa.uint16()),
                ("transaction_hash", pa.string()),
                ("old_tick_size", pa.decimal128(9, 4)),
                ("new_tick_size", pa.decimal128(9, 4)),
            ]
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pmxt.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
            loaded = ProductionSourceLoader(GammaClient(), 7).load_downloaded_pmxt(
                path,
                "https://example.test/hour.parquet",
                (market(),),
                '"etag"',
                max_filtered_rows=2,
            )
        self.assertEqual(
            [event.receive_ts_ns for event in loaded.events],
            [START_NS - 3_600_000_000_000, START_NS],
        )

    def test_every_event_type_and_reconstruction(self) -> None:
        events = decode_rows(pmxt_rows(), "fixture.parquet")
        self.assertEqual({item.event_type for item in events}, set(EventType))
        states = BookReconstructor().reconstruct(events)
        up = [state for state in states if state.token_id == "1"]
        self.assertEqual(up[-1].bids[0].price, up[1].bids[0].price)
        self.assertEqual(up[-1].tick_size, up[2].tick_size)
        self.assertEqual(len(up), 3)  # trade event is preserved but cannot mutate the book

    def test_zero_size_deletes_level(self) -> None:
        rows = pmxt_rows(False)
        rows.append(
            {
                "timestamp_received": START_NS // 1_000_000 + 1,
                "timestamp": START_NS // 1_000_000 + 1,
                "market": CONDITION,
                "event_type": "price_change",
                "asset_id": "1",
                "side": "BUY",
                "price": "0.40",
                "size": "0",
                "best_ask": "0.60",
            }
        )
        states = BookReconstructor().reconstruct(decode_rows(rows, "fixture"))
        self.assertEqual([item for item in states if item.token_id == "1"][-1].bids, ())

    def test_duplicate_is_deduplicated_and_same_timestamp_updates_remain_ordered(self) -> None:
        rows = pmxt_rows(False)
        events = decode_rows([*rows, dict(rows[0])], "fixture")
        self.assertEqual(len(events), 2)
        conflicting = dict(rows[0])
        conflicting["bids"] = '[["0.40","11"]]'
        ordered = decode_rows([*rows, conflicting], "fixture")
        self.assertEqual(len(ordered), 3)
        changed = next(
            event for event in ordered if event.bids and event.bids[0].size == Decimal("11")
        )
        original = next(
            event for event in ordered if event.bids and event.bids[0].size == Decimal("10")
        )
        self.assertGreater(changed.source_row, original.source_row)

    def test_flattened_price_change_batch_validates_after_all_rows(self) -> None:
        snapshot = pmxt_rows(False)[0]
        delete = {
            **pmxt_rows()[2],
            "timestamp_received": START_NS // 1_000_000 + 10,
            "timestamp": START_NS // 1_000_000 + 9,
            "side": "SELL",
            "price": "0.60",
            "size": "0",
            "best_bid": "0.40",
            "best_ask": "0.70",
        }
        replacement = {
            **delete,
            "timestamp": START_NS // 1_000_000 + 10,
            "price": "0.70",
            "size": "8",
        }
        states = BookReconstructor().reconstruct(
            decode_rows([snapshot, delete, replacement], "fixture")
        )
        self.assertEqual(states[-1].asks[0].price, Decimal("0.70"))

    def test_exporter_exact_timestamp_tie_restores_native_reverse_row_order(self) -> None:
        snapshot = pmxt_rows(False)[0]
        archived_later = {
            **pmxt_rows()[2],
            "timestamp_received": START_NS // 1_000_000 + 10,
            "timestamp": START_NS // 1_000_000 + 9,
            "side": "SELL",
            "price": "0.60",
            "size": "0",
            "best_bid": "0.40",
            "best_ask": "0.70",
        }
        archived_earlier = {
            **archived_later,
            "price": "0.70",
            "size": "8",
            "best_ask": "0.60",
        }
        events = decode_rows([snapshot, archived_later, archived_earlier], "fixture")
        price_changes = [event for event in events if event.event_type is EventType.PRICE_CHANGE]
        self.assertEqual([event.price for event in price_changes], [Decimal("0.70"), Decimal("0.60")])
        states = BookReconstructor().reconstruct(events)
        self.assertEqual(states[-1].asks[0].price, Decimal("0.70"))

    def test_native_bbo_prunes_stale_better_level_without_zero_update(self) -> None:
        snapshot = pmxt_rows(False)[0]
        inward = {
            **pmxt_rows()[2],
            "side": "BUY",
            "price": "0.30",
            "size": "12",
            "best_bid": "0.30",
            "best_ask": "0.60",
        }
        states = BookReconstructor().reconstruct(decode_rows([snapshot, inward], "fixture"))
        self.assertEqual(states[-1].bids[0].price, Decimal("0.30"))
        self.assertNotIn(Decimal("0.40"), {level.price for level in states[-1].bids})

    def test_native_empty_side_bbo_sentinel_is_not_a_quote(self) -> None:
        snapshot = pmxt_rows(False)[0]
        empty_bid = {
            **pmxt_rows()[2],
            "side": "BUY",
            "price": "0.40",
            "size": "0",
            "best_bid": "0",
            "best_ask": "0.60",
        }
        states = BookReconstructor().reconstruct(decode_rows([snapshot, empty_bid], "fixture"))
        self.assertEqual(states[-1].bids, ())

    def test_unanchored_prefix_is_discarded_until_first_full_snapshot(self) -> None:
        increment = {
            **pmxt_rows()[2],
            "timestamp": START_NS // 1_000_000 - 2_000,
            "timestamp_received": START_NS // 1_000_000 - 2_000,
        }
        snapshots = [
            {
                **row,
                "timestamp": row["timestamp"] - 1_000,
                "timestamp_received": row["timestamp_received"] - 1_000,
            }
            for row in pmxt_rows(False)
        ]
        states = BookReconstructor().reconstruct(
            decode_rows([increment, *snapshots, pmxt_rows()[2]], "fixture")
        )
        up = [state for state in states if state.token_id == "1"]
        self.assertEqual(len(up), 2)
        self.assertEqual(up[0].receive_ts_ns, START_NS - 1_000_000_000)
        self.assertEqual(up[-1].bids[0].price, Decimal("0.45"))

    def test_stream_without_any_snapshot_fails_closed(self) -> None:
        for event in (pmxt_rows()[2], pmxt_rows()[3]):
            with self.assertRaises(MissingInitialSnapshotError):
                BookReconstructor().reconstruct(decode_rows([event], "fixture"))

    def test_malformed_negative_and_best_quote_inconsistency_fail(self) -> None:
        raw = pmxt_rows(False)[0]
        malformed = dict(raw)
        malformed["bids"] = "not-json"
        with self.assertRaises((SourceError, json.JSONDecodeError)):
            decode_rows([malformed], "fixture")
        update = pmxt_rows()[2]
        bad = dict(update)
        bad["best_bid"] = "0.44"
        with self.assertRaises(ReconstructionError):
            BookReconstructor().reconstruct(decode_rows([*pmxt_rows(False), bad], "fixture"))

    def test_out_of_order_input_is_sorted_by_receive_time(self) -> None:
        rows = pmxt_rows()
        events = decode_rows(reversed(rows), "fixture")
        keys = [event.order_key for event in events if event.token_id == "1"]
        self.assertEqual(keys, sorted(keys))


class ResampleTests(unittest.TestCase):
    def test_no_lookahead_and_exact_grid(self) -> None:
        events = decode_rows(pmxt_rows(), "fixture")
        states = BookReconstructor().reconstruct(events)
        samples, gaps = resample_200ms(market(), states)
        self.assertEqual(gaps, [])
        self.assertEqual(len(samples), 36000)
        at_start = next(
            item for item in samples if item.token_id == "1" and item.grid_ts_ns == START_NS
        )
        at_200 = next(
            item
            for item in samples
            if item.token_id == "1" and item.grid_ts_ns == START_NS + 200_000_000
        )
        self.assertEqual(at_start.bids[0].price, states[0].bids[0].price)
        self.assertEqual(str(at_200.bids[0].price), "0.45")
        assert at_200.asof_receive_ts_ns is not None
        self.assertLessEqual(at_200.asof_receive_ts_ns, at_200.grid_ts_ns)

    def test_pre_snapshot_is_gap_not_empty_book(self) -> None:
        rows = pmxt_rows(False)
        for row in rows:
            row["timestamp_received"] += 200
        states = BookReconstructor().reconstruct(decode_rows(rows, "fixture"))
        samples, gaps = resample_200ms(market(), states)
        self.assertEqual(gaps[0], (START_NS, START_NS + 200_000_000))
        self.assertFalse(any(item.grid_ts_ns == START_NS for item in samples))

    def test_unknown_token_rejected(self) -> None:
        state = BookReconstructor().reconstruct(decode_rows(pmxt_rows(False), "fixture"))[0]
        with self.assertRaises(ReconstructionError):
            resample_200ms(market(), [replace(state, token_id="999")])
