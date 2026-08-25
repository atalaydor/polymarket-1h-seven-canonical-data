"""Bounded PMXT v2 forensic probe for one seven-asset market hour."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import urllib.request
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from canonical_data.discovery import GammaClient
from canonical_data.errors import ReconstructionError
from canonical_data.httpclient import USER_AGENT
from canonical_data.models import Asset, BookEvent
from canonical_data.pmxt import BookReconstructor, read_pmxt_parquet

PMXT = "https://r2v2.pmxt.dev/polymarket_orderbook_{hour}.parquet"
CONFLICT = re.compile(r"receive_ts_ns=(\d+).+token_id=(\d+)")


def _download(url: str, target: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=120) as response, target.open("wb") as output:
        while chunk := response.read(8 * 1024 * 1024):
            output.write(chunk)


def _event(event: BookEvent) -> dict[str, object]:
    return {
        "receive_ts_ns": event.receive_ts_ns,
        "source_ts_ns": event.source_ts_ns,
        "source_row": event.source_row,
        "event_type": event.event_type.value,
        "token_id": event.token_id,
        "side": event.side,
        "price": None if event.price is None else str(event.price),
        "size": None if event.size is None else str(event.size),
        "best_bid": None if event.best_bid is None else str(event.best_bid),
        "best_ask": None if event.best_ask is None else str(event.best_ask),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market-start", default="2026-08-05T18:00:00Z")
    args = parser.parse_args()
    start = datetime.fromisoformat(args.market_start.replace("Z", "+00:00")).astimezone(UTC)
    start_s = int(start.timestamp())
    client = GammaClient()
    markets = {}
    for asset in Asset:
        market, _payload, url = client.fetch_market(asset, start_s)
        markets[asset] = market
        print(
            json.dumps(
                {
                    "record": "gamma_identity",
                    "asset": asset.value,
                    "market_id": market.market_id,
                    "condition_id": market.condition_id,
                    "tokens": [market.token_up, market.token_down],
                    "market_start_ns": market.market_start_ns,
                    "market_end_ns": market.market_end_ns,
                    "url": url,
                },
                sort_keys=True,
            )
        )

    bounds = {
        market.condition_id: (market.market_start_ns - 3_600_000_000_000, market.market_end_ns)
        for market in markets.values()
    }
    conditions = set(bounds)
    tokens = {
        token
        for market in markets.values()
        for token in (market.token_up, market.token_down)
    }
    all_events: list[BookEvent] = []
    with tempfile.TemporaryDirectory(prefix="pmxt-forensic-") as raw_dir:
        root = Path(raw_dir)
        for hour in (start - timedelta(hours=1), start):
            url = PMXT.format(hour=hour.strftime("%Y-%m-%dT%H"))
            path = root / f"{hour:%Y-%m-%dT%H}.parquet"
            _download(url, path)
            events = read_pmxt_parquet(
                path,
                conditions,
                tokens,
                url,
                max_scanned_rows=200_000_000,
                max_output_rows=2_000_000,
                receive_bounds_by_condition=bounds,
            )
            all_events.extend(events)
            print(
                json.dumps(
                    {
                        "record": "source_object",
                        "url": url,
                        "byte_length": path.stat().st_size,
                        "filtered_events": len(events),
                    },
                    sort_keys=True,
                )
            )

    reconstructor = BookReconstructor()
    for asset, market in markets.items():
        events = [event for event in all_events if event.condition_id == market.condition_id]
        counts = Counter(event.event_type.value for event in events)
        result: dict[str, object] = {
            "record": "reconstruction",
            "asset": asset.value,
            "events": len(events),
            "event_types": dict(sorted(counts.items())),
            "first_receive_ts_ns": min(
                (event.receive_ts_ns for event in events if event.receive_ts_ns is not None),
                default=None,
            ),
            "token_event_counts": {
                token: sum(event.token_id == token for event in events)
                for token in (market.token_up, market.token_down)
            },
        }
        try:
            states = reconstructor.reconstruct(events)
            result.update(status="PASS", states=len(states))
        except ReconstructionError as exc:
            detail = str(exc)
            result.update(status="EVENT_CONFLICT", detail=detail)
            match = CONFLICT.search(detail)
            if match is not None:
                receive_ns, token_id = int(match.group(1)), match.group(2)
                ordered = sorted(
                    (event for event in events if event.token_id == token_id),
                    key=lambda event: event.order_key,
                )
                positions = [
                    index
                    for index, event in enumerate(ordered)
                    if event.receive_ts_ns == receive_ns
                ]
                if positions:
                    lower = max(0, positions[0] - 2)
                    upper = min(len(ordered), positions[-1] + 3)
                    result["conflict_neighborhood"] = [
                        _event(event) for event in ordered[lower:upper]
                    ]
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
