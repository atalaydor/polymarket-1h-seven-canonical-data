"""Official Gamma discovery and authority binding."""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from zoneinfo import ZoneInfo

from canonical_data.audit import canonical_json_bytes
from canonical_data.errors import IdentityError, UnresolvedMarketError
from canonical_data.httpclient import USER_AGENT
from canonical_data.models import Asset, Market, Outcome, QualityTier

SLUG = re.compile(
    r"^(bitcoin|ethereum|solana|xrp|dogecoin|bnb|hype)-up-or-down-"
    r"([a-z]+)-([0-9]{1,2})-([0-9]{4})-([0-9]{1,2})(am|pm)-et$"
)
CONDITION = re.compile(r"^0x[0-9a-f]{64}$")
BINANCE_TRADE = re.compile(
    r"^https://www\.binance\.com/en/trade/(BTC|ETH|SOL|XRP|DOGE|BNB|HYPE)_USDT$"
)
EASTERN = ZoneInfo("America/New_York")
SLUG_NAMES = {
    Asset.BTC: "bitcoin",
    Asset.ETH: "ethereum",
    Asset.SOL: "solana",
    Asset.XRP: "xrp",
    Asset.DOGE: "dogecoin",
    Asset.BNB: "bnb",
    Asset.HYPE: "hype",
}
SLUG_ASSETS = {name: asset for asset, name in SLUG_NAMES.items()}
ASSET_RULE_NAMES = {
    Asset.BTC: ("btc", "bitcoin"),
    Asset.ETH: ("eth", "ethereum", "ether"),
    Asset.SOL: ("sol", "solana"),
    Asset.XRP: ("xrp", "ripple"),
    Asset.DOGE: ("doge", "dogecoin"),
    Asset.BNB: ("bnb", "binance coin"),
    Asset.HYPE: ("hype", "hyperliquid"),
}

FetchPayload = Callable[[str, int], bytes]


def hourly_slug(asset: Asset, market_start_s: int) -> str:
    start = datetime.fromtimestamp(market_start_s, UTC)
    if start.minute or start.second or start.microsecond:
        raise IdentityError("1h market start is not hour aligned")
    local = start.astimezone(EASTERN)
    hour = local.hour % 12 or 12
    meridiem = "am" if local.hour < 12 else "pm"
    return (
        f"{SLUG_NAMES[asset]}-up-or-down-{local.strftime('%B').lower()}-"
        f"{local.day}-{local.year}-{hour}{meridiem}-et"
    )


def _slug_identity(slug: str) -> tuple[Asset, int]:
    match = SLUG.fullmatch(slug)
    if match is None:
        raise IdentityError("unsupported 1h market slug")
    asset = SLUG_ASSETS[match.group(1)]
    month_name, day, year, hour, meridiem = match.groups()[1:]
    try:
        local = datetime.strptime(
            f"{month_name} {day} {year} {hour}{meridiem}", "%B %d %Y %I%p"
        ).replace(tzinfo=EASTERN)
    except ValueError as exc:
        raise IdentityError("invalid 1h market slug timestamp") from exc
    start_s = int(local.astimezone(UTC).timestamp())
    if hourly_slug(asset, start_s) != slug:
        raise IdentityError("1h market slug is not canonical")
    return asset, start_s


def _bounded_fetch(url: str, max_bytes: int) -> bytes:
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        length = response.headers.get("Content-Length")
        if length is not None and int(length) > max_bytes:
            raise IdentityError("Gamma payload exceeds configured bound")
        payload = cast(bytes, response.read(max_bytes + 1))
    if len(payload) > max_bytes:
        raise IdentityError("Gamma payload exceeds configured bound")
    return payload


class GammaClient:
    def __init__(self, fetch: FetchPayload = _bounded_fetch, max_payload_bytes: int = 1_000_000):
        self.fetch = fetch
        self.max_payload_bytes = max_payload_bytes

    def fetch_slug_payload(self, asset: Asset, market_start_s: int) -> tuple[bytes, str]:
        slug = hourly_slug(asset, market_start_s)
        url = f"https://gamma-api.polymarket.com/events/slug/{urllib.parse.quote(slug)}"
        payload = self.fetch(url, self.max_payload_bytes)
        return payload, url

    def fetch_market(self, asset: Asset, market_start_s: int) -> tuple[Market, bytes, str]:
        payload, url = self.fetch_slug_payload(asset, market_start_s)
        markets = discover([payload])
        if len(markets) != 1:
            raise IdentityError("Gamma slug lookup did not return exactly one official market")
        return markets[0], payload, url


def _json_list(value: Any, name: str) -> list[Any]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        raise IdentityError(f"{name} must be a list")
    return parsed


def _official_outcome(prices: list[Any]) -> Outcome:
    values = [Decimal(str(item)) for item in prices]
    if values == [Decimal(1), Decimal(0)]:
        return Outcome.UP
    if values == [Decimal(0), Decimal(1)]:
        return Outcome.DOWN
    if values == [Decimal("0.5"), Decimal("0.5")]:
        return Outcome.SPLIT
    raise IdentityError("closed market lacks an unambiguous official outcome")


def _rules_bind_source(asset: Asset, rules: str, source_url: object) -> str:
    """Bind the exact Binance 1-hour candle named by the controlling rules."""
    declared = source_url.strip().rstrip("/") if isinstance(source_url, str) else ""
    rules_lower = rules.lower()
    rules_asset_identity = any(
        re.search(rf"(?<![a-z0-9]){re.escape(name)}(?![a-z0-9])", rules_lower)
        for name in ASSET_RULE_NAMES[asset]
    )
    match = BINANCE_TRADE.fullmatch(declared)
    expected_pair = f"{asset.value.lower()}/usdt"
    if (
        match is not None
        and match.group(1) == asset.value
        and rules_asset_identity
        and expected_pair in rules_lower
        and "1 hour candle" in rules_lower
        and "binance" in rules_lower
    ):
        return declared
    rules_digest = hashlib.sha256(rules.encode()).hexdigest()
    excerpt = " ".join(rules.split())[:500]
    raise IdentityError(
        "rules do not bind the frozen Binance 1-hour candle: "
        f"asset={asset.value} resolutionSource={declared!r} "
        f"rules_sha256={rules_digest} rules_excerpt={excerpt!r}"
    )


def bind_gamma_market(event: dict[str, Any], retrieved_payload: bytes) -> Market:
    markets = event.get("markets")
    if not isinstance(markets, list) or len(markets) != 1 or not isinstance(markets[0], dict):
        raise IdentityError("event must contain exactly one market")
    raw = markets[0]
    slug = raw.get("slug")
    if not isinstance(slug, str):
        raise IdentityError("market slug is missing")
    asset, start_s = _slug_identity(slug)
    start_ns = start_s * 1_000_000_000
    end_ns = start_ns + 3_600_000_000_000
    expected_end = datetime.fromtimestamp(start_s + 3_600, UTC)
    raw_end = raw.get("endDate")
    if not isinstance(raw_end, str):
        raise IdentityError("official 1h market end is missing")
    parsed_end = datetime.fromisoformat(raw_end.replace("Z", "+00:00"))
    if parsed_end.tzinfo is None or parsed_end.astimezone(UTC) != expected_end:
        raise IdentityError("official market end does not match the slug's 1-hour window")
    outcomes = [str(value).upper() for value in _json_list(raw.get("outcomes"), "outcomes")]
    tokens = [str(value) for value in _json_list(raw.get("clobTokenIds"), "clobTokenIds")]
    if (
        outcomes != ["UP", "DOWN"]
        or len(tokens) != 2
        or not all(token.isdigit() for token in tokens)
    ):
        raise IdentityError("outcome/token mapping must be exactly Up,Down")
    condition_id = raw.get("conditionId")
    if not isinstance(condition_id, str) or CONDITION.fullmatch(condition_id) is None:
        raise IdentityError("invalid condition id")
    rules = raw.get("description")
    source_url = raw.get("resolutionSource")
    if not isinstance(rules, str) or not rules.strip():
        raise IdentityError("rules are missing")
    bound_source_url = _rules_bind_source(asset, rules, source_url)
    if "greater than or equal" not in rules.lower() or "otherwise" not in rules.lower():
        raise IdentityError("rules do not prove Up/Down comparison semantics")
    event_id = str(event.get("id", ""))
    market_id = str(raw.get("id", ""))
    if not event_id or not market_id:
        raise IdentityError("official ids are missing")
    if raw.get("closed") is not True:
        raise UnresolvedMarketError(slug, market_id, condition_id)
    outcome = _official_outcome(_json_list(raw.get("outcomePrices"), "outcomePrices"))
    evidence_digest = hashlib.sha256(retrieved_payload).hexdigest()
    rules_digest = hashlib.sha256(rules.encode()).hexdigest()
    return Market(
        asset=asset,
        event_id=event_id,
        market_id=market_id,
        condition_id=condition_id,
        token_up=tokens[0],
        token_down=tokens[1],
        market_start_ns=start_ns,
        market_end_ns=end_ns,
        rules_text_sha256=rules_digest,
        resolution_source_url=bound_source_url,
        official_outcome=outcome,
        official_resolution_ts_ns=None,
        quality_tier=QualityTier.TIER_A,
        evidence_sha256=evidence_digest,
    )


def discover(payloads: Iterable[bytes]) -> list[Market]:
    markets: list[Market] = []
    seen: dict[str, bytes] = {}
    for payload in payloads:
        decoded = json.loads(payload)
        events = decoded if isinstance(decoded, list) else [decoded]
        for event in events:
            if not isinstance(event, dict):
                raise IdentityError("Gamma response contains a non-object")
            canonical = canonical_json_bytes(event)
            market = bind_gamma_market(event, payload)
            previous = seen.get(market.condition_id)
            if previous is not None and previous != canonical:
                raise IdentityError("conflicting official market identity")
            seen[market.condition_id] = canonical
            markets.append(market)
    return sorted(
        {market.condition_id: market for market in markets}.values(),
        key=lambda item: item.condition_id,
    )
