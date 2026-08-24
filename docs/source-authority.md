# Source authority

Evidence was reproduced on 2026-08-24 and is rechecked by the remote canary.

| Source | Controlling role |
|---|---|
| [Polymarket market-data documentation](https://docs.polymarket.com/market-data/overview) and [Gamma event API](https://docs.polymarket.com/api-reference/events/get-event-by-slug) | Exact event/market slug, series, one-hour end boundary, condition, Up/Down tokens, rules, Binance resolution URL, and official outcome. |
| [PMXT v2 overview](https://archive.pmxt.dev/docs/v2-data-overview) and [catalog](https://archive.pmxt.dev/Polymarket/v2) | Credential-free hourly Polymarket market-channel order-book events, accepted only after exact official condition/token filtering. |
| Official market rules linking `https://www.binance.com/en/trade/ASSET_USDT` | Settlement semantics: Up iff the finalized ASSET/USDT one-hour candle close is greater than or equal to its open; otherwise Down. No terminal order-book or cross-venue inference substitutes for the official outcome. |

Hourly slugs are canonical Eastern-Time identities such as
`bitcoin-up-or-down-april-13-2026-4pm-et`, with corresponding full-name prefixes for Ethereum,
Solana, XRP, Dogecoin, BNB, and HYPE. The canary derives each asset's Gamma series from a fresh
slug response, paginates it, and requires all 2,717 expected UTC starts from
2026-04-18T20:00:00Z through 2026-08-10T00:00:00Z. Missing, duplicate, divergent, unresolved, or
non-hour-aligned identities abort the unlock.

The PMXT catalog contains 2,835 objects from 2026-04-13T19 through 2026-08-10T00. The three absent
keys are 2026-06-11T04, T05, and T06; PMXT documents absent empty-hour objects as zero-event hours.
The 19:00 first object supplies the mandatory one-hour causal warm-up, yielding the exclusive
validation cutoff 2026-08-10T01:00:00Z. A catalog-listed object returning 404 is an authority
conflict, not an exclusion.

PMXT `book` rows are full snapshots; `price_change` and `tick_size_change` are incremental. An
unanchored prefix is discarded. No snapshot is `NO_INITIAL_SNAPSHOT`; a first snapshot after the
market starts is `SOURCE_GAP`; a contradictory update after anchoring is `EVENT_CONFLICT`.
