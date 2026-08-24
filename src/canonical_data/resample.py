"""Causal receive-time-as-of 200 ms derivation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from canonical_data.errors import ReconstructionError
from canonical_data.models import NS_PER_MS, BookState, Market, Sample200ms


def resample_200ms(
    market: Market, states: Iterable[BookState]
) -> tuple[list[Sample200ms], list[tuple[int, int]]]:
    by_token: dict[str, list[BookState]] = defaultdict(list)
    for state in states:
        if state.condition_id != market.condition_id:
            raise ReconstructionError("state condition does not match market")
        by_token[state.token_id].append(state)
    expected_tokens = {market.token_up, market.token_down}
    if set(by_token) - expected_tokens:
        raise ReconstructionError("state contains an unknown outcome token")
    samples: list[Sample200ms] = []
    gaps: list[tuple[int, int]] = []
    interval_ns = 200 * NS_PER_MS
    for token in sorted(expected_tokens):
        token_states = sorted(
            by_token.get(token, []),
            key=lambda state: (
                state.receive_ts_ns if state.receive_ts_ns is not None else state.source_ts_ns,
                state.sequence,
            ),
        )
        index = 0
        current: BookState | None = None
        gap_start: int | None = None
        for grid in range(market.market_start_ns, market.market_end_ns, interval_ns):
            while index < len(token_states):
                candidate = token_states[index]
                effective_ts = (
                    candidate.receive_ts_ns
                    if candidate.receive_ts_ns is not None
                    else candidate.source_ts_ns
                )
                if effective_ts > grid:
                    break
                current = token_states[index]
                index += 1
            if current is None:
                if gap_start is None:
                    gap_start = grid
                continue
            if gap_start is not None:
                gaps.append((gap_start, grid))
                gap_start = None
            current_effective_ts = (
                current.receive_ts_ns if current.receive_ts_ns is not None else current.source_ts_ns
            )
            stale_ms = (grid - current_effective_ts) // NS_PER_MS
            if stale_ms < 0:
                raise ReconstructionError("lookahead detected")
            samples.append(
                Sample200ms(
                    condition_id=market.condition_id,
                    token_id=token,
                    grid_ts_ns=grid,
                    asof_source_ts_ns=current.source_ts_ns,
                    asof_receive_ts_ns=current.receive_ts_ns,
                    bids=current.bids,
                    asks=current.asks,
                    stale_ms=stale_ms,
                    source_event_sequence=current.sequence,
                    quality_tier=current.quality_tier,
                    native_interval_ms=current.native_interval_ms,
                )
            )
        if gap_start is not None:
            gaps.append((gap_start, market.market_end_ns))
    return samples, gaps
