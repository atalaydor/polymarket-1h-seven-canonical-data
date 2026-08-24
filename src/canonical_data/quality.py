"""Fail-closed 1-hour quality and exclusion classification."""

from __future__ import annotations

from canonical_data.models import Exclusion, ExclusionReason, Market, QualityTier


def classify(
    market: Market, has_pmxt: bool, gaps: list[tuple[int, int]]
) -> tuple[QualityTier, Exclusion | None]:
    if market.timeframe != "1h":
        return _excluded(market, ExclusionReason.UNSUPPORTED_TIMEFRAME, "only 1h is frozen")
    if market.official_outcome.value == "UNRESOLVED":
        return _excluded(market, ExclusionReason.UNRESOLVED_MARKET, "official outcome unresolved")
    if has_pmxt and not gaps:
        return QualityTier.TIER_A, None
    reason = (
        ExclusionReason.SOURCE_GAP if gaps or not has_pmxt else ExclusionReason.NO_INITIAL_SNAPSHOT
    )
    evidence = None
    if gaps:
        evidence = {
            "gap_count": len(gaps),
            "gap_intervals_ns": [[start, end] for start, end in gaps],
            "gap_total_ns": sum(end - start for start, end in gaps),
        }
    return _excluded(
        market,
        reason,
        "PMXT fidelity cannot satisfy the frozen 1h tier",
        evidence,
    )


def _excluded(
    market: Market,
    reason: ExclusionReason,
    detail: str,
    extra_evidence: dict[str, object] | None = None,
) -> tuple[QualityTier, Exclusion]:
    return QualityTier.EXCLUDED, Exclusion(
        market.market_id,
        reason,
        detail,
        {
            "condition_id": market.condition_id,
            "market_evidence_sha256": market.evidence_sha256,
            "official_outcome": market.official_outcome.value,
            "resolution_source_url": market.resolution_source_url,
            **(extra_evidence or {}),
        },
    )
