# Polymarket 1-hour seven-asset canonical data

This public repository is the independent control plane and permanent GitHub authority for
historical Polymarket 1-hour Up/Down markets for BTC, ETH, SOL, XRP, DOGE, BNB, and HYPE.

The frozen source interval is 2026-04-18T20:00:00Z through the exclusive
2026-08-10T01:00:00Z cutoff: 2,717 hourly market starts per asset and 805 asset/UTC-day
partitions. The first PMXT object is causal warm-up only. Every official hourly identity is
reproduced from its asset-specific Gamma series before production can unlock.

Production runs as four isolated six-hour causal segments per day. Authenticated transient
fragments are assembled deterministically, then a short release-group writer publishes immutable
content-addressed assets. GitHub Releases are canonical; local source files, Actions artifacts,
and repository ledgers are not.

See [source authority](docs/source-authority.md), [dataset contract](docs/dataset-contract.md),
[quality policy](docs/data-quality-policy.md), and [operations](docs/pipeline-operations.md).
