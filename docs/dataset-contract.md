# Dataset contract

Dataset identity is `polymarket-1h-seven-v1`. The universe is exactly BTC, ETH, SOL, XRP, DOGE,
BNB, and HYPE; timeframe is exactly `1h`; partition identity is `asset/1h/UTC-date`.

The frozen interval is `[2026-04-18T20:00:00Z, 2026-08-10T01:00:00Z)`. It contains 2,717 market
starts per asset across 115 UTC dates and therefore 805 finite asset/day partitions. Partial boundary
days are canonical partitions.

Every partition publishes exactly six content-addressed assets: markets, native book events, 200 ms
book samples, underlying observations, exclusions, and manifest. A partition is durable only when
the exact asset set, embedded identities, byte lengths, and SHA-256 names verify by authenticated
redownload. Production and canary Release namespaces are disjoint.

Completeness is determined only from remote Releases. Repository ledgers and certification reports
are derived checkpoints. Final certification requires all planned partitions, exact release groups,
reconciled TIER_A/TIER_B/EXCLUDED totals, and zero authority anomalies.
