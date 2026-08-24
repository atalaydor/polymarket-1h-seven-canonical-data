# Data quality policy

`TIER_A` requires a fresh official Gamma hourly identity, exact 3,600-second end boundary, Up/Down
token mapping, resolved official outcome, rules-bound Binance ASSET/USDT one-hour candle, and a
causally anchored gap-free PMXT reconstruction.

`TIER_B` is reserved in the schema but has no production admission rule in version 1.0; its expected
count is zero. It cannot be inferred from weaker venue data.

`EXCLUDED` is a durable canonical outcome when exact evidence establishes an allowed failure such as
`NO_INITIAL_SNAPSHOT`, `SOURCE_GAP`, `EVENT_CONFLICT`, or an officially unresolved market. Unknown
identity, wrong rules, wrong interval, missing catalog-listed source, or ambiguous outcome is an
authority conflict and aborts rather than becoming an exclusion.

Sampling is strictly as-of receive time on the 200 ms grid. Events after a grid point or market end
are never visible. Exclusions serialize the exact causal gap or conflicting token/object/row evidence.
