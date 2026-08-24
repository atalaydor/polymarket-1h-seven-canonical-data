# Pipeline operations

The `canary` dispatch first reproduces the complete seven-series market inventory and PMXT catalog.
It then selects the newest common resolved window, retrieves only its two causal PMXT objects, runs
all seven partitions, publishes to the isolated canary namespace, redownloads every asset, repeats
verification as an authenticated no-op, and commits the authority receipt.

Production is launched through the accelerated bounded-batch workflow. Its explicit matrix is
validated against the fresh remote inventory, expected release groups, exact unfinished assets, and
the frozen plan. Four days run in parallel; each day fans out to four compute segments, assembles,
then publishes through the short release lock.

Interruption is safe at acquisition, fragment, bundle, file-upload, or partition boundaries. A
relaunch derives unfinished authority from Releases rather than trusting a local ledger. Bounded
retry applies only to transient transport failures. Semantic conflicts and resource breakers fail
closed. Final certification is allowed only after all 805 planned partitions reconcile with no
duplicate, divergent, partial, unexpected, or out-of-plan authority.
