# Storage and execution architecture

```text
remote Releases -> exact unfinished asset/day authority
                -> four parallel six-hour causal compute segments per day
                -> authenticated one-day transient fragment checkpoints
                -> deterministic day assembly outside release locks
                -> authenticated transient partition bundle
                -> short release-group single-writer mutation and redownload
                -> remote reconciliation/no-op -> transient input deletion
```

Each segment owns six disjoint hourly market starts and includes its own one-hour warm-up. It scans
at most seven PMXT objects. Four days may execute concurrently, so at most 16 expensive read-only
compute jobs run in parallel. Only the final Release mutation and verification use a release-group
concurrency lock.

Source objects are streamed once per segment across all unfinished assets. Markets finalize as soon
as their causal end object closes. Only compressed condition-keyed fragments survive; full-day raw
materialization, late duplicate indexes, and permanent source spools are forbidden. Segment
receipts bind the run, day, exact start cover, source identities, commit, byte lengths, and SHA-256
digests. Assembly rejects missing, overlapping, substituted, unexpected, or out-of-plan inputs.

GitHub Releases plus embedded manifests are permanent authority. Actions artifacts are authenticated
transport with one-day retention only. The planner redownloads remote state, makes durable matches
no-ops, resumes compatible partial uploads, and fails on duplicates, divergence, partial sets,
unexpected files, or wrong release groups.
