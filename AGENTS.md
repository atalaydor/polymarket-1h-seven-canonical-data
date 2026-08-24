# Repository instructions

- This repository contains only canonical historical-data tooling, contracts, evidence, and Releases for actual Polymarket 1-hour Up/Down markets for BTC, ETH, SOL, XRP, DOGE, BNB, and HYPE.
- Never add credentials, wallets, private strategy material, other market durations, other assets, unrelated venues, or live-trading functionality.
- GitHub Actions is limited to finite public historical processing on free standard public-repository runners. Paid runners, private minutes, perpetual jobs, and Actions artifacts or caches as canonical authority are prohibited.
- PMXT inputs are transient and must never be committed or retained on Windows. Published partitions are immutable and content-addressed.
- Source claims require a URL and access date. Fresh official Gamma identity, rules, token mapping, end time, and outcome control. Rules bind the exact Binance ASSET/USDT one-hour candle.
- Run unit tests, Ruff, strict mypy, workflow validation, and `git diff --check` for production changes.
