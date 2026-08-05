# CLI-owned apply / sync progress narrative extraction

This project does **not** pursue a refactor that moves remaining human-readable apply / Incremental Capture progress, ALERT, quarantine, and backpressure companion lines out of the Deployment runtime into the CLI adapter as the sole formatter.

## Why this is out of scope

Structured operator events and Observability assembly already cover the locality problem that motivated the optional slice:

- Runtime emits structured JSON via `emit_event` for Initial Load, Incremental Capture, Delivery, Backpressure, Poison quarantine, Schema Change block, and Platform Store disk warn.
- CLI `status` and Prometheus consume one `assemble_observability_surface` — no forked health math (ADR-0008, issue #174).
- Handbook documents intentional dual emission: structured JSON lines **plus** human-readable companions from the process logs.

Forcing the leftover println → CLI move would invent a continuous-run event sink, rewrite RQG / Lab scrapers that assert Operator wording, and churn three handbook locales without changing Operator-visible outcomes. Parent #199 explicitly allowed declining this last optional slice when stronger candidates removed the pain.

If dual-format drift or println coupling becomes an active blocker, reopen with concrete breakage evidence rather than a pure ownership refactor.

## Prior requests

- #209 — "Optional: structured Operator progress / ALERT events" (parent #199)
