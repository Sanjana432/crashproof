# Recorded development validation

Validation date: **2026-11-20**. Environment: Windows 11, Python 3.12.14, Temporal CLI 1.10.1, Temporal server 1.32.2 and Python SDK 1.36.1. Direct tested dependency versions are recorded in `constraints-tested.txt`.

## Automated checks

**26 tests passed:** 16 transactional business-state tests, 4 HTTP/interface contract tests and 6 integrations against an actual local Temporal service. The integration tests use isolated queues and business databases, not a fake workflow scheduler.

Coverage includes concurrent request deduplication, no inventory oversell, duplicate charge suppression, refund/release idempotency, payload-key mismatch rejection, manual approval, actual Temporal activity retries and compensation. A regression test verifies that late state-update activities cannot reopen an order after compensation begins. Another verifies that completed workflow IDs cannot be reused by dispatch retries.

Browser verification exercised a completed workflow, an identical-request replay and a fulfillment rejection with refund and inventory release. Desktop and 390-pixel mobile layouts were checked; captured previews are included in this directory.

## Actual process-crash evidence

The automated demonstration launched a dedicated worker subprocess, committed a simulated charge, exited that process with code `78` before activity acknowledgement, then launched a fresh worker. The workflow recovered and completed with **one committed charge event and one deduplicated charge attempt**. The recorded elapsed time includes process startup, the configured activity timeout and recovery.

See [raw crash-recovery record](validation/crash-recovery.json). This is a real process exit, not a mocked exception or a generated transcript.

## Measured workload

See [raw benchmark record](validation/benchmark.json). The recorded run submitted 30 workflows with concurrency 5 against a local Temporal service and an isolated SQLite business database. All 30 completed; there were no reported workflow errors, and the charge-count invariant passed. Recorded workflow latency was p50 **2.4428 seconds** and p95 **3.2292 seconds**; total measured throughput was **1.939 completed workflows/second**.

These measurements describe this development workload and machine. They exclude HTTP and browser latency and are not a production capacity claim. Rerun `python -m crashproof benchmark` to measure another environment. No PostgreSQL adapter, cloud deployment, real payment provider or multi-host durability benchmark was exercised.

## CI

GitHub Actions installs a real Temporal service and runs the tests plus the worker crash/recovery demonstration on Python 3.11 and 3.12. The repository Actions page records the remote outcome; local results do not substitute for that check.
