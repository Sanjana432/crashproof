# Demo and troubleshooting

Start the real Temporal dev server, the API and a dedicated worker as described in the README. API and worker processes must use the same `CRASHPROOF_DB` and `CRASHPROOF_TASK_QUEUE`; separate integration-test queues intentionally use different databases.

## Crash experiment

Use `python -m crashproof demo` for a repeatable isolated run. It records the actual first worker exit, restarts a fresh worker, waits for workflow completion and checks exactly one committed charge event. The activity start-to-close timeout is five seconds, so a crash recovery includes an intentional timeout/retry delay.

For the dashboard version, start `python -m crashproof worker --allow-crash`, select **Crash after charge**, submit the order, then restart that worker after it exits. Do not run several dashboard workers on the same queue during this demonstration: another worker could recover automatically before you show the restart.

## Request replay

The replay button preserves the exact submitted payload and idempotency key. Change the request while reusing its key through the API to demonstrate HTTP `409`. Click **New key** before creating a different order from the form.

## Manual approval

A flagged order reserves inventory, then waits for an approval/rejection signal. The browser shows its approval buttons once the persisted status is `awaiting_approval`. A fifteen-minute timeout compensates the reservation. This demonstrates a durable wait; the worker can restart while it waits.

## Troubleshooting

- **Queued orders do not move:** confirm that a worker is running, that it uses the same queue/database as the API, and inspect its output.
- **Temporal unavailable:** start the service on the configured address. Accepted requests remain in the outbox and dispatch on reconnection.
- **Crash experiment is compensated immediately:** the worker was started without `--allow-crash`; restart a dedicated crash-enabled worker and submit a new request key.
- **Order is `manual_review` or `compensating`:** inspect the business ledger and Temporal history before making a correction. The application does not provide an automatic reset that hides an unfinished recovery.
- **Temporal database fails to open:** create the parent `data` directory before starting the dev server.

Interview discussion: explain the difference between at-least-once activity execution and an idempotent effect, the outbox dispatch gap, compensation versus rollback, why metrics need a stated workload, and which guarantees depend on preserving both databases.
