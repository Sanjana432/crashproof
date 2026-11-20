# Architecture and correctness boundaries

## Two independent durable records

Temporal owns deterministic workflow history. The SQLite business database owns order requests, inventory, reservations, simulated charges and business events. A durable workflow cannot make an arbitrary external payment API exactly-once. The payment service must accept a stable idempotency key and return the previously committed result when an activity repeats.

In this laboratory, each business effect is keyed by `order_id`. SQLite uniqueness constraints and `BEGIN IMMEDIATE` transactions serialize changes. Inventory decrements use a conditional update, so concurrent reservations cannot reduce stock below zero. Prices come from the server catalog, not the request.

## Closing the API-to-workflow dispatch gap

The order request and its payload hash are inserted together. An `accepted` row is the outbox entry. The API attempts immediate dispatch; a background loop keeps retrying outstanding accepted rows after failures or an API restart. The workflow ID is deterministically derived from the request key, and `REJECT_DUPLICATE` prevents another execution even after the original workflow closes.

The database is marked queued only when the order is still accepted. A worker that has already advanced the status cannot have its status overwritten by a delayed dispatcher acknowledgement. Outbox workers can race safely because Temporal enforces workflow-ID uniqueness and all business effects are idempotent.

## Real commit-boundary crash

The payment transaction writes the charge and consumes the fault flag atomically. After that transaction commits, a crash-enabled dedicated worker calls `os._exit(78)` before returning the activity result. There is therefore a real committed side effect and no acknowledged activity result. A fresh worker polls the same Temporal task queue, the timed-out activity retries, and the existing charge is returned without another side effect.

The flag is opt-in and only terminates the worker process that was started for the experiment. The automated demo uses an isolated task queue and database. It does not kill arbitrary processes or the API/Temporal service.

## Saga and uncertain outcomes

On activity failure, the workflow marks the order compensating, inspects the fulfillment state and performs idempotent refund/release actions. Refund is attempted even when charge completion was uncertain; a missing charge is harmless. Late reservation, charge and fulfillment calls cannot create new effects after compensation begins. Consumed fulfillment is not automatically reversed: that situation enters `manual_review` for operator investigation.

Compensation activities have bounded retries. If they remain unavailable, the workflow can fail while the local order remains `compensating`. This is deliberate evidence of unfinished recovery, not a claim that all failures are automatically solved. Workflow termination and deletion of durable state are outside the automatic recovery guarantee.

## Actual telemetry and evidence

The OpenTelemetry SDK creates spans for business operations and records order IDs and activity attempts. All operations for an order use an order-correlated trace context across worker restarts. The lightweight JSON-lines exporter stores real completed spans locally. These are business-operation spans, not a complete distributed trace of every Temporal server RPC.

The event timeline is a transactional business audit trail. Temporal workflow history is the engine's execution record. Both are kept distinct from OpenTelemetry spans. `/metrics` provides gauges derived from current business state.

## Primary references

- [Temporal durable execution](https://docs.temporal.io/durable-execution)
- [Temporal activity retries and idempotency](https://docs.temporal.io/activities)
- [Python SDK](https://docs.temporal.io/develop/python)
- [OpenTelemetry Python instrumentation](https://opentelemetry.io/docs/languages/python/instrumentation/)
