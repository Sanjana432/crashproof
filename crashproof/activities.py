import os
from temporalio import activity
from temporalio.exceptions import ApplicationError
from .store import BusinessFailure, Conflict, Store
from .telemetry import Telemetry


class OrderActivities:
    def __init__(self, store: Store, allow_crash=False):
        self.store = store
        self.allow_crash = allow_crash
        self.telemetry = Telemetry(store.path)

    def guarded(self, function, value):
        with self.telemetry.span(function.__name__, value):
            try:
                return function(value)
            except (BusinessFailure, Conflict) as error:
                raise ApplicationError(str(error), type="BusinessFailure", non_retryable=True) from error

    @activity.defn
    def reserve_inventory(self, order_id: str) -> dict:
        return self.guarded(self.store.reserve, order_id)

    @activity.defn
    def charge_payment(self, order_id: str) -> dict:
        order = self.store.detail(order_id)
        if order["fault"] == "worker_crash" and not self.allow_crash:
            raise ApplicationError("Crash injection is disabled. Start a dedicated worker with --allow-crash.", non_retryable=True)
        result = self.guarded(self.store.charge, order_id)
        if result.get("declined"):
            raise ApplicationError("Simulated payment declined", type="PaymentDeclined", non_retryable=True)
        if result.get("transient_failure"):
            raise RuntimeError("Simulated temporary payment-service failure")
        if result.get("crash"):
            print("CRASHPROOF: charge committed; intentionally terminating this worker before acknowledging the activity.", flush=True)
            os._exit(78)
        return result

    @activity.defn
    def fulfill_order(self, order_id: str) -> dict:
        return self.guarded(self.store.fulfill, order_id)

    @activity.defn
    def refund_payment(self, order_id: str) -> dict:
        return self.guarded(self.store.refund, order_id)

    @activity.defn
    def release_inventory(self, order_id: str) -> dict:
        return self.guarded(self.store.release, order_id)

    @activity.defn
    def inspect_order(self, order_id: str) -> dict:
        order = self.guarded(self.store.detail, order_id)
        return {"fulfilled": bool(order["reservation"] and order["reservation"]["state"] == "consumed")}

    @activity.defn
    def set_order_status(self, request: dict) -> dict:
        with self.telemetry.span("set_status", request["order_id"]):
            try:
                self.store.set_status(request["order_id"], request["status"], request.get("reason", ""))
            except (BusinessFailure, Conflict) as error:
                raise ApplicationError(str(error), non_retryable=True) from error
        return {"status": request["status"]}

    def registered(self):
        return [self.reserve_inventory, self.charge_payment, self.fulfill_order,
                self.refund_payment, self.release_inventory, self.set_order_status, self.inspect_order]
