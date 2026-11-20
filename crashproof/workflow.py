"""Deterministic orchestration only. Every business write is an Activity."""
import asyncio
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError


@workflow.defn
class OrderWorkflow:
    def __init__(self):
        self.approval = None

    @workflow.signal
    def approve(self, approved: bool):
        if self.approval is None:
            self.approval = approved

    async def call(self, name, argument, *, compensation=False):
        return await workflow.execute_activity(name, argument,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=3),
                                     maximum_attempts=10 if compensation else 4), result_type=dict)

    async def state(self, order_id, status, reason=""):
        return await self.call("set_order_status", {"order_id": order_id, "status": status, "reason": reason}, compensation=True)

    async def compensate(self, order_id, reason):
        await self.state(order_id, "compensating", reason)
        observed = await self.call("inspect_order", order_id, compensation=True)
        if observed["fulfilled"]:
            reason = "Fulfillment committed before completion was confirmed; inspect the ledger before manual correction"
            await self.state(order_id, "manual_review", reason)
            return {"order_id": order_id, "status": "manual_review", "reason": reason}
        # Refund even when charge completion was uncertain. The service handles a missing charge.
        await self.call("refund_payment", order_id, compensation=True)
        await self.call("release_inventory", order_id, compensation=True)
        await self.state(order_id, "compensated", reason)
        return {"order_id": order_id, "status": "compensated", "reason": reason}

    @workflow.run
    async def run(self, request: dict) -> dict:
        order_id = request["order_id"]
        try:
            await self.state(order_id, "reserving")
            await self.call("reserve_inventory", order_id)
            if request.get("requires_approval"):
                await self.state(order_id, "awaiting_approval")
                try:
                    await workflow.wait_condition(lambda: self.approval is not None, timeout=timedelta(minutes=15))
                except asyncio.TimeoutError:
                    return await self.compensate(order_id, "Approval timed out")
                if not self.approval:
                    return await self.compensate(order_id, "Order rejected by reviewer")
            await self.state(order_id, "charging")
            await self.call("charge_payment", order_id)
            await self.state(order_id, "fulfilling")
            await self.call("fulfill_order", order_id)
            await self.state(order_id, "completed")
            return {"order_id": order_id, "status": "completed"}
        except ActivityError as error:
            return await self.compensate(order_id, str(error.cause or error)[:300])
