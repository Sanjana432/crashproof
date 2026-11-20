import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import Worker
from crashproof.activities import OrderActivities
from crashproof.config import Settings
from crashproof.service import Dispatcher
from crashproof.store import Store
from crashproof.telemetry import read_spans
from crashproof.workflow import OrderWorkflow


@unittest.skipUnless(os.getenv("TEMPORAL_ADDRESS"), "Set TEMPORAL_ADDRESS to run real Temporal integration tests")
class TemporalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name)/"business.sqlite3")
        self.queue = "crashproof-test-"+uuid.uuid4().hex
        self.settings = Settings(address=os.environ["TEMPORAL_ADDRESS"], queue=self.queue, database=self.store.path)
        self.client = await Client.connect(self.settings.address)
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.actions = OrderActivities(self.store)
        self.worker = Worker(self.client, task_queue=self.queue, workflows=[OrderWorkflow], activities=self.actions.registered(), activity_executor=self.executor)
        await self.worker.__aenter__()

    async def asyncTearDown(self):
        await self.worker.__aexit__(None, None, None)
        self.executor.shutdown(wait=True)
        self.temp.cleanup()

    def create(self, fault="none", requires_approval=False):
        order, _ = self.store.create_order("integration-"+uuid.uuid4().hex,
            {"customer":"Integration", "sku":"keyboard", "quantity":1, "fault":fault, "requires_approval":requires_approval})
        return order

    async def execute(self, fault="none"):
        order = self.create(fault)
        result = await asyncio.wait_for(self.client.execute_workflow(OrderWorkflow.run, {"order_id":order["id"]}, id=order["id"], task_queue=self.queue), 30)
        return order, result, self.store.detail(order["id"])

    async def test_happy_path_records_real_spans(self):
        order, result, detail = await self.execute()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(detail["reservation"]["state"], "consumed")
        self.assertTrue(any(span["name"] == "business.charge" for span in read_spans(self.store.path, order["id"])))

    async def test_transient_payment_is_retried_by_temporal(self):
        _, result, detail = await self.execute("payment_retry")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(sum(e["kind"] == "payment_attempt" for e in detail["events"]), 2)
        self.assertEqual(sum(e["kind"] == "payment_charged" for e in detail["events"]), 1)

    async def test_declined_payment_releases_inventory(self):
        _, result, detail = await self.execute("payment_decline")
        self.assertEqual(result["status"], "compensated")
        self.assertIsNone(detail["payment"])
        self.assertEqual(detail["reservation"]["state"], "released")

    async def test_fulfillment_failure_refunds_and_releases(self):
        _, result, detail = await self.execute("fulfillment_fail")
        self.assertEqual(result["status"], "compensated")
        self.assertEqual(detail["payment"]["state"], "refunded")
        self.assertEqual(detail["reservation"]["state"], "released")

    async def test_manual_approval_signal(self):
        order = self.create(requires_approval=True)
        handle = await self.client.start_workflow(OrderWorkflow.run, {"order_id":order["id"], "requires_approval": True}, id=order["id"], task_queue=self.queue)
        for _ in range(100):
            if self.store.detail(order["id"])["status"] == "awaiting_approval":
                break
            await asyncio.sleep(.05)
        await handle.signal(OrderWorkflow.approve, True)
        result = await asyncio.wait_for(handle.result(), 20)
        self.assertEqual(result["status"], "completed")

    async def test_dispatch_rejects_workflow_id_reuse_after_completion(self):
        order, _, _ = await self.execute()
        dispatcher = Dispatcher(self.settings, self.store)
        dispatcher.client = self.client
        await dispatcher.dispatch(order)
        self.assertEqual(self.store.detail(order["id"])["status"], "completed")
        with self.assertRaises(WorkflowAlreadyStartedError):
            await self.client.start_workflow(OrderWorkflow.run, {"order_id":order["id"]}, id=order["id"], task_queue=self.queue, id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE)
