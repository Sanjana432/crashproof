import asyncio
from datetime import timedelta
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from .workflow import OrderWorkflow


class Dispatcher:
    """Persistent accepted orders are a local transactional outbox.

    The API retries these rows after restart. A deterministic workflow ID prevents
    duplicate workflow starts across retries, dispatch races and API restarts.
    """
    def __init__(self, settings, store):
        self.settings = settings
        self.store = store
        self.client = None

    async def connect(self):
        if self.client is None:
            self.client = await asyncio.wait_for(Client.connect(self.settings.address, namespace=self.settings.namespace), 3)
        return self.client

    async def dispatch(self, order):
        client = await self.connect()
        try:
            await client.start_workflow(OrderWorkflow.run,
                {"order_id": order["id"], "requires_approval": bool(order["requires_approval"])},
                id=order["id"], task_queue=self.settings.queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                execution_timeout=timedelta(hours=1))
        except WorkflowAlreadyStartedError:
            pass
        self.store.mark_queued(order["id"])

    async def loop(self):
        while True:
            for order in self.store.pending():
                try:
                    await self.dispatch(order)
                except Exception:
                    # Keep the accepted row intact and retry after reconnect/restart.
                    self.client = None
                    break
            await asyncio.sleep(1)
