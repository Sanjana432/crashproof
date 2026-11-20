import asyncio
from concurrent.futures import ThreadPoolExecutor
from temporalio.client import Client
from temporalio.worker import Worker
from .activities import OrderActivities
from .config import Settings
from .store import Store
from .workflow import OrderWorkflow


async def run_worker(allow_crash=False):
    settings = Settings.from_env()
    client = await Client.connect(settings.address, namespace=settings.namespace)
    actions = OrderActivities(Store(settings.database), allow_crash=allow_crash)
    print(f"Worker listening on {settings.queue}; crash injection {'enabled' if allow_crash else 'disabled'}.", flush=True)
    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(client, task_queue=settings.queue, workflows=[OrderWorkflow],
                        activities=actions.registered(), activity_executor=executor,
                        max_concurrent_activities=16)
        await worker.run()
