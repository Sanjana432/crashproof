import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import statistics
from time import perf_counter
import uuid
from temporalio.client import Client
from temporalio.worker import Worker
from .activities import OrderActivities
from .config import Settings
from .store import Store
from .workflow import OrderWorkflow


async def run_benchmark(orders=30, concurrency=5, output="benchmark-results/latest.json"):
    if not 1 <= orders <= 1000 or not 1 <= concurrency <= 32:
        raise ValueError("Use 1–1000 orders and concurrency between 1 and 32")
    settings = Settings.from_env()
    run_id = uuid.uuid4().hex[:12]
    database = Path("data")/"benchmarks"/run_id/"business.sqlite3"
    store = Store(database)
    actions = OrderActivities(store)
    client = await Client.connect(settings.address, namespace=settings.namespace)
    queue = "crashproof-bench-"+run_id
    gate = asyncio.Semaphore(concurrency)
    latencies, errors = [], []

    async def single(index):
        async with gate:
            started = perf_counter()
            order, _ = store.create_order(f"bench-{run_id}-{index}",
                {"customer": "Benchmark", "sku": "keyboard", "quantity": 1, "fault": "none", "requires_approval": False})
            try:
                result = await asyncio.wait_for(client.execute_workflow(OrderWorkflow.run, {"order_id": order["id"]},
                                                 id=order["id"], task_queue=queue), 120)
                if result["status"] != "completed":
                    raise RuntimeError(result["status"])
                latencies.append(perf_counter()-started)
            except Exception as error:
                errors.append(type(error).__name__)

    with ThreadPoolExecutor(max_workers=16) as executor:
        async with Worker(client, task_queue=queue, workflows=[OrderWorkflow], activities=actions.registered(), activity_executor=executor):
            started = perf_counter()
            await asyncio.gather(*(single(i) for i in range(orders)))
            elapsed = perf_counter()-started
    ordered = sorted(latencies)
    percentile = lambda p: round(ordered[min(len(ordered)-1, max(0, int((len(ordered)-1)*p)))], 4) if ordered else None
    result = {"at": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
              "scope": "real Temporal workflow + SQLite business operations; excludes HTTP/browser latency",
              "platform": platform.platform(), "python": platform.python_version(),
              "orders": orders, "concurrency": concurrency, "completed": len(latencies), "errors": errors,
              "elapsed_seconds": round(elapsed, 4), "completed_per_second": round(len(latencies)/elapsed, 3),
              "latency_seconds": {"p50": percentile(.5), "p95": percentile(.95), "max": round(max(latencies), 4) if latencies else None},
              "invariants": {"one_charge_per_completed_order": store.snapshot()["metrics"]["active_charges"] == len(latencies)}}
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    if errors:
        raise RuntimeError("Some benchmark workflows failed; inspect the result file")
    return result
