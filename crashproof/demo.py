"""A real worker-process death, after a committed side effect and before ACK."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from time import perf_counter
import uuid
from temporalio.client import Client
from .config import Settings
from .store import Store
from .workflow import OrderWorkflow


async def run_demo(output="data/demo-result.json"):
    settings = Settings.from_env()
    run_id = uuid.uuid4().hex[:12]
    folder = Path("data")/"crash-demos"/run_id
    folder.mkdir(parents=True, exist_ok=True)
    database = (folder/"business.sqlite3").resolve()
    queue = "crashproof-demo-"+run_id
    store = Store(database)
    order, _ = store.create_order("demo-"+run_id,
        {"customer": "Crash demo", "sku": "keyboard", "quantity": 1, "fault": "worker_crash", "requires_approval": False})
    client = await Client.connect(settings.address, namespace=settings.namespace)
    env = {**os.environ, "CRASHPROOF_DB": str(database), "CRASHPROOF_TASK_QUEUE": queue}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1]) + os.pathsep + env.get("PYTHONPATH", "")
    worker = None
    started = perf_counter()
    log = (folder/"worker.log").open("w", encoding="utf-8")
    try:
        worker = subprocess.Popen([sys.executable, "-m", "crashproof", "worker", "--allow-crash"], env=env, stdout=log, stderr=subprocess.STDOUT)
        handle = await client.start_workflow(OrderWorkflow.run, {"order_id": order["id"]}, id=order["id"], task_queue=queue)
        exit_code = await asyncio.wait_for(asyncio.to_thread(worker.wait), 40)
        if exit_code != 78:
            raise RuntimeError(f"Expected intentional worker exit 78; got {exit_code}. See {folder/'worker.log'}")
        at_crash = store.detail(order["id"])
        if not at_crash["payment"] or at_crash["payment"]["state"] != "charged":
            raise AssertionError("The side effect was not committed at the crash boundary")
        print("Worker exited after the charge committed. Restarting a fresh worker…", flush=True)
        worker = subprocess.Popen([sys.executable, "-m", "crashproof", "worker", "--allow-crash"], env=env, stdout=log, stderr=subprocess.STDOUT)
        result = await asyncio.wait_for(handle.result(), 45)
        final = store.detail(order["id"])
        counts = Counter(e["kind"] for e in final["events"])
        assert result["status"] == "completed", result
        assert counts["payment_charged"] == 1, counts
        assert counts["charge_deduplicated"] >= 1, counts
        record = {"run_id": run_id, "at": datetime.now(timezone.utc).isoformat(),
                  "scenario": "actual worker exit after committed simulated charge, followed by fresh-worker recovery",
                  "workflow_id": order["id"], "first_worker_exit_code": exit_code,
                  "elapsed_seconds": round(perf_counter()-started, 3), "result": result,
                  "charge_effects": counts["payment_charged"], "deduplicated_charge_attempts": counts["charge_deduplicated"],
                  "business_database": str(folder/"business.sqlite3"), "events": final["events"]}
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(record, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in record.items() if k != "events"}, indent=2))
        print(f"Evidence written to {target}")
        return record
    finally:
        if worker and worker.poll() is None:
            worker.terminate()
            await asyncio.wait_for(asyncio.to_thread(worker.wait), 10)
        log.close()
