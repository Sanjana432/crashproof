import asyncio
from contextlib import asynccontextmanager, suppress
import json
from pathlib import Path
import re
from typing import Annotated, Literal
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from .config import Settings
from .service import Dispatcher
from .store import BusinessFailure, Conflict, Store
from .workflow import OrderWorkflow
from .telemetry import read_spans

STATIC = Path(__file__).parent/"static"


class OrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    customer: str = Field(min_length=1, max_length=100)
    sku: Literal["keyboard", "headphones", "dock"] = "keyboard"
    quantity: int = Field(default=1, ge=1, le=100, strict=True)
    fault: Literal["none", "payment_retry", "payment_decline", "fulfillment_fail", "worker_crash"] = "none"
    requires_approval: bool = False


class ApprovalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    approved: bool


def create_app(settings=None, dispatcher_override=None):
    settings = settings or Settings.from_env()
    store = Store(settings.database)
    dispatcher = dispatcher_override or Dispatcher(settings, store)

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(dispatcher.loop())
        yield
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    app = FastAPI(title="CrashProof", version="1.0.0", lifespan=lifespan,
                  description="Local Temporal order workflow laboratory. Simulated payments only.")
    app.state.store = store
    app.state.dispatcher = dispatcher
    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC/"index.html")

    @app.get("/api/health")
    async def health():
        try:
            client = await dispatcher.connect()
            healthy = await asyncio.wait_for(client.service_client.check_health(), 2)
        except Exception:
            healthy = False
        return {"api": "ready", "temporal": "connected" if healthy else "unavailable",
                "task_queue": settings.queue, "address": settings.address,
                "note": "A connected server does not imply a worker is running."}

    @app.get("/api/dashboard")
    def dashboard():
        return store.snapshot()

    @app.post("/api/orders", status_code=202)
    async def order(request: OrderRequest, idempotency_key: Annotated[str, Header(min_length=8, max_length=128)]):
        if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", idempotency_key):
            raise HTTPException(400, "Use 8–128 letters, numbers, hyphens or underscores for Idempotency-Key")
        try:
            value, created = store.create_order(idempotency_key, request.model_dump())
        except Conflict as error:
            raise HTTPException(409, str(error)) from error
        except BusinessFailure as error:
            raise HTTPException(400, str(error)) from error
        dispatch = "already dispatched"
        if value["status"] == "accepted":
            try:
                await dispatcher.dispatch(value)
                dispatch = "queued"
            except Exception:
                dispatch = "saved locally; dispatcher will retry when Temporal is available"
        return {"order": store.detail(value["id"]), "created": created, "dispatch": dispatch}

    @app.get("/api/orders/{order_id}")
    def detail(order_id: str):
        try:
            return store.detail(order_id)
        except BusinessFailure as error:
            raise HTTPException(404, str(error)) from error

    @app.post("/api/orders/{order_id}/approval")
    async def approval(order_id: str, request: ApprovalRequest):
        try:
            value = store.detail(order_id)
        except BusinessFailure as error:
            raise HTTPException(404, str(error)) from error
        if value["status"] != "awaiting_approval":
            raise HTTPException(409, "This order is not awaiting approval")
        try:
            client = await dispatcher.connect()
            await client.get_workflow_handle(order_id).signal(OrderWorkflow.approve, request.approved)
        except Exception as error:
            raise HTTPException(503, "Approval could not be delivered. Retry after checking Temporal.") from error
        return {"delivered": True}

    @app.get("/api/orders/{order_id}/history")
    async def history(order_id: str):
        try:
            store.detail(order_id)
        except BusinessFailure as error:
            raise HTTPException(404, str(error)) from error
        try:
            client = await dispatcher.connect()
            result = await client.get_workflow_handle(order_id).fetch_history()
            return {"workflow_id": order_id, "history": json.loads(result.to_json())}
        except Exception as error:
            raise HTTPException(503, "Temporal history is unavailable. The local business event timeline remains available.") from error

    @app.get("/api/orders/{order_id}/spans")
    def spans(order_id: str):
        try:
            store.detail(order_id)
        except BusinessFailure as error:
            raise HTTPException(404, str(error)) from error
        return {"spans": read_spans(settings.database, order_id), "note": "OpenTelemetry business-operation spans; Temporal workflow history is a separate record."}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        counts = store.snapshot()["metrics"]
        lines = ["# Local business-state gauges; simulated payments only."]
        for name, value in counts.items():
            lines.extend([f"# TYPE crashproof_{name} gauge", f"crashproof_{name} {value}"])
        return "\n".join(lines)+"\n"

    return app
