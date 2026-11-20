"""Actual OpenTelemetry SDK spans, exported locally without a collector."""
from contextlib import contextmanager
import json
from pathlib import Path
from threading import Lock
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags


class LocalSpanExporter(SpanExporter):
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = Lock()

    def export(self, spans):
        # Telemetry failure must not turn a committed business write into a retry.
        try:
            with self.lock, self.path.open("a", encoding="utf-8") as output:
                for span in spans:
                    record = {"trace_id": f"{span.context.trace_id:032x}", "span_id": f"{span.context.span_id:016x}",
                              "name": span.name, "start_ns": span.start_time, "end_ns": span.end_time,
                              "duration_ms": round((span.end_time-span.start_time)/1e6, 3),
                              "status": span.status.status_code.name, "attributes": dict(span.attributes or {})}
                    output.write(json.dumps(record)+"\n")
            return SpanExportResult.SUCCESS
        except OSError:
            return SpanExportResult.FAILURE


class Telemetry:
    def __init__(self, database):
        self.provider = TracerProvider(resource=Resource.create({"service.name": "crashproof-business-worker"}))
        self.provider.add_span_processor(SimpleSpanProcessor(LocalSpanExporter(Path(database).parent/"spans.jsonl")))
        self.tracer = self.provider.get_tracer("crashproof", "1.0.0")

    @contextmanager
    def span(self, name, order_id):
        # Link all business operations to an order-scoped trace across worker restarts.
        trace_id = int(order_id.removeprefix("cp-"), 16)
        parent = SpanContext(trace_id=trace_id, span_id=1, is_remote=False, trace_flags=TraceFlags(1))
        context = trace.set_span_in_context(NonRecordingSpan(parent))
        with self.tracer.start_as_current_span("business."+name, context=context, attributes={"order.id": order_id}) as span:
            try:
                from temporalio import activity
                info = activity.info()
                span.set_attribute("temporal.activity.attempt", info.attempt)
                span.set_attribute("temporal.workflow.id", info.workflow_id)
            except RuntimeError:
                pass
            yield span


def read_spans(database, order_id):
    path = Path(database).parent/"spans.jsonl"
    if not path.exists():
        return []
    spans = []
    with path.open(encoding="utf-8") as source:
        for line in source:
            try:
                item = json.loads(line)
                if item.get("attributes", {}).get("order.id") == order_id:
                    spans.append(item)
            except json.JSONDecodeError:
                continue
    return spans[-200:]
