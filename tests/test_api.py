import asyncio
from pathlib import Path
import tempfile
import unittest
from fastapi.testclient import TestClient
from crashproof.api import create_app
from crashproof.config import Settings
from crashproof.store import Store


class FakeDispatcher:
    """Only HTTP contract tests use this controlled adapter; integration tests use Temporal."""
    def __init__(self, store):
        self.store = store
    async def loop(self):
        await asyncio.Event().wait()
    async def dispatch(self, order):
        self.store.mark_queued(order["id"])
    async def connect(self):
        raise ConnectionError("Intentional offline contract test")


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        settings = Settings(database=str(Path(self.temp.name)/"api.sqlite3"))
        self.client = TestClient(create_app(settings, FakeDispatcher(Store(settings.database))))
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_dashboard_and_static_interface(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(len(self.client.get("/api/dashboard").json()["inventory"]), 3)
        self.assertIn("crashproof_orders", self.client.get("/metrics").text)

    def test_duplicate_and_mismatched_request_contract(self):
        headers = {"Idempotency-Key": "api-request-123"}
        payload = {"customer": "API test", "sku": "keyboard", "quantity": 1}
        first = self.client.post("/api/orders", json=payload, headers=headers)
        again = self.client.post("/api/orders", json=payload, headers=headers)
        changed = self.client.post("/api/orders", json={**payload, "quantity": 2}, headers=headers)
        self.assertEqual(first.status_code, 202)
        self.assertFalse(again.json()["created"])
        self.assertEqual(changed.status_code, 409)

    def test_invalid_inputs_and_missing_key(self):
        for body in ({"customer": "x", "quantity": True}, {"customer":"x", "quantity":0}, {"customer":"x", "surprise": 1}):
            self.assertEqual(self.client.post("/api/orders", json=body, headers={"Idempotency-Key":"valid-request-key"}).status_code, 422)
        self.assertEqual(self.client.post("/api/orders", json={"customer":"x"}).status_code, 422)

    def test_missing_order_and_health_are_explicit(self):
        self.assertEqual(self.client.get("/api/orders/missing").status_code, 404)
        self.assertEqual(self.client.get("/api/health").json()["temporal"], "unavailable")
