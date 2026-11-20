from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from crashproof.store import BusinessFailure, Conflict, Store


def payload(**changes):
    return {"customer": "Test", "sku": "keyboard", "quantity": 1, "fault": "none", "requires_approval": False, **changes}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name)/"business.sqlite3")
        self.addCleanup(self.temp.cleanup)

    def order(self, key="test-order-123", **changes):
        return self.store.create_order(key, payload(**changes))[0]

    def test_duplicate_request_is_identical(self):
        first, created = self.store.create_order("duplicate-key", payload())
        second, again = self.store.create_order("duplicate-key", payload())
        self.assertTrue(created)
        self.assertFalse(again)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(self.store.snapshot()["metrics"]["orders"], 1)

    def test_key_payload_mismatch_is_rejected(self):
        self.order()
        with self.assertRaises(Conflict):
            self.order(quantity=2)

    def test_same_key_concurrent_submissions_create_one_order(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: self.store.create_order("concurrent-key", payload()), range(30)))
        self.assertEqual(sum(created for _, created in results), 1)
        self.assertEqual(len({order["id"] for order, _ in results}), 1)

    def test_inventory_cannot_be_oversold_under_concurrency(self):
        with self.store.transaction() as db:
            db.execute("UPDATE inventory SET available=5 WHERE sku='keyboard'")
        orders = [self.order(f"inventory-{i}") for i in range(12)]
        def reserve(order):
            try:
                self.store.reserve(order["id"])
                return True
            except BusinessFailure:
                return False
        with ThreadPoolExecutor(max_workers=12) as pool:
            success = sum(pool.map(reserve, orders))
        self.assertEqual(success, 5)
        self.assertEqual(next(i for i in self.store.snapshot()["inventory"] if i["sku"] == "keyboard")["available"], 0)

    def test_reserve_and_release_are_idempotent(self):
        order = self.order()
        self.store.reserve(order["id"])
        self.store.reserve(order["id"])
        self.assertTrue(self.store.release(order["id"])["released"])
        self.assertFalse(self.store.release(order["id"])["released"])
        self.assertEqual(next(i for i in self.store.snapshot()["inventory"] if i["sku"] == "keyboard")["available"], 5000)

    def test_payment_duplicate_returns_existing_effect(self):
        order = self.order()
        self.store.reserve(order["id"])
        results = [self.store.charge(order["id"]) for _ in range(4)]
        self.assertEqual(sum(not r["deduplicated"] for r in results), 1)
        data = self.store.detail(order["id"])
        self.assertEqual(sum(e["kind"] == "payment_charged" for e in data["events"]), 1)

    def test_transient_payment_failure_happens_only_once(self):
        order = self.order(fault="payment_retry")
        self.store.reserve(order["id"])
        self.assertTrue(self.store.charge(order["id"])["transient_failure"])
        self.assertTrue(self.store.charge(order["id"])["charged"])

    def test_crash_flag_and_charge_commit_together(self):
        order = self.order(fault="worker_crash")
        self.store.reserve(order["id"])
        self.assertTrue(self.store.charge(order["id"])["crash"])
        restarted = Store(self.store.path)
        replay = restarted.charge(order["id"])
        self.assertFalse(replay["crash"])
        self.assertTrue(replay["deduplicated"])

    def test_decline_creates_no_charge(self):
        order = self.order(fault="payment_decline")
        self.store.reserve(order["id"])
        self.assertTrue(self.store.charge(order["id"])["declined"])
        self.assertIsNone(self.store.detail(order["id"])["payment"])

    def test_refund_is_idempotent_and_blocks_recharge(self):
        order = self.order()
        self.store.reserve(order["id"])
        self.store.charge(order["id"])
        self.assertTrue(self.store.refund(order["id"])["refunded"])
        self.assertFalse(self.store.refund(order["id"])["refunded"])
        with self.assertRaises(BusinessFailure):
            self.store.charge(order["id"])

    def test_late_reservation_is_rejected_after_compensation(self):
        order = self.order()
        self.store.set_status(order["id"], "compensating")
        self.store.set_status(order["id"], "compensated")
        with self.assertRaises(BusinessFailure):
            self.store.reserve(order["id"])

    def test_late_fulfillment_is_rejected_during_compensation(self):
        order = self.order()
        self.store.reserve(order["id"])
        self.store.charge(order["id"])
        self.store.set_status(order["id"], "compensating")
        with self.assertRaises(BusinessFailure):
            self.store.fulfill(order["id"])
        self.assertTrue(self.store.refund(order["id"])["refunded"])

    def test_fulfilled_order_cannot_be_automatically_refunded(self):
        order = self.order()
        self.store.reserve(order["id"])
        self.store.charge(order["id"])
        self.store.fulfill(order["id"])
        self.assertFalse(self.store.release(order["id"])["released"])
        with self.assertRaises(BusinessFailure):
            self.store.refund(order["id"])

    def test_unknown_order_and_invalid_state(self):
        with self.assertRaises(BusinessFailure):
            self.store.detail("missing")
        with self.assertRaises(ValueError):
            self.store.set_status("missing", "invented")

    def test_terminal_status_cannot_be_overwritten(self):
        order = self.order()
        for status in ("reserving", "charging", "fulfilling", "completed"):
            self.store.set_status(order["id"], status)
        with self.assertRaises(Conflict):
            self.store.set_status(order["id"], "reserving")

    def test_compensation_fence_cannot_be_reopened_by_stale_activity(self):
        order = self.order()
        self.store.set_status(order["id"], "reserving")
        self.store.set_status(order["id"], "compensating")
        self.store.refund(order["id"])
        self.store.release(order["id"])
        for stale in ("reserving", "charging", "fulfilling", "awaiting_approval"):
            with self.subTest(stale=stale), self.assertRaises(Conflict):
                self.store.set_status(order["id"], stale)
        with self.assertRaises(BusinessFailure):
            self.store.reserve(order["id"])
        self.store.set_status(order["id"], "compensated")
        detail = self.store.detail(order["id"])
        self.assertIsNone(detail["reservation"])
        self.assertIsNone(detail["payment"])
