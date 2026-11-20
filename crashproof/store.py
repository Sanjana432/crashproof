"""Transactional business state and simulated external side effects.

Temporal persists workflow history separately. This SQLite database is the local
inventory/payment service. BEGIN IMMEDIATE serializes state-changing operations.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3


class Conflict(ValueError):
    pass


class BusinessFailure(ValueError):
    pass


def now():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path="data/business.sqlite3"):
        self.path = str(Path(path).resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS inventory (
                    sku TEXT PRIMARY KEY, name TEXT NOT NULL,
                    price_cents INTEGER NOT NULL CHECK(price_cents>0),
                    available INTEGER NOT NULL CHECK(available>=0));
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                    payload_hash TEXT NOT NULL, customer TEXT NOT NULL,
                    sku TEXT NOT NULL, quantity INTEGER NOT NULL CHECK(quantity>0),
                    total_cents INTEGER NOT NULL CHECK(total_cents>0),
                    fault TEXT NOT NULL, requires_approval INTEGER NOT NULL,
                    status TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reservations (
                    order_id TEXT PRIMARY KEY REFERENCES orders(id),
                    sku TEXT NOT NULL, quantity INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('reserved','consumed','released')));
                CREATE TABLE IF NOT EXISTS charges (
                    order_id TEXT PRIMARY KEY REFERENCES orders(id),
                    amount_cents INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('charged','refunded')));
                CREATE TABLE IF NOT EXISTS faults (
                    order_id TEXT PRIMARY KEY REFERENCES orders(id),
                    payment_attempts INTEGER NOT NULL DEFAULT 0,
                    crash_consumed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL REFERENCES orders(id),
                    at TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS events_order ON events(order_id,sequence);
            """)
            db.executemany("INSERT OR IGNORE INTO inventory VALUES (?,?,?,?)", [
                ("keyboard", "Mechanical keyboard", 7900, 5000),
                ("headphones", "Studio headphones", 12900, 5000),
                ("dock", "USB-C workstation dock", 9900, 5000),
            ])

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=3, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=3000")
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _event(self, db, order_id, kind, detail):
        db.execute("INSERT INTO events(order_id,at,kind,detail) VALUES (?,?,?,?)",
                   (order_id, now(), kind, json.dumps(detail, sort_keys=True)))

    def _order(self, db, order_id):
        row = db.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if row is None:
            raise BusinessFailure("Order not found")
        return dict(row)

    def create_order(self, key, payload):
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        order_id = "cp-" + hashlib.sha256(key.encode()).hexdigest()[:32]
        with self.transaction() as db:
            existing = db.execute("SELECT * FROM orders WHERE idempotency_key=?", (key,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise Conflict("That idempotency key was already used with a different request")
                return dict(existing), False
            item = db.execute("SELECT * FROM inventory WHERE sku=?", (payload["sku"],)).fetchone()
            if item is None:
                raise BusinessFailure("Unknown SKU")
            if not isinstance(payload["quantity"], int) or not 1 <= payload["quantity"] <= 100:
                raise BusinessFailure("Quantity must be between 1 and 100")
            stamp = now()
            db.execute("""INSERT INTO orders
                (id,idempotency_key,payload_hash,customer,sku,quantity,total_cents,fault,requires_approval,status,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,'accepted',?,?)""",
                (order_id, key, digest, payload["customer"], payload["sku"], payload["quantity"],
                 item["price_cents"] * payload["quantity"], payload["fault"], int(payload["requires_approval"]), stamp, stamp))
            db.execute("INSERT INTO faults(order_id) VALUES (?)", (order_id,))
            self._event(db, order_id, "order_accepted", {"request_digest": digest, "fault": payload["fault"]})
            return self._order(db, order_id), True

    def set_status(self, order_id, status, reason=""):
        allowed = {"accepted", "queued", "reserving", "awaiting_approval", "charging", "fulfilling", "completed", "compensating", "compensated", "failed", "manual_review"}
        if status not in allowed:
            raise ValueError("Invalid order state")
        with self.transaction() as db:
            order = self._order(db, order_id)
            transitions = {
                "accepted": {"queued", "reserving", "compensating"},
                "queued": {"reserving", "compensating"},
                "reserving": {"awaiting_approval", "charging", "compensating"},
                "awaiting_approval": {"charging", "compensating"},
                "charging": {"fulfilling", "compensating"},
                "fulfilling": {"completed", "compensating"},
                "compensating": {"compensated", "manual_review", "failed"},
                "completed": set(), "compensated": set(), "failed": set(), "manual_review": set(),
            }
            if status != order["status"] and status not in transitions[order["status"]]:
                raise Conflict(f"Invalid or stale state transition: {order['status']} → {status}")
            if order["status"] != status:
                db.execute("UPDATE orders SET status=?,reason=?,updated_at=? WHERE id=?", (status, reason[:1000], now(), order_id))
                self._event(db, order_id, "state_changed", {"from": order["status"], "to": status, "reason": reason[:1000]})

    def mark_queued(self, order_id):
        with self.transaction() as db:
            db.execute("UPDATE orders SET status='queued',updated_at=? WHERE id=? AND status='accepted'", (now(), order_id))

    def pending(self):
        with self.transaction() as db:
            return [dict(r) for r in db.execute("SELECT * FROM orders WHERE status='accepted' ORDER BY created_at LIMIT 50")]

    def reserve(self, order_id):
        with self.transaction() as db:
            order = self._order(db, order_id)
            existing = db.execute("SELECT * FROM reservations WHERE order_id=?", (order_id,)).fetchone()
            if existing:
                if existing["state"] == "released":
                    raise BusinessFailure("Reservation was already released")
                self._event(db, order_id, "reservation_deduplicated", {})
                return {"reserved": True, "deduplicated": True}
            if order["status"] not in {"accepted", "queued", "reserving"}:
                raise BusinessFailure("A late reservation cannot modify an advanced or compensated order")
            updated = db.execute("UPDATE inventory SET available=available-? WHERE sku=? AND available>=?",
                                 (order["quantity"], order["sku"], order["quantity"]))
            if not updated.rowcount:
                raise BusinessFailure("Insufficient inventory")
            db.execute("INSERT INTO reservations VALUES (?,?,?,'reserved')", (order_id, order["sku"], order["quantity"]))
            self._event(db, order_id, "inventory_reserved", {"sku": order["sku"], "quantity": order["quantity"]})
            return {"reserved": True, "deduplicated": False}

    def charge(self, order_id):
        with self.transaction() as db:
            order = self._order(db, order_id)
            old = db.execute("SELECT * FROM charges WHERE order_id=?", (order_id,)).fetchone()
            attempts = db.execute("SELECT * FROM faults WHERE order_id=?", (order_id,)).fetchone()
            db.execute("UPDATE faults SET payment_attempts=payment_attempts+1 WHERE order_id=?", (order_id,))
            self._event(db, order_id, "payment_attempt", {"attempt": attempts["payment_attempts"]+1})
            if old:
                if old["state"] != "charged":
                    raise BusinessFailure("Payment was already refunded")
                self._event(db, order_id, "charge_deduplicated", {"amount_cents": old["amount_cents"]})
                return {"charged": True, "deduplicated": True, "crash": False}
            if order["status"] in {"compensating", "compensated", "failed", "manual_review", "completed"}:
                raise BusinessFailure("Order is being compensated")
            reservation = db.execute("SELECT state FROM reservations WHERE order_id=?", (order_id,)).fetchone()
            if not reservation or reservation["state"] != "reserved":
                raise BusinessFailure("Payment requires an active reservation")
            if order["fault"] == "payment_decline":
                self._event(db, order_id, "payment_declined", {})
                return {"declined": True}
            if order["fault"] == "payment_retry" and attempts["payment_attempts"] == 0:
                self._event(db, order_id, "payment_transient_failure", {})
                return {"transient_failure": True}
            db.execute("INSERT INTO charges VALUES (?,?,'charged')", (order_id, order["total_cents"]))
            crash = order["fault"] == "worker_crash" and not attempts["crash_consumed"]
            if crash:
                db.execute("UPDATE faults SET crash_consumed=1 WHERE order_id=?", (order_id,))
            self._event(db, order_id, "payment_charged", {"amount_cents": order["total_cents"]})
            if crash:
                self._event(db, order_id, "crash_boundary_committed", {"note": "Worker exits after this transaction commits and before reporting activity completion"})
            return {"charged": True, "deduplicated": False, "crash": bool(crash)}

    def fulfill(self, order_id):
        with self.transaction() as db:
            order = self._order(db, order_id)
            if order["status"] in {"compensating", "compensated", "failed", "manual_review"}:
                raise BusinessFailure("A late fulfillment cannot modify a compensated order")
            if order["fault"] == "fulfillment_fail":
                raise BusinessFailure("Simulated fulfillment rejection")
            payment = db.execute("SELECT state FROM charges WHERE order_id=?", (order_id,)).fetchone()
            if not payment or payment["state"] != "charged":
                raise BusinessFailure("Fulfillment requires a successful payment")
            old = db.execute("SELECT state FROM reservations WHERE order_id=?", (order_id,)).fetchone()
            if not old or old["state"] == "released":
                raise BusinessFailure("Fulfillment requires an active reservation")
            if old["state"] == "consumed":
                return {"fulfilled": True, "deduplicated": True}
            db.execute("UPDATE reservations SET state='consumed' WHERE order_id=?", (order_id,))
            self._event(db, order_id, "order_fulfilled", {})
            return {"fulfilled": True, "deduplicated": False}

    def refund(self, order_id):
        with self.transaction() as db:
            self._order(db, order_id)
            reservation = db.execute("SELECT state FROM reservations WHERE order_id=?", (order_id,)).fetchone()
            if reservation and reservation["state"] == "consumed":
                raise BusinessFailure("Fulfillment is already committed; automatic refund requires manual review")
            updated = db.execute("UPDATE charges SET state='refunded' WHERE order_id=? AND state='charged'", (order_id,))
            if updated.rowcount:
                self._event(db, order_id, "payment_refunded", {})
            return {"refunded": bool(updated.rowcount)}

    def release(self, order_id):
        with self.transaction() as db:
            self._order(db, order_id)
            row = db.execute("SELECT * FROM reservations WHERE order_id=?", (order_id,)).fetchone()
            if row and row["state"] == "reserved":
                db.execute("UPDATE inventory SET available=available+? WHERE sku=?", (row["quantity"], row["sku"]))
                db.execute("UPDATE reservations SET state='released' WHERE order_id=?", (order_id,))
                self._event(db, order_id, "inventory_released", {"quantity": row["quantity"]})
                return {"released": True}
            return {"released": False}

    def detail(self, order_id):
        with self.transaction() as db:
            order = self._order(db, order_id)
            order["events"] = [{**dict(r), "detail": json.loads(r["detail"])} for r in db.execute("SELECT * FROM events WHERE order_id=? ORDER BY sequence", (order_id,))]
            payment = db.execute("SELECT * FROM charges WHERE order_id=?", (order_id,)).fetchone()
            order["payment"] = dict(payment) if payment else None
            reservation = db.execute("SELECT * FROM reservations WHERE order_id=?", (order_id,)).fetchone()
            order["reservation"] = dict(reservation) if reservation else None
            return order

    def snapshot(self):
        with self.transaction() as db:
            orders = [dict(r) for r in db.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 100")]
            inventory = [dict(r) for r in db.execute("SELECT * FROM inventory ORDER BY sku")]
            stats = {r["status"]: r["count"] for r in db.execute("SELECT status,COUNT(*) AS count FROM orders GROUP BY status")}
            total = db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            charged = db.execute("SELECT COUNT(*) FROM charges WHERE state='charged'").fetchone()[0]
            dedup = db.execute("SELECT COUNT(*) FROM events WHERE kind='charge_deduplicated'").fetchone()[0]
            refunds = db.execute("SELECT COUNT(*) FROM charges WHERE state='refunded'").fetchone()[0]
            return {"orders": orders, "inventory": inventory, "stats": stats,
                    "metrics": {"orders": total, "completed": stats.get("completed", 0), "active_charges": charged, "deduplicated_charges": dedup, "refunds": refunds}}
