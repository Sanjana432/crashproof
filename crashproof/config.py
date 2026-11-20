from dataclasses import dataclass
import os


@dataclass
class Settings:
    address: str = "127.0.0.1:7233"
    namespace: str = "default"
    queue: str = "crashproof-orders"
    database: str = "data/business.sqlite3"

    @classmethod
    def from_env(cls):
        return cls(address=os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
                   namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
                   queue=os.getenv("CRASHPROOF_TASK_QUEUE", "crashproof-orders"),
                   database=os.getenv("CRASHPROOF_DB", "data/business.sqlite3"))
