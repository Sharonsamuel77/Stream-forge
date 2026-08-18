from rocksdict import Rdict
from pathlib import Path


BASE_DB_PATH = "data/rocksdb"


class RocksDBStateStore:
    """RocksDB-backed state store scoped to a Kafka partition."""

    def __init__(self, partition):
        self.partition = partition

        db_path = Path(BASE_DB_PATH) / f"partition_{partition}"
        db_path.mkdir(parents=True, exist_ok=True)

        self.db_path = str(db_path)
        self.db = Rdict(self.db_path)

        print(
            f"Loading state from RocksDB partition {partition}..."
        )

        count = sum(1 for _ in self.db.items())

        print(
            f"Recovered {count} records from RocksDB"
        )

    def put(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db.get(key)

    def delete(self, key):
        if key in self.db:
            del self.db[key]

    def all(self):
        return list(self.db.items())

    def count(self):
        return len(self.db)

    def recovery_info(self):
        return {
            "status": "Recovered",
            "state_store": "RocksDB",
            "partition": self.partition,
            "records": len(self.db),
        }

    def close(self):
        self.db.close()